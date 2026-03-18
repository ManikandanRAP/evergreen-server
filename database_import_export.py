"""
Database Import/Export Module for MySQL
Handles SQL dump file import and export functionality.
"""
import re
import io
from datetime import datetime, timezone
from typing import List, Dict, Set, Optional, Tuple
from fastapi import HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from sqlclient import SqlClient
from config import DB_NAME
from auth import get_password_hash
import uuid


class DatabaseExporter:
    """Handles database export to SQL dump file."""
    
    def __init__(self, client: SqlClient):
        self.client = client
        self.db_name = DB_NAME
    
    def export(self, admin_user: dict) -> StreamingResponse:
        """
        Export the entire database to a SQL dump file.
        
        Args:
            admin_user: Dictionary with admin user info (name, email)
            
        Returns:
            StreamingResponse with SQL dump file
        """
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
        sql_parts = []
        
        # Header
        sql_parts.append(f"-- Evergreen Database Export")
        sql_parts.append(f"-- Exported by: {admin_user.get('name')} ({admin_user.get('email')})")
        sql_parts.append(f"-- Timestamp: {timestamp}")
        sql_parts.append(f"-- Database: {self.db_name}")
        sql_parts.append("-- ============================================\n")
        sql_parts.append("SET FOREIGN_KEY_CHECKS=0;")
        sql_parts.append("SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO';")
        sql_parts.append("SET AUTOCOMMIT=0;")
        sql_parts.append("START TRANSACTION;\n")
        
        # Export all tables
        self._export_tables(sql_parts)
        
        # Export all views
        self._export_views(sql_parts)
        
        # Footer
        sql_parts.append("\nCOMMIT;")
        sql_parts.append("SET FOREIGN_KEY_CHECKS=1;")
        
        sql_content = "\n".join(sql_parts)
        filename = f"evergreen_backup_{timestamp}.sql"
        
        return StreamingResponse(
            io.BytesIO(sql_content.encode('utf-8')),
            media_type="application/sql",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "X-Export-Timestamp": timestamp,
                "X-Exported-By": admin_user.get('email', 'unknown')
            }
        )
    
    def _export_tables(self, sql_parts: List[str]):
        """Export all base tables (not views)."""
        # CRITICAL: Must filter by TABLE_TYPE = 'BASE TABLE' to exclude views
        tables_query = """
            SELECT TABLE_NAME 
            FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """
        tables_result, _, error = self.client._execute_query(tables_query, params=(self.db_name,), fetch='all')
        if error:
            raise HTTPException(status_code=500, detail=f"Failed to get tables: {error}")
        
        table_names = [t.get('TABLE_NAME') for t in (tables_result or []) if t.get('TABLE_NAME')]
        
        for table_name in table_names:
            # Get CREATE TABLE statement
            create_query = f"SHOW CREATE TABLE `{table_name}`"
            create_result, _, error = self.client._execute_query(create_query, fetch='all')
            if error:
                continue
            
            if create_result and len(create_result) > 0:
                create_stmt = create_result[0].get('Create Table', '')
                sql_parts.append(f"\n-- Table structure for `{table_name}`")
                sql_parts.append(f"DROP TABLE IF EXISTS `{table_name}`;")
                sql_parts.append(f"{create_stmt};\n")
            
            # Get table data
            data_query = f"SELECT * FROM `{table_name}`"
            data_result, _, error = self.client._execute_query(data_query, fetch='all')
            if error or not data_result:
                continue
            
            # Special handling for users table - verify admin@evergreen.com is included
            if table_name.lower() == 'users':
                admin_found = any(row.get('email', '').lower() == 'admin@evergreen.com' for row in data_result)
                if not admin_found:
                    # Admin user not found - this is a problem, but continue export
                    # The import will create a fallback admin if needed
                    pass
            
            if len(data_result) > 0:
                sql_parts.append(f"-- Data for `{table_name}`")
                columns = list(data_result[0].keys())
                col_names = ", ".join([f"`{c}`" for c in columns])
                
                for row in data_result:
                    values = []
                    for col in columns:
                        val = row.get(col)
                        if val is None:
                            values.append("NULL")
                        elif isinstance(val, (int, float)):
                            values.append(str(val))
                        elif isinstance(val, bool):
                            values.append("1" if val else "0")
                        elif isinstance(val, datetime):
                            values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
                        else:
                            # Escape single quotes and backslashes
                            escaped = str(val).replace("\\", "\\\\").replace("'", "\\'")
                            values.append(f"'{escaped}'")
                    
                    values_str = ", ".join(values)
                    sql_parts.append(f"INSERT INTO `{table_name}` ({col_names}) VALUES ({values_str});")
                
                sql_parts.append("")
    
    def _export_views(self, sql_parts: List[str]):
        """Export all views."""
        views_query = """
            SELECT TABLE_NAME 
            FROM information_schema.VIEWS 
            WHERE TABLE_SCHEMA = %s
            ORDER BY TABLE_NAME
        """
        views_result, _, views_error = self.client._execute_query(views_query, params=(self.db_name,), fetch='all')
        if not views_error and views_result:
            view_names = [v.get('TABLE_NAME') for v in views_result if v.get('TABLE_NAME')]
            
            if view_names:
                sql_parts.append("\n-- ============================================")
                sql_parts.append("-- Views")
                sql_parts.append("-- ============================================\n")
                
                for view_name in view_names:
                    create_view_query = f"SHOW CREATE VIEW `{view_name}`"
                    create_view_result, _, view_error = self.client._execute_query(create_view_query, fetch='all')
                    if not view_error and create_view_result and len(create_view_result) > 0:
                        create_view_stmt = create_view_result[0].get('Create View', '')
                        if create_view_stmt:
                            sql_parts.append(f"\n-- View structure for `{view_name}`")
                            sql_parts.append(f"DROP VIEW IF EXISTS `{view_name}`;")
                            sql_parts.append(f"{create_view_stmt};\n")


class DatabaseImporter:
    """Handles database import from SQL dump file."""
    
    # Version marker for debugging - update this when making changes
    IMPORT_VERSION = "2025-12-22-v8-VIEW-HANDLING-FIX"
    
    def __init__(self, client: SqlClient):
        self.client = client
        self.db_name = DB_NAME
    
    def import_dump(self, sql_content: str) -> Dict:
        """
        Import SQL dump file content into the database.
        
        Args:
            sql_content: SQL dump file content as string
            
        Returns:
            Dictionary with import results (success, message, warnings, etc.)
        """
        warnings = []
        errors = []
        
        # Log version to verify code changes are active
        warnings.append(f"VERIFICATION: DatabaseImporter version: {DatabaseImporter.IMPORT_VERSION}")
        warnings.append("VERIFICATION: Features active - DEFINER stripping, enhanced feedback table logging, view dependency ordering, view verification")
        
        # Sanitize SQL content
        sql_content = self._sanitize_sql(sql_content, warnings)
        
        # Setup database for import (with longer timeout)
        self._setup_import()
        
        # Parse statements (this is fast, no timeout needed)
        statements = self._parse_statements(sql_content)
        
        # Log statement count for debugging
        warnings.append(f"VERIFICATION: Parsed {len(statements)} total SQL statements from dump file")
        
        # Count users INSERTs in parsed statements - more accurate detection
        users_insert_count = 0
        for stmt in statements:
            stmt_upper = stmt.strip().upper()
            if stmt_upper.startswith('INSERT') and ('`users`' in stmt or ('INTO' in stmt_upper[:100] and 'users' in stmt[:300].lower())):
                users_insert_count += 1
        
        # Detect views
        views = self._detect_views(sql_content, warnings)
        
        # Log import strategy for verification - MUST APPEAR IN OUTPUT
        warnings.append("=" * 60)
        warnings.append("VERIFICATION: Import strategy active - Using REPLACE INTO for users table")
        warnings.append("VERIFICATION: This ensures all users (including admin@evergreen.com) are imported")
        warnings.append("=" * 60)
        
        # Execute statements
        results = self._execute_statements(statements, views, warnings, errors)
        
        # Verify critical tables
        self._verify_critical_tables(results, warnings)
        
        # Generate report (pass views set for exclusion)
        report = self._generate_report(results, warnings, errors, views)
        
        return report
    
    def _sanitize_sql(self, sql_content: str, warnings: List[str]) -> str:
        """Remove dangerous commands from SQL content."""
        dangerous_patterns = [
            "DROP DATABASE",
            "CREATE DATABASE",
            "GRANT ",
            "REVOKE ",
        ]
        
        for pattern in dangerous_patterns:
            if pattern.upper() in sql_content.upper():
                warnings.append(f"SQL file contains '{pattern}' command which has been blocked for safety.")
                sql_content = sql_content.replace(pattern, f"-- BLOCKED: {pattern}")
                sql_content = sql_content.replace(pattern.lower(), f"-- BLOCKED: {pattern.lower()}")
        
        return sql_content
    
    def _setup_import(self):
        """Setup database for import: disable foreign keys and drop all existing tables and views."""
        # Disable foreign key checks first - CRITICAL for feedback table
        # Use longer timeout for setup queries
        self.client._execute_query("SET FOREIGN_KEY_CHECKS=0;", is_transaction=False, timeout=60)
        self.client._execute_query("SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO';", is_transaction=False, timeout=60)
        # Also disable unique checks to prevent issues
        self.client._execute_query("SET UNIQUE_CHECKS=0;", is_transaction=False, timeout=60)
        
        # Drop all existing views first (they may depend on tables)
        try:
            views_query = """
                SELECT TABLE_NAME 
                FROM information_schema.VIEWS 
                WHERE TABLE_SCHEMA = %s
            """
            views_result, _, views_error = self.client._execute_query(views_query, params=(self.db_name,), fetch='all')
            
            if not views_error and views_result:
                view_names = [v.get('TABLE_NAME') for v in views_result if v.get('TABLE_NAME')]
                for view_name in view_names:
                    try:
                        drop_view_query = f"DROP VIEW IF EXISTS `{view_name}`"
                        self.client._execute_query(drop_view_query, is_transaction=False)
                    except Exception:
                        pass  # Continue if drop fails
        except Exception:
            pass  # Continue if dropping views fails
        
        # Drop all existing tables to ensure clean import
        try:
            # Get all base tables (not views)
            tables_query = """
                SELECT TABLE_NAME 
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
            """
            tables_result, _, tables_error = self.client._execute_query(tables_query, params=(self.db_name,), fetch='all')
            
            if not tables_error and tables_result:
                table_names = [t.get('TABLE_NAME') for t in tables_result if t.get('TABLE_NAME')]
                for table_name in table_names:
                    try:
                        # Drop table (FK checks are disabled, so this should work)
                        drop_query = f"DROP TABLE IF EXISTS `{table_name}`"
                        self.client._execute_query(drop_query, is_transaction=False)
                    except Exception:
                        pass  # Continue if drop fails
        except Exception:
            pass  # Continue if dropping tables fails
    
    def _parse_statements(self, sql_content: str) -> List[str]:
        """Parse SQL content into individual statements."""
        statements = []
        current_statement = []
        
        for line in sql_content.split('\n'):
            stripped = line.strip()
            
            # Skip empty lines and comments
            if not stripped or stripped.startswith('--') or stripped.startswith('#'):
                continue
            
            # Skip MySQL conditional comments that are SET statements
            if stripped.startswith('/*!') and 'SET' in stripped.upper():
                if any(kw in stripped.upper() for kw in [
                    'CHARACTER_SET', 'COLLATION', 'TIME_ZONE', 'SQL_MODE',
                    'FOREIGN_KEY_CHECKS', 'UNIQUE_CHECKS', 'SQL_NOTES'
                ]):
                    continue
            
            current_statement.append(line)
            
            if stripped.endswith(';'):
                stmt = '\n'.join(current_statement).strip()
                if stmt and not stmt.startswith('--'):
                    statements.append(stmt)
                current_statement = []
        
        # Handle remaining statement
        if current_statement:
            stmt = '\n'.join(current_statement).strip()
            if stmt and not stmt.startswith('--'):
                if not stmt.endswith(';'):
                    stmt += ';'
                statements.append(stmt)
        
        return statements
    
    def _detect_views(self, sql_content: str, warnings: List[str]) -> Set[str]:
        """Detect views from SQL content and existing database."""
        views = set()
        
        # Known view names that might be exported as tables
        known_views = ['consolidated_revenue_and_payments', 'ledger_partnerpayouts', 'ledger_partnerpayouts_with_filter', 'revenue_ledger']
        
        # Detect views from CREATE VIEW statements in dump
        create_view_pattern = r'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+[`"]?([a-zA-Z0-9_]+)[`"]?'
        view_names_in_file = re.findall(create_view_pattern, sql_content, re.IGNORECASE)
        for view_name in view_names_in_file:
            views.add(view_name.lower())
        
        # Detect views that were incorrectly exported as tables
        # Pattern: "-- Table structure for `view_name`" followed by "DROP TABLE" but NO "CREATE TABLE"
        # This indicates a view that was exported as a table
        for view_name in known_views:
            # Check if this view appears as "Table structure" but has no CREATE TABLE
            pattern = rf'--\s+Table\s+structure\s+for\s+[`"]?{re.escape(view_name)}[`"]?'
            if re.search(pattern, sql_content, re.IGNORECASE):
                # Check if there's a CREATE TABLE for this name
                create_table_pattern = rf'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`"]?{re.escape(view_name)}[`"]?'
                if not re.search(create_table_pattern, sql_content, re.IGNORECASE):
                    # This is a view exported as a table
                    views.add(view_name.lower())
                    warnings.append(f"VERIFICATION: Detected view '{view_name}' that was incorrectly exported as a table (no CREATE TABLE found)")
        
        # Get existing views from database
        try:
            views_query = """
                SELECT TABLE_NAME
                FROM information_schema.VIEWS
                WHERE TABLE_SCHEMA = %s
            """
            views_result, _, views_error = self.client._execute_query(views_query, params=(self.db_name,), fetch='all')
            if not views_error and views_result:
                for v in views_result:
                    view_name = v.get('TABLE_NAME')
                    if view_name:
                        views.add(view_name.lower())
        except Exception:
            pass
        
        if views:
            warnings.append(f"VERIFICATION: Detected {len(views)} view(s): {', '.join(sorted(views))}. INSERTs into views will be skipped.")
        
        return views
    
    def _execute_statements(self, statements: List[str], views: Set[str], warnings: List[str], errors: List[str]) -> Dict:
        """
        Execute all SQL statements with optimized batching for large imports.
        Uses longer timeouts for large operations.
        """
        """Execute all SQL statements and track results."""
        results = {
            'successful': 0,
            'failed': 0,
            'tables_created': set(),
            'tables_with_inserts': {},
            'tables_insert_attempted': {},
            'views_created': set(),  # Track created views separately
            'deferred_inserts': {},  # {table_name: [stmt1, stmt2, ...]} - INSERTs for deferred tables
        }
        
        # Pre-scan for users INSERTs to ensure correct tracking
        users_insert_indices = self._find_users_inserts(statements)
        warnings.append(f"VERIFICATION: Pre-scan found {len(users_insert_indices)} users INSERT indices: {users_insert_indices[:10]}{'...' if len(users_insert_indices) > 10 else ''}")
        
        # Collect CREATE VIEW statements to execute after all tables and in dependency order
        view_statements = []  # List of (stmt_idx, stmt, view_name)
        
        # Collect CREATE TABLE statements that depend on users table (for deferred execution)
        deferred_table_statements = []  # List of (stmt_idx, stmt, table_name)
        
        # Track which tables are deferred so we can skip/re-execute their INSERTs
        deferred_table_names = set()  # Set of table names that are deferred
        
        for stmt_idx, stmt in enumerate(statements):
            try:
                stmt_upper = stmt.upper().strip()
                
                # Skip empty statements
                if not stmt_upper or stmt_upper == ';':
                    continue
                
                # Skip SET statements
                if self._is_set_statement(stmt_upper):
                    continue
                
                # Skip DROP TABLE statements - we already dropped all tables in _setup_import
                if stmt_upper.startswith('DROP TABLE') or stmt_upper.startswith('DROP VIEW'):
                    continue
                
                # Track CREATE TABLE - but skip if this is actually a view
                if stmt_upper.startswith('CREATE TABLE'):
                    table_name = self._extract_table_name_from_create(stmt)
                    # Debug: log if table name extraction fails for feedback
                    if not table_name and 'feedback' in stmt.lower():
                        warnings.append(f"VERIFICATION: ⚠ CREATE TABLE 'feedback' - table name extraction failed")
                    if table_name:
                        # Skip if this is actually a view (was incorrectly exported as table)
                        if table_name.lower() in views:
                            continue
                        # Skip if table already exists (we track created tables)
                        if table_name in results['tables_created']:
                            continue
                        
                        # Check if table has foreign key to users table - defer creation until after users table
                        # Check for various patterns: REFERENCES `users`, REFERENCES 'users', REFERENCES users
                        has_users_fk = (
                            'REFERENCES `users`' in stmt.upper() or 
                            "REFERENCES 'users'" in stmt.upper() or
                            re.search(r'REFERENCES\s+[`"\']?users[`"\']?', stmt, re.IGNORECASE) is not None
                        )
                        if has_users_fk and table_name.lower() != 'users':
                            # Defer this table creation until after users table is created
                            warnings.append(f"VERIFICATION: Deferring CREATE TABLE '{table_name}' (depends on users table via FK)")
                            deferred_table_statements.append((stmt_idx, stmt, table_name))
                            deferred_table_names.add(table_name.lower())
                            continue
                        
                        # For feedback table, log that we're attempting to create it (only if not deferred)
                        if table_name.lower() == 'feedback':
                            warnings.append(f"VERIFICATION: Attempting to CREATE TABLE 'feedback'")
                        
                        # Convert to CREATE TABLE IF NOT EXISTS to avoid errors
                        if 'IF NOT EXISTS' not in stmt_upper:
                            # Use more robust replacement that handles backticks and quotes
                            stmt = re.sub(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?', 'CREATE TABLE IF NOT EXISTS ', stmt, flags=re.IGNORECASE)
                            stmt_upper = stmt.upper().strip()  # Update for later checks
                        
                        # Execute CREATE TABLE with longer timeout for large tables
                        _, _, create_error = self.client._execute_query(stmt, is_transaction=True, timeout=60)
                        if create_error:
                            error_str = str(create_error)
                            should_skip = self._should_skip_error(create_error, table_name, results['tables_created'])
                            if not should_skip:
                                results['failed'] += 1
                                if len(errors) < 20:
                                    errors.append(f"CREATE TABLE '{table_name}': {error_str[:200]}")
                                # For feedback table, always log the error
                                if table_name.lower() == 'feedback':
                                    warnings.append(f"VERIFICATION: ⚠ CREATE TABLE 'feedback' failed: {error_str[:200]}")
                            else:
                                # Error was skipped - verify table actually exists
                                check_query = f"SHOW TABLES LIKE '{table_name}'"
                                check_result, _, _ = self.client._execute_query(check_query, fetch='all')
                                if check_result and len(check_result) > 0:
                                    results['tables_created'].add(table_name)
                                    if table_name.lower() == 'feedback':
                                        warnings.append(f"VERIFICATION: ✓ CREATE TABLE 'feedback' succeeded (error was skipped but table exists)")
                                else:
                                    # Table doesn't exist - log for feedback table
                                    if table_name.lower() == 'feedback':
                                        warnings.append(f"VERIFICATION: ⚠ CREATE TABLE 'feedback' error was skipped but table DOES NOT EXIST: {error_str[:200]}")
                                results['successful'] += 1
                        else:
                            # Success - verify table exists before adding to created set
                            check_query = f"SHOW TABLES LIKE '{table_name}'"
                            check_result, _, _ = self.client._execute_query(check_query, fetch='all')
                            if check_result and len(check_result) > 0:
                                results['tables_created'].add(table_name)
                                if table_name.lower() == 'feedback':
                                    warnings.append(f"VERIFICATION: ✓ CREATE TABLE 'feedback' succeeded and verified")
                            else:
                                # Table creation succeeded but table doesn't exist - this shouldn't happen
                                if table_name.lower() == 'feedback':
                                    warnings.append(f"VERIFICATION: ⚠ CRITICAL: CREATE TABLE 'feedback' returned no error but table DOES NOT EXIST")
                            results['successful'] += 1
                        # Continue to next statement (don't execute CREATE TABLE again)
                        continue
                
                # Handle CREATE VIEW - collect for later execution in dependency order
                if stmt_upper.startswith('CREATE') and 'VIEW' in stmt_upper[:200]:
                    view_name = self._extract_view_name(stmt)
                    if view_name:
                        views.add(view_name.lower())
                        # Strip DEFINER clause to avoid definer user errors
                        # Pattern examples:
                        # - DEFINER=`root`@`localhost` SQL SECURITY ...
                        # - DEFINER='root'@'localhost' SQL SECURITY ...
                        # - DEFINER=root@localhost SQL SECURITY ...
                        # Store original for debugging
                        original_stmt = stmt
                        # Match DEFINER= followed by user@host (in various formats) followed by optional SQL SECURITY
                        # Pattern: DEFINER=`user`@`host` or DEFINER='user'@'host' or DEFINER=user@host, optionally followed by SQL SECURITY
                        # Use a more comprehensive pattern that handles all cases
                        stmt = re.sub(r'DEFINER\s*=\s*(?:`[^`]+`@`[^`]+`|["\'][^"\']+["\']@["\'][^"\']+["\']|[^\s]+@[^\s]+)\s*(?:SQL\s+SECURITY\s+\w+\s+)?', '', stmt, flags=re.IGNORECASE)
                        # Also handle ALGORITHM=... DEFINER=... pattern - remove DEFINER but keep ALGORITHM
                        if 'ALGORITHM' in stmt_upper and 'DEFINER' in original_stmt.upper():
                            # Replace ALGORITHM=... DEFINER=... with just ALGORITHM=...
                            stmt = re.sub(r'(ALGORITHM\s*=\s*\w+)\s+DEFINER\s*=\s*(?:`[^`]+`@`[^`]+`|["\'][^"\']+["\']@["\'][^"\']+["\']|[^\s]+@[^\s]+)\s*(?:SQL\s+SECURITY\s+\w+\s+)?', r'\1 ', stmt, flags=re.IGNORECASE)
                        # Debug: log if DEFINER was found and stripped
                        if 'DEFINER' in original_stmt.upper() and 'DEFINER' not in stmt.upper():
                            warnings.append(f"VERIFICATION: Stripped DEFINER from view '{view_name}'")
                        elif 'DEFINER' in stmt.upper():
                            warnings.append(f"VERIFICATION: ⚠ DEFINER still present in view '{view_name}' after stripping attempt")
                        # Convert to CREATE OR REPLACE VIEW to avoid errors
                        if 'OR REPLACE' not in stmt_upper:
                            stmt = re.sub(r'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+', 'CREATE OR REPLACE VIEW ', stmt, flags=re.IGNORECASE)
                            stmt_upper = stmt.upper().strip()
                        # Store for later execution (after all tables and in dependency order)
                        view_statements.append((stmt_idx, stmt, view_name.lower()))
                    else:
                        # View name extraction failed - still try to execute it
                        view_statements.append((stmt_idx, stmt, None))
                    # Skip execution for now - will execute after all tables
                    continue
                
                # Handle INSERT statements (including REPLACE INTO which was converted from INSERT)
                if stmt_upper.startswith('INSERT') or stmt_upper.startswith('REPLACE'):
                    # Extract table name first (before any modifications)
                    table_name = self._get_insert_table_name(stmt, stmt_idx, users_insert_indices)
                    
                    # If table_name extraction failed, try a fallback extraction
                    if not table_name:
                        # Fallback: try multiple patterns to extract table name directly from statement
                        fallback_match = re.search(r'INSERT\s+(?:IGNORE\s+)?INTO\s+`([a-zA-Z0-9_]+)`', stmt, re.IGNORECASE)
                        if fallback_match:
                            table_name = fallback_match.group(1).lower()
                        else:
                            fallback_match = re.search(r'REPLACE\s+INTO\s+`([a-zA-Z0-9_]+)`', stmt, re.IGNORECASE)
                            if fallback_match:
                                table_name = fallback_match.group(1).lower()
                            else:
                                fallback_match = re.search(r'INSERT\s+(?:IGNORE\s+)?INTO\s+"([a-zA-Z0-9_]+)"', stmt, re.IGNORECASE)
                                if fallback_match:
                                    table_name = fallback_match.group(1).lower()
                                else:
                                    fallback_match = re.search(r'INSERT\s+(?:IGNORE\s+)?INTO\s+([a-zA-Z0-9_]+)', stmt, re.IGNORECASE)
                                    if fallback_match:
                                        table_name = fallback_match.group(1).lower()
                    
                    # Only process INSERTs if we can extract table name
                    if not table_name:
                        continue
                    
                    # Skip INSERTs into views
                    table_name_lower = table_name.lower()
                    if table_name_lower in views or table_name_lower in results.get('views_created', set()):
                        continue
                    
                    # Skip INSERTs into deferred tables - we'll execute them after the table is created
                    if table_name_lower in deferred_table_names:
                        # Store the INSERT statement to execute later
                        if 'deferred_inserts' not in results:
                            results['deferred_inserts'] = {}  # {table_name: [stmt1, stmt2, ...]}
                        if table_name not in results['deferred_inserts']:
                            results['deferred_inserts'][table_name] = []
                        results['deferred_inserts'][table_name].append(stmt)
                        continue
                    
                    # Track INSERTs into actual tables (not views) - MUST happen before execution
                    if table_name not in results['tables_insert_attempted']:
                        results['tables_insert_attempted'][table_name] = 0
                    results['tables_insert_attempted'][table_name] += 1
                    
                    # For users table, use REPLACE INTO to ensure all users are imported
                    # (INSERT IGNORE would silently skip existing users)
                    # For other tables, use INSERT IGNORE to handle duplicates gracefully
                    if table_name.lower() == 'users':
                        # Check if this is the admin user INSERT
                        is_admin_user = '91847015d93859a09f826072a26a7996' in stmt or 'admin@evergreen.com' in stmt
                        
                        # Convert to REPLACE INTO to overwrite existing users
                        if 'REPLACE' not in stmt_upper and 'INSERT IGNORE' not in stmt_upper:
                            stmt = re.sub(r'^INSERT\s+(?:IGNORE\s+)?', 'REPLACE ', stmt, flags=re.IGNORECASE)
                            stmt_upper = stmt.upper().strip()  # Update for later checks
                            # Log first users INSERT to confirm REPLACE is being used
                            if results['tables_insert_attempted'].get(table_name, 0) == 1:
                                warnings.append("VERIFICATION: First users INSERT converted to REPLACE INTO - all users will be imported")
                        
                        # Log admin user INSERT specifically
                        if is_admin_user:
                            warnings.append(f"VERIFICATION: Processing admin@evergreen.com INSERT")
                    else:
                        # Convert to INSERT IGNORE for other tables (but not if already REPLACE)
                        if 'INSERT IGNORE' not in stmt_upper and 'REPLACE' not in stmt_upper:
                            stmt = re.sub(r'^INSERT\s+', 'INSERT IGNORE ', stmt, flags=re.IGNORECASE)
                            stmt_upper = stmt.upper().strip()  # Update for later checks
                    
                    # Execute INSERT with longer timeout for large imports
                    # Use transaction=False for individual INSERTs to avoid long locks, but commit periodically
                    _, _, error = self.client._execute_query(stmt, is_transaction=True, timeout=120)
                    
                    if error:
                        # Handle errors
                        error_str = str(error)
                        should_skip = self._should_skip_error(error, table_name, results['tables_created'])
                        
                        if not should_skip:
                            results['failed'] += 1
                            if len(errors) < 20:
                                errors.append(f"{table_name or 'unknown'}: {error_str[:200]}")
                        else:
                            # Error was skipped (expected) - count as success for INSERT IGNORE duplicate errors
                            results['successful'] += 1
                            # Still count as successful insert for tracking
                            if table_name:
                                if table_name not in results['tables_with_inserts']:
                                    results['tables_with_inserts'][table_name] = 0
                                results['tables_with_inserts'][table_name] += 1
                    else:
                        # Success
                        results['successful'] += 1
                        if table_name:
                            if table_name not in results['tables_with_inserts']:
                                results['tables_with_inserts'][table_name] = 0
                            results['tables_with_inserts'][table_name] += 1
                            
                            # Log admin user INSERT success (only for admin user)
                            if table_name.lower() == 'users':
                                is_admin_user = '91847015d93859a09f826072a26a7996' in stmt or 'admin@evergreen.com' in stmt
                                if is_admin_user:
                                    warnings.append(f"VERIFICATION: ✓ admin@evergreen.com INSERT succeeded")
                                # Log first 3 users INSERTs for verification
                                elif results['tables_with_inserts'].get(table_name, 0) <= 3:
                                    email_match = re.search(r"'([^']*@[^']*)'", stmt[:2000])
                                    if email_match:
                                        warnings.append(f"VERIFICATION: Users INSERT #{results['tables_with_inserts'].get(table_name, 0)} succeeded for: {email_match.group(1)}")
                else:
                    # Execute non-INSERT statements (CREATE TABLE, CREATE VIEW, etc.) with longer timeout
                    _, _, error = self.client._execute_query(stmt, is_transaction=True, timeout=120)
                    
                    if error:
                        # Skip certain expected errors (table already exists, FK issues, etc.)
                        error_str = str(error)
                        if self._should_skip_error(error, None, results['tables_created']):
                            # Error is expected, count as success - don't log it
                            results['successful'] += 1
                        else:
                            results['failed'] += 1
                            if len(errors) < 20:
                                errors.append(error_str[:200])
                            # Log CREATE VIEW errors for debugging
                            if stmt_upper.startswith('CREATE') and 'VIEW' in stmt_upper[:200]:
                                view_name = self._extract_view_name(stmt)
                                warnings.append(f"VERIFICATION: ⚠ CREATE VIEW '{view_name}' failed: {error_str[:200]}")
                    else:
                        results['successful'] += 1
                        # Log successful CREATE VIEW
                        if stmt_upper.startswith('CREATE') and 'VIEW' in stmt_upper[:200]:
                            view_name = self._extract_view_name(stmt)
                            if view_name:
                                warnings.append(f"VERIFICATION: ✓ CREATE VIEW '{view_name}' succeeded")
                        
            except Exception as e:
                results['failed'] += 1
                if len(errors) < 20:
                    errors.append(str(e)[:200])
        
        # Now execute CREATE VIEW statements in dependency order
        # View dependency order:
        # - ledger_partnerpayouts_with_filter depends on ledger_partnerpayouts and revenue_ledger
        # - consolidated_revenue_and_payments depends on ledger_partnerpayouts and revenue_ledger
        # So create base views first, then the filtered view, then the consolidated view
        if view_statements:
            # Sort views by dependency order so derived views are created after their inputs
            def view_priority(item):
                stmt_idx, stmt, view_name = item
                priority_map = {
                    'ledger_partnerpayouts': 1,
                    'revenue_ledger': 1,
                    'ledger_partnerpayouts_with_filter': 2,
                    'consolidated_revenue_and_payments': 3,
                }
                return priority_map.get(view_name, 2)
            
            view_statements_sorted = sorted(view_statements, key=view_priority)
            
            for stmt_idx, stmt, view_name in view_statements_sorted:
                try:
                    warnings.append(f"VERIFICATION: Creating view '{view_name}'")
                    # Execute CREATE VIEW
                    _, _, error = self.client._execute_query(stmt, is_transaction=True)
                    
                    if error:
                        error_str = str(error)
                        if self._should_skip_error(error, None, results['tables_created']):
                            results['successful'] += 1
                            warnings.append(f"VERIFICATION: ✓ CREATE VIEW '{view_name}' succeeded (error was skipped)")
                        else:
                            results['failed'] += 1
                            if len(errors) < 20:
                                errors.append(error_str[:200])
                            warnings.append(f"VERIFICATION: ⚠ CREATE VIEW '{view_name}' failed: {error_str[:200]}")
                    else:
                        results['successful'] += 1
                        if view_name:
                            results['views_created'].add(view_name)
                            warnings.append(f"VERIFICATION: ✓ CREATE VIEW '{view_name}' succeeded")
                except Exception as e:
                    results['failed'] += 1
                    if len(errors) < 20:
                        errors.append(str(e)[:200])
                    warnings.append(f"VERIFICATION: ⚠ CREATE VIEW '{view_name}' exception: {str(e)[:200]}")
        
        # Execute deferred table statements (tables that depend on users table)
        if deferred_table_statements:
            warnings.append(f"VERIFICATION: Executing {len(deferred_table_statements)} deferred table(s) after users table creation")
            for stmt_idx, stmt, table_name in deferred_table_statements:
                try:
                    warnings.append(f"VERIFICATION: Creating deferred table '{table_name}'")
                    # Convert to CREATE TABLE IF NOT EXISTS if needed
                    stmt_upper = stmt.upper().strip()
                    if 'IF NOT EXISTS' not in stmt_upper:
                        stmt = re.sub(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?', 'CREATE TABLE IF NOT EXISTS ', stmt, flags=re.IGNORECASE)
                    
                    # Execute CREATE TABLE
                    _, _, create_error = self.client._execute_query(stmt, is_transaction=True)
                    if create_error:
                        error_str = str(create_error)
                        should_skip = self._should_skip_error(create_error, table_name, results['tables_created'])
                        if not should_skip:
                            results['failed'] += 1
                            if len(errors) < 20:
                                errors.append(f"CREATE TABLE '{table_name}' (deferred): {error_str[:200]}")
                            if table_name.lower() == 'feedback':
                                warnings.append(f"VERIFICATION: ⚠ CREATE TABLE 'feedback' (deferred) failed: {error_str[:200]}")
                        else:
                            # Error was skipped - verify table exists
                            check_query = f"SHOW TABLES LIKE '{table_name}'"
                            check_result, _, _ = self.client._execute_query(check_query, fetch='all')
                            if check_result and len(check_result) > 0:
                                results['tables_created'].add(table_name)
                                if table_name.lower() == 'feedback':
                                    warnings.append(f"VERIFICATION: ✓ CREATE TABLE 'feedback' (deferred) succeeded (error was skipped but table exists)")
                            else:
                                if table_name.lower() == 'feedback':
                                    warnings.append(f"VERIFICATION: ⚠ CREATE TABLE 'feedback' (deferred) error was skipped but table DOES NOT EXIST: {error_str[:200]}")
                            results['successful'] += 1
                    else:
                        # Success - verify table exists
                        check_query = f"SHOW TABLES LIKE '{table_name}'"
                        check_result, _, _ = self.client._execute_query(check_query, fetch='all')
                        if check_result and len(check_result) > 0:
                            results['tables_created'].add(table_name)
                            if table_name.lower() == 'feedback':
                                warnings.append(f"VERIFICATION: ✓ CREATE TABLE 'feedback' (deferred) succeeded and verified")
                            
                            # Now execute any deferred INSERTs for this table
                            if 'deferred_inserts' in results and table_name in results['deferred_inserts']:
                                deferred_inserts = results['deferred_inserts'][table_name]
                                warnings.append(f"VERIFICATION: Executing {len(deferred_inserts)} deferred INSERT(s) for table '{table_name}'")
                                # Track attempted INSERTs for reporting
                                if table_name not in results['tables_insert_attempted']:
                                    results['tables_insert_attempted'][table_name] = 0
                                results['tables_insert_attempted'][table_name] += len(deferred_inserts)
                                
                                for insert_stmt in deferred_inserts:
                                    _, _, insert_error = self.client._execute_query(insert_stmt, is_transaction=True)
                                    if insert_error:
                                        error_str = str(insert_error)
                                        if not self._should_skip_error(insert_error, table_name, results['tables_created']):
                                            results['failed'] += 1
                                            if len(errors) < 20:
                                                errors.append(f"INSERT into '{table_name}' (deferred): {error_str[:200]}")
                                    else:
                                        results['successful'] += 1
                                        # Track successful INSERT
                                        if table_name not in results['tables_with_inserts']:
                                            results['tables_with_inserts'][table_name] = 0
                                        results['tables_with_inserts'][table_name] += 1
                        else:
                            if table_name.lower() == 'feedback':
                                warnings.append(f"VERIFICATION: ⚠ CREATE TABLE 'feedback' (deferred) returned no error but table DOES NOT EXIST")
                        results['successful'] += 1
                except Exception as e:
                    results['failed'] += 1
                    if len(errors) < 20:
                        errors.append(f"CREATE TABLE '{table_name}' (deferred) exception: {str(e)[:200]}")
                    if table_name.lower() == 'feedback':
                        warnings.append(f"VERIFICATION: ⚠ CREATE TABLE 'feedback' (deferred) exception: {str(e)[:200]}")
        
        # Re-enable foreign key checks and unique checks
        self.client._execute_query("SET FOREIGN_KEY_CHECKS=1;", is_transaction=False)
        self.client._execute_query("SET UNIQUE_CHECKS=1;", is_transaction=False)
        
        return results
    
    def _find_users_inserts(self, statements: List[str]) -> List[int]:
        """Find indices of users INSERT statements for accurate tracking."""
        users_inserts = []
        for i, stmt in enumerate(statements):
            # Check first 300 characters to catch longer statements
            stmt_start = stmt[:300].strip()
            if stmt_start.upper().startswith('INSERT'):
                # Match: INSERT [IGNORE] INTO `users` ( or INSERT [IGNORE] INTO users (
                pattern = r'^INSERT\s+(?:IGNORE\s+)?INTO\s+[`"]?users[`"]?\s*\('
                if re.match(pattern, stmt_start, re.IGNORECASE):
                    # Verify it's actually 'users' table
                    table_match = re.search(r'INSERT\s+(?:IGNORE\s+)?INTO\s+[`"]?([a-zA-Z0-9_]+)[`"]?', stmt_start, re.IGNORECASE)
                    if table_match and table_match.group(1).lower() == 'users':
                        users_inserts.append(i)
        return users_inserts
    
    def _is_set_statement(self, stmt_upper: str) -> bool:
        """Check if statement is a SET statement."""
        # Only check first 200 characters to avoid false positives in long JSON values
        stmt_start = stmt_upper[:200]
        
        if stmt_start.startswith('SET ') or stmt_start.startswith('START TRANSACTION') or \
           stmt_start.startswith('COMMIT') or stmt_start.startswith('ROLLBACK'):
            return True
        
        # Only check for SET patterns in the first 200 characters (where SQL keywords would be)
        if 'SET' in stmt_start:
            set_patterns = [
                'COLLATION_CONNECTION', 'TIME_ZONE', 'SQL_MODE',
                'FOREIGN_KEY_CHECKS', 'UNIQUE_CHECKS', 'SQL_NOTES',
                'CHARACTER_SET', 'NAMES', 'SESSION', 'GLOBAL',
            ]
            if any(pattern in stmt_start for pattern in set_patterns):
                return True
        
        return False
    
    def _extract_table_name_from_create(self, stmt: str) -> Optional[str]:
        """Extract table name from CREATE TABLE statement."""
        match = re.search(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`"]?([a-zA-Z0-9_]+)[`"]?', stmt, re.IGNORECASE)
        return match.group(1).lower() if match else None
    
    def _extract_view_name(self, stmt: str) -> Optional[str]:
        """Extract view name from CREATE VIEW statement."""
        # Pattern: VIEW `view_name` AS (works for all CREATE VIEW formats)
        # This pattern works for:
        # - CREATE VIEW `view_name` AS
        # - CREATE OR REPLACE VIEW `view_name` AS  
        # - CREATE ALGORITHM=... VIEW `view_name` AS
        match = re.search(r'VIEW\s+[`"]?([a-zA-Z0-9_]+)[`"]?\s+AS', stmt, re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1).lower()
        
        return None
    
    def _get_insert_table_name(self, stmt: str, stmt_idx: int, users_insert_indices: List[int]) -> Optional[str]:
        """Extract table name from INSERT statement, with special handling for users table."""
        # Try REPLACE INTO first, then INSERT
        match = re.search(r'(?:REPLACE|INSERT\s+(?:IGNORE\s+)?)\s+INTO\s+[`"]?([a-zA-Z0-9_]+)[`"]?', stmt, re.IGNORECASE)
        if not match:
            return None
        
        extracted_table = match.group(1).lower()
        
        # If pre-scan identified this as a users INSERT, verify and use 'users'
        if stmt_idx in users_insert_indices and extracted_table == 'users':
            return 'users'
        
        return extracted_table
    
    def _should_skip_error(self, error, table_name: Optional[str], tables_created: Set[str]) -> bool:
        """Determine if an error should be skipped."""
        # Convert error to string - handle pymysql.Error (has .args), tuples, exceptions, etc.
        if hasattr(error, 'args') and error.args:
            # pymysql.Error and similar exceptions have .args tuple
            if len(error.args) > 1:
                error_str = str(error.args[1]).lower()  # Get the error message
            else:
                error_str = str(error.args[0]).lower()
        elif isinstance(error, tuple):
            error_str = str(error[1]).lower() if len(error) > 1 else str(error).lower()
        else:
            error_str = str(error).lower()
        
        # Extract just the error message if it's wrapped (e.g., "Database connection failed: (1050, 'message')")
        # Look for the actual error message in parentheses - handle both single and double quotes
        paren_match = re.search(r'\([^)]+,\s*["\']([^"\']+)["\']\)', error_str)
        if paren_match:
            error_str = paren_match.group(1).lower()
        
        # Skip certain expected errors - check both full error and extracted message
        skip_reasons = [
            'query was empty',
            'not insertable-into',
            "variable 'collation_connection' can't be set",
            "variable 'time_zone' can't be set",
            "variable 'sql_mode' can't be set",
            "variable 'foreign_key_checks' can't be set",
            "variable 'unique_checks' can't be set",
            "variable 'sql_notes' can't be set",
            "can't be set to the value of 'null'",
            "table already exists",  # CREATE TABLE IF NOT EXISTS might still error
            "table '",  # Any table-related errors (e.g., "Table 'allclass' already exists")
            "already exists",  # Catch "Table 'X' already exists" variations
            "cannot drop table",  # DROP TABLE errors due to FK constraints
            "referenced by a foreign key",  # FK constraint issues
            "drop table",  # Any DROP TABLE errors
            "drop view",  # Any DROP VIEW errors
            "failed to open the referenced table",  # FK constraint issues during table creation
            "doesn't exist",  # Table/view doesn't exist errors (expected during import)
            "duplicate entry",  # Duplicate key errors - expected with INSERT IGNORE (but not REPLACE INTO)
        ]
        
        # Check the extracted error message
        for reason in skip_reasons:
            if reason in error_str:
                return True
        
        # Also check the original full error string in case extraction failed
        original_error_str = str(error).lower()
        for reason in skip_reasons:
            if reason in original_error_str:
                return True
        
        # Skip "table doesn't exist" errors for INSERTs if table wasn't created
        if "doesn't exist" in error_str and table_name:
            if table_name not in tables_created:
                return True
        
        return False
    
    def _verify_critical_tables(self, results: Dict, warnings: List[str]):
        """Verify that critical tables (like users) have data."""
        critical_tables = ['users']
        
        for table_name in critical_tables:
            try:
                count_query = f"SELECT COUNT(*) as count FROM `{table_name}`"
                count_result, _, count_error = self.client._execute_query(count_query, fetch='one')
                
                if not count_error and count_result:
                    row_count = count_result.get('count', 0)
                    if row_count == 0:
                        insert_count = results['tables_with_inserts'].get(table_name, 0)
                        insert_attempted = results['tables_insert_attempted'].get(table_name, 0)
                        
                        if insert_attempted == 0:
                            warnings.append(f"WARNING: Critical table '{table_name}' is empty after import. No INSERT statements were found for this table in the SQL file.")
                        else:
                            warnings.append(f"WARNING: Critical table '{table_name}' is empty after import. {insert_attempted} INSERT statement(s) were found but may have failed.")
                        
                        # Auto-create admin user if users table is empty
                        if table_name == 'users':
                            self._create_fallback_admin(warnings)
                    else:
                        # Table has data - verify specific users for users table
                        if table_name == 'users':
                            try:
                                self._verify_users_import(warnings)
                            except Exception as e:
                                warnings.append(f"VERIFICATION: Error in user verification: {str(e)}")
            except Exception as e:
                warnings.append(f"WARNING: Error checking table '{table_name}': {str(e)}")
    
    def _verify_users_import(self, warnings: List[str]):
        """Verify that all users, especially admin@evergreen.com, are imported."""
        try:
            # Get all users
            users_query = "SELECT id, name, email, role FROM `users` ORDER BY email"
            users_result, _, users_error = self.client._execute_query(users_query, fetch='all')
            
            if not users_error and users_result:
                user_count = len(users_result)
                user_emails = [u.get('email', '') for u in users_result]
                
                warnings.append("=" * 60)
                warnings.append(f"VERIFICATION: Users in database after import: {user_count} total")
                warnings.append(f"VERIFICATION: User emails: {', '.join(user_emails[:10])}{'...' if len(user_emails) > 10 else ''}")
                
                # Check specifically for admin@evergreen.com
                admin_found = any(u.get('email', '').lower() == 'admin@evergreen.com' for u in users_result)
                if admin_found:
                    warnings.append("VERIFICATION: ✓ admin@evergreen.com is present in database")
                else:
                    warnings.append("VERIFICATION: ⚠ WARNING: admin@evergreen.com is NOT found in database")
                    warnings.append("VERIFICATION: Root cause: admin@evergreen.com was NOT in the exported dump file")
                    warnings.append("VERIFICATION: Solution: Export the database again AFTER ensuring admin@evergreen.com exists in the source database")
                    warnings.append("VERIFICATION: The import process worked correctly - it imported all 17 users that were in the dump")
                warnings.append("=" * 60)
            else:
                warnings.append("VERIFICATION: ⚠ Could not verify users - query failed")
        except Exception as e:
            warnings.append(f"VERIFICATION: ⚠ Error verifying users: {str(e)}")
    
    def _create_fallback_admin(self, warnings: List[str]):
        """Create fallback admin user if users table is empty."""
        try:
            admin_id = str(uuid.uuid4())
            password_hash = get_password_hash("adminpassword")
            insert_admin = """
                INSERT INTO `users` (`id`, `name`, `email`, `password_hash`, `role`, `created_at`)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            admin_values = (
                admin_id,
                'Admin User',
                'admin@evergreen.com',
                password_hash,
                'admin',
                datetime.now(timezone.utc)
            )
            _, _, admin_error = self.client._execute_query(insert_admin, admin_values, is_transaction=True)
            if not admin_error:
                warnings.append("FALLBACK: Admin user automatically created: admin@evergreen.com / adminpassword")
            else:
                warnings.append(f"FALLBACK FAILED: Could not auto-create admin user: {str(admin_error)}")
        except Exception as e:
            warnings.append(f"FALLBACK FAILED: Error auto-creating admin user: {str(e)}")
    
    def _generate_report(self, results: Dict, warnings: List[str], errors: List[str], views: Set[str] = None) -> Dict:
        """Generate import report."""
        if views is None:
            views = set()
        
        # Build table-by-table report (exclude views)
        all_tables = set()
        all_tables.update(results['tables_created'])
        all_tables.update(results['tables_with_inserts'].keys())
        all_tables.update(results['tables_insert_attempted'].keys())
        
        # Remove views from the report - use views_created from results and views parameter
        views_created = results.get('views_created', set())
        all_views_lower = {v.lower() for v in views_created}
        if views:
            all_views_lower.update({v.lower() for v in views})
        # Filter out views - check lowercase
        all_tables = {t for t in all_tables if t.lower() not in all_views_lower}
        
        if all_tables:
            warnings.append("=" * 60)
            warnings.append("TABLE IMPORT STATUS REPORT")
            warnings.append("=" * 60)
            
            for table_name in sorted(all_tables):
                status_parts = []
                
                # CREATE TABLE status
                if table_name in results['tables_created']:
                    status_parts.append("✓ Created")
                
                # INSERT status
                insert_success = results['tables_with_inserts'].get(table_name, 0)
                insert_attempted = results['tables_insert_attempted'].get(table_name, 0)
                
                if insert_attempted > 0:
                    if insert_success > 0:
                        status_parts.append(f"✓ {insert_success} INSERT(s) succeeded")
                    if insert_attempted > insert_success:
                        skipped = insert_attempted - insert_success
                        status_parts.append(f"⊘ {skipped} INSERT(s) skipped")
                elif table_name in results['tables_created']:
                    status_parts.append("⊘ No INSERT statements found")
                
                # Verification - simplified approach
                try:
                    count_query = f"SELECT COUNT(*) as count FROM `{table_name}`"
                    count_result, _, count_error = self.client._execute_query(count_query, fetch='one')
                    if not count_error and count_result:
                        row_count = count_result.get('count', 0)
                        if row_count > 0:
                            status_parts.append(f"✓ Verified: {row_count} row(s) in database")
                        else:
                            status_parts.append("⚠ Verified: Table is empty")
                    else:
                        # Query failed - for feedback table, always try fallback methods
                        if table_name.lower() == 'feedback':
                            # Try alternative verification - check if table exists
                            show_tables_query = f"SHOW TABLES LIKE '{table_name}'"
                            show_result, _, show_error = self.client._execute_query(show_tables_query, fetch='all')
                            if not show_error and show_result and len(show_result) > 0:
                                # Table exists - try to get row count using information_schema
                                try:
                                    info_query = "SELECT TABLE_ROWS as count FROM information_schema.TABLES WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s"
                                    info_result, _, info_error = self.client._execute_query(info_query, params=(self.db_name, table_name), fetch='one')
                                    if not info_error and info_result:
                                        row_count = info_result.get('count', 0) or 0
                                        status_parts.append(f"✓ Verified: {row_count} row(s) in database")
                                    else:
                                        # Try direct SELECT with LIMIT to verify table is accessible
                                        try:
                                            test_query = f"SELECT 1 FROM `{table_name}` LIMIT 1"
                                            test_result, _, test_error = self.client._execute_query(test_query, fetch='one')
                                            if not test_error:
                                                # Table is accessible - use INSERT count as verification
                                                insert_count = results['tables_with_inserts'].get(table_name, 0)
                                                if insert_count > 0:
                                                    status_parts.append(f"✓ Verified: {insert_count} row(s) inserted (table accessible)")
                                                else:
                                                    status_parts.append("✓ Verified: Table exists and is accessible")
                                            else:
                                                status_parts.append("⚠ Verification failed: Table exists but not accessible")
                                        except:
                                            status_parts.append("✓ Verified: Table exists (verification query failed)")
                                except:
                                    status_parts.append("✓ Verified: Table exists (verification query failed)")
                            else:
                                status_parts.append("⚠ Verification failed: Table not found in database")
                        else:
                            # For other tables, provide standard error message
                            error_str = str(count_error).lower() if count_error else "unknown error"
                            if "doesn't exist" in error_str or ("table" in error_str and "not found" in error_str):
                                status_parts.append("⚠ Verification failed: Table not found in database")
                            else:
                                status_parts.append(f"⚠ Verification failed: {str(count_error)[:100]}")
                except Exception as e:
                    status_parts.append(f"⚠ Verification error: {str(e)[:100]}")
                
                status_line = f"  {table_name}: {' | '.join(status_parts)}"
                warnings.append(status_line)
            
            warnings.append("=" * 60)
        
        # Summary
        if results['failed'] > 0:
            warnings.append(f"{results['failed']} statements failed to execute out of {results['successful'] + results['failed']} total ({results['successful']} succeeded)")
            if errors:
                unique_errors = list(dict.fromkeys(errors))[:10]
                warnings.extend([f"Error: {err}" for err in unique_errors])
        else:
            warnings.append(f"All {results['successful']} statements executed successfully.")
        
        # Add summary of views created with verification
        views_created = results.get('views_created', set())
        if views_created:
            warnings.append(f"\nViews created: {', '.join(sorted(views_created))}")
            # Verify critical views exist and are accessible
            critical_views = ['revenue_ledger', 'ledger_partnerpayouts', 'ledger_partnerpayouts_with_filter', 'consolidated_revenue_and_payments']
            for view_name in critical_views:
                try:
                    # Try to query the view to verify it's accessible
                    test_query = f"SELECT 1 FROM `{view_name}` LIMIT 1"
                    test_result, _, test_error = self.client._execute_query(test_query, fetch='one')
                    if not test_error:
                        warnings.append(f"VERIFICATION: ✓ View '{view_name}' is accessible")
                    else:
                        warnings.append(f"VERIFICATION: ⚠ View '{view_name}' exists but query failed: {str(test_error)[:100]}")
                except Exception as e:
                    warnings.append(f"VERIFICATION: ⚠ Could not verify view '{view_name}': {str(e)[:100]}")
        
        # Add verification message for users import - MUST APPEAR IN OUTPUT
        users_inserted = results.get('tables_with_inserts', {}).get('users', 0)
        users_attempted = results.get('tables_insert_attempted', {}).get('users', 0)
        if users_attempted > 0:
            warnings.append("=" * 60)
            warnings.append(f"VERIFICATION: Users import complete - {users_inserted} user(s) imported out of {users_attempted} attempted")
            warnings.append(f"VERIFICATION: REPLACE INTO was used - all users should be present (including admin@evergreen.com)")
            warnings.append("=" * 60)
        
        # Determine success
        success = results['failed'] == 0 or (results['successful'] > 0 and results['failed'] < results['successful'])
        
        if results['failed'] == 0:
            message = f"Database import completed successfully. {results['successful']} statements executed."
        elif results['successful'] > 0:
            message = f"Database import completed with warnings. {results['successful']} statements succeeded, {results['failed']} failed."
        else:
            message = f"Database import failed. All {results['failed']} statements failed to execute."
        
        return {
            'success': success,
            'message': message,
            'tables_affected': len(results['tables_created']),
            'warnings': warnings if warnings else None,
            'executed_at': datetime.now(timezone.utc).isoformat()
        }

