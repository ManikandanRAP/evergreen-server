import pymysql
import json
import os
import time
import base64
from auth import get_password_hash
from contextlib import contextmanager
from pydantic import BaseModel
from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT
from utils.date_normalizer import *
from models import FeedbackCreate # Import FeedbackCreate
import uuid
from datetime import datetime, timezone


# This dictionary provides a definitive, complete mapping from the Python
# model's field names (snake_case) to the actual database column names.
# This resolves all naming inconsistencies in one place.
def _serialize_contract_links(links):
    if links is None:
        return None
    if isinstance(links, str):
        return links
    serialized = []
    for item in links:
        if isinstance(item, dict):
            serialized.append({"url": item.get("url"), "label": item.get("label")})
        elif hasattr(item, "model_dump"):
            serialized.append(item.model_dump())
        elif hasattr(item, "dict"):
            serialized.append(item.dict())
        else:
            serialized.append(item)
    return json.dumps(serialized)


def _hydrate_show_record(show: dict) -> dict:
    if not show:
        return show
    annual_usd_raw = show.get("annual_usd")
    if isinstance(annual_usd_raw, str):
        try:
            annual_usd = json.loads(annual_usd_raw)
        except json.JSONDecodeError:
            annual_usd = {}
    else:
        annual_usd = annual_usd_raw if isinstance(annual_usd_raw, dict) else {}
    show["annual_usd"] = annual_usd
    show["revenue_2023"] = annual_usd.get("2023", 0)
    show["revenue_2024"] = annual_usd.get("2024", 0)
    show["revenue_2025"] = annual_usd.get("2025", 0)

    links_raw = show.get("contract_links")
    if isinstance(links_raw, str):
        try:
            show["contract_links"] = json.loads(links_raw)
        except json.JSONDecodeError:
            show["contract_links"] = []
    elif links_raw is None:
        show["contract_links"] = []
    if "hosts" not in show:
        show["hosts"] = []
    return show


def _serialize_hosts(hosts):
    if not hosts:
        return []
    serialized = []
    for item in hosts:
        if isinstance(item, dict):
            serialized.append(item)
        elif hasattr(item, "model_dump"):
            serialized.append(item.model_dump())
        elif hasattr(item, "dict"):
            serialized.append(item.dict())
        else:
            serialized.append(item)
    return serialized


def _host_row_has_data(host: dict) -> bool:
    return any(host.get(k) for k in ("contact_name", "contact_address", "contact_phone", "contact_email"))


def _format_hosts_for_api(rows):
    hosts = []
    for row in rows or []:
        hosts.append({
            "position": row.get("position"),
            "contact_name": row.get("contact_name"),
            "contact_address": row.get("contact_address"),
            "contact_phone": row.get("contact_phone"),
            "contact_email": row.get("contact_email"),
        })
    return hosts


def _enum_to_value(v):
    if v is None:
        return None
    return v.value if hasattr(v, "value") else v


COLUMN_MAPPING = {
    # Basic Info
    "title": "title",
    "show_type": "show_type",
    "media_type": "media_type",
    "relationship_level": "relationship_level",
    "first_episode_date": "first_episode_date",
    "subnetwork_id": "subnetwork_id",
    "is_rate_card": "rate_card",
    "is_original": "is_original",
    "genre_name": "genre_name",
    "ranking_category": "ranking_category",

    # Financial
    "minimum_guarantee": "minimum_guarantee",
    "evergreen_ownership_pct": "evergreen_ownership_pct",
    "base_cpm_usd": "base_cpm_usd",
    "span_cpm_usd": "span_cpm_usd",
    "has_sponsorship_revenue": "has_sponsorship_revenue",
    "has_non_evergreen_revenue": "has_non_evergreen_revenue",
    "has_myco_ledger_access": "has_myco_ledger_access",
    "has_flightpath_access": "has_flightpath_access",
    "has_branded_revenue": "has_branded_revenue",
    "has_marketing_revenue": "has_marketing_revenue",
    "has_web_mgmt_revenue": "has_web_mgmt_revenue",

    # Contract Splits
    "side_bonus_percent": "side_bonus_percent",
    "youtube_ads_percent": "youtube_ads_percent",
    "subscriptions_percent": "subscriptions_percent",
    "standard_ads_percent": "standard_ads_percent",
    "sponsorship_ad_fp_lead_percent": "sponsorship_ad_fp_lead_percent",
    "sponsorship_ad_partner_lead_percent": "sponsorship_ad_partner_lead_percent",
    "sponsorship_ad_partner_sold_percent": "sponsorship_ad_partner_sold_percent",
    "programmatic_ads_span_percent": "programmatic_ads_span_percent",
    "merchandise_percent": "merchandise_percent",
    "branded_revenue_percent": "branded_revenue_percent",
    "marketing_services_revenue_percent": "marketing_services_revenue_percent",

    # Hands Off Splits
    "direct_customer_hands_off_percent": "direct_customer_hands_off_percent",
    "youtube_hands_off_percent": "youtube_hands_off_percent",
    "subscription_hands_off_percent": "subscription_hands_off_percent",

    # Content Details
    "cadence": "cadence",
    "pre_roll_ad_slots": "pre_roll_ad_slots",
    "mid_roll_ad_slots": "mid_roll_ad_slots",
    "post_roll_ad_slots": "post_roll_ad_slots",
    "us_listeners_pct": "us_listeners_pct",
    "avg_show_length_mins": "avg_show_length_mins",
    "show_host_contact": "show_host_contact",
    "primary_show_contact": "primary_show_contact",
    "show_producer_contact": "show_producer_contact",
    "primary_contact_name": "primary_contact_name",
    "primary_contact_address": "primary_contact_address",
    "primary_contact_phone": "primary_contact_phone",
    "primary_contact_email": "primary_contact_email",
    "producer_contact_name": "producer_contact_name",
    "producer_contact_address": "producer_contact_address",
    "producer_contact_phone": "producer_contact_phone",
    "producer_contact_email": "producer_contact_email",
    "contract_links": "contract_links",

    # Demographics
    "age_demographic": "age_demographic",
    "gender": "gender",
    "show_status": "show_status",
    "is_undersized": "is_undersized",

    # Internal / Other
    "qbo_show_id": "qbo_show_id",
    "qbo_show_name": "qbo_show_name",
    "id": "id",
    "annual_usd": "annual_usd",
}


class DatabaseConnectionError(Exception):
    """Custom exception for database connection issues"""
    pass

class DatabaseCredentialsError(Exception):
    """Custom exception for database credential issues"""
    pass

async def validation_exception_handler(request: Request, exc: RequestValidationError):
    print("Validation error:", exc.errors())
    return JSONResponse(status_code=422, content={"detail": exc.errors()})

def test_database_connection():
    try:
        connection = pymysql.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor, port=DB_PORT, connect_timeout=5
        )
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        connection.close()
        return True, None
    except pymysql.err.OperationalError as e:
        error_code = e.args[0]
        if error_code == 1045: return False, f"Database credentials invalid: {str(e)}"
        elif error_code == 2003: return False, f"Cannot connect to database server at {DB_HOST}: {DB_PORT}."
        elif error_code == 1049: return False, f"Database '{DB_NAME}' does not exist on the server."
        else: return False, f"Database connection failed: {str(e)}"
    except Exception as e:
        return False, f"Unexpected database error: {str(e)}"

@contextmanager
def get_db_connection(retries=3, timeout=30):
    """
    Get database connection with retry logic.
    
    Args:
        retries: Number of retry attempts (default: 3)
        timeout: Connection timeout in seconds (default: 30, longer for imports)
    """
    connection = None
    last_error = None
    
    for attempt in range(retries):
        connection = None
        try:
            connection = pymysql.connect(
                host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME,
                cursorclass=pymysql.cursors.DictCursor, port=DB_PORT, 
                connect_timeout=timeout, read_timeout=timeout, write_timeout=timeout
            )
            try:
                yield connection
                # If we get here, everything succeeded
                return  # Success - exit the retry loop
            except pymysql.err.OperationalError as query_error:
                # Exception occurred during query execution (inside yield)
                error_code = query_error.args[0] if query_error.args else None
                error_msg = query_error.args[1] if len(query_error.args) > 1 else str(query_error)
                print(f"ERROR in context manager (during yield): Code={error_code}, Message={error_msg}")
                import traceback
                traceback.print_exc()
                if error_code == 2013:  # Lost connection during query
                    # Close connection and retry
                    if connection:
                        try:
                            connection.close()
                        except:
                            pass
                        connection = None
                    if attempt < retries - 1:
                        import time
                        time.sleep(2 ** attempt)
                        continue  # Retry
                    raise DatabaseConnectionError(f"Lost connection to MySQL server during query: {str(query_error)}")
                else:
                    # Other query errors - don't retry, just propagate
                    raise
            except pymysql.Error as query_error:
                # Other pymysql errors
                error_code = query_error.args[0] if query_error.args else None
                error_msg = query_error.args[1] if len(query_error.args) > 1 else str(query_error)
                print(f"ERROR in context manager (pymysql.Error): Code={error_code}, Message={error_msg}")
                import traceback
                traceback.print_exc()
                raise
            except Exception as query_error:
                # Non-connection errors during query - propagate normally
                print(f"ERROR in context manager (Exception): {type(query_error).__name__}: {str(query_error)}")
                import traceback
                traceback.print_exc()
                raise
            finally:
                # Always close connection after yield completes (success or error)
                if connection:
                    try:
                        connection.close()
                    except:
                        pass
                    connection = None
        except pymysql.err.OperationalError as e:
            last_error = e
            error_code = e.args[0]
            if error_code == 1045: 
                raise DatabaseCredentialsError(f"Database credentials invalid: {str(e)}")
            elif error_code == 2003: 
                if attempt < retries - 1:
                    import time
                    time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s, 4s
                    continue  # Retry connection
                raise DatabaseConnectionError(f"Cannot connect to database server at {DB_HOST}:{DB_PORT}. Check DB_HOST environment variable.")
            elif error_code == 1049: 
                raise DatabaseConnectionError(f"Database '{DB_NAME}' does not exist")
            elif error_code == 2013:  # Lost connection during query
                if attempt < retries - 1:
                    import time
                    time.sleep(2 ** attempt)
                    continue  # Retry connection
                raise DatabaseConnectionError(f"Lost connection to MySQL server during query. This may indicate database server issues or network problems.")
            else: 
                if attempt < retries - 1:
                    import time
                    time.sleep(2 ** attempt)
                    continue
                raise DatabaseConnectionError(f"Database connection failed: {str(e)}")
        except Exception as e:
            last_error = e
            if attempt < retries - 1:
                import time
                time.sleep(2 ** attempt)
                continue
            raise DatabaseConnectionError(f"Unexpected database error: {str(e)}")
        finally:
            if connection:
                try:
                    connection.close()
                except:
                    pass
                connection = None
    
    # If we get here, all retries failed
    if last_error:
        raise DatabaseConnectionError(f"Failed to connect after {retries} attempts: {str(last_error)}")

class SqlClient:
    _last_verification_ts = 0.0
    _verify_interval_sec = 30.0

    def __init__(self):
        # Avoid running an extra round-trip "SELECT 1" on every request.
        # Verify periodically so we keep the fail-fast behavior without adding latency.
        now = time.monotonic()
        if now - SqlClient._last_verification_ts > SqlClient._verify_interval_sec:
            self.verify_connection()
            SqlClient._last_verification_ts = now

    def verify_connection(self):
        success, error = test_database_connection()
        if not success:
            raise DatabaseConnectionError(f"Failed to initialize database client: {error}")
        print("Database connection verified successfully")

    def _execute_query(self, query: str, params: tuple = None, fetch: str = None, is_transaction=False, timeout=30):
        """
        Execute a database query with retry logic.
        
        Args:
            query: SQL query string
            params: Query parameters tuple
            fetch: 'one', 'all', or None
            is_transaction: Whether to commit after execution
            timeout: Connection timeout in seconds (longer for imports)
        """
        try:
            with get_db_connection(retries=3, timeout=timeout) as db:
                try:
                    with db.cursor() as cursor:
                        cursor.execute(query, params)
                        rows_affected = cursor.rowcount
                        if fetch == 'one': result = cursor.fetchone()
                        elif fetch == 'all': result = cursor.fetchall()
                        else: result = None
                        if is_transaction: db.commit()
                        return result, rows_affected, None
                except pymysql.Error as sql_error:
                    # Log the actual SQL error with details
                    error_code = sql_error.args[0] if sql_error.args else None
                    error_msg = sql_error.args[1] if len(sql_error.args) > 1 else str(sql_error)
                    print(f"SQL ERROR in _execute_query: Code={error_code}, Message={error_msg}")
                    print(f"Query (first 200 chars): {query[:200]}")
                    if params:
                        print(f"Params: {params}")
                    import traceback
                    traceback.print_exc()
                    return None, 0, sql_error
                except Exception as query_error:
                    print(f"UNEXPECTED ERROR in query execution: {type(query_error).__name__}: {str(query_error)}")
                    import traceback
                    traceback.print_exc()
                    return None, 0, query_error
        except (DatabaseConnectionError, DatabaseCredentialsError) as e:
            print(f"Database connection/credential error: {e}")
            return None, 0, e
        except Exception as e:
            print(f"UNEXPECTED ERROR in _execute_query: {type(e).__name__}: {str(e)}")
            import traceback
            traceback.print_exc()
            return None, 0, e

    def _index_exists(self, table_name: str, index_name: str):
        sql = """
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = %s
          AND table_name = %s
          AND index_name = %s
        LIMIT 1
        """
        result, _, error = self._execute_query(sql, (DB_NAME, table_name, index_name), fetch='one')
        if error:
            return False, error
        return bool(result), None

    @staticmethod
    def _dict_row_value(row: dict, *keys: str):
        if not row:
            return None
        for key in keys:
            if key in row:
                return row[key]
            upper = key.upper()
            if upper in row:
                return row[upper]
            lower = key.lower()
            if lower in row:
                return row[lower]
        return None

    def _get_table_type(self, table_name: str):
        sql = """
        SELECT table_type
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name = %s
        LIMIT 1
        """
        result, _, error = self._execute_query(sql, (DB_NAME, table_name), fetch='one')
        if error:
            return None, error
        if not result:
            return None, None
        return self._dict_row_value(result, "table_type"), None

    def ensure_performance_indexes(self):
        """
        Create high-value indexes for auth, ledger, notices, feedback, inbox messages,
        and other hot read paths. Safe to run repeatedly; existing indexes are skipped.
        """
        index_specs = [
            # --- Auth / settings ---
            ("users", "idx_users_email", "CREATE INDEX idx_users_email ON users (email)"),
            ("users", "idx_users_id", "CREATE INDEX idx_users_id ON users (id)"),
            # --- Revenue ledger ---
            ("revenue_ledger", "idx_revenue_ledger_vendor_qbo_id", "CREATE INDEX idx_revenue_ledger_vendor_qbo_id ON revenue_ledger (vendor_qbo_id)"),
            ("revenue_ledger", "idx_revenue_ledger_vendor_invoice_date", "CREATE INDEX idx_revenue_ledger_vendor_invoice_date ON revenue_ledger (vendor_qbo_id, invoice_date)"),
            ("revenue_ledger", "idx_revenue_ledger_invoice_doc_payment_waiting", "CREATE INDEX idx_revenue_ledger_invoice_doc_payment_waiting ON revenue_ledger (invoice_doc_number, tot_payment_amounts, partner_comp_waiting)"),
            ("ledger_partnerpayouts", "idx_lpp_vendor_docnumber", "CREATE INDEX idx_lpp_vendor_docnumber ON ledger_partnerpayouts (vendor_qbo_id, docnumber)"),
            ("ledger_partnerpayouts", "idx_lpp_docnumber", "CREATE INDEX idx_lpp_docnumber ON ledger_partnerpayouts (docnumber)"),
            # --- Vendor split lookups ---
            ("split_history", "idx_split_history_vendor_qbo_id", "CREATE INDEX idx_split_history_vendor_qbo_id ON split_history (vendor_qbo_id)"),
            ("split_history", "idx_split_history_vendor_qbo_vendor_name", "CREATE INDEX idx_split_history_vendor_qbo_vendor_name ON split_history (vendor_qbo_id, vendor_name)"),
            ("split_history", "idx_split_history_show_vendor_eff", "CREATE INDEX idx_split_history_show_vendor_eff ON split_history (show_qbo_id, vendor_qbo_id, effective_date)"),
            # --- Shows list / archive views ---
            ("shows", "idx_shows_archived", "CREATE INDEX idx_shows_archived ON shows (is_archived)"),
            ("shows", "idx_shows_archived_at", "CREATE INDEX idx_shows_archived_at ON shows (is_archived, archived_at)"),
            # --- Feedbacks admin list (GET /feedbacks) ---
            ("feedback", "idx_feedback_created_at", "CREATE INDEX idx_feedback_created_at ON feedback (created_at)"),
            ("feedback", "idx_feedback_created_by", "CREATE INDEX idx_feedback_created_by ON feedback (created_by)"),
            ("feedback", "idx_feedback_type", "CREATE INDEX idx_feedback_type ON feedback (type)"),
            ("feedback", "idx_feedback_status", "CREATE INDEX idx_feedback_status ON feedback (status)"),
            ("feedback", "idx_feedback_completed_at", "CREATE INDEX idx_feedback_completed_at ON feedback (completed_at)"),
            ("feedback", "idx_feedback_completed_by", "CREATE INDEX idx_feedback_completed_by ON feedback (completed_by)"),
            ("feedback", "idx_feedback_status_created_at", "CREATE INDEX idx_feedback_status_created_at ON feedback (status, created_at)"),
            ("feedback", "idx_feedback_status_completed_at", "CREATE INDEX idx_feedback_status_completed_at ON feedback (status, completed_at)"),
            # --- Myco notices list + scheduler (GET /notices, due reminders) ---
            ("myco_notices", "idx_myco_notices_status", "CREATE INDEX idx_myco_notices_status ON myco_notices (status)"),
            ("myco_notices", "idx_myco_notices_show_id", "CREATE INDEX idx_myco_notices_show_id ON myco_notices (show_id)"),
            ("myco_notices", "idx_myco_notices_next_send", "CREATE INDEX idx_myco_notices_next_send ON myco_notices (status, next_send_at)"),
            ("myco_notices", "idx_myco_notices_due_date", "CREATE INDEX idx_myco_notices_due_date ON myco_notices (due_date)"),
            ("myco_notices", "idx_myco_notices_created_at", "CREATE INDEX idx_myco_notices_created_at ON myco_notices (created_at)"),
            ("myco_notices", "idx_myco_notices_status_created_at", "CREATE INDEX idx_myco_notices_status_created_at ON myco_notices (status, created_at)"),
            ("myco_notices", "idx_myco_notices_notice_type", "CREATE INDEX idx_myco_notices_notice_type ON myco_notices (notice_type)"),
            ("myco_notices", "idx_myco_notices_created_by", "CREATE INDEX idx_myco_notices_created_by ON myco_notices (created_by)"),
            # --- Notice delivery history (notice detail view) ---
            ("myco_notice_deliveries", "idx_notice_deliveries_notice", "CREATE INDEX idx_notice_deliveries_notice ON myco_notice_deliveries (notice_id)"),
            ("myco_notice_deliveries", "idx_notice_deliveries_notice_sent", "CREATE INDEX idx_notice_deliveries_notice_sent ON myco_notice_deliveries (notice_id, sent_at)"),
            # --- Inbox messages + unread badge (GET /inbox/messages, /unread-count) ---
            ("inbox_messages", "idx_inbox_messages_user", "CREATE INDEX idx_inbox_messages_user ON inbox_messages (user_id, read_at)"),
            ("inbox_messages", "idx_inbox_messages_created", "CREATE INDEX idx_inbox_messages_created ON inbox_messages (created_at)"),
            ("inbox_messages", "idx_inbox_messages_user_created", "CREATE INDEX idx_inbox_messages_user_created ON inbox_messages (user_id, created_at)"),
            # --- User login activity admin page ---
            ("user_login_activity", "idx_user_login_activity_time_id", "CREATE INDEX idx_user_login_activity_time_id ON user_login_activity (occurred_at_utc DESC, id DESC)"),
            ("user_login_activity", "idx_user_login_activity_action_status_time_id", "CREATE INDEX idx_user_login_activity_action_status_time_id ON user_login_activity (action, status, occurred_at_utc DESC, id DESC)"),
            ("user_login_activity", "idx_user_login_activity_email_time_id", "CREATE INDEX idx_user_login_activity_email_time_id ON user_login_activity (user_email, occurred_at_utc DESC, id DESC)"),
            ("user_login_activity", "idx_user_login_activity_request_id_time", "CREATE INDEX idx_user_login_activity_request_id_time ON user_login_activity (request_id, occurred_at_utc DESC)"),
            ("user_login_activity", "idx_user_login_activity_name_time_id", "CREATE INDEX idx_user_login_activity_name_time_id ON user_login_activity (user_name, occurred_at_utc DESC, id DESC)"),
        ]

        for table_name, index_name, create_sql in index_specs:
            table_type, table_type_error = self._get_table_type(table_name)
            if table_type_error:
                print(f"Skipping index check for {table_name}.{index_name}: {table_type_error}")
                continue
            if table_type is None:
                print(f"Skipping index {index_name}: table '{table_name}' not found")
                continue
            if table_type != "BASE TABLE":
                print(f"Skipping index {index_name}: '{table_name}' is {table_type}")
                continue

            exists, exists_error = self._index_exists(table_name, index_name)
            if exists_error:
                print(f"Skipping index check for {table_name}.{index_name}: {exists_error}")
                continue
            if exists:
                continue

            _, _, create_error = self._execute_query(create_sql, is_transaction=True)
            if create_error:
                # Duplicate key name can happen in concurrent startups; treat as success.
                error_code = create_error.args[0] if getattr(create_error, "args", None) else None
                if error_code == 1061:
                    continue
                # Missing table: skip gracefully so startup doesn't fail in partial schemas.
                if error_code == 1146:
                    print(f"Skipping index {index_name}: table '{table_name}' not found")
                    continue
                # Can't create index on a view; skip safely.
                if error_code == 1347:
                    print(f"Skipping index {index_name}: '{table_name}' is a view")
                    continue
                print(f"Failed creating index {index_name} on {table_name}: {create_error}")
            else:
                print(f"Ensured index {index_name} on {table_name}")

    def ensure_user_login_activity_schema(self):
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS user_login_activity (
            id BIGINT NOT NULL AUTO_INCREMENT,
            event_uuid CHAR(36) NOT NULL,
            occurred_at_utc DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            user_id VARCHAR(64) NULL,
            user_email VARCHAR(255) NOT NULL,
            user_name VARCHAR(255) NULL,
            user_role VARCHAR(64) NULL,
            action ENUM('LOGIN','LOGOUT') NOT NULL,
            status ENUM('SUCCESS','FAILED') NOT NULL DEFAULT 'SUCCESS',
            request_id VARCHAR(128) NULL,
            session_id VARCHAR(128) NULL,
            ip_hash CHAR(64) NULL,
            user_agent VARCHAR(512) NULL,
            failure_reason VARCHAR(255) NULL,
            metadata_json JSON NULL,
            PRIMARY KEY (id),
            UNIQUE KEY uq_user_login_activity_event_uuid (event_uuid),
            KEY idx_user_login_activity_time (occurred_at_utc),
            KEY idx_user_login_activity_email_time (user_email, occurred_at_utc),
            KEY idx_user_login_activity_action_time (action, occurred_at_utc),
            KEY idx_user_login_activity_status_time (status, occurred_at_utc)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        _, _, error = self._execute_query(create_table_sql, is_transaction=True)
        if error:
            raise error

    def _column_exists(self, table_name: str, column_name: str) -> bool:
        sql = """
        SELECT COUNT(*) AS cnt
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND column_name = %s
        """
        row, _, error = self._execute_query(sql, (DB_NAME, table_name, column_name), fetch="one")
        if error or not row:
            return False
        return int(self._dict_row_value(row, "cnt") or 0) > 0

    def ensure_inbox_schema(self):
        """
        Self-heal inbox storage after deploys that skip 4-database.sh:
        rename user_notifications → inbox_messages, add metadata, system_settings.
        """
        legacy_type, _ = self._get_table_type("user_notifications")
        inbox_type, _ = self._get_table_type("inbox_messages")

        if legacy_type == "BASE TABLE" and inbox_type != "BASE TABLE":
            _, _, rename_err = self._execute_query(
                "RENAME TABLE user_notifications TO inbox_messages",
                is_transaction=True,
            )
            if rename_err:
                print(f"WARNING: Could not rename user_notifications to inbox_messages: {rename_err}")
            else:
                print("Renamed user_notifications to inbox_messages")

        inbox_type, _ = self._get_table_type("inbox_messages")
        if inbox_type != "BASE TABLE":
            return

        if not self._column_exists("inbox_messages", "metadata"):
            _, _, alter_err = self._execute_query(
                "ALTER TABLE inbox_messages ADD COLUMN metadata JSON NULL AFTER notice_id",
                is_transaction=True,
            )
            if alter_err:
                print(f"WARNING: Could not add inbox_messages.metadata: {alter_err}")
            else:
                print("Added inbox_messages.metadata column")

        if not self._column_exists("inbox_messages", "pinned_at"):
            _, _, pin_err = self._execute_query(
                "ALTER TABLE inbox_messages ADD COLUMN pinned_at DATETIME NULL AFTER read_at",
                is_transaction=True,
            )
            if pin_err:
                print(f"WARNING: Could not add inbox_messages.pinned_at: {pin_err}")
            else:
                print("Added inbox_messages.pinned_at column")

        create_settings_sql = """
        CREATE TABLE IF NOT EXISTS system_settings (
            setting_key VARCHAR(64) NOT NULL PRIMARY KEY,
            setting_value VARCHAR(255) NOT NULL,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        _, _, settings_err = self._execute_query(create_settings_sql, is_transaction=True)
        if settings_err:
            print(f"WARNING: Could not ensure system_settings: {settings_err}")
            return

        _, _, seed_err = self._execute_query(
            """
            INSERT IGNORE INTO system_settings (setting_key, setting_value, updated_at)
            VALUES ('inbox_retention_days', '365', UTC_TIMESTAMP())
            """,
            is_transaction=True,
        )
        if seed_err:
            print(f"WARNING: Could not seed inbox retention setting: {seed_err}")

    def ensure_myco_notice_contacts_schema(self):
        """
        Normalize notice contacts into myco_notice_contacts (1–3 per notice).
        Backfills Contact 1 from legacy myco_notices.contact_* columns, then drops them.
        """
        notices_type, _ = self._get_table_type("myco_notices")
        if notices_type != "BASE TABLE":
            return

        create_sql = """
        CREATE TABLE IF NOT EXISTS myco_notice_contacts (
            id CHAR(36) PRIMARY KEY,
            notice_id CHAR(36) NOT NULL,
            position TINYINT NOT NULL,
            contact_name VARCHAR(255) NOT NULL,
            contact_email VARCHAR(255) NULL,
            contact_phone VARCHAR(64) NULL,
            contact_source ENUM('auto_primary', 'manual') NOT NULL DEFAULT 'manual',
            myco_user_id CHAR(36) NULL,
            channel_email BOOLEAN NOT NULL DEFAULT FALSE,
            channel_text BOOLEAN NOT NULL DEFAULT FALSE,
            channel_myco BOOLEAN NOT NULL DEFAULT FALSE,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            UNIQUE KEY uq_myco_notice_contacts_notice_position (notice_id, position),
            INDEX idx_myco_notice_contacts_notice (notice_id),
            INDEX idx_myco_notice_contacts_email (contact_email),
            INDEX idx_myco_notice_contacts_myco_user (myco_user_id),
            CONSTRAINT fk_myco_notice_contacts_notice
                FOREIGN KEY (notice_id) REFERENCES myco_notices(id) ON DELETE CASCADE,
            CONSTRAINT fk_myco_notice_contacts_myco_user
                FOREIGN KEY (myco_user_id) REFERENCES users(id) ON DELETE SET NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        _, _, create_err = self._execute_query(create_sql, is_transaction=True)
        if create_err:
            print(f"WARNING: Could not ensure myco_notice_contacts: {create_err}")
            return

        contacts_type, _ = self._get_table_type("myco_notice_contacts")
        if contacts_type == "BASE TABLE" and not self._column_exists("myco_notice_contacts", "channel_email"):
            _, _, chan_err = self._execute_query(
                """
                ALTER TABLE myco_notice_contacts
                    ADD COLUMN channel_email BOOLEAN NOT NULL DEFAULT FALSE AFTER myco_user_id,
                    ADD COLUMN channel_text BOOLEAN NOT NULL DEFAULT FALSE AFTER channel_email,
                    ADD COLUMN channel_myco BOOLEAN NOT NULL DEFAULT FALSE AFTER channel_text
                """,
                is_transaction=True,
            )
            if chan_err:
                print(f"WARNING: Could not add myco_notice_contacts channel columns: {chan_err}")
            else:
                print("Added per-contact channel columns on myco_notice_contacts")

        if self._column_exists("myco_notices", "contact_name"):
            backfill_sql = """
            INSERT INTO myco_notice_contacts (
                id, notice_id, position, contact_name, contact_email, contact_phone,
                contact_source, myco_user_id, created_at, updated_at
            )
            SELECT
                UUID(),
                n.id,
                1,
                n.contact_name,
                n.contact_email,
                n.contact_phone,
                COALESCE(n.contact_source, 'manual'),
                n.myco_recipient_user_id,
                COALESCE(n.created_at, UTC_TIMESTAMP()),
                COALESCE(n.updated_at, UTC_TIMESTAMP())
            FROM myco_notices n
            WHERE n.contact_name IS NOT NULL
              AND TRIM(n.contact_name) <> ''
              AND NOT EXISTS (
                  SELECT 1 FROM myco_notice_contacts c WHERE c.notice_id = n.id
              )
            """
            _, _, backfill_err = self._execute_query(backfill_sql, is_transaction=True)
            if backfill_err:
                print(f"WARNING: Could not backfill myco_notice_contacts: {backfill_err}")
                return
            print("Backfilled myco_notice_contacts from legacy notice contact columns")

            fk_sql = """
            SELECT CONSTRAINT_NAME AS constraint_name
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = 'myco_notices'
              AND COLUMN_NAME = 'myco_recipient_user_id'
              AND REFERENCED_TABLE_NAME IS NOT NULL
            LIMIT 1
            """
            fk_row, _, fk_err = self._execute_query(fk_sql, (DB_NAME,), fetch="one")
            if not fk_err and fk_row:
                fk_name = self._dict_row_value(fk_row, "constraint_name")
                if fk_name:
                    _, _, drop_fk_err = self._execute_query(
                        f"ALTER TABLE myco_notices DROP FOREIGN KEY `{fk_name}`",
                        is_transaction=True,
                    )
                    if drop_fk_err:
                        print(f"WARNING: Could not drop myco_notices FK {fk_name}: {drop_fk_err}")
                        return

            drop_cols = []
            for col in (
                "contact_name",
                "contact_email",
                "contact_phone",
                "contact_source",
                "myco_recipient_user_id",
            ):
                if self._column_exists("myco_notices", col):
                    drop_cols.append(f"DROP COLUMN `{col}`")
            if drop_cols:
                _, _, drop_err = self._execute_query(
                    f"ALTER TABLE myco_notices {', '.join(drop_cols)}",
                    is_transaction=True,
                )
                if drop_err:
                    print(f"WARNING: Could not drop legacy myco_notices contact columns: {drop_err}")
                else:
                    print("Dropped legacy myco_notices contact columns")

        deliveries_type, _ = self._get_table_type("myco_notice_deliveries")
        if deliveries_type == "BASE TABLE" and not self._column_exists(
            "myco_notice_deliveries", "contact_position"
        ):
            _, _, pos_err = self._execute_query(
                """
                ALTER TABLE myco_notice_deliveries
                    ADD COLUMN contact_position TINYINT NULL AFTER recipient
                """,
                is_transaction=True,
            )
            if pos_err:
                print(f"WARNING: Could not add myco_notice_deliveries.contact_position: {pos_err}")
            else:
                print("Added contact_position column on myco_notice_deliveries")

    def _fetch_hosts_by_show_ids(self, show_ids):
        if not show_ids:
            return {}
        placeholders = ", ".join(["%s"] * len(show_ids))
        sql = (
            f"SELECT show_id, position, contact_name, contact_address, contact_phone, contact_email "
            f"FROM show_hosts WHERE show_id IN ({placeholders}) ORDER BY show_id, position"
        )
        rows, _, error = self._execute_query(sql, tuple(show_ids), fetch="all")
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                raise error
            return {}
        grouped = {}
        for row in rows or []:
            grouped.setdefault(row["show_id"], []).append(row)
        return grouped

    def _attach_hosts_to_shows(self, shows):
        if not shows:
            return shows
        show_ids = [s.get("id") for s in shows if s.get("id")]
        grouped = self._fetch_hosts_by_show_ids(show_ids)
        for show in shows:
            show["hosts"] = _format_hosts_for_api(grouped.get(show.get("id"), []))
        return shows

    def _replace_show_hosts(self, show_id: str, hosts):
        hosts = _serialize_hosts(hosts)
        delete_sql = "DELETE FROM show_hosts WHERE show_id = %s"
        _, _, delete_error = self._execute_query(delete_sql, (show_id,), is_transaction=True)
        if delete_error:
            return delete_error

        for host in hosts:
            if not _host_row_has_data(host):
                continue
            host_id = str(uuid.uuid4())
            insert_sql = """
                INSERT INTO show_hosts
                (id, show_id, position, contact_name, contact_address, contact_phone, contact_email)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                host_id,
                show_id,
                host.get("position"),
                host.get("contact_name"),
                host.get("contact_address"),
                host.get("contact_phone"),
                host.get("contact_email"),
            )
            _, _, insert_error = self._execute_query(insert_sql, values, is_transaction=True)
            if insert_error:
                return insert_error
        return None

    def get_all_podcasts(self):
        sql = "SELECT * FROM shows WHERE is_archived = FALSE OR is_archived IS NULL"
        shows, _, error = self._execute_query(sql, fetch='all')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)): raise error
            return []

        for i, show in enumerate(shows):
            shows[i] = _hydrate_show_record(show)
        return self._attach_hosts_to_shows(shows)

    def get_podcast_by_id(self, show_id: str):
        sql = "SELECT * FROM shows WHERE id = %s"
        show, _, error = self._execute_query(sql, (show_id,), fetch='one')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                raise error
            return None, str(error)
        if not show:
            return None, None
        show = _hydrate_show_record(show)
        self._attach_hosts_to_shows([show])
        return show, None

    def get_podcasts_for_partner(self, partner_id: str):
        user, error = self.get_user_by_id(partner_id)
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                raise error
            return [], str(error)
        if not user:
            return [], "Partner not found"

        vendor_qbo_id = user.get("mapped_vendor_qbo_id")
        if not vendor_qbo_id:
            return [], None

        sql = """
            SELECT DISTINCT s.*
            FROM shows s
            INNER JOIN split_history sh ON s.qbo_show_id = sh.show_qbo_id
            WHERE sh.vendor_qbo_id = %s
              AND (s.is_archived = FALSE OR s.is_archived IS NULL)
        """
        shows, _, error = self._execute_query(sql, (vendor_qbo_id,), fetch="all")
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                raise error
            return [], str(error)

        for i, show in enumerate(shows or []):
            shows[i] = _hydrate_show_record(show)
        return self._attach_hosts_to_shows(shows or []), None

    def filter_podcasts(self, filters: dict):
        query = "SELECT * FROM shows"
        where_clauses = []
        values = []
        for key, value in filters.items():
            if value is not None:
                if isinstance(value, bool): value = 1 if value else 0
                where_clauses.append(f"`{key}` = %s")
                values.append(value)
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        results, _, error = self._execute_query(query, tuple(values), fetch='all')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)): raise error
            return None, error
        for i, show in enumerate(results or []):
            results[i] = _hydrate_show_record(show)
        self._attach_hosts_to_shows(results or [])
        return results, None

    def delete_user(self, user_id: str):
        try:
            unassociate_sql = "DELETE FROM show_partners WHERE partner_id = %s"
            self._execute_query(unassociate_sql, (user_id,), is_transaction=True)
            delete_sql = "DELETE FROM users WHERE id = %s"
            _, rows_affected, error = self._execute_query(delete_sql, (user_id,), is_transaction=True)
            if error:
                if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)): raise error
                return False, str(error)
            if rows_affected == 0: return False, "User not found"
            return True, None
        except (DatabaseConnectionError, DatabaseCredentialsError):
            raise

    def create_podcast_(self, show_data):
        try:
            print('in create function')
            print(show_data)

            show_id = os.urandom(16).hex()
            show_dict = show_data.dict()
            show_dict['id'] = show_id
            show_dict.pop("annual_usd", None)

            show_dict["subnetwork_name"] = show_dict.pop("subnetwork_id")
            show_dict["rate_card"] = show_dict.pop("is_rate_card")

            annual_usd_data = {
                "2023": str(show_dict.pop("revenue_2023", None)),
                "2024": str(show_dict.pop("revenue_2024", None)),
                "2025": str(show_dict.pop("revenue_2025", None)),
            }

            if any(value is not None for value in annual_usd_data.values()):
                show_dict["annual_usd"] = json.dumps(annual_usd_data)

            print(show_dict)

            columns = ', '.join([f'`{k}`' for k in show_dict.keys()])
            placeholders = ', '.join(['%s'] * len(show_dict))
            sql = f"INSERT INTO shows ({columns}) VALUES ({placeholders})"
            values = tuple(show_dict.values())
            print(sql, values)

            _, _, error = self._execute_query(sql, values, is_transaction=True)
            if error:
                return None, error
            fetch_sql = "SELECT * FROM shows WHERE id = %s"
            new_show, _, fetch_error = self._execute_query(fetch_sql, (show_id,), fetch='one')
            print('new show',new_show)

            if 'annual_usd' in new_show and isinstance(new_show['annual_usd'], str):
                try:
                    new_show['annual_usd'] = json.loads(new_show['annual_usd'])
                except json.JSONDecodeError:
                    new_show['annual_usd'] = {}
            annual_usd = new_show.get('annual_usd', {})
            new_show['revenue_2023'] = annual_usd.get('2023', 0)
            new_show['revenue_2024'] = annual_usd.get('2024', 0)
            new_show['revenue_2025'] = annual_usd.get('2025', 0)
            if fetch_error:
                return None, fetch_error
            return new_show, None
        except Exception as e:
            print(e)

    def check_duplicate_show(self, title: str, exclude_id: str = None):
        """Check if a show with the given title already exists (case-insensitive)"""
        try:
            if exclude_id:
                sql = "SELECT * FROM shows WHERE LOWER(title) = LOWER(%s) AND id != %s"
                params = (title, exclude_id)
            else:
                sql = "SELECT * FROM shows WHERE LOWER(title) = LOWER(%s)"
                params = (title,)
            
            existing_show, _, error = self._execute_query(sql, params, fetch='one')
            if error:
                return None, error
            
            return existing_show, None
        except Exception as e:
            return None, str(e)

    def check_duplicate_show_with_archive_status(self, title: str, exclude_id: str = None):
        """Check if a show with the given title already exists, distinguishing between archived and active shows"""
        try:
            if exclude_id:
                sql = """
                SELECT *, 
                       CASE WHEN is_archived = TRUE THEN 'archived' ELSE 'active' END as status
                FROM shows 
                WHERE LOWER(title) = LOWER(%s) AND id != %s
                """
                params = (title, exclude_id)
            else:
                sql = """
                SELECT *, 
                       CASE WHEN is_archived = TRUE THEN 'archived' ELSE 'active' END as status
                FROM shows 
                WHERE LOWER(title) = LOWER(%s)
                """
                params = (title,)
            
            existing_show, _, error = self._execute_query(sql, params, fetch='one')
            if error:
                return None, error
            
            return existing_show, None
        except Exception as e:
            return None, str(e)

    def check_duplicate_shows_bulk(self, show_titles: list):
        """Check multiple show titles for duplicates in one query"""
        try:
            if not show_titles:
                return [], None
            
            # Create placeholders for the IN clause
            placeholders = ', '.join(['LOWER(%s)'] * len(show_titles))
            sql = f"SELECT * FROM shows WHERE LOWER(title) IN ({placeholders})"
            
            existing_shows, _, error = self._execute_query(sql, show_titles, fetch='all')
            if error:
                return None, error
            
            return existing_shows, None
        except Exception as e:
            return None, str(e)

    def create_podcast(self, show_data, user_name=None, user_id=None):
        try:
            print('in create function')
            print('show_data:', show_data)
            print('user_name:', user_name)
            print('user_id:', user_id)

            # Check for duplicate before creating
            existing_show, error = self.check_duplicate_show(show_data.title)
            if error:
                return None, error
            if existing_show:
                return None, f"Show with title '{show_data.title}' already exists"

            show_id = os.urandom(16).hex()
            show_dict = show_data.model_dump() if hasattr(show_data, "model_dump") else show_data.dict()
            show_dict['id'] = show_id
            hosts = show_dict.pop("hosts", None)
            show_dict.pop("annual_usd", None)

            show_dict["rate_card"] = show_dict.pop("is_rate_card")

            show_dict["first_episode_date"] = normalize_mysql_date(show_dict.get("first_episode_date"))
            
            # Set creation metadata
            show_dict["created_by"] = user_name or "System"
            show_dict["created_by_id"] = user_id or "system"
            show_dict["created_at"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # --- Normalize revenues ---
            def _norm(v):
                if v in (None, "None", "", "null", "NULL"):
                    return "0.0"
                try:
                    return f"{float(v):.1f}"
                except (TypeError, ValueError):
                    return "0.0"

            r2023 = _norm(show_dict.pop("revenue_2023", None))
            r2024 = _norm(show_dict.pop("revenue_2024", None))
            r2025 = _norm(show_dict.pop("revenue_2025", None))

            annual_usd_data = {"2023": r2023, "2024": r2024, "2025": r2025}
            show_dict["annual_usd"] = json.dumps(annual_usd_data)
            # --- end normalize ---

            if "cadence" in show_dict:
                show_dict["cadence"] = _enum_to_value(show_dict.get("cadence"))
            if "show_status" in show_dict:
                show_dict["show_status"] = _enum_to_value(show_dict.get("show_status"))
            if "age_demographic" in show_dict:
                show_dict["age_demographic"] = _enum_to_value(show_dict.get("age_demographic"))
            if "contract_links" in show_dict:
                show_dict["contract_links"] = _serialize_contract_links(show_dict.get("contract_links"))
            show_dict.pop("is_active", None)
            for legacy_key in ("show_host_contact", "primary_show_contact", "show_producer_contact"):
                show_dict.pop(legacy_key, None)
            for legacy_key in ("host_contact_name", "host_contact_address", "host_contact_phone", "host_contact_email"):
                show_dict.pop(legacy_key, None)

            print(show_dict)

            columns = ', '.join([f'`{k}`' for k in show_dict.keys()])
            placeholders = ', '.join(['%s'] * len(show_dict))
            sql = f"INSERT INTO shows ({columns}) VALUES ({placeholders})"
            values = tuple(show_dict.values())
            print(sql, values)

            _, _, error = self._execute_query(sql, values, is_transaction=True)
            if error:
                return None, error

            if hosts is not None:
                host_error = self._replace_show_hosts(show_id, hosts)
                if host_error:
                    return None, host_error

            fetch_sql = "SELECT * FROM shows WHERE id = %s"
            new_show, _, fetch_error = self._execute_query(fetch_sql, (show_id,), fetch='one')
            print('new show', new_show)

            new_show = _hydrate_show_record(new_show)
            self._attach_hosts_to_shows([new_show])

            if fetch_error:
                return None, fetch_error
            return new_show, None
        except Exception as e:
            print(e)

    def update_podcast(self, show_id: str, show_data: BaseModel):
        try:
            update_dict = show_data.model_dump(exclude_unset=True)
            if not update_dict: return None, "No update data provided"

            hosts = update_dict.pop("hosts", None)
            db_ready_dict = {}
            for model_key, db_col in COLUMN_MAPPING.items():
                if model_key in update_dict:
                    val = update_dict[model_key]
                    if model_key == "contract_links":
                        val = _serialize_contract_links(val)
                    elif model_key in ("cadence", "show_status", "age_demographic"):
                        val = _enum_to_value(val)
                    db_ready_dict[db_col] = val
            update_dict.pop("is_active", None)
            print("update_dict", update_dict)

            if "revenue_2023" in update_dict or "revenue_2024" in update_dict or "revenue_2025" in update_dict:
                existing_show, _ = self.get_podcast_by_id(show_id)
                annual_usd_data = {}
                if existing_show and isinstance(existing_show.get('annual_usd'), dict):
                    annual_usd_data = existing_show.get('annual_usd')

                def _norm(v):
                    if v in (None, "None", "", "null", "NULL"):
                        return "0.0"
                    try:
                        return f"{float(v):.1f}"
                    except (TypeError, ValueError):
                        return "0.0"

                r2023 = _norm(update_dict.pop("revenue_2023", None))
                r2024 = _norm(update_dict.pop("revenue_2024", None))
                r2025 = _norm(update_dict.pop("revenue_2025", None))

                annual_usd_data = {"2023": r2023, "2024": r2024, "2025": r2025}
                db_ready_dict["annual_usd"] = json.dumps(annual_usd_data)

            set_clause = ", ".join([f"`{key}` = %s" for key in db_ready_dict.keys()])
            sql_update = f"UPDATE shows SET {set_clause} WHERE id = %s"
            values = list(db_ready_dict.values()) + [show_id]

            _, rows_affected, error = self._execute_query(sql_update, tuple(values), is_transaction=True)
            if error: raise error

            if hosts is not None:
                host_error = self._replace_show_hosts(show_id, hosts)
                if host_error:
                    return None, host_error

            if rows_affected == 0:
                # No rows changed (values may be identical). Verify existence and return current row.
                existing, _ = self.get_podcast_by_id(show_id)
                if existing:
                    return existing, None
                return None, f"Podcast with id {show_id} not found"

            return self.get_podcast_by_id(show_id)
        except Exception as e:
            print(f"Error updating podcast: {e}")
            return None, str(e)

    def delete_podcast(self, show_id: str):
        try:
            sql = "DELETE FROM shows WHERE id = %s"
            _, rows_affected, error = self._execute_query(sql, (show_id,), is_transaction=True)
            if error: raise error
            if rows_affected == 0: return False, f"Podcast with id {show_id} not found"
            return True, None
        except (DatabaseConnectionError, DatabaseCredentialsError):
            raise

    def bulk_delete_podcasts(self, show_ids: list):
        """Bulk delete multiple shows by their IDs"""
        try:
            if not show_ids:
                return {"successful": 0, "failed": 0, "errors": []}
            
            # Create placeholders for the IN clause
            placeholders = ', '.join(['%s'] * len(show_ids))
            sql = f"DELETE FROM shows WHERE id IN ({placeholders})"
            
            # Execute the bulk delete
            _, rows_affected, error = self._execute_query(sql, tuple(show_ids), is_transaction=True)
            
            if error:
                return {
                    "successful": 0,
                    "failed": len(show_ids),
                    "errors": [str(error)]
                }
            
            return {
                "successful": rows_affected,
                "failed": len(show_ids) - rows_affected,
                "errors": []
            }
        except Exception as e:
            return {
                "successful": 0,
                "failed": len(show_ids),
                "errors": [str(e)]
            }

    def get_all_vendors(self):
        sql = "SELECT DISTINCT vendor_name, vendor_qbo_id FROM split_history WHERE vendor_name IS NOT NULL AND vendor_qbo_id IS NOT NULL"
        vendors, _, error = self._execute_query(sql, fetch='all')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)): raise error
            return []
        return vendors

    def get_vendor_name_by_qbo_id(self, vendor_qbo_id: int):
        sql = """
        SELECT vendor_name
        FROM split_history
        WHERE vendor_qbo_id = %s
          AND vendor_name IS NOT NULL
        LIMIT 1
        """
        vendor, _, error = self._execute_query(sql, (vendor_qbo_id,), fetch='one')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                raise error
            return None
        return vendor.get("vendor_name") if vendor else None

    def get_all_users(self):
        sql = "SELECT * FROM users"
        users, _, error = self._execute_query(sql, fetch='all')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)): raise error
        return users, error

    def get_user_by_email(self, email: str):
        sql = "SELECT * FROM users WHERE email = %s"
        user, _, error = self._execute_query(sql, (email,), fetch='one')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)): raise error
        # Parse settings JSON if it exists
        if user and user.get('settings'):
            if isinstance(user['settings'], str):
                try:
                    user['settings'] = json.loads(user['settings'])
                except json.JSONDecodeError:
                    user['settings'] = None
        return user, error

    def get_user_by_id(self, user_id: str):
        sql = "SELECT * FROM users WHERE id = %s"
        user, _, error = self._execute_query(sql, (user_id,), fetch='one')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)): raise error
        # Parse settings JSON if it exists
        if user and user.get('settings'):
            if isinstance(user['settings'], str):
                try:
                    user['settings'] = json.loads(user['settings'])
                except json.JSONDecodeError:
                    user['settings'] = None
        return user, error

    def create_user_login_activity(
        self,
        *,
        event_uuid: str,
        user_id: str = None,
        user_email: str,
        user_name: str = None,
        user_role: str = None,
        action: str,
        status: str = "SUCCESS",
        request_id: str = None,
        session_id: str = None,
        ip_hash: str = None,
        user_agent: str = None,
        failure_reason: str = None,
        metadata_json: dict = None,
    ):
        sql = """
        INSERT INTO user_login_activity
        (event_uuid, user_id, user_email, user_name, user_role, action, status, request_id, session_id, ip_hash, user_agent, failure_reason, metadata_json)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        metadata_value = json.dumps(metadata_json) if metadata_json is not None else None
        _, _, error = self._execute_query(
            sql,
            (event_uuid, user_id, user_email, user_name, user_role, action, status, request_id, session_id, ip_hash, user_agent, failure_reason, metadata_value),
            is_transaction=True,
        )
        if error:
            # Duplicate event UUID means retry/idempotent replay; treat as success.
            error_code = error.args[0] if getattr(error, "args", None) else None
            if error_code == 1062:
                return True, None
            return False, str(error)
        return True, None

    def get_user_login_activity(
        self,
        *,
        page_size: int = 25,
        cursor: str = None,
        action: str = None,
        status: str = None,
        user_email: str = None,
        from_utc: str = None,
        to_utc: str = None,
        query: str = None,
        include_total: bool = False,
    ):
        def _decode_cursor(raw: str):
            if not raw:
                return None, None
            try:
                padded = raw + "=" * (-len(raw) % 4)
                decoded = base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8")
                payload = json.loads(decoded)
                cur_ts = payload.get("occurred_at_utc")
                cur_id = int(payload.get("id"))
                if not cur_ts:
                    return None, "Invalid cursor payload"
                return {"occurred_at_utc": cur_ts, "id": cur_id}, None
            except Exception:
                return None, "Invalid cursor format"

        def _encode_cursor(row: dict):
            payload = {
                "occurred_at_utc": str(row.get("occurred_at_utc")),
                "id": int(row.get("id")),
            }
            raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")

        where_clauses = []
        params = []

        if action:
            where_clauses.append("user_login_activity.action = %s")
            params.append(action)
        if status:
            where_clauses.append("user_login_activity.status = %s")
            params.append(status)
        if user_email:
            where_clauses.append("user_login_activity.user_email = %s")
            params.append(user_email)
        if from_utc:
            where_clauses.append("user_login_activity.occurred_at_utc >= %s")
            params.append(from_utc)
        if to_utc:
            where_clauses.append("user_login_activity.occurred_at_utc <= %s")
            params.append(to_utc)
        if query:
            q = query.strip()
            if q:
                # Search user_email / user_name only (no request_id — avoids extra predicate work).
                prefix_q = f"{q}%"
                looks_like_email = "@" in q
                has_space = " " in q

                if looks_like_email:
                    where_clauses.append("user_login_activity.user_email LIKE %s")
                    params.append(prefix_q)
                elif q.isdigit():
                    # Numeric token search (e.g. "4000") should match suffix/infix in
                    # names/emails like "Perf User 4000" / "perf_user_4000@...".
                    contains_q = f"%{q}%"
                    where_clauses.append(
                        "(user_login_activity.user_email LIKE %s OR "
                        "user_login_activity.user_name LIKE %s)"
                    )
                    params.extend([contains_q, contains_q])
                elif has_space:
                    where_clauses.append("user_login_activity.user_name LIKE %s")
                    params.append(prefix_q)
                else:
                    where_clauses.append(
                        "(user_login_activity.user_email LIKE %s OR "
                        "user_login_activity.user_name LIKE %s)"
                    )
                    params.extend([prefix_q, prefix_q])

        decoded_cursor, cursor_error = _decode_cursor(cursor)
        if cursor_error:
            return [], None, False, None, cursor_error

        if decoded_cursor:
            where_clauses.append(
                "(user_login_activity.occurred_at_utc < %s OR "
                "(user_login_activity.occurred_at_utc = %s AND user_login_activity.id < %s))"
            )
            params.extend([decoded_cursor["occurred_at_utc"], decoded_cursor["occurred_at_utc"], decoded_cursor["id"]])

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        total = None
        if include_total:
            count_sql = f"SELECT COUNT(*) AS total FROM user_login_activity {where_sql}"
            count_row, _, count_error = self._execute_query(count_sql, tuple(params), fetch='one')
            if count_error:
                return [], None, False, None, str(count_error)
            total = int((count_row or {}).get("total") or 0)

        list_sql = f"""
        SELECT user_login_activity.id, user_login_activity.event_uuid, user_login_activity.occurred_at_utc,
               user_login_activity.user_id, user_login_activity.user_email, user_login_activity.user_name,
               user_login_activity.user_role, user_login_activity.action, user_login_activity.status,
               user_login_activity.request_id, user_login_activity.session_id, user_login_activity.ip_hash,
               user_login_activity.user_agent, user_login_activity.failure_reason, user_login_activity.metadata_json,
               NULL AS member_since_utc
        FROM user_login_activity
        {where_sql}
        ORDER BY user_login_activity.occurred_at_utc DESC, user_login_activity.id DESC
        LIMIT %s
        """
        list_params = tuple(params) + (page_size + 1,)
        rows, _, list_error = self._execute_query(list_sql, list_params, fetch='all')
        if list_error:
            return [], None, False, total, str(list_error)

        has_more = len(rows or []) > page_size
        sliced_rows = (rows or [])[:page_size]
        next_cursor = _encode_cursor(sliced_rows[-1]) if has_more and sliced_rows else None

        for row in sliced_rows:
            metadata = row.get("metadata_json")
            if isinstance(metadata, str):
                try:
                    row["metadata_json"] = json.loads(metadata)
                except json.JSONDecodeError:
                    row["metadata_json"] = None

        return sliced_rows, next_cursor, has_more, total, None

    def get_user_settings(self, user_id: str):
        """Get user settings by user ID"""
        sql = "SELECT settings FROM users WHERE id = %s"
        result, _, error = self._execute_query(sql, (user_id,), fetch='one')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)): raise error
            return None, str(error)
        if not result:
            return None, "User not found"
        
        settings = result.get('settings')
        if settings and isinstance(settings, str):
            try:
                settings = json.loads(settings)
            except json.JSONDecodeError:
                settings = None
        return settings, None

    def update_user_settings(self, user_id: str, settings: dict):
        """Update user settings (merges with existing JSON)."""
        try:
            if not user_id:
                return False, "User not found"

            existing, lookup_error = self.get_user_settings(user_id)
            if lookup_error:
                return False, lookup_error

            merged_settings = {**(existing or {}), **(settings or {})}
            # Deep-merge nested preference objects so partial client payloads
            # never wipe sibling keys (pagination, tableDensity, listViews).
            for nested_key in ("pagination", "tableDensity", "listViews"):
                incoming = (settings or {}).get(nested_key)
                previous = (existing or {}).get(nested_key)
                if isinstance(incoming, dict) or isinstance(previous, dict):
                    merged_settings[nested_key] = {
                        **(previous if isinstance(previous, dict) else {}),
                        **(incoming if isinstance(incoming, dict) else {}),
                    }
            settings_json = json.dumps(merged_settings)
            sql = "UPDATE users SET settings = %s WHERE id = %s"
            _, rows_affected, error = self._execute_query(sql, (settings_json, user_id), is_transaction=True)
            if error:
                return False, str(error)
            if rows_affected == 0:
                # MySQL can report 0 rows when values are unchanged; verify the user exists.
                user, user_error = self.get_user_by_id(user_id)
                if user_error or not user:
                    return False, "User not found"
            return True, None
        except Exception as e:
            return False, str(e)

    # ---------- NEW: update_user ----------
    def update_user(self, user_id: str, **kwargs):
        """
        Update a user row. Allowed fields: name, email, password_hash, role, mapped_vendor_qbo_id.
        Returns (ok: bool, error: Optional[str])
        """
        allowed = {"name", "email", "password_hash", "role", "mapped_vendor_qbo_id"}
        update_dict = {k: v for k, v in kwargs.items() if k in allowed}

        if not update_dict:
            return False, "No fields to update"

        set_clause = ", ".join([f"{col} = %s" for col in update_dict.keys()])
        sql = f"UPDATE users SET {set_clause} WHERE id = %s"
        params = tuple(update_dict.values()) + (user_id,)

        _, rows_affected, error = self._execute_query(sql, params, is_transaction=True)
        if error:
            return False, str(error)
        if rows_affected == 0:
            return False, "User not found"
        return True, None
    # --------- end update_user ----------

    def get_split_shows_for_user(self, user: dict):
        # Allow admin and internal to view the full list; partners get their own only
        if user.get('role') in ('admin', 'internal', 'internal_full_access', 'internal_show_access'):
            sql = "SELECT DISTINCT show_name, show_qbo_id FROM split_history WHERE show_name IS NOT NULL AND show_qbo_id IS NOT NULL"
            params = None
        elif user.get('role') == 'partner' and user.get('mapped_vendor_qbo_id'):
            sql = "SELECT DISTINCT show_name, show_qbo_id FROM split_history WHERE vendor_qbo_id = %s AND show_name IS NOT NULL AND show_qbo_id IS NOT NULL"
            params = (user['mapped_vendor_qbo_id'],)
        else:
            return [], None
        shows, _, error = self._execute_query(sql, params, fetch='all')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)): raise error
            return [], str(error)
        return shows, None

    def get_split_vendors_for_show(self, show_qbo_id: int):
        sql = "SELECT DISTINCT vendor_name, vendor_qbo_id FROM split_history WHERE show_qbo_id = %s AND vendor_name IS NOT NULL AND vendor_qbo_id IS NOT NULL"
        vendors, _, error = self._execute_query(sql, (show_qbo_id,), fetch='all')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)): raise error
            return [], str(error)
        return vendors, None

    def get_splits(self, show_qbo_id: int, vendor_qbo_id: int):
        sql = "SELECT * FROM split_history WHERE show_qbo_id = %s AND vendor_qbo_id = %s ORDER BY effective_date DESC"
        splits, _, error = self._execute_query(sql, (show_qbo_id, vendor_qbo_id), fetch='all')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)): raise error
            return [], str(error)
        return splits, None

    def create_split(self, split_data):
        insert_sql = """
        INSERT INTO split_history (show_qbo_id, vendor_qbo_id, show_name, vendor_name, partner_pct_ads, partner_pct_programmatic, effective_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            split_data.show_qbo_id, split_data.vendor_qbo_id, split_data.show_name,
            split_data.vendor_name, split_data.partner_pct_ads,
            split_data.partner_pct_programmatic, split_data.effective_date,
        )
        try:
            with get_db_connection() as db:
                with db.cursor() as cursor:
                    cursor.execute(insert_sql, params)
                    new_split_id = cursor.lastrowid
                    db.commit()
                    if new_split_id:
                        select_sql = "SELECT * FROM split_history WHERE split_id = %s"
                        cursor.execute(select_sql, (new_split_id,))
                        new_split = cursor.fetchone()
                        return new_split, None
                    else:
                        return None, "Failed to create new split record; could not get new ID."
        except (DatabaseConnectionError, DatabaseCredentialsError) as e:
            return None, str(e)
        except pymysql.Error as e:
            return None, str(e)
        except Exception as e:
            return None, str(e)

    # NEW: delete split by split_id
    def delete_split(self, split_id: int):
        sql = "DELETE FROM split_history WHERE split_id = %s"
        _, rows_affected, error = self._execute_query(sql, (split_id,), is_transaction=True)
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                raise error
            return False, str(error)
        if rows_affected == 0:
            return False, "Split not found"
        return True, None

    def get_all_split_history(self):
        """Get all split history records for admin view."""
        sql = "SELECT * FROM split_history ORDER BY split_id DESC"
        splits, _, error = self._execute_query(sql, (), fetch='all')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                raise error
            return None, str(error)
        return splits, None

    def update_split(self, split_id: int, split_data):
        """Update an existing split record."""
        sql = """
        UPDATE split_history 
        SET partner_pct_ads = %s, 
            partner_pct_programmatic = %s, 
            effective_date = %s
        WHERE split_id = %s
        """
        _, rows_affected, error = self._execute_query(
            sql, 
            (split_data.partner_pct_ads, split_data.partner_pct_programmatic, split_data.effective_date, split_id), 
            is_transaction=True
        )
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                raise error
            return None, str(error)
        if rows_affected == 0:
            return None, "Split not found"
        
        # Return the updated split
        select_sql = "SELECT * FROM split_history WHERE split_id = %s"
        updated_split, _, select_error = self._execute_query(select_sql, (split_id,), fetch='one')
        if select_error:
            return None, str(select_error)
        return updated_split, None

    def get_catalog_all_shows(self):
        """Return all shows from allclass for independent mapping dropdowns."""
        sql = """
            SELECT
                id AS show_qbo_id,
                name AS show_name
            FROM allclass
            WHERE name IS NOT NULL AND id IS NOT NULL
            ORDER BY name
        """
        shows, _, error = self._execute_query(sql, fetch='all')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                raise error
            return [], str(error)
        return shows, None

    def get_catalog_all_vendors(self):
        """Return all vendors from allvendors for independent mapping dropdowns."""
        sql = """
            SELECT
                id AS vendor_qbo_id,
                displayname AS vendor_name
            FROM allvendors
            WHERE displayname IS NOT NULL AND id IS NOT NULL
            ORDER BY displayname
        """
        vendors, _, error = self._execute_query(sql, fetch='all')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                raise error
            return [], str(error)
        return vendors, None

    def get_ledger(self, partner_id: str = None):
        if partner_id:
            sql = """
            SELECT 
                invoice_classref_name as show_name,
                customer_invoice as customer,
                invoice_date,
                invoice_description,
                invoice_amount,
                invoice_doc_number,
                evergreen_percentage,
                partner_percentage,
                evergreen_compensation,
                partner_compensation,
                tot_payment_amounts as effective_payment_received,
                outstanding_balance,
                partner_comp_waiting,
                evergreen_outstanding,
                partner_outstanding
            FROM revenue_ledger 
            WHERE vendor_qbo_id = %s
            """
            params = (partner_id,)
        else:
            sql = """
            SELECT 
                invoice_classref_name as show_name,
                customer_invoice as customer,
                invoice_date,
                invoice_description,
                invoice_amount,
                invoice_doc_number,
                evergreen_percentage,
                partner_percentage,
                evergreen_compensation,
                partner_compensation,
                tot_payment_amounts as effective_payment_received,
                outstanding_balance,
                partner_comp_waiting,
                evergreen_outstanding,
                partner_outstanding
            FROM revenue_ledger
            """
            params = None
        ledger, _, error = self._execute_query(sql, params, fetch='all')
        if error and isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
            raise error
        return ledger, error

    def get_partner_payouts(self, partner_id: str = None):
        if partner_id:
            sql = """
            SELECT
                lpp.docnumber as bill_number,
                lpp.txndate as bill_date,
                lpp.bill_description,
                lpp.bill_amount,
                lpp.txnids_Payment as payment_id,
                lpp.date_of_payment,
                lpp.effective_billed_amount_paid,
                lpp.billed_amount_outstanding,
                lpp.show_qbo_name as show_name,
                lpp.vendor_qbo_name
            FROM ledger_partnerpayouts lpp
            LEFT JOIN revenue_ledger rl
                ON rl.invoice_doc_number = lpp.docnumber
               AND rl.tot_payment_amounts = 0
               AND rl.partner_comp_waiting > 0
            WHERE lpp.vendor_qbo_id = %s
              AND rl.invoice_doc_number IS NULL
            """
            params = (partner_id,)
        else:
            sql = """
            SELECT
                lpp.docnumber as bill_number,
                lpp.txndate as bill_date,
                lpp.bill_description,
                lpp.bill_amount,
                lpp.txnids_Payment as payment_id,
                lpp.date_of_payment,
                lpp.effective_billed_amount_paid,
                lpp.billed_amount_outstanding,
                lpp.show_qbo_name as show_name,
                lpp.vendor_qbo_name
            FROM ledger_partnerpayouts lpp
            LEFT JOIN revenue_ledger rl
                ON rl.invoice_doc_number = lpp.docnumber
               AND rl.tot_payment_amounts = 0
               AND rl.partner_comp_waiting > 0
            WHERE rl.invoice_doc_number IS NULL
            """
            params = None

        ledger, _, error = self._execute_query(sql, params, fetch='all')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                raise error
            return None, f"SQL Error querying partner payouts: {str(error)}"
        return ledger or [], None


    # ===== NEW (additions for feedback feature) =====

    def create_feedback(self, feedback_data: FeedbackCreate, user_id: str):
        feedback_id = str(uuid.uuid4())
        created_time = datetime.now(timezone.utc)

        sql = """
        INSERT INTO feedback (id, title, type, description, created_by, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = (
            feedback_id,
            feedback_data.title,
            feedback_data.type.value,
            feedback_data.description,
            user_id,
            created_time
        )
        _, _, error = self._execute_query(sql, params, is_transaction=True)
        if error:
            return None, str(error)

        # Fetch the newly created feedback to return it
        new_feedback, err = self.get_feedback_by_id(feedback_id)
        return new_feedback, err

    def get_feedback_by_id(self, feedback_id: str):
        sql = """
        SELECT
            f.*,
            u.name as createdByName,
            cu.name as completedByName
        FROM feedback f
        LEFT JOIN users u ON f.created_by = u.id
        LEFT JOIN users cu ON f.completed_by = cu.id
        WHERE f.id = %s
        """
        feedback_row, _, error = self._execute_query(sql, (feedback_id,), fetch='one')
        if error:
            return None, str(error)
        return feedback_row, None

    def get_all_feedbacks(self, status_filter: str = None, *, summary: bool = True):
        if summary:
            base_sql = """
            SELECT
                f.id,
                f.title,
                f.type,
                f.status,
                f.created_by,
                f.created_at,
                f.completed_at,
                f.completed_by,
                f.updated_at,
                u.name as createdByName,
                cu.name as completedByName
            FROM feedback f
            LEFT JOIN users u ON f.created_by = u.id
            LEFT JOIN users cu ON f.completed_by = cu.id
            """
        else:
            base_sql = """
            SELECT
                f.*,
                u.name as createdByName,
                cu.name as completedByName
            FROM feedback f
            LEFT JOIN users u ON f.created_by = u.id
            LEFT JOIN users cu ON f.completed_by = cu.id
            """
        params = ()

        if status_filter:
            base_sql += " WHERE f.status = %s"
            params = (status_filter,)

        base_sql += """
        ORDER BY
            CASE WHEN f.status = 'Completed' THEN f.completed_at ELSE f.created_at END DESC,
            f.created_at DESC
        """

        feedbacks, _, error = self._execute_query(base_sql, params if params else None, fetch='all')
        if error:
            return [], str(error)
        return feedbacks, None

    def update_feedback_status(self, feedback_id: str, status: str, admin_user_id: str, resolution_note: str = None):
        existing, existing_error = self.get_feedback_by_id(feedback_id)
        if existing_error:
            return None, existing_error
        if not existing:
            return None, "Feedback not found"

        set_fields = ["status = %s", "updated_at = %s"]
        values = [status, datetime.now(timezone.utc)]

        # Completion metadata rules:
        # - Set completer+timestamp when moving to Completed
        # - Clear them when moving away from Completed (reopen flow)
        if status == "Completed":
            set_fields.extend(["completed_at = %s", "completed_by = %s"])
            values.extend([datetime.now(timezone.utc), admin_user_id])
        elif existing.get("status") == "Completed":
            set_fields.extend(["completed_at = NULL", "completed_by = NULL"])

        if resolution_note is not None:
            set_fields.append("resolution_note = %s")
            values.append(resolution_note)

        sql = f"UPDATE feedback SET {', '.join(set_fields)} WHERE id = %s"
        values.append(feedback_id)

        _, rows_affected, error = self._execute_query(sql, tuple(values), is_transaction=True)
        if error:
            return None, str(error)
        if rows_affected == 0:
            return None, "Feedback not found"

        return self.get_feedback_by_id(feedback_id)

    def update_feedback_resolution(self, feedback_id: str, resolution_note: str = None):
        existing, existing_error = self.get_feedback_by_id(feedback_id)
        if existing_error:
            return None, existing_error
        if not existing:
            return None, "Feedback not found"

        sql = """
        UPDATE feedback
        SET resolution_note = %s,
            updated_at = %s
        WHERE id = %s
        """
        params = (resolution_note, datetime.now(timezone.utc), feedback_id)
        _, rows_affected, error = self._execute_query(sql, params, is_transaction=True)
        if error:
            return None, str(error)
        if rows_affected == 0:
            return None, "Feedback not found"

        return self.get_feedback_by_id(feedback_id)

    def delete_feedback(self, feedback_id: str):
        sql = "DELETE FROM feedback WHERE id = %s"
        _, rows_affected, error = self._execute_query(sql, (feedback_id,), is_transaction=True)
        if error:
            return False, str(error)
        if rows_affected == 0:
            return False, "Feedback not found"
        return True, None

    def get_allclass_items(self):
        sql = "SELECT id, name FROM allclass ORDER BY name ASC"
        results, _, error = self._execute_query(sql, fetch="all")
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                raise error
            return None, error
        return results or [], None

    # Archive methods
    def archive_podcast(self, show_id: str, user_name: str, user_id: str):
        """Archive a podcast"""
        try:
            sql = """
            UPDATE shows 
            SET is_archived = TRUE, 
                archived_at = NOW(), 
                archived_by = %s,
                archived_by_id = %s
            WHERE id = %s
            """
            _, rows_affected, error = self._execute_query(sql, (user_name, user_id, show_id), is_transaction=True)
            if error:
                return None, str(error)
            if rows_affected == 0:
                return None, "Show not found"
            
            # Return updated show
            return self.get_podcast_by_id(show_id)
        except Exception as e:
            return None, str(e)

    def unarchive_podcast(self, show_id: str, user_name: str, user_id: str):
        """Unarchive a podcast"""
        try:
            sql = """
            UPDATE shows 
            SET is_archived = FALSE, 
                archived_at = NULL, 
                archived_by = NULL,
                archived_by_id = NULL
            WHERE id = %s
            """
            _, rows_affected, error = self._execute_query(sql, (show_id,), is_transaction=True)
            if error:
                return None, str(error)
            if rows_affected == 0:
                return None, "Show not found"
            
            # Return updated show
            return self.get_podcast_by_id(show_id)
        except Exception as e:
            return None, str(e)

    def get_archived_podcasts(self):
        """Get all archived podcasts"""
        try:
            sql = "SELECT * FROM shows WHERE is_archived = TRUE ORDER BY archived_at DESC"
            shows, _, error = self._execute_query(sql, fetch='all')
            if error:
                if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                    raise error
                return None, str(error)

            for i, show in enumerate(shows or []):
                shows[i] = _hydrate_show_record(show)

            return self._attach_hosts_to_shows(shows or []), None
        except Exception as e:
            return None, str(e)

    def bulk_archive_podcasts(self, show_ids: list, user_name: str, user_id: str):
        """Bulk archive multiple podcasts"""
        try:
            if not show_ids:
                return {"successful": 0, "failed": 0, "message": "No shows to archive"}, None
            
            placeholders = ', '.join(['%s'] * len(show_ids))
            sql = f"""
            UPDATE shows 
            SET is_archived = TRUE, 
                archived_at = NOW(), 
                archived_by = %s,
                archived_by_id = %s
            WHERE id IN ({placeholders})
            """
            params = [user_name, user_id] + show_ids
            _, rows_affected, error = self._execute_query(sql, tuple(params), is_transaction=True)
            
            if error:
                return None, str(error)
            
            return {
                "successful": rows_affected,
                "failed": len(show_ids) - rows_affected,
                "message": f"Successfully archived {rows_affected} shows"
            }, None
        except Exception as e:
            return None, str(e)

    def bulk_unarchive_podcasts(self, show_ids: list, user_name: str, user_id: str):
        """Bulk unarchive multiple podcasts"""
        try:
            if not show_ids:
                return {"successful": 0, "failed": 0, "message": "No shows to unarchive"}, None
            
            placeholders = ', '.join(['%s'] * len(show_ids))
            sql = f"""
            UPDATE shows 
            SET is_archived = FALSE, 
                archived_at = NULL, 
                archived_by = NULL,
                archived_by_id = NULL
            WHERE id IN ({placeholders})
            """
            _, rows_affected, error = self._execute_query(sql, tuple(show_ids), is_transaction=True)
            
            if error:
                return None, str(error)
            
            return {
                "successful": rows_affected,
                "failed": len(show_ids) - rows_affected,
                "message": f"Successfully unarchived {rows_affected} shows"
            }, None
        except Exception as e:
            return None, str(e)
