import pymysql
import json
import os
from auth import get_password_hash
from contextlib import contextmanager
from pydantic import BaseModel
from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT

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
    """
    Test database connection with current credentials.
    Returns tuple (success: bool, error_message: str)
    """
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor,
            port=DB_PORT,
            connect_timeout=5  # Add timeout for connection attempts
        )
        
        # Test with a simple query
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        
        connection.close()
        return True, None
        
    except pymysql.err.OperationalError as e:
        error_code = e.args[0]
        if error_code == 1045:  # Access denied
            return False, f"Database credentials invalid: {str(e)}"
        elif error_code == 2003:  # Can't connect to server
            return False, f"Cannot connect to database server at {DB_HOST}: {DB_PORT}. Server may be down or unreachable."
        elif error_code == 1049:  # Unknown database
            return False, f"Database '{DB_NAME}' does not exist on the server."
        else:
            return False, f"Database connection failed: {str(e)}"
    except Exception as e:
        return False, f"Unexpected database error: {str(e)}"

@contextmanager
def get_db_connection():
    """
    Provides a database connection using a context manager with proper error handling.
    """
    connection = None
    try:
        print("Connecting to database...")
        print(f"DB_HOST: {DB_HOST}")
        print(f"DB_USER: {DB_USER}")
        print(f"DB_PASSWORD: {DB_PASSWORD}")
        print(f"DB_NAME: {DB_NAME}")
        print(f"DB_PORT: {DB_PORT}")
        connection = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor,
            port=DB_PORT,
            connect_timeout=5
        )
        yield connection
        
    except pymysql.err.OperationalError as e:
        error_code = e.args[0]
        if error_code == 1045:  # Access denied
            raise DatabaseCredentialsError(f"Database credentials invalid: {str(e)}")
        elif error_code == 2003:  # Can't connect to server
            raise DatabaseConnectionError(f"Cannot connect to database server at {DB_HOST}:{DB_PORT}. Server may be down or unreachable.")
        elif error_code == 1049:  # Unknown database
            raise DatabaseConnectionError(f"Database '{DB_NAME}' does not exist")
        else:
            raise DatabaseConnectionError(f"Database connection failed: {str(e)}")
    except Exception as e:
        raise DatabaseConnectionError(f"Unexpected database error: {str(e)}")
    finally:
        if connection:
            connection.close()

class SqlClient:
    def __init__(self):
        """Initialize the SQL client and verify database connection"""
        self.verify_connection()
    
    def verify_connection(self):
        """Verify database connection on initialization"""
        success, error = test_database_connection()
        if not success:
            raise DatabaseConnectionError(f"Failed to initialize database client: {error}")
        print("Database connection verified successfully")
    
    def _execute_query(self, query: str, params: tuple = None, fetch: str = None, is_transaction=False):
        """Common function to execute SQL queries with improved error handling."""
        try:
            with get_db_connection() as db:
                with db.cursor() as cursor:
                    rows_affected = cursor.execute(query, params)
                    if fetch == 'one':
                        result = cursor.fetchone()
                    elif fetch == 'all':
                        result = cursor.fetchall()
                    else:
                        result = None
                    
                    if is_transaction:
                        db.commit()
                    
                    return result, rows_affected, None
                    
        except (DatabaseConnectionError, DatabaseCredentialsError) as e:
            print(f"Database connection error: {e}")
            return None, 0, e
        except pymysql.Error as e:
            print(f"Database query error: {e}")
            return None, 0, e
        except Exception as e:
            print(f"Unexpected error during query execution: {e}")
            return None, 0, e

    def get_all_podcasts(self):
        sql = "SELECT * FROM shows"
        shows, _, error = self._execute_query(sql, fetch='all')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                raise error
            return []
        
        for show in shows:
            annual_usd_raw = show.get('annual_usd')
            
            if isinstance(annual_usd_raw, str):
                try:
                    annual_usd = json.loads(annual_usd_raw)
                except json.JSONDecodeError:
                    annual_usd = {}
            elif isinstance(annual_usd_raw, dict):
                annual_usd = annual_usd_raw
            else:
                annual_usd = {}

            show['annual_usd'] = annual_usd
            show['revenue_2023'] = annual_usd.get('2023', 0)
            show['revenue_2024'] = annual_usd.get('2024', 0)
            show['revenue_2025'] = annual_usd.get('2025', 0)

        return shows


    def get_podcast_by_id(self, show_id: str):
        sql = "SELECT * FROM shows WHERE id = %s"
        show, _, error = self._execute_query(sql, (show_id,), fetch='one')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                raise error
            return None, str(error)
        return show, None

    def filter_podcasts(self, filters: dict):
        query = "SELECT * FROM shows"
        where_clauses = []
        values = []

        for key, value in filters.items():
            if value is not None:
                if isinstance(value, bool):
                    value = 1 if value else 0
                where_clauses.append(f"`{key}` = %s")
                values.append(value)

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        results, _, error = self._execute_query(query, tuple(values), fetch='all')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                raise error
            return None, error
        return results, None

    def delete_user(self, user_id: str):
        try:
            # First, delete any associations
            unassociate_sql = "DELETE FROM show_partners WHERE partner_id = %s"
            self._execute_query(unassociate_sql, (user_id,), is_transaction=True)

            # Then, delete the user
            delete_sql = "DELETE FROM users WHERE id = %s"
            _, rows_affected, error = self._execute_query(delete_sql, (user_id,), is_transaction=True)
            if error:
                if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                    raise error  
                return False, str(error)
            if rows_affected == 0:
                return False, "User not found"
            return True, None
        except (DatabaseConnectionError, DatabaseCredentialsError):
            raise

    def create_podcast(self, show_data):
        try:
            # Check for duplicate title before proceeding
            check_sql = "SELECT id FROM shows WHERE title = %s"
            existing_show, _, check_error = self._execute_query(check_sql, (show_data.title,), fetch='one')

            if check_error:
                if isinstance(check_error, (DatabaseConnectionError, DatabaseCredentialsError)):
                    raise check_error
                return None, f"Database error while checking for existing show: {str(check_error)}"

            if existing_show:
                return None, f"A show with the title '{show_data.title}' already exists."

            show_id = os.urandom(16).hex()
            show_dict = show_data.dict(by_alias=True)
            show_dict['id'] = show_id
            show_dict.pop("annual_usd", None)
            
            # Field transformations
            show_dict["region"] = show_dict.pop("region")
            show_dict["primary_education"] = show_dict.pop("primary_education")
            show_dict["secondary_education"] = show_dict.pop("secondary_education")
            show_dict["isUndersized"] = show_dict.pop("isUndersized")
            show_dict["isActive"] = show_dict.pop("isActive")
            show_dict["evergreen_production_staff_name"] = show_dict.pop("evergreen_production_staff_name")
            show_dict["genre_name"] = show_dict.pop("genre_name")
            show_dict["qbo_show_name"] = show_dict.pop("show_name_in_qbo")
            
            annual_usd_data = {
                "2023": str(show_dict.pop("revenue_2023", 0) or 0),
                "2024": str(show_dict.pop("revenue_2024", 0) or 0),
                "2025": str(show_dict.pop("revenue_2025", 0) or 0),
            }
            show_dict["annual_usd"] = json.dumps(annual_usd_data)

            columns = ', '.join([f'`{k}`' for k in show_dict.keys()])
            placeholders = ', '.join(['%s'] * len(show_dict))
            sql = f"INSERT INTO shows ({columns}) VALUES ({placeholders})"
            values = tuple(show_dict.values())

            _, _, error = self._execute_query(sql, values, is_transaction=True)
            if error:
                if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                    raise error
                return None, error            
            
            # Fetch the created show
            fetch_sql = "SELECT * FROM shows WHERE id = %s"
            new_show, _, fetch_error = self._execute_query(fetch_sql, (show_id,), fetch='one')
            
            
            if fetch_error:
                if isinstance(fetch_error, (DatabaseConnectionError, DatabaseCredentialsError)):
                    raise fetch_error
                return None, fetch_error
            
            # Process annual_usd field
            if 'annual_usd' in new_show and isinstance(new_show['annual_usd'], str):
                try:
                    new_show['annual_usd'] = json.loads(new_show['annual_usd'])
                except json.JSONDecodeError:
                    new_show['annual_usd'] = {}

            annual_usd = new_show.get('annual_usd') or {}
            if not isinstance(annual_usd, dict):
                annual_usd = {}

            new_show['revenue_2023'] = annual_usd.get('2023', 0)
            new_show['revenue_2024'] = annual_usd.get('2024', 0)
            new_show['revenue_2025'] = annual_usd.get('2025', 0)

            return new_show, None

            
        except (DatabaseConnectionError, DatabaseCredentialsError):
            raise
        except Exception as e:
            print(f"Error creating podcast: {e}")
            return None, str(e)

    def update_podcast(self, show_id: str, show_data: BaseModel):
        try:
            print(show_data)
            if not show_data.model_fields_set:
                return None, "No update data provided"

            show_dict = show_data.model_dump(exclude_unset=True)
            show_dict.pop("annual_usd", None)

            # Field transformations
            if "genre_name" in show_dict:
                show_dict["genre_name"] = show_dict.pop("genre_name")
            if "show_name_in_qbo" in show_dict:
                show_dict["qbo_show_name"] = show_dict.pop("show_name_in_qbo")

            # Handle revenue fields
            annual_usd_data = {}
            annual_usd_data = {}
            for year, field in [("2023", "revenue_2023"), ("2024", "revenue_2024"), ("2025", "revenue_2025")]:
                annual_usd_data[year] = str(show_dict.pop(field, 0) or 0)

            show_dict["annual_usd"] = json.dumps(annual_usd_data)

            # Build and execute update query
            set_clause = ", ".join([f"{key} = %s" for key in show_dict.keys()])
            sql_update = f"UPDATE shows SET {set_clause} WHERE id = %s"
            values = list(show_dict.values()) + [show_id]

            _, rows_affected, error = self._execute_query(sql_update, tuple(values), is_transaction=True)
            if error:
                if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                    raise error
                return None, str(error)
            if rows_affected == 0:
                return None, f"Podcast with id {show_id} not found"

            # Fetch updated show
            sql_select = "SELECT * FROM shows WHERE id = %s"
            updated_show, _, error = self._execute_query(sql_select, (show_id,), fetch='one')
            if error:
                if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                    raise error
                return None, str(error)

            # Process annual_usd field
            show = updated_show
            if 'annual_usd' in show and isinstance(show['annual_usd'], str):
                try:
                    show['annual_usd'] = json.loads(show['annual_usd'])
                except json.JSONDecodeError:
                    show['annual_usd'] = {}
            annual_usd = show.get('annual_usd', {})
            show['revenue_2023'] = annual_usd.get('2023', 0)
            show['revenue_2024'] = annual_usd.get('2024', 0)
            show['revenue_2025'] = annual_usd.get('2025', 0)

            return updated_show, None
            
        except (DatabaseConnectionError, DatabaseCredentialsError):
            raise
        except Exception as e:
            print(f"Error updating podcast: {e}")
            return None, str(e)

    def delete_podcast(self, show_id: str):
        try:
            sql = "DELETE FROM shows WHERE id = %s"
            _, rows_affected, error = self._execute_query(sql, (show_id,), is_transaction=True)
            if error:
                if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                    raise error
                return False, str(error)
            if rows_affected == 0:
                return False, f"Podcast with id {show_id} not found"
            return True, None
        except (DatabaseConnectionError, DatabaseCredentialsError):
            raise

    # Add other methods with similar error handling...
    def get_all_users(self):
        sql = "SELECT * FROM users"
        users, _, error = self._execute_query(sql, fetch='all')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                raise error
        return users, error

    def get_user_by_email(self, email: str):
        sql = "SELECT * FROM users WHERE email = %s"
        user, _, error = self._execute_query(sql, (email,), fetch='one')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                raise error
        return user, error

    def get_user_by_id(self, user_id: str):
        sql = "SELECT * FROM users WHERE id = %s"
        user, _, error = self._execute_query(sql, (user_id,), fetch='one')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
                raise error
        return user, error