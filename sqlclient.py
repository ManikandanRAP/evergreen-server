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
from utils.date_normalizer import *
from models import FeedbackCreate # Import FeedbackCreate
import uuid
from datetime import datetime, timezone


# This dictionary provides a definitive, complete mapping from the Python
# model's field names (snake_case) to the actual database column names.
# This resolves all naming inconsistencies in one place.
COLUMN_MAPPING = {
    # Basic Info
    "title": "title",
    "show_type": "show_type",
    "media_type": "media_type",
    "relationship_level": "relationship_level",
    "start_date": "start_date",
    "subnetwork_id": "subnetwork_id",
    "is_tentpole": "tentpole",
    "is_original": "is_original",
    "genre_name": "genre_name",

    # Financial
    "minimum_guarantee": "minimum_guarantee",
    "evergreen_ownership_pct": "evergreen_ownership_pct",
    "latest_cpm_usd": "latest_cpm_usd",
    "has_sponsorship_revenue": "has_sponsorship_revenue",
    "has_non_evergreen_revenue": "has_non_evergreen_revenue",
    "requires_partner_access": "requires_partner_access",
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
    "shows_per_year": "shows_per_year",
    "ad_slots": "ad_slots",
    "avg_show_length_mins": "avg_show_length_mins",
    "show_host_contact": "show_host_contact",
    "show_primary_contact": "show_primary_contact",
    "evergreen_production_staff_name": "evergreen_production_staff_name",

    # Demographics
    "age_demographic": "age_demographic",
    "gender": "gender",
    "region": "region",
    "primary_education": "primary_education",
    "secondary_education": "secondary_education",
    "is_active": "is_active",
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
def get_db_connection():
    connection = None
    try:
        connection = pymysql.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor, port=DB_PORT, connect_timeout=5
        )
        yield connection
    except pymysql.err.OperationalError as e:
        error_code = e.args[0]
        if error_code == 1045: raise DatabaseCredentialsError(f"Database credentials invalid: {str(e)}")
        elif error_code == 2003: raise DatabaseConnectionError(f"Cannot connect to database server at {DB_HOST}:{DB_PORT}.")
        elif error_code == 1049: raise DatabaseConnectionError(f"Database '{DB_NAME}' does not exist")
        else: raise DatabaseConnectionError(f"Database connection failed: {str(e)}")
    except Exception as e:
        raise DatabaseConnectionError(f"Unexpected database error: {str(e)}")
    finally:
        if connection:
            connection.close()

class SqlClient:
    def __init__(self):
        self.verify_connection()

    def verify_connection(self):
        success, error = test_database_connection()
        if not success:
            raise DatabaseConnectionError(f"Failed to initialize database client: {error}")
        print("Database connection verified successfully")

    def _execute_query(self, query: str, params: tuple = None, fetch: str = None, is_transaction=False):
        try:
            with get_db_connection() as db:
                with db.cursor() as cursor:
                    rows_affected = cursor.execute(query, params)
                    if fetch == 'one': result = cursor.fetchone()
                    elif fetch == 'all': result = cursor.fetchall()
                    else: result = None
                    if is_transaction: db.commit()
                    return result, rows_affected, None
        except (DatabaseConnectionError, DatabaseCredentialsError) as e:
            return None, 0, e
        except pymysql.Error as e:
            return None, 0, e
        except Exception as e:
            return None, 0, e

    def get_all_podcasts(self):
        sql = "SELECT * FROM shows"
        shows, _, error = self._execute_query(sql, fetch='all')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)): raise error
            return []

        for show in shows:
            annual_usd_raw = show.get('annual_usd')
            if isinstance(annual_usd_raw, str):
                try: annual_usd = json.loads(annual_usd_raw)
                except json.JSONDecodeError: annual_usd = {}
            else:
                annual_usd = annual_usd_raw if isinstance(annual_usd_raw, dict) else {}
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
        if not show:
            return None, None
        # Normalize annual_usd like in get_all_podcasts
        annual_usd_raw = show.get('annual_usd')
        if isinstance(annual_usd_raw, str):
            try:
                annual_usd = json.loads(annual_usd_raw)
            except json.JSONDecodeError:
                annual_usd = {}
        else:
            annual_usd = annual_usd_raw if isinstance(annual_usd_raw, dict) else {}
        show['annual_usd'] = annual_usd
        show['revenue_2023'] = annual_usd.get('2023', 0)
        show['revenue_2024'] = annual_usd.get('2024', 0)
        show['revenue_2025'] = annual_usd.get('2025', 0)
        return show, None

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
            show_dict["tentpole"] = show_dict.pop("is_tentpole")

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

    def create_podcast(self, show_data):
        try:
            print('in create function')
            print(show_data)

            # Check for duplicate before creating
            existing_show, error = self.check_duplicate_show(show_data.title)
            if error:
                return None, error
            if existing_show:
                return None, f"Show with title '{show_data.title}' already exists"

            show_id = os.urandom(16).hex()
            show_dict = show_data.dict()
            show_dict['id'] = show_id
            show_dict.pop("annual_usd", None)

            show_dict["tentpole"] = show_dict.pop("is_tentpole")

            show_dict["start_date"] = normalize_mysql_date(show_dict["start_date"])

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
            print('new show', new_show)

            if 'annual_usd' in new_show and isinstance(new_show['annual_usd'], str):
                try:
                    new_show['annual_usd'] = json.loads(new_show['annual_usd'])
                except json.JSONDecodeError:
                    new_show['annual_usd'] = {}

            annual_usd = new_show.get('annual_usd', {})
            # keep as strings to match storage ("0.0")
            new_show['revenue_2023'] = annual_usd.get('2023', "0.0")
            new_show['revenue_2024'] = annual_usd.get('2024', "0.0")
            new_show['revenue_2025'] = annual_usd.get('2025', "0.0")

            if fetch_error:
                return None, fetch_error
            return new_show, None
        except Exception as e:
            print(e)

    def update_podcast(self, show_id: str, show_data: BaseModel):
        try:
            update_dict = show_data.model_dump(exclude_unset=True)
            if not update_dict: return None, "No update data provided"

            db_ready_dict = {}
            for model_key, db_col in COLUMN_MAPPING.items():
                if model_key in update_dict:
                    db_ready_dict[db_col] = update_dict[model_key]
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
        return user, error

    def get_user_by_id(self, user_id: str):
        sql = "SELECT * FROM users WHERE id = %s"
        user, _, error = self._execute_query(sql, (user_id,), fetch='one')
        if error:
            if isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)): raise error
        return user, error

    # ---------- NEW: update_user ----------
    def update_user(self, user_id: str, **kwargs):
        """
        Update a user row. Allowed fields: name, email, password_hash, mapped_vendor_qbo_id.
        Returns (ok: bool, error: Optional[str])
        """
        allowed = {"name", "email", "password_hash", "mapped_vendor_qbo_id"}
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
        if user.get('role') in ('admin', 'internal'):
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
        INSERT INTO split_history (show_qbo_id, vendor_qbo_id, show_name, vendor_name, evergreen_pct_ads, evergreen_pct_programmatic, effective_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            split_data.show_qbo_id, split_data.vendor_qbo_id, split_data.show_name,
            split_data.vendor_name, split_data.evergreen_pct_ads,
            split_data.evergreen_pct_programmatic, split_data.effective_date,
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
            sql = "SELECT * FROM revenue_ledger WHERE vendor_qbo_id = %s"
            params = (partner_id,)
        else:
            sql = "SELECT * FROM revenue_ledger"
            params = None
        ledger, _, error = self._execute_query(sql, params, fetch='all')
        if error and isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
            raise error
        return ledger, error

    def get_partner_payouts(self, partner_id: str = None):
        if partner_id:
            sql = "SELECT * FROM ledger_partnerpayouts WHERE vendor_qbo_id = %s"
            params = (partner_id,)
        else:
            sql = "SELECT * FROM ledger_partnerpayouts"
            params = None

        ledger, _, error = self._execute_query(sql, params, fetch='all')

        if error and isinstance(error, (DatabaseConnectionError, DatabaseCredentialsError)):
            raise error
        return ledger, error

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
        SELECT f.*, u.name as createdByName
        FROM feedback f
        JOIN users u ON f.created_by = u.id
        WHERE f.id = %s
        """
        feedback_row, _, error = self._execute_query(sql, (feedback_id,), fetch='one')
        if error:
            return None, str(error)
        return feedback_row, None

    def get_all_feedbacks(self):
        sql = """
        SELECT f.*, u.name as createdByName
        FROM feedback f
        JOIN users u ON f.created_by = u.id
        ORDER BY f.created_at DESC
        """
        feedbacks, _, error = self._execute_query(sql, fetch='all')
        if error:
            return [], str(error)
        return feedbacks, None

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
