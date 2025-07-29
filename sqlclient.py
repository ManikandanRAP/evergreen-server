import pymysql
import os
import json
from auth import get_password_hash
from contextlib import contextmanager
from pydantic import BaseModel
from fastapi import Request
from fastapi.exceptions import RequestValidationError

# --- Configuration ---
DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_USER = os.environ.get("DB_USER", "root")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "rootpassword")
DB_NAME = os.environ.get("DB_NAME", "evergreen")

async def validation_exception_handler(request: Request, exc: RequestValidationError):
    print("Validation error:", exc.errors())
    return JSONResponse(status_code=422, content={"detail": exc.errors()})

@contextmanager
def get_db_connection():
    connection = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )
    try:
        yield connection
    finally:
        connection.close()

class SqlClient:
    def _execute_query(self, query: str, params: tuple = None, fetch: str = None, is_transaction=False):
        """Common function to execute SQL queries."""
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
        except pymysql.Error as e:
            # In a real app, you'd want to log this error.
            return None, 0, e
    def _execute_query_(self, query: str, params: tuple = None, fetch: str = None, is_transaction=False):
        """Common function to execute SQL queries."""
        try:
            with get_db_connection() as db:
                with db.cursor() as cursor:
                    rows_affected = cursor.execute(query, params)
                    
                    # Save column names before fetching results
                    columns = [desc[0] for desc in cursor.description] if cursor.description else []
                    
                    if fetch == 'one':
                        rows = cursor.fetchone()
                    elif fetch == 'all':
                        rows = cursor.fetchall()
                    else:
                        rows = None

                    if is_transaction:
                        db.commit()

                    return rows, columns, None
        except pymysql.Error as e:
            return None, [], e


    def get_all_podcasts_(self):
        sql = "SELECT * FROM shows"
        shows, _, error = self._execute_query(sql, fetch='all')
        if error:
            return []
        return shows
    import json

    def get_all_podcasts(self):
        sql = "SELECT * FROM shows"
        shows, _, error = self._execute_query(sql, fetch='all')
        if error:
            return []
        for show in shows:
            print("Type: ", type(show['annual_usd']))

            # if 'annual_usd' in show and isinstance(show['annual_usd'], str):
            #     try:
            #         show['annual_usd'] = {
            #             k: float(v) if isinstance(v, (int, float, str)) and v not in (None, "") else 0.0
            #             for k, v in json.loads(show['annual_usd']).items()
            #         }
            #     except (json.JSONDecodeError, ValueError, TypeError):
            #         show['annual_usd'] = {}
            # print(show['annual_usd'],type(show['annual_usd']))
            if 'annual_usd' in show and isinstance(show['annual_usd'], str):
                try:
                    show['annual_usd'] = json.loads(show['annual_usd'])
                except json.JSONDecodeError:
                    show['annual_usd'] = {}
                annual_usd = show.get('annual_usd', {})
                show['revenue_2023'] = annual_usd.get('2023', 0)
                show['revenue_2024'] = annual_usd.get('2024', 0)
                show['revenue_2025'] = annual_usd.get('2025', 0)
            if 'genre_name' in show and isinstance(show['genre_name'], str):
                print("Type: ", type(show['genre_name']))
                # try:
                #     show['genre_name'] = json.loads(show['genre_name'])
                # except json.JSONDecodeError:
                #     show['genre_name'] = {} 
                            
            

        return shows
    def get_all_podcasts_(self):
        sql = "SELECT * FROM shows"
        rows, columns, error = self._execute_query(sql, fetch='all')

        if error or not rows:
            return []

        shows = [dict(zip(columns, row)) for row in rows]

        for show in shows:
            if 'annual_usd' in show and isinstance(show['annual_usd'], str):
                try:
                    show['annual_usd'] = json.loads(show['annual_usd'])
                except json.JSONDecodeError:
                    show['annual_usd'] = {}

        return shows






    def get_podcast_by_id(self, show_id: str):
        sql = "SELECT * FROM shows WHERE id = %s"
        show, _, error = self._execute_query(sql, (show_id,), fetch='one')
        if error:
            return None, str(error)
        return show, None

    def filter_podcasts(self, filters: dict):
        query = "SELECT * FROM shows"
        where_clauses = []
        values = []

        for key, value in filters.items():
            if value is not None:
                # For boolean values, SQL expects 1 or 0
                if isinstance(value, bool):
                    value = 1 if value else 0
                where_clauses.append(f"`{key}` = %s")
                values.append(value)

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        results, _, error = self._execute_query(query, tuple(values), fetch='all')
        if error:
            return None, error
        return results, None

    def delete_user(self, user_id: str):
        # First, delete any associations this user has with shows to maintain referential integrity.
        unassociate_sql = "DELETE FROM show_partners WHERE partner_id = %s"
        self._execute_query(unassociate_sql, (user_id,), is_transaction=True)
        # We don't check for errors here, as the user may not have any associations.

        # Then, delete the user.
        delete_sql = "DELETE FROM users WHERE id = %s"
        rows_affected, _, error = self._execute_query(delete_sql, (user_id,), is_transaction=True)
        if error:
            return False, str(error)
        if rows_affected == 0:
            return False, "User not found"
        return True, None

    def unassociate_partner_from_show(self, show_id: str, partner_id: str):
        sql = "DELETE FROM show_partners WHERE show_id = %s AND partner_id = %s"
        rows_affected, _, error = self._execute_query(sql, (show_id, partner_id), is_transaction=True)
        if error:
            return False, str(error)
        if rows_affected == 0:
            return False, "Association not found"
        return True, None

    def update_podcast_(self, show_id: str, show_data: BaseModel):
        # Pydantic's exclude_unset=True can be tricky. A more robust check is needed.
        # We check if any of the fields in the model have been set by the client.
        print('in update podcast',show_id,show_data)
        if not show_data.model_fields_set:
            return None, "No update data provided"
        # show_dict = show_data.dict()
        # show_dict['id'] = show_id
        # show_dict.pop("annual_usd", None)
        
        # # show_dict.pop("subnetwork_id", None)
        # try:
        #     show_dict["subnetwork_name"] = show_dict.pop("subnetwork_id")

        #     show_dict["genre_name"] = show_dict.pop("genre_name")
        #     show_dict["qbo_show_name"] = show_dict.pop("show_name_in_qbo")
        #     annual_usd_data = {
        #         "2023": str(show_dict.pop("revenue_2023", None)),
        #         "2024": str(show_dict.pop("revenue_2024", None)),
        #         "2025": str(show_dict.pop("revenue_2025", None)),
        #     }

        #     if any(value is not None for value in annual_usd_data.values()):
        #         show_dict["annual_usd"] = json.dumps(annual_usd_data)

        #     print(show_data, type(show_data))
        # except:
        #     pass
        # show_data = " ".join(
        #         f"{k}='{v}'" if v is not None else f"{k}=None" for k, v in show_dict.items()
        #     )
        
        update_data = show_data.model_dump(exclude_unset=True)

        set_clause = ", ".join([f"{key} = %s" for key in update_data.keys()])
        sql_update = f"UPDATE shows SET {set_clause} WHERE id = %s"
        values = list(update_data.values()) + [show_id]

        _, rows_affected, error = self._execute_query(sql_update, tuple(values), is_transaction=True)
        if error:
            return None, str(error)
        if rows_affected == 0:
            return None, f"Podcast with id {show_id} not found"

        sql_select = "SELECT * FROM shows WHERE id = %s"
        updated_show, _, error = self._execute_query(sql_select, (show_id,), fetch='one')
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
        return updated_show, str(error) if error else None

    def delete_podcast(self, show_id: str):
        # sql = "DELETE FROM demographic WHERE show_id = %s"
        # _, rows_affected, error = self._execute_query(sql, (show_id,), is_transaction=True)
        # if error:
        #     return False, str(error)
        # if rows_affected == 0:
        #     return False, f"Podcast with id {show_id} not found"

        
        sql = "DELETE FROM shows WHERE id = %s"
        _, rows_affected, error = self._execute_query(sql, (show_id,), is_transaction=True)
        if error:
            return False, str(error)
        if rows_affected == 0:
            return False, f"Podcast with id {show_id} not found"
        return True, None

    def update_password(self, user_id: str, new_password: str):
        password_hash = get_password_hash(new_password)
        sql = "UPDATE users SET password_hash = %s WHERE id = %s"
        _, rows_affected, error = self._execute_query(sql, (password_hash, user_id), is_transaction=True)
        if error:
            return False, str(error)
        if rows_affected == 0:
            return False, f"User with id {user_id} not found"
        return True, None

    def get_user_by_email(self, email: str):
        sql = "SELECT * FROM users WHERE email = %s"
        user, _, error = self._execute_query(sql, (email,), fetch='one')
        return user, error

    def get_user_by_id(self, user_id: str):
        sql = "SELECT * FROM users WHERE id = %s"
        user, _, error = self._execute_query(sql, (user_id,), fetch='one')
        return user, error

    def create_partner(self, partner_data):
        user_id = os.urandom(16).hex()
        password_hash = get_password_hash(partner_data.password)
        partner_id = os.urandom(16).hex()

        sql_user = "INSERT INTO users (id, name, email, password_hash, role) VALUES (%s, %s, %s, %s, 'partner')"
        sql_partner = "INSERT INTO partners (id, user_id) VALUES (%s, %s)"
        sql_select = "SELECT * FROM users WHERE id = %s"

        try:
            with get_db_connection() as db:
                with db.cursor() as cursor:
                    cursor.execute(sql_user, (user_id, partner_data.name, partner_data.email, password_hash))
                    cursor.execute(sql_partner, (partner_id, user_id))
                    db.commit()
                    cursor.execute(sql_select, (user_id,))
                    new_user = cursor.fetchone()
                    return new_user, None
        except pymysql.IntegrityError:
            return None, f"Partner with email {partner_data.email} already exists."
        except pymysql.Error as e:
            return None, str(e)

    def associate_partner_with_show(self, show_id: str, partner_id: str):
        association_id = os.urandom(16).hex()
        sql = "INSERT INTO show_partners (id, show_id, partner_id) VALUES (%s, %s, %s)"
        
        _, _, error = self._execute_query(sql, (association_id, show_id, partner_id), is_transaction=True)
        
        if isinstance(error, pymysql.IntegrityError):
            return None, f"Show with id {show_id} or Partner with id {partner_id} not found, or association already exists."
        if error:
            return None, str(error)
            
        return {"message": "Partner associated successfully", "show_id": show_id, "partner_id": partner_id}, None

    def get_podcasts_for_partner(self, partner_id: str):
        sql = """
            SELECT s.* 
            FROM shows s
            JOIN show_partners sp ON s.id = sp.show_id
            WHERE sp.partner_id = %s
        """
        podcasts, _, error = self._execute_query(sql, (partner_id,), fetch='all')
        if error:
            return [], str(error)
        return podcasts, None

    def create_podcast(self, show_data):
        try:
            print('in create function')
            print(show_data)
            
            show_id = os.urandom(16).hex()
            show_dict = show_data.dict()
            show_dict['id'] = show_id
            show_dict.pop("annual_usd", None)
            
            # show_dict.pop("subnetwork_id", None)
            # show_dict["subnetwork_name"] = show_dict.pop("subnetwork_id")
            show_dict["region"] = show_dict.pop("region")
            show_dict["primary_education"] = show_dict.pop("primary_education")
            show_dict["secondary_education"] = show_dict.pop("secondary_education")
            show_dict["isUndersized"] = show_dict.pop("isUndersized")
            show_dict["isActive"] = show_dict.pop("isActive")
            show_dict["evergreen_production_staff_name"] = show_dict.pop("evergreen_production_staff_name")
            

            show_dict["genre_name"] = show_dict.pop("genre_name")
            show_dict["qbo_show_name"] = show_dict.pop("show_name_in_qbo")
            annual_usd_data = {
                "2023": str(show_dict.pop("revenue_2023", None)),
                "2024": str(show_dict.pop("revenue_2024", None)),
                "2025": str(show_dict.pop("revenue_2025", None)),
            }

            if any(value is not None for value in annual_usd_data.values()):
                show_dict["annual_usd"] = json.dumps(annual_usd_data)



            columns = ', '.join([f'`{k}`' for k in show_dict.keys()])
            placeholders = ', '.join(['%s'] * len(show_dict))
            sql = f"INSERT INTO shows ({columns}) VALUES ({placeholders})"
            # sql=f"INSERT INTO shows (id,title) VALUES (1101,'test')"
            values = tuple(show_dict.values())
            print(sql, values)
            # _, _, error = self._execute_query(sql, is_transaction=True)

            _, _, error = self._execute_query(sql, values, is_transaction=True)
            if error:
                return None, error            
            fetch_sql = "SELECT * FROM shows WHERE id = %s"
            new_show, _, fetch_error = self._execute_query(fetch_sql, (show_id,), fetch='one')
            print('new show',new_show)
            # if 'annual_usd' in new_show and isinstance(new_show['annual_usd'], str):
            #     try:
            #         new_show['annual_usd'] = {
            #             k: float(v) if isinstance(v, (int, float, str)) and v not in (None, "") else 0.0
            #             for k, v in json.loads(new_show['annual_usd']).items()
            #         }
            #     except (json.JSONDecodeError, ValueError, TypeError):
            #         new_show['annual_usd'] = {}
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
    def update_podcast(self, show_id: str, show_data: BaseModel):
        print('in update podcast', show_id, show_data)

        if not show_data.model_fields_set:
            return None, "No update data provided"

        # Step 1: Convert to dict and prepare for transformation
        show_dict = show_data.model_dump(exclude_unset=True)
        show_dict['id'] = show_id
        show_dict.pop("annual_usd", None)

        # Optional: remove subnetwork_id if renaming elsewhere
        # show_dict.pop("subnetwork_id", None)
        # show_dict["subnetwork_name"] = show_dict.pop("subnetwork_id")

        try:
            # Rename fields as needed
            show_dict["genre_name"] = show_dict.pop("genre_name")
            show_dict["qbo_show_name"] = show_dict.pop("show_name_in_qbo")

            # Extract revenue fields and convert to string for JSON
            annual_usd_data = {
                "2023": str(show_dict.pop("revenue_2023", None)),
                "2024": str(show_dict.pop("revenue_2024", None)),
                "2025": str(show_dict.pop("revenue_2025", None)),
            }

            # Only add annual_usd if any value is provided
            if any(value is not None for value in annual_usd_data.values()):
                show_dict["annual_usd"] = json.dumps(annual_usd_data)

        except KeyError as e:
            print(f"Field missing during transformation: {e}")

        # Step 2: Build SQL update
        set_clause = ", ".join([f"{key} = %s" for key in show_dict.keys()])
        sql_update = f"UPDATE shows SET {set_clause} WHERE id = %s"
        values = list(show_dict.values()) + [show_id]

        _, rows_affected, error = self._execute_query(sql_update, tuple(values), is_transaction=True)
        if error:
            return None, str(error)
        if rows_affected == 0:
            return None, f"Podcast with id {show_id} not found"

        # Step 3: Fetch updated show
        sql_select = "SELECT * FROM shows WHERE id = %s"
        updated_show, _, error = self._execute_query(sql_select, (show_id,), fetch='one')

        # Step 4: Unpack annual_usd into year-specific fields
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

        return updated_show, str(error) if error else None
