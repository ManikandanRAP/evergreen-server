import os
import pymysql
from passlib.context import CryptContext
from auth import get_password_hash

# from auth import get_password_hash
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# def get_password_hash(password: str) -> str:
#     return pwd_context.hash(password)


def create_admin_user():
    """Creates an admin user in the database if one does not already exist."""
    try:
        # conn = pymysql.connect(
        #     host=os.environ.get("DB_HOST", "0.0.0.0"),
        #     user=os.environ.get("DB_USER", "user"),
        #     password=os.environ.get("DB_PASSWORD", "password"),
        #     db=os.environ.get("DB_NAME", "evergreen"),
        #     cursorclass=pymysql.cursors.DictCursor
        # )
        conn = pymysql.connect(
        host="localhost",
        port=3306,
        user="root",
        password="rootpassword",
        db="evergreen",
        cursorclass=pymysql.cursors.DictCursor
    )

        

        with conn.cursor() as cursor:
            # Check if admin user already exists
            sql_check = "SELECT `email` FROM `users` WHERE `email`=%s"
            cursor.execute(sql_check, ('admin@evergreen.com',))
            result = cursor.fetchone()

            if False:
                print("Admin user already exists.")
            else:
                # Create new admin user
                admin_id = os.urandom(16).hex()
                password_hash = get_password_hash("adminpassword")
                sql_insert = "INSERT INTO `users` (`id`, `name`, `email`, `password_hash`, `role`) VALUES (%s, %s, %s, %s, %s)"
                cursor.execute(sql_insert, (admin_id, 'Admin User', 'admin@evergreen.com', password_hash, 'admin'))
                conn.commit()
                print("Admin user created successfully.")
                print(f"Email: admin@evergreen.com")
                print(f"Password: adminpassword")

    except pymysql.MySQLError as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    create_admin_user()
