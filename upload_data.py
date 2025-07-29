import pymysql
import os

# --- Database Configuration ---
# Values from docker-compose.yml
DB_HOST = "localhost"  # Use localhost when running script from the host
DB_USER = "root"
DB_PASSWORD = "rootpassword"
DB_NAME = "evergreen"
DB_PORT = 3307
SQL_DUMP_FILE = os.path.join(os.path.dirname(__file__), 'Dump20250719.sql')

def execute_sql_from_file(cursor, filepath):
    """
    Reads a .sql file and executes the statements.
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
        # Split by semicolon followed by a newline, which is a common delimiter for dumps.
        # This is more robust than splitting by just ';'.
        commands = content.split(';\n')
        for command in commands:
            # Strip whitespace and skip empty commands.
            command = command.strip()
            if command:
                try:
                    cursor.execute(command)
                except pymysql.MySQLError as e:
                    print(f"Skipping statement due to error: {e}\nStatement: '{command[:100]}...'\n")

def upload_data_from_dump():
    """
    Connects to the database and executes the SQL statements from the dump file.
    """
    conn = None
    try:
        # Establish a connection to the database
        conn = pymysql.connect(host=DB_HOST,
                               user=DB_USER,
                               password=DB_PASSWORD,
                               database=DB_NAME,
                               port=DB_PORT,
                               charset='utf8mb4',
                               cursorclass=pymysql.cursors.DictCursor)

        print(f"Successfully connected to database '{DB_NAME}' at {DB_HOST}:{DB_PORT}.")

        with conn.cursor() as cursor:
            print(f"Executing SQL dump file: {SQL_DUMP_FILE}")
            execute_sql_from_file(cursor, SQL_DUMP_FILE)

        conn.commit()
        print("\nData upload process completed successfully.")

    except pymysql.MySQLError as e:
        print(f"Database operation failed: {e}")
    except FileNotFoundError:
        print(f"Error: SQL dump file not found at {SQL_DUMP_FILE}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    upload_data_from_dump()
