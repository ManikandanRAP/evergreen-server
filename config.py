import configparser
import os

# Create a parser
config = configparser.ConfigParser()

# Read the config file (if it exists)
config.read('config.ini')

# --- Database Settings ---
# Prefer environment variables (for Docker), fall back to config.ini
DB_HOST = os.environ.get('DB_HOST') or config.get('database', 'host', fallback='db')
DB_USER = os.environ.get('DB_USER') or config.get('database', 'user', fallback='root')
DB_PASSWORD = os.environ.get('DB_PASSWORD') or config.get('database', 'password', fallback='rootpassword')
DB_NAME = os.environ.get('DB_NAME') or config.get('database', 'name', fallback='evergreen')
DB_PORT = int(os.environ.get('DB_PORT', 0)) or config.getint('database', 'db_port', fallback=3306)

# Log database configuration for debugging (without exposing password)
if DB_HOST:
    print(f"Database configuration: DB_HOST={DB_HOST}, DB_PORT={DB_PORT}, DB_NAME={DB_NAME}, DB_USER={DB_USER}")

# --- JWT Settings ---
SECRET_KEY = os.environ.get('SECRET_KEY') or config.get('jwt', 'secret_key', fallback='change_me_secret_key')
ALGORITHM = config.get('jwt', 'algorithm', fallback='HS256')
ACCESS_TOKEN_EXPIRE_MINUTES = config.getint('jwt', 'access_token_expire_minutes', fallback=1440)
ACCESS_TOKEN_EXPIRE_HOURS = config.getint('jwt', 'access_token_expire_hours', fallback=24)
REFRESH_TOKEN_EXPIRE_DAYS = config.getint('jwt', 'refresh_token_expire_days', fallback=7)