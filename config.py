import configparser

# Create a parser
config = configparser.ConfigParser()

# Read the config file
config.read('config.ini')

# --- Database Settings ---
DB_HOST = config.get('database', 'host')
DB_USER = config.get('database', 'user')
DB_PASSWORD = config.get('database', 'password')
DB_NAME = config.get('database', 'name')
DB_PORT = config.getint('database', 'db_port')

# --- JWT Settings ---
SECRET_KEY = config.get('jwt', 'secret_key')
ALGORITHM = config.get('jwt', 'algorithm')
ACCESS_TOKEN_EXPIRE_MINUTES = config.getint('jwt', 'access_token_expire_minutes')
ACCESS_TOKEN_EXPIRE_HOURS = config.getint('jwt', 'access_token_expire_hours')
REFRESH_TOKEN_EXPIRE_DAYS = config.getint('jwt', 'refresh_token_expire_days')