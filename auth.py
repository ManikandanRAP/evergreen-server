from datetime import datetime, timedelta
from typing import Optional, Tuple

from jose import JWTError, jwt
from passlib.context import CryptContext
from config import SECRET_KEY, ALGORITHM, ACCESS_TOKEN_EXPIRE_HOURS, REFRESH_TOKEN_EXPIRE_DAYS

# Password Hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)

# Internal token creation helper
def _create_token(data: dict, expires_delta: timedelta) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + expires_delta
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

# Create both access and refresh tokens
def create_token_pair(data: dict) -> Tuple[str, str]:
    access_token = _create_token(data, timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS))
    refresh_token = _create_token(data, timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS))
    return access_token, refresh_token

# Decode and validate a token (access or refresh)
def decode_token(token: str) -> Optional[dict]:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None
