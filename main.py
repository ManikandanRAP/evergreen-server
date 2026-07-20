import uvicorn
from fastapi import FastAPI, Depends, HTTPException, status, Response, UploadFile, File, BackgroundTasks, Form, Request
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.responses import StreamingResponse, ORJSONResponse, JSONResponse
from jose import JWTError, jwt
from typing import Optional, List, Any, Dict
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import uuid
import io
import re
import os
import time
import json
import hashlib

from services.env_loader import load_local_env

load_local_env()

from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.encoders import jsonable_encoder

try:
    import orjson
except Exception:
    orjson = None

try:
    import redis as redis_lib
except Exception:
    redis_lib = None

from models import (
    Show,
    User,
    Token,
    TokenData,
    PartnerCreate,
    PasswordUpdate,
    ShowUpdate,
    ShowCreate,
    MediaType,
    RelationshipLevel,
    ShowType,
    UserResponse,
    UserCreate,
    Split,
    SplitCreate,
    UserListItem,
    UserUpdate,
    UserSettingsUpdate,  # Import for user settings
    FeedbackCreate, # Import Feedback models
    Feedback,
    FeedbackListItem,
    FeedbackStatus,
    FeedbackStatusUpdate,
    FeedbackResolutionUpdate,
    BaseModel,
    UsernameCheckRequest,
    UsernameCheckResponse,
    GENRE_MAP,
    MEDIA_TYPE_MAP,
    REL_LEVEL_MAP,
    SHOW_TYPE_MAP,
    CADENCE_MAP,
    RANKING_CATEGORY_MAP,
    AGE_DEMOGRAPHICS,
)
from sqlclient import SqlClient
from auth import create_access_token, verify_password, get_password_hash

# ----------------------
# Developer live logs (in-memory; per-process)
# ----------------------
DEV_LOG_MAX = 500
_dev_log_seq_by_email: Dict[str, int] = {}
_dev_logs_by_email: Dict[str, List[Dict[str, Any]]] = {}

def _devlog(email: Optional[str], type_: str, message: str, details: Optional[str] = None):
    if not email:
        return
    seq = _dev_log_seq_by_email.get(email, 0) + 1
    _dev_log_seq_by_email[email] = seq
    entry: Dict[str, Any] = {
        "seq": seq,
        "timestamp": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
        "type": type_,
        "message": message,
    }
    if details:
        entry["details"] = details
    arr = _dev_logs_by_email.setdefault(email, [])
    arr.append(entry)
    if len(arr) > DEV_LOG_MAX:
        _dev_logs_by_email[email] = arr[-DEV_LOG_MAX:]

class DeveloperLogsResponse(BaseModel):
    logs: List[Dict[str, Any]]
    next_after: int


class UserLoginActivityListResponse(BaseModel):
    items: List[Dict[str, Any]]
    total: Optional[int] = None
    total_is_estimate: bool = False
    page_size: int
    next_cursor: Optional[str] = None
    has_more: bool = False


def _hash_ip(ip_value: Optional[str]) -> Optional[str]:
    if not ip_value:
        return None
    return hashlib.sha256(ip_value.strip().encode("utf-8")).hexdigest()


def _request_info(request: Optional[Request]) -> Dict[str, Optional[str]]:
    if request is None:
        return {"request_id": str(uuid.uuid4()), "session_id": None, "ip_hash": None, "user_agent": None}

    request_id = request.headers.get("x-request-id") or request.headers.get("x-correlation-id") or str(uuid.uuid4())
    session_id = request.headers.get("x-session-id")
    forwarded_for = request.headers.get("x-forwarded-for")
    client_host = request.client.host if request.client else None
    ip_raw = (forwarded_for.split(",")[0].strip() if forwarded_for else client_host)
    user_agent = request.headers.get("user-agent")
    return {
        "request_id": request_id,
        "session_id": session_id,
        "ip_hash": _hash_ip(ip_raw),
        "user_agent": user_agent,
    }


def _request_source(request: Optional[Request]) -> str:
    if request is None:
        return "unknown"
    explicit_source = request.headers.get("x-client-source")
    if explicit_source:
        return explicit_source.strip().lower()
    user_agent = (request.headers.get("user-agent") or "").lower()
    if "mobile" in user_agent or "android" in user_agent or "iphone" in user_agent:
        return "mobile_web"
    if user_agent:
        return "web"
    return "unknown"


def _write_login_activity(
    *,
    client: SqlClient,
    action: str,
    status_value: str,
    request: Optional[Request],
    user_data: Optional[Dict[str, Any]] = None,
    fallback_email: Optional[str] = None,
    failure_reason: Optional[str] = None,
    metadata_json: Optional[Dict[str, Any]] = None,
):
    started = time.perf_counter()
    req = _request_info(request)
    email = (user_data or {}).get("email") or fallback_email
    if not email:
        return

    merged_meta: Dict[str, Any] = dict(metadata_json or {})
    if user_data:
        ca = user_data.get("created_at")
        if ca is not None:
            try:
                if hasattr(ca, "isoformat"):
                    merged_meta["member_since"] = ca.isoformat()
                elif isinstance(ca, str) and ca.strip():
                    merged_meta["member_since"] = ca.strip()
                else:
                    merged_meta["member_since"] = str(ca)
            except Exception:
                pass
    metadata_out = merged_meta if merged_meta else None

    client.create_user_login_activity(
        event_uuid=str(uuid.uuid4()),
        user_id=(user_data or {}).get("id"),
        user_email=email,
        user_name=(user_data or {}).get("name"),
        user_role=(user_data or {}).get("role"),
        action=action,
        status=status_value,
        request_id=req["request_id"],
        session_id=req["session_id"],
        ip_hash=req["ip_hash"],
        user_agent=req["user_agent"],
        failure_reason=failure_reason,
        metadata_json=metadata_out,
    )
    elapsed_ms = (time.perf_counter() - started) * 1000
    threshold_ms = int(os.environ.get("LOGIN_ACTIVITY_SLOW_WRITE_MS", "100"))
    if elapsed_ms >= threshold_ms:
        print(
            f"PERF login_activity_write_slow action={action} status={status_value} "
            f"elapsed_ms={elapsed_ms:.2f} threshold_ms={threshold_ms}"
        )
from config import SECRET_KEY, ALGORITHM

FastJSONResponse = ORJSONResponse if orjson is not None else JSONResponse

app = FastAPI(
    title="Evergreen Podcasts API",
    description="API for managing podcasts and partners with JWT authentication.",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    default_response_class=FastJSONResponse,
)

app.add_middleware(GZipMiddleware, minimum_size=1024, compresslevel=6)

LEDGER_CACHE_TTL_SECONDS = int(os.environ.get("LEDGER_CACHE_TTL_SECONDS", "3600"))
LOGIN_ACTIVITY_SLOW_QUERY_MS = int(os.environ.get("LOGIN_ACTIVITY_SLOW_QUERY_MS", "300"))
_ledger_cache_memory: Dict[str, tuple[float, Any]] = {}
_redis_client = None
if redis_lib and os.environ.get("REDIS_URL"):
    try:
        _redis_client = redis_lib.Redis.from_url(os.environ["REDIS_URL"], decode_responses=False)
        _redis_client.ping()
        print("Ledger cache backend: redis")
    except Exception as e:
        print(f"WARNING: Redis unavailable, falling back to in-memory cache: {e}")
        _redis_client = None
else:
    print("Ledger cache backend: in-memory")


def _cache_decode_payload(payload: bytes):
    if payload is None:
        return None
    if orjson:
        return orjson.loads(payload)
    return json.loads(payload.decode("utf-8"))


def _normalize_json_content(value: Any):
    # Normalize DB-native values (e.g., Decimal) for strict JSON serializers.
    return jsonable_encoder(value, custom_encoder={Decimal: float})


def _cache_encode_payload(value: Any) -> bytes:
    value = _normalize_json_content(value)
    if orjson:
        return orjson.dumps(value)
    return json.dumps(value).encode("utf-8")


def _cache_get(key: str):
    if _redis_client is not None:
        try:
            payload = _redis_client.get(key)
            return _cache_decode_payload(payload) if payload else None
        except Exception as e:
            print(f"WARNING: Redis cache get failed for {key}: {e}")

    cached = _ledger_cache_memory.get(key)
    if not cached:
        return None
    expires_at, value = cached
    if time.time() >= expires_at:
        _ledger_cache_memory.pop(key, None)
        return None
    return value


def _cache_set(key: str, value: Any, ttl: int = LEDGER_CACHE_TTL_SECONDS):
    safe_value = _normalize_json_content(value)
    if _redis_client is not None:
        try:
            _redis_client.setex(key, ttl, _cache_encode_payload(safe_value))
        except Exception as e:
            print(f"WARNING: Redis cache set failed for {key}: {e}")
    _ledger_cache_memory[key] = (time.time() + ttl, safe_value)


def invalidate_ledger_cache():
    _ledger_cache_memory.clear()
    if _redis_client is not None:
        try:
            for key in _redis_client.scan_iter(match="ledger:*"):
                _redis_client.delete(key)
            for key in _redis_client.scan_iter(match="partner_payouts:*"):
                _redis_client.delete(key)
        except Exception as e:
            print(f"WARNING: Redis cache invalidation failed: {e}")


def _ledger_cache_key(current_user: Dict[str, Any], endpoint: str) -> str:
    role = current_user.get("role") or "unknown"
    if role == "partner":
        vendor = current_user.get("mapped_vendor_qbo_id") or "none"
        return f"{endpoint}:partner:{vendor}"
    return f"{endpoint}:{role}:all"


@app.on_event("startup")
def ensure_startup_indexes():
    """
    Ensure critical query indexes exist for ledger, notices, feedbacks,
    notifications, shows, and admin pages. Self-healing after DB restores.
    """
    try:
        client = SqlClient()
        client.ensure_performance_indexes()
        client.ensure_user_login_activity_schema()
        client.ensure_inbox_schema()
        client.ensure_myco_notice_contacts_schema()
    except Exception as e:
        # Startup should continue; API can still serve while DB/admin fixes are applied.
        print(f"WARNING: Failed to ensure performance indexes on startup: {e}")
    invalidate_ledger_cache()
    _set_cache_refresh_meta(datetime.now(timezone.utc), LEDGER_CACHE_TTL_SECONDS)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

# ----------------------
# Auth helpers
# ----------------------
def get_token_email(token: str = Depends(oauth2_scheme)) -> str:
    """
    Decode JWT and return email without touching the database.
    This is required for endpoints that must work while the DB is being replaced.
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: Optional[str] = payload.get("sub")
        if not email:
            raise JWTError("missing sub")
        return email
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            raise credentials_exception
        token_data = TokenData(email=email)
    except JWTError:
        raise credentials_exception

    client = SqlClient()
    user, _ = client.get_user_by_email(email=token_data.email)
    if user is None:
        raise credentials_exception
    return user

def get_current_active_user(current_user: User = Depends(get_current_user)):
    return current_user

def resolve_current_user_id(current_user) -> str:
    """Resolve the DB user id from the auth dependency (dict or Pydantic model)."""
    if isinstance(current_user, dict):
        user_id = current_user.get("id")
        email = current_user.get("email")
    else:
        user_id = getattr(current_user, "id", None)
        email = getattr(current_user, "email", None)
    if user_id:
        return str(user_id)
    if email:
        client = SqlClient()
        user, _ = client.get_user_by_email(email)
        if user and user.get("id"):
            return str(user["id"])
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not resolve user identity",
    )

def get_admin_user(current_user: User = Depends(get_current_active_user)):
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Not enough permissions")
    return current_user

def get_admin_or_internal_user(current_user: User = Depends(get_current_active_user)):
    if current_user.get("role") not in ("admin", "internal_full_access", "internal_show_access"):
        raise HTTPException(status_code=403, detail="Not enough permissions")
    return current_user

def get_notices_manager(current_user: User = Depends(get_current_active_user)):
    if current_user.get("role") not in ("admin", "internal_full_access"):
        raise HTTPException(status_code=403, detail="Not enough permissions")
    return current_user

def _cancel_active_notices_for_show(show_id: str, user_id: Optional[str] = None) -> None:
    from myco_notices_db import MycoNoticesDb
    _, err = MycoNoticesDb().cancel_notices_for_show(show_id, user_id)
    if err:
        print(f"WARNING: failed to auto-cancel notices for show {show_id}: {err}")

def _cancel_active_notices_for_shows(show_ids: List[str], user_id: Optional[str] = None) -> None:
    from myco_notices_db import MycoNoticesDb
    db = MycoNoticesDb()
    for show_id in show_ids:
        _, err = db.cancel_notices_for_show(show_id, user_id)
        if err:
            print(f"WARNING: failed to auto-cancel notices for show {show_id}: {err}")

# ----------------------
# Auth endpoints
# ----------------------
@app.post("/login")
def login_for_access_token(request: Request, form_data: OAuth2PasswordRequestForm = Depends()):
    client = SqlClient()
    user, _ = client.get_user_by_email(email=form_data.username)
    if not user:
        _write_login_activity(
            client=client,
            action="LOGIN",
            status_value="FAILED",
            request=request,
            fallback_email=form_data.username,
            failure_reason="INVALID_CREDENTIALS",
            metadata_json={
                "source": _request_source(request),
                "auth_method": "password",
                "result": "invalid_credentials",
            },
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not verify_password(form_data.password, user.get("password_hash")):
        _write_login_activity(
            client=client,
            action="LOGIN",
            status_value="FAILED",
            request=request,
            user_data=user,
            fallback_email=user.get("email") or form_data.username,
            failure_reason="INVALID_CREDENTIALS",
            metadata_json={
                "source": _request_source(request),
                "auth_method": "password",
                "result": "invalid_credentials",
            },
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    _write_login_activity(
        client=client,
        action="LOGIN",
        status_value="SUCCESS",
        request=request,
        user_data=user,
        metadata_json={
            "source": _request_source(request),
            "auth_method": "password",
            "result": "success",
        },
    )
    access_token = create_access_token(data={"sub": user.get("email")})
    return {"access_token": access_token, "token_type": "bearer"}


@app.post("/logout")
def logout_user(request: Request, current_user: User = Depends(get_current_active_user)):
    client = SqlClient()
    _write_login_activity(
        client=client,
        action="LOGOUT",
        status_value="SUCCESS",
        request=request,
        user_data=current_user,
        metadata_json={
            "source": _request_source(request),
            "logout_reason": "user_initiated",
            "result": "success",
        },
    )
    return {"message": "Logged out successfully"}


@app.get("/admin/user-login-activity", response_model=UserLoginActivityListResponse)
def get_user_login_activity(
    page_size: int = 25,
    cursor: Optional[str] = None,
    include_total: bool = False,
    action: Optional[str] = None,
    status_value: Optional[str] = None,
    user_email: Optional[str] = None,
    from_utc: Optional[str] = None,
    to_utc: Optional[str] = None,
    query: Optional[str] = None,
    admin: User = Depends(get_admin_user),
):
    started = time.perf_counter()
    _ = admin
    safe_page_size = min(max(1, page_size), 200)

    if action and action not in ("LOGIN", "LOGOUT"):
        raise HTTPException(status_code=400, detail="Invalid action filter")
    if status_value and status_value not in ("SUCCESS", "FAILED"):
        raise HTTPException(status_code=400, detail="Invalid status filter")

    client = SqlClient()
    items, next_cursor, has_more, total, error = client.get_user_login_activity(
        page_size=safe_page_size,
        cursor=cursor,
        action=action,
        status=status_value,
        user_email=user_email,
        from_utc=from_utc,
        to_utc=to_utc,
        query=query,
        include_total=include_total,
    )
    if error:
        raise HTTPException(status_code=500, detail=error)
    elapsed_ms = (time.perf_counter() - started) * 1000
    if elapsed_ms >= LOGIN_ACTIVITY_SLOW_QUERY_MS:
        print(
            "PERF login_activity_read_slow "
            f"elapsed_ms={elapsed_ms:.2f} threshold_ms={LOGIN_ACTIVITY_SLOW_QUERY_MS} "
            f"page_size={safe_page_size} has_more={has_more} include_total={include_total}"
        )
    return UserLoginActivityListResponse(
        items=items,
        total=total,
        total_is_estimate=False,
        page_size=safe_page_size,
        next_cursor=next_cursor,
        has_more=has_more,
    )

@app.get("/users/me", response_model=User)
def read_users_me(current_user: User = Depends(get_current_active_user)):
    # Populate mapped_vendor_name if mapped_vendor_qbo_id exists
    if current_user.get("mapped_vendor_qbo_id"):
        client = SqlClient()
        vid = current_user.get("mapped_vendor_qbo_id")
        if vid is not None:
            try:
                current_user["mapped_vendor_name"] = client.get_vendor_name_by_qbo_id(int(vid))
            except Exception:
                current_user["mapped_vendor_name"] = None
    
    return current_user

@app.post("/users/check-username", response_model=UsernameCheckResponse)
async def check_username_availability(request: UsernameCheckRequest, current_user: User = Depends(get_current_active_user)):
    """Check if a username (email) is available for registration"""
    client = SqlClient()
    existing_user, error = client.get_user_by_email(request.username)
    
    if error:
        raise HTTPException(status_code=500, detail="Database error occurred while checking username availability")
    
    # If user exists, username is not available
    available = existing_user is None
    
    return UsernameCheckResponse(available=available)

# ----------------------
# User Settings
# ----------------------
@app.get("/users/me/settings")
def get_user_settings(current_user: User = Depends(get_current_active_user)):
    """Get the current user's settings"""
    client = SqlClient()
    settings, error = client.get_user_settings(resolve_current_user_id(current_user))
    if error:
        if error == "User not found":
            raise HTTPException(status_code=404, detail=error)
        raise HTTPException(status_code=500, detail=str(error))
    # Return empty dict if no settings exist yet
    return settings or {}

@app.put("/users/me/settings")
def update_user_settings(
    settings_update: UserSettingsUpdate,
    current_user: User = Depends(get_current_active_user)
):
    """Update the current user's settings"""
    client = SqlClient()
    success, error = client.update_user_settings(
        resolve_current_user_id(current_user),
        settings_update.settings,
    )
    if not success:
        if error == "User not found":
            raise HTTPException(status_code=404, detail=error)
        raise HTTPException(status_code=500, detail=str(error))
    return {"message": "Settings updated successfully", "settings": settings_update.settings}

# ----------------------
# Users (admin only)
# ----------------------
@app.post("/create_users", response_model=UserResponse)
def create_user(user_data: UserCreate, admin: User = Depends(get_admin_user)):
    # Admin can create admin/partner/internal
    client = SqlClient()
    existing_user, _ = client.get_user_by_email(user_data.email)
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")

    user_id = str(uuid.uuid4())
    hashed_password = get_password_hash(user_data.password)
    sql = """
    INSERT INTO users (id, name, email, password_hash, role, created_at, mapped_vendor_qbo_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        user_id,
        user_data.name,
        user_data.email,
        hashed_password,
        user_data.role,  # "admin" | "partner" | "internal_full_access" | "internal_show_access"
        datetime.now(timezone.utc),
        user_data.mapped_vendor_qbo_id,
    )
    _, _, error = client._execute_query(sql, values, is_transaction=True)
    if error:
        raise HTTPException(status_code=500, detail="Error inserting user into DB")

    # Auto-link Staff Directory when Admin/Internal account is created
    try:
        from staff_directory_db import StaffDirectoryDb
        from models import STAFF_LINK_ELIGIBLE_ROLES

        role_val = user_data.role.value if hasattr(user_data.role, "value") else str(user_data.role)
        staff_db = StaffDirectoryDb(client)
        if role_val in STAFF_LINK_ELIGIBLE_ROLES:
            staff_db.link_staff_by_user_email(user_id, user_data.email)
        elif role_val == "partner":
            staff_db.clear_staff_link_for_user(user_id)
    except Exception as link_err:
        print(f"Staff directory auto-link on user create skipped: {link_err}")

    return {
        "id": user_id,
        "name": user_data.name,
        "email": user_data.email,
        "role": user_data.role,
        "mapped_vendor_qbo_id": user_data.mapped_vendor_qbo_id,
    }

@app.get("/users", response_model=List[UserListItem])
def list_users(admin: User = Depends(get_admin_or_internal_user)):
    client = SqlClient()
    users, error = client.get_all_users()
    if error:
        raise HTTPException(status_code=500, detail=str(error))

    # Map vendor_qbo_id -> vendor_name
    vendors = client.get_all_vendors()
    vendor_map = {}
    for v in vendors or []:
        vid = v.get("vendor_qbo_id")
        vname = v.get("vendor_name") or v.get("vendor_qbo_name") or v.get("displayname")
        if vid is not None and vname:
            try:
                vendor_map[int(vid)] = vname
            except Exception:
                pass

    result = []
    for u in users or []:
        vid = u.get("mapped_vendor_qbo_id")
        result.append(
            {
                "id": str(u.get("id")),
                "name": u.get("name"),
                "email": u.get("email"),
                "role": u.get("role"),
                "created_at": u.get("created_at"),
                "mapped_vendor_qbo_id": vid,
                "mapped_vendor_name": (vendor_map.get(int(vid)) if vid is not None else None),
            }
        )
    return result

@app.get("/users/{user_id}", response_model=User)
def get_user_by_id(user_id: str, admin: User = Depends(get_admin_or_internal_user)):
    client = SqlClient()
    user, error = client.get_user_by_id(user_id)
    if error:
        raise HTTPException(status_code=500, detail=str(error))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user

@app.put("/users/{user_id}", response_model=UserListItem)
def update_user(user_id: str, payload: UserUpdate, admin: User = Depends(get_admin_user)):
    client = SqlClient()

    # Ensure user exists
    existing_user, err = client.get_user_by_id(user_id)
    if err:
        raise HTTPException(status_code=500, detail=str(err))
    if not existing_user:
        raise HTTPException(status_code=404, detail="User not found")

    # Email uniqueness if changing
    if payload.email and payload.email != existing_user.get("email"):
        other, e2 = client.get_user_by_email(payload.email)
        if e2:
            raise HTTPException(status_code=500, detail=str(e2))
        if other and other.get("id") != user_id:
            raise HTTPException(status_code=400, detail="Email already registered")

    # Build update kwargs only for provided fields
    update_kwargs = {}
    if "name" in payload.model_fields_set:
        update_kwargs["name"] = payload.name
    if "email" in payload.model_fields_set:
        update_kwargs["email"] = payload.email
    if "role" in payload.model_fields_set:
        new_role = payload.role.value if payload.role is not None else None
        update_kwargs["role"] = new_role
        if new_role != "partner" and "mapped_vendor_qbo_id" not in payload.model_fields_set:
            if existing_user.get("mapped_vendor_qbo_id") is not None:
                update_kwargs["mapped_vendor_qbo_id"] = None
    if "mapped_vendor_qbo_id" in payload.model_fields_set:
        update_kwargs["mapped_vendor_qbo_id"] = payload.mapped_vendor_qbo_id
    if payload.password:
        update_kwargs["password_hash"] = get_password_hash(payload.password)

    effective_role = update_kwargs.get("role", existing_user.get("role"))
    effective_vendor = update_kwargs.get(
        "mapped_vendor_qbo_id",
        existing_user.get("mapped_vendor_qbo_id"),
    )
    if effective_role == "partner" and not effective_vendor:
        raise HTTPException(status_code=400, detail="Vendor is required for partner users")

    ok, e3 = client.update_user(user_id=user_id, **update_kwargs)
    if not ok:
        raise HTTPException(status_code=500, detail=str(e3))

    # Staff Directory auto-link / unlink based on role + email
    try:
        from staff_directory_db import StaffDirectoryDb
        from models import STAFF_LINK_ELIGIBLE_ROLES

        staff_db = StaffDirectoryDb(client)
        final_role = str(effective_role or "")
        final_email = update_kwargs.get("email", existing_user.get("email"))
        if final_role == "partner":
            staff_db.clear_staff_link_for_user(user_id)
        elif final_role in STAFF_LINK_ELIGIBLE_ROLES and final_email:
            staff_db.link_staff_by_user_email(user_id, final_email)
    except Exception as link_err:
        print(f"Staff directory auto-link on user update skipped: {link_err}")

    # Re-fetch and return enriched row
    updated, e4 = client.get_user_by_id(user_id)
    if e4:
        raise HTTPException(status_code=500, detail=str(e4))
    if not updated:
        raise HTTPException(status_code=500, detail="Updated user fetch failed")

    vendors = client.get_all_vendors()
    vendor_map = {}
    for v in vendors or []:
        vid = v.get("vendor_qbo_id")
        vname = v.get("vendor_name") or v.get("vendor_qbo_name") or v.get("displayname")
        if vid is not None and vname:
            try:
                vendor_map[int(vid)] = vname
            except Exception:
                pass

    vid = updated.get("mapped_vendor_qbo_id")
    return {
        "id": str(updated.get("id")),
        "name": updated.get("name"),
        "email": updated.get("email"),
        "role": updated.get("role"),
        "created_at": updated.get("created_at"),
        "mapped_vendor_qbo_id": vid,
        "mapped_vendor_name": (vendor_map.get(int(vid)) if vid is not None else None),
    }

@app.delete("/users/{user_id}", status_code=204)
def delete_user(user_id: str, admin: User = Depends(get_admin_user)):
    # Prevent self-deletion
    if str(admin.get("id")) == str(user_id):
        raise HTTPException(status_code=400, detail="You cannot delete your own account.")
    client = SqlClient()
    success, error = client.delete_user(user_id)
    if not success:
        # If sqlclient returns "User not found", map to 404
        if error and "not found" in error.lower():
            raise HTTPException(status_code=404, detail=error)
        raise HTTPException(status_code=500, detail=error or "Failed to delete user")
    # 204 No Content
    return Response(status_code=204)

# ----------------------
# Feedbacks
# ----------------------
@app.post("/feedbacks", response_model=Feedback, status_code=status.HTTP_201_CREATED)
def create_feedback(
    feedback_data: FeedbackCreate,
    current_user: User = Depends(get_current_active_user)
):
    client = SqlClient()
    user_id = current_user.get("id")
    # Unpack the tuple returned by the client
    new_feedback, error = client.create_feedback(feedback_data, user_id)
    if error:
        raise HTTPException(status_code=500, detail=str(error))
    # Return only the data, not the tuple
    return new_feedback

@app.get("/feedbacks", response_model=List[FeedbackListItem])
def get_all_feedbacks(status: Optional[FeedbackStatus] = None, admin: User = Depends(get_admin_user)):
    client = SqlClient()
    feedbacks, error = client.get_all_feedbacks(status.value if status else None, summary=True)
    if error:
        raise HTTPException(status_code=500, detail=str(error))
    return feedbacks

@app.get("/feedbacks/export", response_model=List[Feedback])
def export_feedbacks(status: Optional[FeedbackStatus] = None, admin: User = Depends(get_admin_user)):
    client = SqlClient()
    feedbacks, error = client.get_all_feedbacks(status.value if status else None, summary=False)
    if error:
        raise HTTPException(status_code=500, detail=str(error))
    return feedbacks

@app.get("/feedbacks/{feedback_id}", response_model=Feedback)
def get_feedback(feedback_id: str, admin: User = Depends(get_admin_user)):
    client = SqlClient()
    feedback, error = client.get_feedback_by_id(feedback_id)
    if error:
        raise HTTPException(status_code=500, detail=str(error))
    if not feedback:
        raise HTTPException(status_code=404, detail="Feedback not found")
    return feedback

@app.patch("/feedbacks/{feedback_id}/status", response_model=Feedback)
def update_feedback_status(
    feedback_id: str,
    payload: FeedbackStatusUpdate,
    admin: User = Depends(get_admin_user),
):
    client = SqlClient()
    updated_feedback, error = client.update_feedback_status(
        feedback_id=feedback_id,
        status=payload.status.value,
        admin_user_id=admin.get("id"),
        resolution_note=payload.resolution_note,
    )
    if error:
        if "not found" in str(error).lower():
            raise HTTPException(status_code=404, detail=error)
        raise HTTPException(status_code=500, detail=error)
    return updated_feedback

@app.patch("/feedbacks/{feedback_id}/resolution", response_model=Feedback)
def update_feedback_resolution(
    feedback_id: str,
    payload: FeedbackResolutionUpdate,
    admin: User = Depends(get_admin_user),
):
    client = SqlClient()
    updated_feedback, error = client.update_feedback_resolution(
        feedback_id=feedback_id,
        resolution_note=payload.resolution_note,
    )
    if error:
        if "not found" in str(error).lower():
            raise HTTPException(status_code=404, detail=error)
        raise HTTPException(status_code=500, detail=error)
    return updated_feedback

@app.delete("/feedbacks/{feedback_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_feedback(feedback_id: str, admin: User = Depends(get_admin_user)):
    client = SqlClient()
    success, error = client.delete_feedback(feedback_id)
    if not success:
        if "not found" in str(error).lower():
            raise HTTPException(status_code=404, detail=error)
        raise HTTPException(status_code=500, detail=error or "Failed to delete feedback")
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# ----------------------
# Shows (READ: admin+internal; WRITE: admin)
# ----------------------
def _load_qbo_name_maps(client: SqlClient):
    """QBO class list from allclass: name -> id (same source as manual create dropdown)."""
    allclass_items, ac_err = client.get_allclass_items()
    valid_qbo_names = set()
    qbo_name_to_id: Dict[str, int] = {}
    if not ac_err and allclass_items:
        for item in allclass_items:
            name = item.get("name")
            id_ = item.get("id")
            if name is not None:
                ns = name.strip()
                valid_qbo_names.add(ns)
                if id_ is not None:
                    qbo_name_to_id[ns] = id_
    return valid_qbo_names, qbo_name_to_id, ac_err


def _resolve_qbo_payload_for_import(
    show_data: ShowCreate,
    valid_qbo_names: set,
    qbo_name_to_id: Dict[str, int],
    row_display_num: int,
    warnings: List[str],
) -> ShowCreate:
    """Match CSV QBO Account Name to allclass; id always derived from name."""
    qbo_name = (show_data.qbo_show_name or "").strip()
    if qbo_name:
        if qbo_name in valid_qbo_names:
            qbo_id = qbo_name_to_id.get(qbo_name)
            return show_data.model_copy(update={"qbo_show_name": qbo_name, "qbo_show_id": qbo_id})
        warnings.append(
            f"Row {row_display_num} ('{show_data.title}'): There are no matching QBO class Names to the show "
            f"'{show_data.qbo_show_name}' specified. The show has been saved with blank QBO Name for you to update later, "
            "by editing the show and selecting the correct QBO Account Name from the dropdown."
        )
        return show_data.model_copy(update={"qbo_show_name": None, "qbo_show_id": None})
    if show_data.qbo_show_id is not None:
        return show_data.model_copy(update={"qbo_show_name": None, "qbo_show_id": None})
    return show_data


@app.post("/podcasts", response_model=Show, status_code=status.HTTP_201_CREATED)
def create_podcast(show_data: ShowCreate, admin: User = Depends(get_admin_user)):
    print('Admin object:', admin)
    print('Admin type:', type(admin))
    print('Admin name:', admin.get('name') if hasattr(admin, 'get') else getattr(admin, 'name', None))
    print('Admin id:', admin.get('id') if hasattr(admin, 'get') else getattr(admin, 'id', None))
    client = SqlClient()
    new_show, error = client.create_podcast(show_data, user_name=admin.get('name'), user_id=admin.get('id'))
    if error:
        raise HTTPException(status_code=400, detail=str(error))
    invalidate_ledger_cache()
    return new_show

@app.post("/podcasts/bulk-import", status_code=status.HTTP_200_OK)
def bulk_create_podcasts(shows_data: List[ShowCreate], admin: User = Depends(get_admin_user)):
    client = SqlClient()
    valid_qbo_names, qbo_name_to_id, _ = _load_qbo_name_maps(client)
    successful_imports = 0
    failed_imports = 0
    errors = []
    warnings: List[str] = []
    for i, show_data in enumerate(shows_data):
        if not show_data.title or not show_data.title.strip():
            failed_imports += 1
            errors.append(f"Row {i + 2}: Show title is missing or empty and is required.")
            continue
        payload = _resolve_qbo_payload_for_import(show_data, valid_qbo_names, qbo_name_to_id, i + 2, warnings)
        new_show, error = client.create_podcast(payload, user_name=admin.get('name'), user_id=admin.get('id'))
        if error:
            failed_imports += 1
            errors.append(f"Row {i + 2} ('{show_data.title}'): {str(error)}")
        else:
            successful_imports += 1

    message = "Bulk import process completed."
    if failed_imports > 0 and successful_imports == 0:
        message = "All show imports failed. Please check the errors below."

    if successful_imports > 0:
        invalidate_ledger_cache()

    result: Dict[str, Any] = {
        "message": message,
        "total": len(shows_data),
        "successful": successful_imports,
        "failed": failed_imports,
        "errors": errors,
    }
    if warnings:
        result["warnings"] = warnings
    return result

@app.post("/podcasts/check-duplicates")
def check_duplicates(shows_data: List[ShowCreate], admin: User = Depends(get_admin_user)):
    """Check for duplicate show titles before import"""
    client = SqlClient()
    
    # Extract titles from the shows data
    titles = [show.title for show in shows_data if show.title and show.title.strip()]
    
    if not titles:
        return {"duplicates": [], "message": "No valid titles to check"}
    
    # Check for existing shows with these titles
    existing_shows, error = client.check_duplicate_shows_bulk(titles)
    if error:
        raise HTTPException(status_code=500, detail=str(error))
    
    # Create a mapping of existing shows by title (case-insensitive)
    existing_by_title = {}
    for show in existing_shows:
        existing_by_title[show['title'].lower()] = show
    
    # Build response with duplicate information
    duplicates = []
    for show_data in shows_data:
        if show_data.title and show_data.title.strip():
            title_lower = show_data.title.lower()
            if title_lower in existing_by_title:
                duplicates.append({
                    "title": show_data.title,
                    "exists": True,
                    "existing_show": existing_by_title[title_lower]
                })
            else:
                duplicates.append({
                    "title": show_data.title,
                    "exists": False,
                    "existing_show": None
                })
    
    return {
        "duplicates": duplicates,
        "total_checked": len(duplicates),
        "duplicates_found": len([d for d in duplicates if d["exists"]]),
        "message": f"Found {len([d for d in duplicates if d['exists']])} duplicate(s) out of {len(duplicates)} shows"
    }

@app.post("/podcasts/check-duplicate")
def check_single_duplicate(show_data: ShowCreate, current_user: User = Depends(get_current_active_user)):
    """Check for duplicate show title for real-time validation"""
    client = SqlClient()
    
    if not show_data.title or not show_data.title.strip():
        return {"exists": False, "existing_show": None, "is_archived": False}
    
    # Check for existing show with this title, including archive status
    existing_show, error = client.check_duplicate_show_with_archive_status(show_data.title)
    if error:
        raise HTTPException(status_code=500, detail=str(error))
    
    is_archived = existing_show and existing_show.get('is_archived', False)
    
    return {
        "exists": existing_show is not None,
        "existing_show": existing_show,
        "is_archived": is_archived
    }

@app.post("/podcasts/bulk-import-with-actions")
def bulk_create_podcasts_with_actions(
    shows_data: List[ShowCreate],
    actions: List[dict],  # [{"title": "Show Name", "action": "create|update|skip"}]
    admin: User = Depends(get_admin_user),
):
    """Bulk import with user-specified actions for duplicates"""
    client = SqlClient()

    valid_qbo_names, qbo_name_to_id, _ = _load_qbo_name_maps(client)

    # Create a mapping of actions by title
    action_map = {action["title"]: action["action"] for action in actions}

    successful_imports = 0
    failed_imports = 0
    updated_imports = 0
    skipped_imports = 0
    errors = []
    warnings = []

    for i, show_data in enumerate(shows_data):
        if not show_data.title or not show_data.title.strip():
            failed_imports += 1
            errors.append(f"Row {i + 2}: Show title is missing or empty and is required.")
            continue

        action = action_map.get(show_data.title, "create")

        if action == "skip":
            skipped_imports += 1
            continue

        # QBO: match CSV to allclass (same as bulk-import / manual create)
        payload = _resolve_qbo_payload_for_import(show_data, valid_qbo_names, qbo_name_to_id, i + 2, warnings)

        if action == "update":
            # Check if show exists for update
            existing_show, error = client.check_duplicate_show(show_data.title)
            if error:
                failed_imports += 1
                errors.append(f"Row {i + 2} ('{show_data.title}'): Error checking for existing show - {str(error)}")
                continue

            if not existing_show:
                failed_imports += 1
                errors.append(f"Row {i + 2} ('{show_data.title}'): Show not found for update")
                continue

            # Update existing show
            updated_show, error = client.update_podcast(existing_show["id"], payload)
            if error:
                failed_imports += 1
                errors.append(f"Row {i + 2} ('{show_data.title}'): Update failed - {str(error)}")
            else:
                updated_imports += 1

        elif action == "create":
            # Create new show (with duplicate check)
            new_show, error = client.create_podcast(payload, user_name=admin.get("name"), user_id=admin.get("id"))
            if error:
                failed_imports += 1
                errors.append(f"Row {i + 2} ('{show_data.title}'): {str(error)}")
            else:
                successful_imports += 1

    message = f"Bulk import completed. Created: {successful_imports}, Updated: {updated_imports}, Skipped: {skipped_imports}, Failed: {failed_imports}"

    if successful_imports > 0 or updated_imports > 0:
        invalidate_ledger_cache()

    return {
        "message": message,
        "total": len(shows_data),
        "successful": successful_imports,
        "updated": updated_imports,
        "skipped": skipped_imports,
        "failed": failed_imports,
        "errors": errors,
        "warnings": warnings,
    }

class ShowFilterParams:
    def __init__(
        self,
        title: Optional[str] = None,
        media_type: Optional[MediaType] = None,
        rate_card: Optional[bool] = None,
        relationship_level: Optional[RelationshipLevel] = None,
        show_type: Optional[ShowType] = None,
        has_sponsorship_revenue: Optional[bool] = None,
        has_non_evergreen_revenue: Optional[bool] = None,
        has_myco_ledger_access: Optional[bool] = None,
        has_branded_revenue: Optional[bool] = None,
        has_marketing_revenue: Optional[bool] = None,
        has_web_mgmt_revenue: Optional[bool] = None,
        is_original: Optional[bool] = None,
    ):
        self.title = title
        self.media_type = media_type
        self.rate_card = rate_card
        self.relationship_level = relationship_level
        self.show_type = show_type
        self.has_sponsorship_revenue = has_sponsorship_revenue
        self.has_non_evergreen_revenue = has_non_evergreen_revenue
        self.has_myco_ledger_access = has_myco_ledger_access
        self.has_branded_revenue = has_branded_revenue
        self.has_marketing_revenue = has_marketing_revenue
        self.has_web_mgmt_revenue = has_web_mgmt_revenue
        self.is_original = is_original

@app.get("/podcasts", response_model=list[Show])
def get_all_podcasts(current_user: User = Depends(get_current_active_user)):
    if current_user.get("role") not in ("admin", "internal", "internal_full_access", "internal_show_access"):
        raise HTTPException(status_code=403, detail="Not enough permissions")
    client = SqlClient()
    return client.get_all_podcasts()

@app.get("/podcasts/filter", response_model=list[Show])
def filter_podcasts(filters: ShowFilterParams = Depends(), current_user: User = Depends(get_current_active_user)):
    if current_user.get("role") not in ("admin", "internal", "internal_full_access", "internal_show_access"):
        raise HTTPException(status_code=403, detail="Not enough permissions")
    client = SqlClient()
    filter_dict = {k: v for k, v in vars(filters).items() if v is not None}
    podcasts, error = client.filter_podcasts(filter_dict)
    if error:
        raise HTTPException(status_code=400, detail=str(error))
    return podcasts

@app.get("/podcasts/archived", response_model=list[Show])
def get_archived_podcasts(current_user: User = Depends(get_current_active_user)):
    """Get all archived shows - Admin and Internal users only"""
    # Check if user is admin or internal
    if current_user.get("role") not in ("admin", "internal", "internal_full_access", "internal_show_access"):
        raise HTTPException(status_code=403, detail="Not enough permissions")
    
    client = SqlClient()
    archived_shows, error = client.get_archived_podcasts()
    if error:
        raise HTTPException(status_code=500, detail=str(error))
    return archived_shows

@app.get("/podcasts/{show_id}", response_model=Show)
def get_podcast(show_id: str, current_user: User = Depends(get_current_active_user)):
    if current_user.get("role") not in ("admin", "internal", "internal_full_access", "internal_show_access"):
        raise HTTPException(status_code=403, detail="Not enough permissions")
    client = SqlClient()
    show, error = client.get_podcast_by_id(show_id)
    if error or not show:
        raise HTTPException(status_code=404, detail="Podcast not found")
    return show

@app.put("/podcasts/{show_id}", response_model=Show)
def update_podcast(show_id: str, show_data: ShowUpdate, admin: User = Depends(get_admin_user)):
    client = SqlClient()
    updated_show, error = client.update_podcast(show_id, show_data)
    if error:
        if "No update data provided" in str(error):
            raise HTTPException(status_code=400, detail=str(error))
        raise HTTPException(status_code=404, detail=str(error))
    invalidate_ledger_cache()
    return updated_show

class BulkDeleteRequest(BaseModel):
    show_ids: List[str]

@app.delete("/podcasts/bulk-delete", status_code=status.HTTP_200_OK)
def bulk_delete_podcasts(request: BulkDeleteRequest, admin: User = Depends(get_admin_user)):
    """Bulk delete multiple shows by their IDs"""
    if not request.show_ids:
        raise HTTPException(status_code=400, detail="No show IDs provided")
    
    client = SqlClient()
    for show_id in request.show_ids:
        _cancel_active_notices_for_show(show_id, admin.get("id"))
    results = client.bulk_delete_podcasts(request.show_ids)
    invalidate_ledger_cache()
    
    return {
        "message": f"Successfully deleted {results['successful']} shows",
        "total_requested": len(request.show_ids),
        "successful": results['successful'],
        "failed": results['failed'],
        "errors": results['errors']
    }

@app.delete("/podcasts/{show_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_podcast(show_id: str, admin: User = Depends(get_admin_user)):
    _cancel_active_notices_for_show(show_id, admin.get("id"))
    client = SqlClient()
    success, error = client.delete_podcast(show_id)
    if not success:
        raise HTTPException(status_code=404, detail=error)
    invalidate_ledger_cache()

# Archive endpoints
@app.patch("/podcasts/{show_id}/archive", response_model=Show)
def archive_podcast(show_id: str, admin: User = Depends(get_admin_user)):
    """Archive a show - Admin only"""
    # Check if user is admin
    if admin.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Only admin users can archive shows")
    
    client = SqlClient()
    archived_show, error = client.archive_podcast(show_id, admin.get("name"), admin.get("id"))
    if error:
        raise HTTPException(status_code=400, detail=str(error))
    _cancel_active_notices_for_show(show_id, admin.get("id"))
    invalidate_ledger_cache()
    return archived_show

@app.patch("/podcasts/{show_id}/unarchive", response_model=Show)
def unarchive_podcast(show_id: str, admin: User = Depends(get_admin_user)):
    """Unarchive a show - Admin only"""
    # Check if user is admin
    if admin.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Only admin users can unarchive shows")
    
    client = SqlClient()
    unarchived_show, error = client.unarchive_podcast(show_id, admin.get("name"), admin.get("id"))
    if error:
        raise HTTPException(status_code=400, detail=str(error))
    invalidate_ledger_cache()
    return unarchived_show

class BulkArchiveRequest(BaseModel):
    show_ids: List[str]

@app.patch("/podcasts/bulk-archive", response_model=dict)
def bulk_archive_podcasts(request: BulkArchiveRequest, admin: User = Depends(get_admin_user)):
    """Bulk archive multiple shows - Admin only"""
    # Check if user is admin
    if admin.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Only admin users can archive shows")
    
    if not request.show_ids:
        raise HTTPException(status_code=400, detail="No show IDs provided")
    
    client = SqlClient()
    result, error = client.bulk_archive_podcasts(request.show_ids, admin.get("name"), admin.get("id"))
    if error:
        raise HTTPException(status_code=400, detail=str(error))
    _cancel_active_notices_for_shows(request.show_ids, admin.get("id"))
    invalidate_ledger_cache()
    return result

@app.patch("/podcasts/bulk-unarchive", response_model=dict)
def bulk_unarchive_podcasts(request: BulkArchiveRequest, admin: User = Depends(get_admin_user)):
    """Bulk unarchive multiple shows - Admin only"""
    # Check if user is admin
    if admin.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Only admin users can unarchive shows")
    
    if not request.show_ids:
        raise HTTPException(status_code=400, detail="No show IDs provided")
    
    client = SqlClient()
    result, error = client.bulk_unarchive_podcasts(request.show_ids, admin.get("name"), admin.get("id"))
    if error:
        raise HTTPException(status_code=400, detail=str(error))
    invalidate_ledger_cache()
    return result

@app.get("/partners/me/podcasts", response_model=list[Show])
def get_my_podcasts(current_user: User = Depends(get_current_active_user)):
    if current_user.get("role") != "partner":
        raise HTTPException(status_code=403, detail="Not enough permissions")
    client = SqlClient()
    podcasts, error = client.get_podcasts_for_partner(current_user.get("id"))
    if error:
        raise HTTPException(status_code=500, detail=str(error))
    return podcasts or []


@app.get("/partners/{partner_id}/podcasts", response_model=list[Show])
def get_podcasts_for_partner(partner_id: str, admin: User = Depends(get_admin_user)):
    client = SqlClient()
    podcasts, error = client.get_podcasts_for_partner(partner_id)
    if error:
        raise HTTPException(status_code=500, detail=str(error))
    return podcasts or []


@app.get("/vendors")
def get_vendors(admin: User = Depends(get_admin_user)):
    client = SqlClient()
    vendors = client.get_all_vendors()
    return vendors

@app.get("/qbo/allclass")
def list_allclass(admin: User = Depends(get_admin_user)) -> List[Dict[str, Any]]:
    client = SqlClient()
    items, err = client.get_allclass_items()
    if err:
        raise HTTPException(status_code=500, detail=str(err))
    return items


@app.get("/show-form-options")
def get_show_form_options(current_user: User = Depends(get_current_active_user)) -> Dict[str, List[str]]:
    """Return all dropdown options for the Create/Edit Show form. Single source of truth from server."""
    return {
        "genres": sorted(set(GENRE_MAP.values())),
        "showTypes": list(SHOW_TYPE_MAP.values()),
        "mediaTypes": ["Video", "Audio", "Both"],
        "relationshipLevels": ["Strong", "Medium", "Weak"],
        "rankingCategories": list(RANKING_CATEGORY_MAP.values()),
        "cadences": ["Daily", "Weekly", "Biweekly", "Monthly", "Ad hoc", "Seasonal", "Inactive"],
        "subnetworks": [
            "CONmunity",
            "Crowd Network",
            "Evergreen",
            "Next Chapter",
            "Osiris Media",
            "Sound Talent Media",
        ],
        "showStatuses": ["Active", "Inactive", "No longer on network"],
        "ageDemographics": AGE_DEMOGRAPHICS,
    }


# ----------------------
# Split management (reads allowed for all logged-in roles, writes admin-only)
# ----------------------
@app.get("/split-management/shows")
def get_split_shows(current_user: User = Depends(get_current_active_user)):
    client = SqlClient()
    shows, error = client.get_split_shows_for_user(current_user)
    if error:
        raise HTTPException(status_code=500, detail=error)
    return shows

@app.get("/split-management/vendors/{show_qbo_id}")
def get_split_vendors(show_qbo_id: int, current_user: User = Depends(get_current_active_user)):
    client = SqlClient()
    vendors, error = client.get_split_vendors_for_show(show_qbo_id)
    if error:
        raise HTTPException(status_code=500, detail=error)
    return vendors

@app.get("/split-management/splits")
def get_splits_for_show_vendor(show_qbo_id: int, vendor_qbo_id: int, current_user: User = Depends(get_current_active_user)):
    client = SqlClient()
    splits, error = client.get_splits(show_qbo_id, vendor_qbo_id)
    if error:
        raise HTTPException(status_code=500, detail=error)
    return splits

@app.post("/split-management/splits", response_model=Split, status_code=status.HTTP_201_CREATED)
def create_new_split(split_data: SplitCreate, admin: User = Depends(get_admin_user)):
    client = SqlClient()
    new_split, error = client.create_split(split_data)
    if error:
        raise HTTPException(status_code=500, detail=error)
    invalidate_ledger_cache()
    return new_split

# NEW: delete a split (admin only)
@app.delete("/split-management/splits/{split_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_split(split_id: int, admin: User = Depends(get_admin_user)):
    client = SqlClient()
    ok, err = client.delete_split(split_id)
    if not ok:
        if err and "not found" in err.lower():
            raise HTTPException(status_code=404, detail=err)
        raise HTTPException(status_code=500, detail=err or "Failed to delete split")
    invalidate_ledger_cache()
    return Response(status_code=status.HTTP_204_NO_CONTENT)

# NEW: get all split history (admin only)
@app.get("/split-management/split-history")
def get_all_split_history(admin: User = Depends(get_admin_user)):
    client = SqlClient()
    splits, error = client.get_all_split_history()
    if error:
        raise HTTPException(status_code=500, detail=error)
    return splits

# NEW: update a split (admin only)
@app.put("/split-management/splits/{split_id}")
def update_split(split_id: int, split_data: SplitCreate, admin: User = Depends(get_admin_user)):
    client = SqlClient()
    updated_split, error = client.update_split(split_id, split_data)
    if error:
        if "not found" in error.lower():
            raise HTTPException(status_code=404, detail=error)
        raise HTTPException(status_code=500, detail=error)
    invalidate_ledger_cache()
    return updated_split

# ----------------------
# Catalog for mapping (admin only)
# ----------------------
@app.get("/split-management/catalog/all-shows")
def catalog_all_shows(admin: User = Depends(get_admin_user)):
    client = SqlClient()
    shows, error = client.get_catalog_all_shows()
    if error:
        raise HTTPException(status_code=500, detail=error)
    return shows

@app.get("/split-management/catalog/all-vendors")
def catalog_all_vendors(admin: User = Depends(get_admin_user)):
    client = SqlClient()
    vendors, error = client.get_catalog_all_vendors()
    if error:
        raise HTTPException(status_code=500, detail=error)
    return vendors

# ----------------------
# Ledger
# ----------------------
@app.get("/ledger")
def get_ledger(current_user: dict = Depends(get_current_active_user)):
    cache_key = _ledger_cache_key(current_user, "ledger")
    cached = _cache_get(cache_key)
    if cached is not None:
        response = FastJSONResponse(content=_normalize_json_content(cached))
        response.headers["X-Cache"] = "HIT"
        response.headers["X-Cache-Key"] = cache_key
        return response

    client = SqlClient()
    if current_user.get("role") in ("admin", "internal", "internal_full_access"):
        ledger, error = client.get_ledger()
    else:
        ledger, error = client.get_ledger(current_user.get("mapped_vendor_qbo_id"))
    if error:
        raise HTTPException(status_code=500, detail=str(error))
    payload = ledger or []
    _cache_set(cache_key, payload)
    response = FastJSONResponse(content=_normalize_json_content(payload))
    response.headers["X-Cache"] = "MISS"
    response.headers["X-Cache-Key"] = cache_key
    return response

@app.get("/partner_payouts")
def get_partners_payouts(current_user: dict = Depends(get_current_active_user)):
    cache_key = _ledger_cache_key(current_user, "partner_payouts")
    cached = _cache_get(cache_key)
    if cached is not None:
        response = FastJSONResponse(content=_normalize_json_content(cached))
        response.headers["X-Cache"] = "HIT"
        response.headers["X-Cache-Key"] = cache_key
        return response

    try:
        client = SqlClient()
        if current_user.get("role") in ("admin", "internal", "internal_full_access"):
            partners_payouts, error = client.get_partner_payouts()
        else:
            partners_payouts, error = client.get_partner_payouts(current_user.get("mapped_vendor_qbo_id"))
        
        if error:
            # Log the error for debugging
            print(f"ERROR in get_partners_payouts: {error}")
            print(f"Error type: {type(error)}")
            import traceback
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=str(error))
        
        if partners_payouts is None:
            print("WARNING: get_partner_payouts returned None")
            _cache_set(cache_key, [])
            response = FastJSONResponse(content=[])
            response.headers["X-Cache"] = "MISS"
            response.headers["X-Cache-Key"] = cache_key
            return response
        
        payload = partners_payouts or []
        _cache_set(cache_key, payload)
        response = FastJSONResponse(content=_normalize_json_content(payload))
        response.headers["X-Cache"] = "MISS"
        response.headers["X-Cache-Key"] = cache_key
        return response
    except HTTPException:
        raise
    except Exception as e:
        # Log unexpected errors
        print(f"UNEXPECTED ERROR in get_partners_payouts: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# ----------------------
# Database Export/Import (Admin only - Developer Options)
# ----------------------
from database_import_export import DatabaseExporter, DatabaseImporter

class DatabaseImportResponse(BaseModel):
    success: bool
    message: str
    tables_affected: Optional[int] = None
    warnings: Optional[List[str]] = None
    executed_at: str

class DatabaseImportJobStartResponse(BaseModel):
    job_id: str
    status: str  # queued|running|succeeded|failed
    message: str
    queued_at: str

class DatabaseImportJobStatusResponse(BaseModel):
    job_id: str
    status: str  # queued|running|succeeded|failed
    message: str
    queued_at: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    result: Optional[DatabaseImportResponse] = None
    error: Optional[str] = None

class CacheRefreshResponse(BaseModel):
    success: bool
    message: str
    invalidated_prefixes: List[str]
    warmed_keys: List[str]
    executed_at: str
    cache_ttl_seconds: int
    next_refresh_at: str

class CacheRefreshStatusResponse(BaseModel):
    success: bool
    executed_at: Optional[str] = None
    cache_ttl_seconds: int
    next_refresh_at: Optional[str] = None
    backend: str

# In-memory import job registry (per-process).
_import_jobs: Dict[str, Dict[str, Any]] = {}
_cache_refresh_meta_memory: Dict[str, Any] = {}

def _cache_backend_name() -> str:
    return "redis" if _redis_client is not None else "in-memory"

def _to_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8")
    return str(value)

def _set_cache_refresh_meta(executed_at: datetime, ttl_seconds: int):
    next_refresh_at = (executed_at + timedelta(seconds=ttl_seconds)).isoformat()
    payload = {
        "executed_at": executed_at.isoformat(),
        "cache_ttl_seconds": ttl_seconds,
        "next_refresh_at": next_refresh_at,
    }
    _cache_refresh_meta_memory.update(payload)

    if _redis_client is not None:
        try:
            # Keep this outside ledger:* invalidation so admins can still read status.
            _redis_client.hset("cache_meta:ledger_refresh", mapping={
                "executed_at": payload["executed_at"],
                "cache_ttl_seconds": str(ttl_seconds),
                "next_refresh_at": payload["next_refresh_at"],
            })
        except Exception as e:
            print(f"WARNING: Failed to persist cache refresh metadata to Redis: {e}")

def _get_cache_refresh_meta() -> Dict[str, Any]:
    if _redis_client is not None:
        try:
            raw = _redis_client.hgetall("cache_meta:ledger_refresh")
            if raw:
                executed_at = _to_text(raw.get("executed_at") if "executed_at" in raw else raw.get(b"executed_at"))
                ttl_raw = _to_text(raw.get("cache_ttl_seconds") if "cache_ttl_seconds" in raw else raw.get(b"cache_ttl_seconds"))
                next_refresh_at = _to_text(raw.get("next_refresh_at") if "next_refresh_at" in raw else raw.get(b"next_refresh_at"))
                ttl_seconds = int(ttl_raw) if ttl_raw and ttl_raw.isdigit() else LEDGER_CACHE_TTL_SECONDS
                return {
                    "executed_at": executed_at,
                    "cache_ttl_seconds": ttl_seconds,
                    "next_refresh_at": next_refresh_at,
                }
        except Exception as e:
            print(f"WARNING: Failed to read cache refresh metadata from Redis: {e}")
    return dict(_cache_refresh_meta_memory)

def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        # stored as isoformat with timezone
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

async def _run_import_job(job_id: str, *, dump_bytes: bytes, filename: str, mode: str, confirm: Optional[str], admin: User):
    # Mark as running
    job = _import_jobs.get(job_id)
    if not job:
        return
    job["status"] = "running"
    job["started_at"] = datetime.now(timezone.utc).isoformat()
    job["message"] = f"Import running ({mode})"
    try:
        client = SqlClient()
        importer = DatabaseImporter(client)
        # Run blocking import in a worker thread to avoid blocking event loop.
        import asyncio
        # Hard timeout so a job can't get stuck "running" forever.
        # (docker-exec has its own timeouts, but this is an extra safety net.)
        result = await asyncio.wait_for(
            asyncio.to_thread(
                importer.import_dump_full_replace,
                dump_bytes,
                filename,
                mode,
                {"name": getattr(admin, "name", None), "email": getattr(admin, "email", None)},
                confirm,
            ),
            timeout=60 * 40,  # 40 minutes
        )
        job["status"] = "succeeded" if result.get("success") else "failed"
        job["finished_at"] = datetime.now(timezone.utc).isoformat()
        job["message"] = result.get("message") or "Import finished"
        job["result"] = result
        if result.get("success"):
            _devlog(admin.get("email"), "success", f"Database import finished ({mode})", result.get("message"))
            invalidate_ledger_cache()
        else:
            _devlog(admin.get("email"), "error", f"Database import finished with errors ({mode})", result.get("message"))
    except Exception as e:
        job["status"] = "failed"
        job["finished_at"] = datetime.now(timezone.utc).isoformat()
        job["message"] = "Import failed"
        job["error"] = str(e)[:800]
        _devlog(admin.get("email"), "error", "Database import failed", str(e)[:500])

@app.get("/admin/database/export")
async def export_database(
    admin: User = Depends(get_admin_user),
):
    """
    Export the current database as a SQL dump file.
    Admin only - requires developer options access.
    """
    try:
        _devlog(admin.get("email"), "command", "Database export started")
        client = SqlClient()
        exporter = DatabaseExporter(client)
        resp = exporter.export(admin)
        _devlog(admin.get("email"), "success", "Database export finished")
        return resp
    except Exception as e:
        _devlog(admin.get("email"), "error", "Database export failed", str(e)[:500])
        raise HTTPException(
            status_code=500,
            detail=f"Database export failed: {str(e)}"
        )

@app.post("/admin/database/import")
async def import_database(
    file: UploadFile = File(...),
    mode: str = Form("legacy_python"),
    confirm: Optional[str] = Form(None),
    admin: User = Depends(get_admin_user),
):
    """
    Import a SQL dump file to restore/replace the current database.
    Admin only - requires developer options access.
    
    WARNING: This will replace existing data!
    
    Note: Large imports may take several minutes. The timeout is set to 30 minutes.
    """
    if not file.filename.endswith('.sql'):
        raise HTTPException(
            status_code=400,
            detail="Invalid file type. Only .sql files are accepted."
        )
    
    try:
        invalidate_ledger_cache()
        _devlog(admin.get("email"), "command", f"Database import started: {file.filename}")
        client = SqlClient()
        importer = DatabaseImporter(client)

        # Legacy path: decode and run Python importer (slow, but works on some envs)
        if mode == "legacy_python":
            content = await file.read()
            sql_content = content.decode('utf-8')
            result = importer.import_dump(sql_content)
            _devlog(admin.get("email"), "success", "Database import finished (legacy_python)", result.get("message"))
            return DatabaseImportResponse(**result)

        # Fast full-replace modes can exceed proxy timeouts; run as an async job and return immediately.
        content = await file.read()
        job_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        _import_jobs[job_id] = {
            "job_id": job_id,
            "status": "queued",
            "message": f"Import queued ({mode})",
            "queued_at": now,
            "started_at": None,
            "finished_at": None,
            "result": None,
            "error": None,
            "admin_email": admin.get("email"),
            "filename": file.filename,
            "mode": mode,
        }
        # Kick off background task (non-blocking)
        import asyncio
        asyncio.create_task(_run_import_job(job_id, dump_bytes=content, filename=file.filename, mode=mode, confirm=confirm, admin=admin))
        return Response(
            content=DatabaseImportJobStartResponse(
                job_id=job_id,
                status="queued",
                message=f"Import started ({mode}). Track progress via job id.",
                queued_at=now,
            ).model_dump_json(),
            media_type="application/json",
            status_code=202,
        )
        
    except UnicodeDecodeError:
        raise HTTPException(
            status_code=400,
            detail="Invalid SQL file encoding. Please ensure the file is UTF-8 encoded."
        )
    except Exception as e:
        error_msg = str(e)
        _devlog(admin.get("email"), "error", "Database import failed", error_msg[:500])
        # Provide more helpful error messages
        if "timeout" in error_msg.lower() or "504" in error_msg:
            raise HTTPException(
                status_code=504,
                detail=f"Import timed out. The file may be too large. Try using the sync script instead, or split the import into smaller chunks. Error: {error_msg[:200]}"
            )
        raise HTTPException(
            status_code=500,
            detail=f"Database import failed: {error_msg[:500]}"
        )

@app.get("/admin/database/import/jobs/{job_id}", response_model=DatabaseImportJobStatusResponse)
async def get_import_job_status(
    job_id: str,
    email: str = Depends(get_token_email),
):
    job = _import_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Import job not found")
    # Basic isolation: only allow the same admin email to view their job
    if job.get("admin_email") and email and job.get("admin_email") != email:
        raise HTTPException(status_code=403, detail="Not allowed to view this import job")
    # If a job is "running" for too long (e.g. process got wedged), mark it failed.
    if (job.get("status") == "running") and not job.get("finished_at"):
        started_at = _parse_iso(job.get("started_at"))
        if started_at:
            elapsed_s = (datetime.now(timezone.utc) - started_at).total_seconds()
            if elapsed_s > 60 * 45:  # 45 minutes
                job["status"] = "failed"
                job["finished_at"] = datetime.now(timezone.utc).isoformat()
                job["message"] = "Import failed (stale job)"
                job["error"] = "Job exceeded maximum runtime. The server likely got stuck; please retry the import."

    result = job.get("result")
    return DatabaseImportJobStatusResponse(
        job_id=job_id,
        status=job.get("status") or "queued",
        message=job.get("message") or "",
        queued_at=job.get("queued_at") or "",
        started_at=job.get("started_at"),
        finished_at=job.get("finished_at"),
        result=(DatabaseImportResponse(**result) if isinstance(result, dict) else None),
        error=job.get("error"),
    )

@app.get("/admin/database/status")
async def get_database_status(
    admin: User = Depends(get_admin_user),
):
    """
    Get current database status and statistics.
    Admin only.
    """
    from config import DB_NAME
    
    client = SqlClient()
    
    try:
        _devlog(admin.get("email"), "info", "Database status requested")
        # Get database name from config instead of DATABASE() function
        database_name = DB_NAME
        
        # Get table information using the database name from config
        # Exclude views - only get BASE TABLE type
        tables_query = """
        SELECT 
            TABLE_NAME as table_name,
            TABLE_ROWS as row_count,
            ROUND(DATA_LENGTH / 1024 / 1024, 2) as data_size_mb,
            ROUND(INDEX_LENGTH / 1024 / 1024, 2) as index_size_mb,
            UPDATE_TIME as last_updated
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_ROWS DESC
        """
        
        # Use parameterized query to avoid SQL injection and handle database name properly
        tables, _, error = client._execute_query(tables_query, params=(database_name,), fetch='all')
        if error:
            # Fallback: try using SHOW TABLES if information_schema fails
            try:
                show_tables_query = "SHOW TABLES"
                tables_result, _, show_error = client._execute_query(show_tables_query, fetch='all')
                if show_error:
                    raise HTTPException(status_code=500, detail=f"Failed to get tables: {str(error)}")
                
                # If we can't get detailed info, return basic table list
                table_names = [list(t.values())[0] for t in (tables_result or [])]
                return {
                    "database_name": database_name,
                    "total_tables": len(table_names),
                    "total_views": 0,
                    "total_rows": 0,
                    "total_data_size_mb": 0,
                    "total_index_size_mb": 0,
                    "total_size_mb": 0,
                    "tables": [{"table_name": name, "row_count": 0, "data_size_mb": 0, "index_size_mb": 0, "last_updated": None} for name in table_names],
                    "checked_at": datetime.now(timezone.utc).isoformat(),
                    "warning": "Detailed table statistics unavailable. Using basic table list."
                }
            except Exception as fallback_error:
                raise HTTPException(status_code=500, detail=f"Failed to get database status: {str(error)}. Fallback also failed: {str(fallback_error)}")

        views_query = """
        SELECT COUNT(*) as total_views
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'VIEW'
        """
        view_count_rows, _, view_error = client._execute_query(views_query, params=(database_name,), fetch='all')
        total_views = 0
        if not view_error and view_count_rows:
            try:
                total_views = int((view_count_rows[0] or {}).get("total_views", 0) or 0)
            except Exception:
                total_views = 0
        
        # Calculate totals
        total_rows = sum(t.get('row_count', 0) or 0 for t in (tables or []))
        total_data_mb = sum(float(t.get('data_size_mb', 0) or 0) for t in (tables or []))
        total_index_mb = sum(float(t.get('index_size_mb', 0) or 0) for t in (tables or []))
        
        return {
            "database_name": database_name,
            "total_tables": len(tables or []),
            "total_views": total_views,
            "total_rows": total_rows,
            "total_data_size_mb": round(total_data_mb, 2),
            "total_index_size_mb": round(total_index_mb, 2),
            "total_size_mb": round(total_data_mb + total_index_mb, 2),
            "tables": tables or [],
            "checked_at": datetime.now(timezone.utc).isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get database status: {str(e)}"
        )

@app.post("/admin/cache/refresh", response_model=CacheRefreshResponse)
async def refresh_ledger_cache(
    warm: bool = True,
    admin: User = Depends(get_admin_user),
):
    """
    Manually invalidate Redis/in-memory ledger-related cache.
    Admin only. Optionally warms admin-scoped cache keys.
    """
    try:
        executed_at = datetime.now(timezone.utc)
        invalidate_ledger_cache()
        _set_cache_refresh_meta(executed_at, LEDGER_CACHE_TTL_SECONDS)
        warmed_keys: List[str] = []

        if warm:
            client = SqlClient()

            ledger, ledger_error = client.get_ledger()
            if ledger_error:
                raise HTTPException(status_code=500, detail=f"Failed to warm ledger cache: {ledger_error}")
            ledger_key = _ledger_cache_key(admin, "ledger")
            _cache_set(ledger_key, ledger or [])
            warmed_keys.append(ledger_key)

            payouts, payouts_error = client.get_partner_payouts()
            if payouts_error:
                raise HTTPException(status_code=500, detail=f"Failed to warm partner payouts cache: {payouts_error}")
            payouts_key = _ledger_cache_key(admin, "partner_payouts")
            _cache_set(payouts_key, payouts or [])
            warmed_keys.append(payouts_key)

        return CacheRefreshResponse(
            success=True,
            message="Ledger cache invalidated successfully." + (" Cache warmup completed." if warm else ""),
            invalidated_prefixes=["ledger:*", "partner_payouts:*"],
            warmed_keys=warmed_keys,
            executed_at=executed_at.isoformat(),
            cache_ttl_seconds=LEDGER_CACHE_TTL_SECONDS,
            next_refresh_at=(executed_at + timedelta(seconds=LEDGER_CACHE_TTL_SECONDS)).isoformat(),
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to refresh cache: {str(e)}"
        )

@app.get("/admin/cache/refresh/status", response_model=CacheRefreshStatusResponse)
async def get_cache_refresh_status(
    admin: User = Depends(get_admin_user),
):
    meta = _get_cache_refresh_meta()
    ttl_seconds = int(meta.get("cache_ttl_seconds") or LEDGER_CACHE_TTL_SECONDS)
    return CacheRefreshStatusResponse(
        success=True,
        executed_at=meta.get("executed_at"),
        cache_ttl_seconds=ttl_seconds,
        next_refresh_at=meta.get("next_refresh_at"),
        backend=_cache_backend_name(),
    )


from notice_routes import register_notice_routes
register_notice_routes(app, get_notices_manager, get_current_active_user)

from twilio_webhook_routes import router as twilio_webhook_router
app.include_router(twilio_webhook_router)

from inbox_routes import register_inbox_routes
register_inbox_routes(app, get_current_active_user, get_admin_user)

from staff_directory_routes import register_staff_directory_routes
register_staff_directory_routes(app, get_admin_or_internal_user, get_admin_user)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)