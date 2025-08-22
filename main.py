import uvicorn
from fastapi import FastAPI, Depends, HTTPException, status, Response
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from typing import Optional, List
from datetime import datetime, timezone
import uuid

from fastapi.middleware.cors import CORSMiddleware

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
    PodcastIn,
    UserListItem,
    UserUpdate,
)
from sqlclient import SqlClient
from auth import create_access_token, verify_password, get_password_hash
from config import SECRET_KEY, ALGORITHM

app = FastAPI(
    title="Evergreen Podcasts API",
    description="API for managing podcasts and partners with JWT authentication.",
    version="2.0.0",
)

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
async def get_current_user(token: str = Depends(oauth2_scheme)):
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

async def get_current_active_user(current_user: User = Depends(get_current_user)):
    return current_user

async def get_admin_user(current_user: User = Depends(get_current_active_user)):
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Not enough permissions")
    return current_user

# ----------------------
# Auth endpoints
# ----------------------
@app.post("/login")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    client = SqlClient()
    user, _ = client.get_user_by_email(email=form_data.username)
    if not user or not verify_password(form_data.password, user.get("password_hash")):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = create_access_token(data={"sub": user.get("email")})
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/users/me", response_model=User)
async def read_users_me(current_user: User = Depends(get_current_active_user)):
    return current_user

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
        user_data.role,  # "admin" | "partner" | "internal"
        datetime.now(timezone.utc),
        user_data.mapped_vendor_qbo_id,
    )
    _, _, error = client._execute_query(sql, values, is_transaction=True)
    if error:
        raise HTTPException(status_code=500, detail="Error inserting user into DB")
    return {
        "id": user_id,
        "name": user_data.name,
        "email": user_data.email,
        "role": user_data.role,
        "mapped_vendor_qbo_id": user_data.mapped_vendor_qbo_id,
    }

@app.get("/users", response_model=List[UserListItem])
def list_users(admin: User = Depends(get_admin_user)):
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
    if "mapped_vendor_qbo_id" in payload.model_fields_set:
        update_kwargs["mapped_vendor_qbo_id"] = payload.mapped_vendor_qbo_id
    if payload.password:
        update_kwargs["password_hash"] = get_password_hash(payload.password)

    ok, e3 = client.update_user(user_id=user_id, **update_kwargs)
    if not ok:
        raise HTTPException(status_code=500, detail=str(e3))

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
# Shows (READ: admin+internal; WRITE: admin)
# ----------------------
@app.post("/podcasts", response_model=Show, status_code=status.HTTP_201_CREATED)
def create_podcast(show_data: ShowCreate, admin: User = Depends(get_admin_user)):
    client = SqlClient()
    new_show, error = client.create_podcast(show_data)
    if error:
        raise HTTPException(status_code=400, detail=str(error))
    return new_show

@app.post("/podcasts/bulk-import", status_code=status.HTTP_200_OK)
def bulk_create_podcasts(shows_data: List[ShowCreate], admin: User = Depends(get_admin_user)):
    client = SqlClient()
    successful_imports = 0
    failed_imports = 0
    errors = []
    for i, show_data in enumerate(shows_data):
        if not show_data.title or not show_data.title.strip():
            failed_imports += 1
            errors.append(f"Row {i + 2}: Show title is missing or empty and is required.")
            continue
        new_show, error = client.create_podcast(show_data)
        if error:
            failed_imports += 1
            errors.append(f"Row {i + 2} ('{show_data.title}'): {str(error)}")
        else:
            successful_imports += 1

    message = "Bulk import process completed."
    if failed_imports > 0 and successful_imports == 0:
        message = "All show imports failed. Please check the errors below."

    return {
        "message": message,
        "total": len(shows_data),
        "successful": successful_imports,
        "failed": failed_imports,
        "errors": errors,
    }

class ShowFilterParams:
    def __init__(
        self,
        title: Optional[str] = None,
        media_type: Optional[MediaType] = None,
        tentpole: Optional[bool] = None,
        relationship_level: Optional[RelationshipLevel] = None,
        show_type: Optional[ShowType] = None,
        has_sponsorship_revenue: Optional[bool] = None,
        has_non_evergreen_revenue: Optional[bool] = None,
        requires_partner_access: Optional[bool] = None,
        has_branded_revenue: Optional[bool] = None,
        has_marketing_revenue: Optional[bool] = None,
        has_web_mgmt_revenue: Optional[bool] = None,
        is_original: Optional[bool] = None,
    ):
        self.title = title
        self.media_type = media_type
        self.tentpole = tentpole
        self.relationship_level = relationship_level
        self.show_type = show_type
        self.has_sponsorship_revenue = has_sponsorship_revenue
        self.has_non_evergreen_revenue = has_non_evergreen_revenue
        self.requires_partner_access = requires_partner_access
        self.has_branded_revenue = has_branded_revenue
        self.has_marketing_revenue = has_marketing_revenue
        self.has_web_mgmt_revenue = has_web_mgmt_revenue
        self.is_original = is_original

@app.get("/podcasts", response_model=list[Show])
def get_all_podcasts(current_user: User = Depends(get_current_active_user)):
    if current_user.get("role") not in ("admin", "internal"):
        raise HTTPException(status_code=403, detail="Not enough permissions")
    client = SqlClient()
    return client.get_all_podcasts()

@app.get("/podcasts/filter", response_model=list[Show])
def filter_podcasts(filters: ShowFilterParams = Depends(), current_user: User = Depends(get_current_active_user)):
    if current_user.get("role") not in ("admin", "internal"):
        raise HTTPException(status_code=403, detail="Not enough permissions")
    client = SqlClient()
    filter_dict = {k: v for k, v in vars(filters).items() if v is not None}
    podcasts, error = client.filter_podcasts(filter_dict)
    if error:
        raise HTTPException(status_code=400, detail=str(error))
    return podcasts

@app.get("/podcasts/{show_id}", response_model=Show)
def get_podcast(show_id: str, current_user: User = Depends(get_current_active_user)):
    if current_user.get("role") not in ("admin", "internal"):
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
    return updated_show

@app.delete("/podcasts/{show_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_podcast(show_id: str, admin: User = Depends(get_admin_user)):
    client = SqlClient()
    success, error = client.delete_podcast(show_id)
    if not success:
        raise HTTPException(status_code=404, detail=error)

@app.get("/vendors")
def get_vendors(admin: User = Depends(get_admin_user)):
    client = SqlClient()
    vendors = client.get_all_vendors()
    return vendors

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
    return new_split

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
async def get_ledger(current_user: dict = Depends(get_current_active_user)):
    client = SqlClient()
    if current_user.get("role") in ("admin", "internal"):
        ledger, error = client.get_ledger()
    else:
        ledger, error = client.get_ledger(current_user.get("mapped_vendor_qbo_id"))
    if error:
        raise HTTPException(status_code=500, detail=str(error))
    return ledger

@app.get("/partner_payouts")
async def get_partners_payouts(current_user: dict = Depends(get_current_active_user)):
    client = SqlClient()
    if current_user.get("role") in ("admin", "internal"):
        partners_payouts, error = client.get_partner_payouts()
    else:
        partners_payouts, error = client.get_partner_payouts(current_user.get("mapped_vendor_qbo_id"))
    if error:
        raise HTTPException(status_code=500, detail=str(error))
    return partners_payouts


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
