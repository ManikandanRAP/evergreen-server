import uvicorn
from fastapi import FastAPI, Depends, HTTPException, status, Response, UploadFile, File
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.responses import StreamingResponse
from jose import JWTError, jwt
from typing import Optional, List, Any, Dict
from datetime import datetime, timezone
import uuid
import io
import re

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
    UserSettingsUpdate,  # Import for user settings
    FeedbackCreate, # Import Feedback models
    Feedback,
    BaseModel,
    UsernameCheckRequest,
    UsernameCheckResponse,
)
from sqlclient import SqlClient
from auth import create_access_token, verify_password, get_password_hash
from config import SECRET_KEY, ALGORITHM

app = FastAPI(
    title="Evergreen Podcasts API",
    description="API for managing podcasts and partners with JWT authentication.",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
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

async def get_admin_or_internal_user(current_user: User = Depends(get_current_active_user)):
    if current_user.get("role") not in ("admin", "internal_full_access", "internal_show_access"):
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
    # Populate mapped_vendor_name if mapped_vendor_qbo_id exists
    if current_user.get("mapped_vendor_qbo_id"):
        client = SqlClient()
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
        
        vid = current_user.get("mapped_vendor_qbo_id")
        if vid is not None:
            current_user["mapped_vendor_name"] = vendor_map.get(int(vid))
    
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
async def get_user_settings(current_user: User = Depends(get_current_active_user)):
    """Get the current user's settings"""
    client = SqlClient()
    settings, error = client.get_user_settings(current_user.get("id"))
    if error:
        raise HTTPException(status_code=500, detail=str(error))
    # Return empty dict if no settings exist yet
    return settings or {}

@app.put("/users/me/settings")
async def update_user_settings(
    settings_update: UserSettingsUpdate,
    current_user: User = Depends(get_current_active_user)
):
    """Update the current user's settings"""
    client = SqlClient()
    success, error = client.update_user_settings(current_user.get("id"), settings_update.settings)
    if not success:
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

@app.get("/feedbacks", response_model=List[Feedback])
def get_all_feedbacks(admin: User = Depends(get_admin_user)):
    client = SqlClient()
    # Unpack the tuple returned by the client
    feedbacks, error = client.get_all_feedbacks()
    if error:
        raise HTTPException(status_code=500, detail=str(error))
    # Return only the data, not the tuple
    return feedbacks

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
        new_show, error = client.create_podcast(show_data, user_name=admin.get('name'), user_id=admin.get('id'))
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
    admin: User = Depends(get_admin_user)
):
    """Bulk import with user-specified actions for duplicates"""
    client = SqlClient()
    
    # Create a mapping of actions by title
    action_map = {action["title"]: action["action"] for action in actions}
    
    successful_imports = 0
    failed_imports = 0
    updated_imports = 0
    skipped_imports = 0
    errors = []
    
    for i, show_data in enumerate(shows_data):
        if not show_data.title or not show_data.title.strip():
            failed_imports += 1
            errors.append(f"Row {i + 2}: Show title is missing or empty and is required.")
            continue
        
        action = action_map.get(show_data.title, "create")
        
        if action == "skip":
            skipped_imports += 1
            continue
        
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
            updated_show, error = client.update_podcast(existing_show['id'], show_data)
            if error:
                failed_imports += 1
                errors.append(f"Row {i + 2} ('{show_data.title}'): Update failed - {str(error)}")
            else:
                updated_imports += 1
        
        elif action == "create":
            # Create new show (with duplicate check)
            new_show, error = client.create_podcast(show_data, user_name=admin.get('name'), user_id=admin.get('id'))
            if error:
                failed_imports += 1
                errors.append(f"Row {i + 2} ('{show_data.title}'): {str(error)}")
            else:
                successful_imports += 1
    
    total_processed = successful_imports + updated_imports + skipped_imports + failed_imports
    
    message = f"Bulk import completed. Created: {successful_imports}, Updated: {updated_imports}, Skipped: {skipped_imports}, Failed: {failed_imports}"
    
    return {
        "message": message,
        "total": len(shows_data),
        "successful": successful_imports,
        "updated": updated_imports,
        "skipped": skipped_imports,
        "failed": failed_imports,
        "errors": errors,
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
        requires_partner_access: Optional[bool] = None,
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
        self.requires_partner_access = requires_partner_access
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
    return updated_show

class BulkDeleteRequest(BaseModel):
    show_ids: List[str]

@app.delete("/podcasts/bulk-delete", status_code=status.HTTP_200_OK)
def bulk_delete_podcasts(request: BulkDeleteRequest, admin: User = Depends(get_admin_user)):
    """Bulk delete multiple shows by their IDs"""
    if not request.show_ids:
        raise HTTPException(status_code=400, detail="No show IDs provided")
    
    client = SqlClient()
    results = client.bulk_delete_podcasts(request.show_ids)
    
    return {
        "message": f"Successfully deleted {results['successful']} shows",
        "total_requested": len(request.show_ids),
        "successful": results['successful'],
        "failed": results['failed'],
        "errors": results['errors']
    }

@app.delete("/podcasts/{show_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_podcast(show_id: str, admin: User = Depends(get_admin_user)):
    client = SqlClient()
    success, error = client.delete_podcast(show_id)
    if not success:
        raise HTTPException(status_code=404, detail=error)

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
    return result

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

# NEW: delete a split (admin only)
@app.delete("/split-management/splits/{split_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_split(split_id: int, admin: User = Depends(get_admin_user)):
    client = SqlClient()
    ok, err = client.delete_split(split_id)
    if not ok:
        if err and "not found" in err.lower():
            raise HTTPException(status_code=404, detail=err)
        raise HTTPException(status_code=500, detail=err or "Failed to delete split")
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
async def get_ledger(current_user: dict = Depends(get_current_active_user)):
    client = SqlClient()
    if current_user.get("role") in ("admin", "internal", "internal_full_access"):
        ledger, error = client.get_ledger()
    else:
        ledger, error = client.get_ledger(current_user.get("mapped_vendor_qbo_id"))
    if error:
        raise HTTPException(status_code=500, detail=str(error))
    return ledger

@app.get("/partner_payouts")
async def get_partners_payouts(current_user: dict = Depends(get_current_active_user)):
    client = SqlClient()
    if current_user.get("role") in ("admin", "internal", "internal_full_access"):
        partners_payouts, error = client.get_partner_payouts()
    else:
        partners_payouts, error = client.get_partner_payouts(current_user.get("mapped_vendor_qbo_id"))
    if error:
        raise HTTPException(status_code=500, detail=str(error))
    return partners_payouts


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

@app.get("/admin/database/export")
async def export_database(admin: User = Depends(get_admin_user)):
    """
    Export the current database as a SQL dump file.
    Admin only - requires developer options access.
    """
    try:
        client = SqlClient()
        exporter = DatabaseExporter(client)
        return exporter.export(admin)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database export failed: {str(e)}"
        )

@app.post("/admin/database/import", response_model=DatabaseImportResponse)
async def import_database(
    file: UploadFile = File(...),
    admin: User = Depends(get_admin_user)
):
    """
    Import a SQL dump file to restore/replace the current database.
    Admin only - requires developer options access.
    
    WARNING: This will replace existing data!
    """
    if not file.filename.endswith('.sql'):
        raise HTTPException(
            status_code=400,
            detail="Invalid file type. Only .sql files are accepted."
        )
    
    try:
        # Read the uploaded file
        content = await file.read()
        sql_content = content.decode('utf-8')
        
        # Import using the new module
        client = SqlClient()
        importer = DatabaseImporter(client)
        result = importer.import_dump(sql_content)
        
        return DatabaseImportResponse(**result)
        
    except UnicodeDecodeError:
        raise HTTPException(
            status_code=400,
            detail="Invalid SQL file encoding. Please ensure the file is UTF-8 encoded."
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database import failed: {str(e)}"
        )

@app.get("/admin/database/status")
async def get_database_status(admin: User = Depends(get_admin_user)):
    """
    Get current database status and statistics.
    Admin only.
    """
    from config import DB_NAME
    
    client = SqlClient()
    
    try:
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
        
        # Calculate totals
        total_rows = sum(t.get('row_count', 0) or 0 for t in (tables or []))
        total_data_mb = sum(float(t.get('data_size_mb', 0) or 0) for t in (tables or []))
        total_index_mb = sum(float(t.get('index_size_mb', 0) or 0) for t in (tables or []))
        
        return {
            "database_name": database_name,
            "total_tables": len(tables or []),
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


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)