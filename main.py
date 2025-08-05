import uvicorn
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from pydantic import BaseModel
from typing import Optional, List
from models import Show, User, Token, TokenData, PartnerCreate, PasswordUpdate, ShowUpdate, ShowCreate, MediaType, RelationshipLevel, ShowType, UserResponse, UserCreate
from sqlclient import SqlClient
from auth import create_access_token, verify_password, get_password_hash
from config import SECRET_KEY, ALGORITHM
from fastapi.middleware.cors import CORSMiddleware
import uuid
from datetime import datetime, timezone

# --- FastAPI App Initialization ---
app = FastAPI(
    title="Evergreen Podcasts API",
    description="API for managing podcasts and partners with JWT authentication.",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

# --- Authentication & Authorization ---

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
    if current_user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail="Not enough permissions")
    # --- THIS LINE IS NOW CORRECTED ---
    return current_user

# --- API Endpoints ---
@app.post("/create_users", response_model=UserResponse)
def create_user(user_data: UserCreate, admin: User = Depends(get_admin_user)):
    client = SqlClient()
    existing_user, _ = client.get_user_by_email(user_data.email)
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    user_id = str(uuid.uuid4())
    hashed_password = get_password_hash(user_data.password)
    sql = """
    INSERT INTO users (id, name, email, password_hash, role, created_at, mapped_partner_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        user_id, user_data.name, user_data.email, hashed_password,
        user_data.role, datetime.now(timezone.utc), user_data.mapped_partner_id,
    )
    _, _, error = client._execute_query(sql, values, is_transaction=True)
    if error:
        raise HTTPException(status_code=500, detail="Error inserting user into DB")
    return {
        "id": user_id, "name": user_data.name, "email": user_data.email,
        "role": user_data.role, "mapped_partner_id": user_data.mapped_partner_id,
    }

@app.post("/login")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    client = SqlClient()
    user, _ = client.get_user_by_email(email=form_data.username)
    if not user or not verify_password(form_data.password, user.get('password_hash')):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = create_access_token(data={"sub": user.get('email')})
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/users/me", response_model=User)
async def read_users_me(current_user: User = Depends(get_current_active_user)):
    return current_user

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
        # Skip rows where title is not provided or empty
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
        "errors": errors
    }

@app.get("/podcasts", response_model=list[Show])
def get_all_podcasts(admin: User = Depends(get_admin_user)):
    client = SqlClient()
    return client.get_all_podcasts()

class ShowFilterParams:
    def __init__(
        self, title: Optional[str] = None, media_type: Optional[MediaType] = None,
        tentpole: Optional[bool] = None, relationship_level: Optional[RelationshipLevel] = None,
        show_type: Optional[ShowType] = None, has_sponsorship_revenue: Optional[bool] = None,
        has_non_evergreen_revenue: Optional[bool] = None, requires_partner_access: Optional[bool] = None,
        has_branded_revenue: Optional[bool] = None, has_marketing_revenue: Optional[bool] = None,
        has_web_mgmt_revenue: Optional[bool] = None, is_original: Optional[bool] = None,
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

@app.get("/podcasts/filter", response_model=list[Show])
def filter_podcasts(filters: ShowFilterParams = Depends(), admin: User = Depends(get_admin_user)):
    client = SqlClient()
    filter_dict = {k: v for k, v in vars(filters).items() if v is not None}
    podcasts, error = client.filter_podcasts(filter_dict)
    if error:
        raise HTTPException(status_code=400, detail=str(error))
    return podcasts

@app.get("/podcasts/{show_id}", response_model=Show)
def get_podcast(show_id: str, admin: User = Depends(get_admin_user)):
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
        if "No update data provided" in error:
            raise HTTPException(status_code=400, detail=error)
        raise HTTPException(status_code=404, detail=error)
    return updated_show

@app.delete("/podcasts/{show_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_podcast(show_id: str, admin: User = Depends(get_admin_user)):
    client = SqlClient()
    success, error = client.delete_podcast(show_id)
    if not success:
        raise HTTPException(status_code=404, detail=error)

@app.post("/partners", response_model=User, status_code=status.HTTP_201_CREATED)
def create_partner(partner_data: PartnerCreate, admin: User = Depends(get_admin_user)):
    client = SqlClient()
    new_user, error = client.create_partner(partner_data)
    if error:
        raise HTTPException(status_code=409, detail=error)
    return new_user

@app.put("/partners/{user_id}/password", status_code=status.HTTP_204_NO_CONTENT)
def update_partner_password(user_id: str, password_data: PasswordUpdate, admin: User = Depends(get_admin_user)):
    client = SqlClient()
    success, error = client.update_password(user_id, password_data.password)
    if not success:
        raise HTTPException(status_code=404, detail=error)

@app.post("/podcasts/{show_id}/partners/{partner_id}", status_code=status.HTTP_201_CREATED)
def associate_partner_with_show(show_id: str, partner_id: str, admin: User = Depends(get_admin_user)):
    client = SqlClient()
    result, error = client.associate_partner_with_show(show_id, partner_id)
    if error:
        raise HTTPException(status_code=404, detail=error)
    return result

@app.delete("/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_user(user_id: str, admin: User = Depends(get_admin_user)):
    client = SqlClient()
    success, error = client.delete_user(user_id)
    if not success:
        raise HTTPException(status_code=404, detail=error)

@app.delete("/podcasts/{show_id}/partners/{partner_id}", status_code=status.HTTP_204_NO_CONTENT)
def unassociate_partner_from_show(show_id: str, partner_id: str, admin: User = Depends(get_admin_user)):
    client = SqlClient()
    success, error = client.unassociate_partner_from_show(show_id, partner_id)
    if not success:
        raise HTTPException(status_code=404, detail=error)

@app.get("/partners/me/podcasts", response_model=list[Show])
def get_my_podcasts(current_user: User = Depends(get_current_active_user)):
    client = SqlClient()
    partner_id = current_user.get('id')
    podcasts, error = client.get_podcasts_for_partner(partner_id)
    if error:
        raise HTTPException(status_code=500, detail=str(error))
    return podcasts

@app.get("/partners/{partner_id}/podcasts", response_model=list[Show])
def get_podcasts_for_partner(partner_id: str, admin: User = Depends(get_admin_user)):
    client = SqlClient()
    podcasts, error = client.get_podcasts_for_partner(partner_id)
    if error:
        raise HTTPException(status_code=500, detail=str(error))
    return podcasts

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)