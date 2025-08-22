from pydantic import BaseModel, Field,EmailStr, constr
from typing import Optional, List
from datetime import date, datetime
from enum import Enum
import uuid
import re
from typing import Optional
from pydantic import field_validator
from utils.date_normalizer import normalize_mysql_date

# ---- Normalization helper ----
def _norm_key(v: Optional[str]) -> str:
    if v is None:
        return ""
    v = str(v).strip()
    v = re.sub(r"\s+", " ", v)  # collapse internal whitespace
    return v.lower()

# ---- Canonical maps (keys are normalized) ----
MEDIA_TYPE_MAP = {"video": "video", "audio": "audio", "both": "both"}
REL_LEVEL_MAP  = {"strong": "strong", "medium": "medium", "weak": "weak"}
SHOW_TYPE_MAP  = {"branded": "Branded", "original": "Original", "partner": "Partner"}

REGION_MAP = {"urban": "Urban", "rural": "Rural", "both": "Both"}

EDU_MAP = {
    "no high school": "No high School",
    "high school": "High School",
    "college": "College",
    "postgraduate": "Postgraduate",
}

GENRE_MAP = {
    "history": "History",
    "human resources": "Human Resources",
    "human interest": "Human Interest",
    "fun & nostalgia": "Fun & Nostalgia",
    "true crime": "True Crime",
    "financial": "Financial",
    "news & politics": "News & Politics",
    "movies": "Movies",
    "music": "Music",
    "religious": "Religious",
    "health & wellness": "Health & Wellness",
    "parenting": "Parenting",
    "lifestyle": "Lifestyle",
    "storytelling": "Storytelling",
    "literature": "Literature",
    "sports": "Sports",
    "pop culture": "Pop Culture",
    "arts": "Arts",
    "business": "Business",
    "philosophy": "Philosophy",
}


class UserCreate(BaseModel):
    name: Optional[str] = None
    email: EmailStr
    password: constr(min_length=6)
    role: str  
    mapped_vendor_qbo_id: Optional[int] = None

class UserResponse(BaseModel):
    id: str
    name: Optional[str]
    email: EmailStr
    role: str
    mapped_vendor_qbo_id: Optional[int]

    class Config:
        orm_mode = True

class SplitCreate(BaseModel):
    show_qbo_id: int
    vendor_qbo_id: int
    show_name: str
    vendor_name: str
    evergreen_pct_ads: float
    evergreen_pct_programmatic: float
    effective_date: date

class Split(BaseModel):
    split_id: int
    show_qbo_id: int
    show_name: str
    vendor_qbo_id: int
    vendor_name: str
    evergreen_pct_ads: float
    evergreen_pct_programmatic: float
    effective_date: date

class Region(str, Enum):
    urban = 'urban'
    rural = 'rural'
    both = 'both'

class SplitType(str, Enum):
    standard = 'standard'
    programmatic = 'programmatic'

class MediaType(str, Enum):
    video = 'video'
    audio = 'audio'
    both = 'both'

class RelationshipLevel(str, Enum):
    strong = 'strong'
    medium = 'medium'
    weak = 'weak'

class ShowType(str, Enum):
    Branded = 'Branded'
    Original = 'Original'
    Partner = 'Partner'

class Role(str, Enum):
    admin = 'admin'
    partner = 'partner'
    internal = 'internal'

class GenreName(str, Enum):
    History = 'History'
    Human_Resources = 'Human Resources'
    Human_Interest = 'Human Interest'
    Fun_Nostalgia = 'Fun & Nostalgia'
    True_Crime = 'True Crime'
    Financial = 'Financial'
    News_Politics = 'News & Politics'
    Movies = 'Movies'
    Music = 'Music'
    Religious = 'Religious'
    Health_Wellness = 'Health & Wellness'
    Parenting = 'Parenting'
    Lifestyle = 'Lifestyle'
    Storytelling = 'Storytelling'
    Literature = 'Literature'
    Sports = 'Sports'
    Pop_Culture = 'Pop Culture'
    Arts = 'Arts'
    Business = 'Business'
    Philosophy = 'Philosophy'

class Demographic(BaseModel):
    show_id: Optional[str] = None
    age_demographic: Optional[str] = None
    gender: Optional[str] = None
    region: Optional[Region] = None
    primary_education: Optional[str] = None
    secondary_education: Optional[str] = None

class Genre(BaseModel):
    id: str
    name: Optional[GenreName] = None

class LedgerTransaction(BaseModel):
    id: str
    transaction_id: Optional[str] = None
    show_id: Optional[str] = None
    payment_date: Optional[date] = None
    amount_received: Optional[float] = None
    customer_name: Optional[str] = None
    advertiser_name: Optional[str] = None
    description: Optional[str] = None

class Partner(BaseModel):
    id: str
    user_id: Optional[str] = None

class RevenueSplit(BaseModel):
    id: str
    advertiser_name: Optional[str] = None
    split_type: Optional[SplitType] = None
    partner_pct: Optional[float] = None
    evergreen_pct: Optional[float] = None
    effective_date: Optional[date] = None

class ShowPartner(BaseModel):
    id: str
    show_id: Optional[str] = None
    partner_id: Optional[str] = None

class ShowCreate(BaseModel):
    # All fields are now consistently snake_case to match Python best practices
    title: str
    minimum_guarantee: Optional[float] = None
    annual_usd: Optional[dict[str, float]] = None
    subnetwork_id: Optional[str] = None
    media_type: Optional[MediaType] = None
    is_tentpole: bool = Field(False, alias='tentpole') # Using alias for DB compatibility
    relationship_level: Optional[RelationshipLevel] = None
    show_type: Optional[ShowType] = None
    evergreen_ownership_pct: Optional[float] = None
    has_sponsorship_revenue: Optional[bool] = False
    has_non_evergreen_revenue: Optional[bool] = False
    requires_partner_access: Optional[bool] = False
    has_branded_revenue: Optional[bool] = False
    has_marketing_revenue: Optional[bool] = False
    has_web_mgmt_revenue: Optional[bool] = False
    genre_name: Optional[str] = None
    is_original: Optional[bool] = False
    shows_per_year: Optional[int] = None
    latest_cpm_usd: Optional[float] = None
    ad_slots: Optional[int] = None
    avg_show_length_mins: Optional[int] = None
    start_date: Optional[date] = None
    show_name_in_qbo: Optional[str] = None
    side_bonus_percent: Optional[float] = None
    youtube_ads_percent: Optional[float] = None
    subscriptions_percent: Optional[float] = None
    standard_ads_percent: Optional[float] = None
    sponsorship_ad_fp_lead_percent: Optional[float] = None
    sponsorship_ad_partner_lead_percent: Optional[float] = None
    sponsorship_ad_partner_sold_percent: Optional[float] = None
    programmatic_ads_span_percent: Optional[float] = None
    merchandise_percent: Optional[float] = None
    branded_revenue_percent: Optional[float] = None
    marketing_services_revenue_percent: Optional[float] = None
    direct_customer_hands_off_percent: Optional[float] = None
    youtube_hands_off_percent: Optional[float] = None
    subscription_hands_off_percent: Optional[float] = None
    revenue_2023: Optional[float] = None
    revenue_2024: Optional[float] = None
    revenue_2025: Optional[float] = None
    evergreen_production_staff_name: Optional[str] = None
    show_host_contact: Optional[str] = None
    show_primary_contact: Optional[str] = None
    age_demographic: Optional[str] = None
    gender: Optional[str] = None
    region: Optional[str] = None
    primary_education: Optional[str] = None
    secondary_education: Optional[str] = None
    is_undersized: Optional[bool] = False
    is_active: Optional[bool] = True

    # --- Case/space-insensitive normalizers for CSV import ---

    @field_validator("media_type", mode="before")
    def _v_media_type(cls, v):
        if v in (None, ""):
            return None
        key = _norm_key(v)
        if key in MEDIA_TYPE_MAP:
            # return Enum instance (your field type is Optional[MediaType])
            return MediaType(MEDIA_TYPE_MAP[key])
        raise ValueError("Invalid value for 'media_type'. Must be one of: video, audio, both.")

    @field_validator("relationship_level", mode="before")
    def _v_relationship_level(cls, v):
        if v in (None, ""):
            return None
        key = _norm_key(v)
        if key in REL_LEVEL_MAP:
            return RelationshipLevel(REL_LEVEL_MAP[key])
        raise ValueError("Invalid value for 'relationship_level'. Must be one of: strong, medium, weak.")

    @field_validator("show_type", mode="before")
    def _v_show_type(cls, v):
        if v in (None, ""):
            return None
        key = _norm_key(v)
        if key in SHOW_TYPE_MAP:
            return ShowType(SHOW_TYPE_MAP[key])
        raise ValueError("Invalid value for 'show_type'. Must be one of: Branded, Original, Partner.")

    @field_validator("region", mode="before")
    def _v_region(cls, v):
        # Field type is Optional[str]; DB expects 'Urban'/'Rural'/'Both'
        if v in (None, ""):
            return None
        key = _norm_key(v)
        if key in REGION_MAP:
            return REGION_MAP[key]
        raise ValueError("Invalid value for 'region'. Must be one of: Urban, Rural, Both.")

    @field_validator("primary_education", "secondary_education", mode="before")
    def _v_education(cls, v, info):
        if v in (None, ""):
            return None
        key = _norm_key(v)
        if key in EDU_MAP:
            return EDU_MAP[key]
        raise ValueError(
            f"Invalid value for '{info.field_name}'. Must be one of: "
            "No high School, High School, College, Postgraduate."
        )

    @field_validator("genre_name", mode="before")
    def _v_genre_name(cls, v):
        if v in (None, ""):
            return None
        key = _norm_key(v)
        if key in GENRE_MAP:
            return GENRE_MAP[key]
        raise ValueError("Invalid value for 'genre_name'.")
    @field_validator("start_date", mode="before")
    @classmethod
    def _normalize_start_date(cls, v):
        # Accept month-first inputs and convert to 'YYYY-MM-DD'
        return normalize_mysql_date(v) if v is not None else None


class Show(BaseModel):
    id: str
    title: Optional[str] = None
    minimum_guarantee: Optional[float] = None
    annual_usd: Optional[dict[str, float]] = None
    subnetwork_id: Optional[str] = None
    media_type: Optional[MediaType] = None
    is_tentpole: bool = Field(False, alias='tentpole')
    relationship_level: Optional[RelationshipLevel] = None
    show_type: Optional[ShowType] = None
    evergreen_ownership_pct: Optional[float] = None
    has_sponsorship_revenue: Optional[bool] = None
    has_non_evergreen_revenue: Optional[bool] = None
    requires_partner_access: Optional[bool] = None
    has_branded_revenue: Optional[bool] = None
    has_marketing_revenue: Optional[bool] = None
    has_web_mgmt_revenue: Optional[bool] = None
    genre_name: Optional[str] = None
    is_original: Optional[bool] = None
    shows_per_year: Optional[int] = None
    latest_cpm_usd: Optional[float] = None
    ad_slots: Optional[int] = None
    avg_show_length_mins: Optional[int] = None
    start_date: Optional[date] = None       
    show_name_in_qbo: Optional[str] = None
    side_bonus_percent: Optional[float] = None
    youtube_ads_percent: Optional[float] = None
    subscriptions_percent: Optional[float] = None
    standard_ads_percent: Optional[float] = None
    sponsorship_ad_fp_lead_percent: Optional[float] = None
    sponsorship_ad_partner_lead_percent: Optional[float] = None
    sponsorship_ad_partner_sold_percent: Optional[float] = None
    programmatic_ads_span_percent: Optional[float] = None
    merchandise_percent: Optional[float] = None
    branded_revenue_percent: Optional[float] = None
    marketing_services_revenue_percent: Optional[float] = None
    direct_customer_hands_off_percent: Optional[float] = None
    youtube_hands_off_percent: Optional[float] = None
    subscription_hands_off_percent: Optional[float] = None
    revenue_2023: Optional[float] = None
    revenue_2024: Optional[float] = None
    revenue_2025: Optional[float] = None
    evergreen_production_staff_name: Optional[str] = None
    show_host_contact: Optional[str] = None
    show_primary_contact: Optional[str] = None
    age_demographic:Optional[str] = None
    gender:Optional[str] = None
    region:Optional[str] = None
    primary_education:Optional[str] = None
    secondary_education:Optional[str] = None
    is_undersized: Optional[bool] = None
    is_active: Optional[bool] = None


class Subnetwork(BaseModel):
    id: str
    name: Optional[str] = None

class User(BaseModel):
    id: str
    name: Optional[str] = None
    email: Optional[str] = None
    password_hash: Optional[str] = None
    role: Optional[Role] = None
    created_at: Optional[datetime] = None

class ShowUpdate(BaseModel):    
    title: Optional[str] = None
    minimum_guarantee: Optional[float] = None
    annual_usd: Optional[dict[str, float]] = None
    subnetwork_id: Optional[str] = None
    media_type: Optional[MediaType] = None
    is_tentpole: Optional[bool] = Field(None, alias='tentpole')
    relationship_level: Optional[RelationshipLevel] = None
    show_type: Optional[ShowType] = None
    evergreen_ownership_pct: Optional[float] = None
    has_sponsorship_revenue: Optional[bool] = None
    has_non_evergreen_revenue: Optional[bool] = None
    requires_partner_access: Optional[bool] = None
    has_branded_revenue: Optional[bool] = None
    has_marketing_revenue: Optional[bool] = None
    has_web_mgmt_revenue: Optional[bool] = None
    genre_name: Optional[str] = None
    is_original: Optional[bool] = None
    shows_per_year: Optional[int] = None
    latest_cpm_usd: Optional[float] = None
    ad_slots: Optional[int] = None
    avg_show_length_mins: Optional[int] = None
    start_date: Optional[date] = None
    show_name_in_qbo: Optional[str] = None
    side_bonus_percent: Optional[float] = None
    youtube_ads_percent: Optional[float] = None
    subscriptions_percent: Optional[float] = None
    standard_ads_percent: Optional[float] = None
    sponsorship_ad_fp_lead_percent: Optional[float] = None
    sponsorship_ad_partner_lead_percent: Optional[float] = None
    sponsorship_ad_partner_sold_percent: Optional[float] = None
    programmatic_ads_span_percent: Optional[float] = None
    merchandise_percent: Optional[float] = None
    branded_revenue_percent: Optional[float] = None
    marketing_services_revenue_percent: Optional[float] = None
    direct_customer_hands_off_percent: Optional[float] = None
    youtube_hands_off_percent: Optional[float] = None
    subscription_hands_off_percent: Optional[float] = None
    revenue_2023: Optional[float] = None
    revenue_2024: Optional[float] = None
    revenue_2025: Optional[float] = None
    evergreen_production_staff_name: Optional[str] = None
    show_host_contact: Optional[str] = None
    show_primary_contact: Optional[str] = None
    age_demographic:Optional[str] = None
    gender:Optional[str] = None
    is_undersized: Optional[bool] = None
    is_active: Optional[bool] = None
    primary_education: Optional[str] = None
    secondary_education: Optional[str] = None
    region: Optional[str] = None
        
    # --- Case/space-insensitive normalizers for CSV import ---

    @field_validator("media_type", mode="before")
    def _v_media_type(cls, v):
        if v in (None, ""):
            return None
        key = _norm_key(v)
        if key in MEDIA_TYPE_MAP:
            # return Enum instance (your field type is Optional[MediaType])
            return MediaType(MEDIA_TYPE_MAP[key])
        raise ValueError("Invalid value for 'media_type'. Must be one of: video, audio, both.")

    @field_validator("relationship_level", mode="before")
    def _v_relationship_level(cls, v):
        if v in (None, ""):
            return None
        key = _norm_key(v)
        if key in REL_LEVEL_MAP:
            return RelationshipLevel(REL_LEVEL_MAP[key])
        raise ValueError("Invalid value for 'relationship_level'. Must be one of: strong, medium, weak.")

    @field_validator("show_type", mode="before")
    def _v_show_type(cls, v):
        if v in (None, ""):
            return None
        key = _norm_key(v)
        if key in SHOW_TYPE_MAP:
            return ShowType(SHOW_TYPE_MAP[key])
        raise ValueError("Invalid value for 'show_type'. Must be one of: Branded, Original, Partner.")

    @field_validator("region", mode="before")
    def _v_region(cls, v):
        # Field type is Optional[str]; DB expects 'Urban'/'Rural'/'Both'
        if v in (None, ""):
            return None
        key = _norm_key(v)
        if key in REGION_MAP:
            return REGION_MAP[key]
        raise ValueError("Invalid value for 'region'. Must be one of: Urban, Rural, Both.")

    @field_validator("primary_education", "secondary_education", mode="before")
    def _v_education(cls, v, info):
        if v in (None, ""):
            return None
        key = _norm_key(v)
        if key in EDU_MAP:
            return EDU_MAP[key]
        raise ValueError(
            f"Invalid value for '{info.field_name}'. Must be one of: "
            "No high School, High School, College, Postgraduate."
        )

    @field_validator("genre_name", mode="before")
    def _v_genre_name(cls, v):
        if v in (None, ""):
            return None
        key = _norm_key(v)
        if key in GENRE_MAP:
            return GENRE_MAP[key]
        raise ValueError("Invalid value for 'genre_name'.")



class PartnerCreate(BaseModel):
    name: str
    email: str
    password: str

class PasswordUpdate(BaseModel):
    password: str

class UserLogin(BaseModel):
    email: str
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    email: Optional[str] = None




class PodcastIn(BaseModel):
    title: str
    show_type: str
    media_type: str
    relationship_level: str
    start_date: Optional[str] = None
    minimum_guarantee: Optional[float] = None
    evergreen_ownership_pct: Optional[float] = None
    genre_name: Optional[str] = None
    subnetwork_id: Optional[str] = None
    tentpole: Optional[bool] = None
    is_original: Optional[bool] = None
    revenue_2023: Optional[float] = None
    revenue_2024: Optional[float] = None
    revenue_2025: Optional[float] = None
    is_active: Optional[bool] = None
    is_undersized: Optional[bool] = None
    standard_ads_percent: Optional[float] = None
    programmatic_ads_span_percent: Optional[float] = None
    has_sponsorship_revenue: Optional[bool] = None
    has_non_evergreen_revenue: Optional[bool] = None
    requires_partner_access: Optional[bool] = None
    has_branded_revenue: Optional[bool] = None
    has_marketing_revenue: Optional[bool] = None
    has_web_mgmt_revenue: Optional[bool] = None

    @field_validator("start_date", mode="before")
    @classmethod
    def _normalize_start_date(cls, v):
        # Accept month-first inputs and convert to 'YYYY-MM-DD'
        return normalize_mysql_date(v) if v is not None else None

# ===== NEW (additions for admin user listing/updating) =====

class UserListItem(BaseModel):
    id: str
    name: Optional[str] = None
    email: EmailStr
    role: Optional[str] = None
    created_at: Optional[datetime] = None
    mapped_vendor_qbo_id: Optional[int] = None
    mapped_vendor_name: Optional[str] = None

class UserUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[EmailStr] = None
    password: Optional[str] = None  # will be hashed server-side if provided
    mapped_vendor_qbo_id: Optional[int] = None
