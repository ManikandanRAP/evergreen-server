from pydantic import BaseModel, Field,EmailStr, constr
from typing import Optional, List
from datetime import date, datetime
from enum import Enum
import uuid

class UserCreate(BaseModel):
    name: Optional[str] = None
    email: EmailStr
    password: constr(min_length=6)
    role: str  
    mapped_partner_id: Optional[int] = None

class UserResponse(BaseModel):
    id: str
    name: Optional[str]
    email: EmailStr
    role: str
    mapped_partner_id: Optional[int]

    class Config:
        orm_mode = True

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
    ageDemographic: Optional[str] = None
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
    title: str
    minimum_guarantee: Optional[float] = None
    annual_usd: Optional[dict[str, float]] = None
    subnetwork_id: Optional[str] = None
    media_type: Optional[MediaType] = None
    tentpole: bool = False
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
    ageDemographic:Optional[str] = None
    gender:Optional[str] = None
    region:Optional[str] = None
    primary_education:Optional[str] = None
    secondary_education:Optional[str] = None
    is_undersized: Optional[bool] = Field(default=None, alias='isUndersized')
    is_active: Optional[bool] = Field(default=None, alias='isActive')

class Show(BaseModel):
    id: str
    title: Optional[str] = None
    minimum_guarantee: Optional[float] = None
    annual_usd: Optional[dict[str, float]] = None
    subnetwork_id: Optional[str] = None
    media_type: Optional[MediaType] = None
    tentpole: bool = False
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
    ageDemographic:Optional[str] = None
    gender:Optional[str] = None
    region:Optional[str] = None
    primary_education:Optional[str] = None
    secondary_education:Optional[str] = None
    is_undersized: Optional[bool] = Field(None, alias='isUndersized')
    is_active: Optional[bool] = Field(None, alias='isActive')


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
    tentpole: bool = False
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
    ageDemographic:Optional[str] = None
    gender:Optional[str] = None
    is_undersized: Optional[bool] = Field(None, alias='isUndersized')
    is_active: Optional[bool] = Field(None, alias='isActive')


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
