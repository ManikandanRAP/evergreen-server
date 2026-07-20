from pydantic import BaseModel, Field,EmailStr, constr, model_validator
from typing import Optional, List, Any
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
    v = re.sub(r"[-–—]+", " ", v)  # treat hyphens and dashes as spaces
    v = re.sub(r"\s+", " ", v)  # collapse internal whitespace
    return v.lower()

# ---- Canonical maps (keys are normalized) ----
MEDIA_TYPE_MAP = {"video": "video", "audio": "audio", "both": "both"}
REL_LEVEL_MAP  = {"strong": "strong", "medium": "medium", "weak": "weak"}
SHOW_TYPE_MAP  = {"branded": "Branded", "original": "Original", "partner": "Partner", "hybrid": "Hybrid"}
CADENCE_MAP = {
    "daily": "Daily",
    "weekly": "Weekly",
    "biweekly": "Biweekly",
    "monthly": "Monthly",
    "ad hoc": "Ad hoc",
    "adhoc": "Ad hoc",
    "seasonal": "Seasonal",
    "inactive": "Inactive",
}

RANKING_CATEGORY_MAP = {"1": "1", "2": "2", "3": "3", "4": "4", "5": "5"}

SUBNETWORKS = [
    "CONmunity",
    "Crowd Network",
    "Evergreen",
    "Next Chapter",
    "Osiris Media",
    "Sound Talent Media",
]

SUBNETWORK_MAP = {_norm_key(name): name for name in SUBNETWORKS}

SHOW_STATUSES = ["Active", "Inactive", "No longer on network"]

SHOW_STATUS_MAP = {
    "active": "Active",
    "inactive": "Inactive",
    "no longer on network": "No longer on network",
}

CADENCE_VALUES = list(CADENCE_MAP.values())

AGE_DEMOGRAPHICS = ["18-24", "25-34", "35-44", "45-54", "55+"]
AGE_DEMOGRAPHIC_MAP = {_norm_key(v): v for v in AGE_DEMOGRAPHICS}
AGE_DEMOGRAPHIC_MAP["55"] = "55+"

MAX_SHOW_HOSTS = 3

# Canonical MYCO genres (19). Keys are normalized via _norm_key for CSV/import.
GENRE_MAP = {
    "arts": "Arts",
    "business": "Business",
    "comedy": "Comedy",
    "education": "Education",
    "fiction": "Fiction",
    "government": "Government",
    "health & fitness": "Health & Fitness",
    "health and fitness": "Health & Fitness",
    "history": "History",
    "kids & family": "Kids & Family",
    "kids and family": "Kids & Family",
    "leisure": "Leisure",
    "music": "Music",
    "news": "News",
    "religion & spirituality": "Religion & Spirituality",
    "religion and spirituality": "Religion & Spirituality",
    "science": "Science",
    "society & culture": "Society & Culture",
    "society and culture": "Society & Culture",
    "sports": "Sports",
    "technology": "Technology",
    "true crime": "True Crime",
    "tv & film": "TV & Film",
    "tv and film": "TV & Film",
}

# ---- Contact / demographic validation helpers (shared by ShowCreate & ShowUpdate) ----

# Excel silently turns ratios like "10/90" into month-name dates ("Oct-90").
# Map the 3-letter month back to its number so "Oct-90" -> "10/90", "May-95" -> "5/95".
_MONTH_ABBR_TO_NUM = {
    "jan": "1", "feb": "2", "mar": "3", "apr": "4", "may": "5", "jun": "6",
    "jul": "7", "aug": "8", "sep": "9", "oct": "10", "nov": "11", "dec": "12",
}

_EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
# Phone: digits with optional + prefix and , spaces, parentheses, dots, and hyphens.
_PHONE_CHARS_RE = re.compile(r"^\+?[\d\s().\-]+$")


def repair_excel_gender(value: Optional[str]) -> Optional[str]:
    """Restore an M/F ratio that Excel turned into a month-name date (e.g. 'Oct-90' -> '10/90')."""
    if value is None:
        return None
    s = str(value).strip()
    m = re.fullmatch(r"([A-Za-z]{3})-(\d{1,4})", s)
    if m:
        month = _MONTH_ABBR_TO_NUM.get(m.group(1).lower())
        if month:
            return f"{month}/{m.group(2)}"
    return s


def _split_contacts(value: str, separators: str) -> List[str]:
    parts = re.split(separators, value)
    return [p.strip() for p in parts if p.strip()]


def validate_email_field(value, field_label: str):
    """Validate one or more emails (separated by ; or ,). Returns the trimmed string or None."""
    if value in (None, ""):
        return None
    s = str(value).strip()
    if not s:
        return None
    for token in _split_contacts(s, r"[;,]"):
        if not _EMAIL_RE.match(token):
            raise ValueError(
                f"{field_label} has an invalid email address: '{token}'. "
                "Use a format like name@example.com (separate multiple emails with ';')."
            )
    return s


def validate_phone_field(value, field_label: str):
    """Validate one or more phone numbers (separated by ; or /). Returns the cleaned string or None."""
    if value in (None, ""):
        return None
    # Tolerate stray underscores / extra whitespace (common Excel/paste artifacts) before validating.
    s = re.sub(r"\s+", " ", str(value).replace("_", "")).strip()
    if not s:
        return None
    for token in _split_contacts(s, r"[;/]|\band\b"):
        if not _PHONE_CHARS_RE.match(token):
            raise ValueError(
                f"{field_label} has an invalid phone number: '{token}'. "
                "Use digits, spaces, +, -, (), and separate multiple numbers with ';'."
            )
        digits = re.sub(r"\D", "", token)
        if len(digits) < 7 or len(digits) > 15:
            raise ValueError(
                f"{field_label} has an invalid phone number: '{token}'. It should contain 7 to 15 digits."
            )
    return s


_LINKEDIN_HOST_RE = re.compile(r"^(?:www\.)?linkedin\.com$", re.IGNORECASE)
_LINKEDIN_PATH_RE = re.compile(r"^/(in|company|pub)/", re.IGNORECASE)


def validate_linkedin_url_field(value, field_label: str = "LinkedIn URL"):
    """Validate a LinkedIn profile/company URL. Empty → None."""
    if value in (None, ""):
        return None
    s = str(value).strip()
    if not s:
        return None
    if not re.match(r"^https?://", s, re.IGNORECASE):
        raise ValueError(f"{field_label} must start with http:// or https://")
    try:
        from urllib.parse import urlparse
        parsed = urlparse(s)
    except Exception:
        raise ValueError(f"{field_label} is not a valid URL")
    host = (parsed.hostname or "").lower()
    if not _LINKEDIN_HOST_RE.match(host):
        raise ValueError(f"{field_label} must be a linkedin.com URL")
    path = parsed.path or ""
    if not _LINKEDIN_PATH_RE.match(path):
        raise ValueError(
            f"{field_label} must be a LinkedIn profile or company URL "
            "(e.g. https://www.linkedin.com/in/username)."
        )
    return s


def validate_age_demographic(value):
    """Strict age bracket enum."""
    if value in (None, ""):
        return None
    s = str(value).strip()
    if not s:
        return None
    key = _norm_key(s)
    if key in AGE_DEMOGRAPHIC_MAP:
        return AGE_DEMOGRAPHIC_MAP[key]
    raise ValueError(
        f"Invalid value for 'age_demographic': '{s}'. "
        f"Must be one of: {', '.join(AGE_DEMOGRAPHICS)}."
    )


def validate_gender(value):
    """M/F ratio; repairs Excel month-name corruption, then enforces the VARCHAR(5) limit."""
    if value in (None, ""):
        return None
    repaired = repair_excel_gender(value)
    if repaired is None:
        return None
    s = str(repaired).strip()
    if not s:
        return None
    if len(s) > 5:
        raise ValueError(
            f"Invalid value for 'gender': '{value}'. Must be 5 characters or fewer, e.g. '60/40'. "
            "If the value looks like a date (e.g. 'Oct-90'), it was a ratio Excel converted — use '10/90'."
        )
    return s


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
    partner_pct_ads: float
    partner_pct_programmatic: float
    effective_date: date

class Split(BaseModel):
    split_id: int
    show_qbo_id: int
    show_name: str
    vendor_qbo_id: int
    vendor_name: str
    partner_pct_ads: float
    partner_pct_programmatic: float
    effective_date: date

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
    Hybrid = 'Hybrid'

class Cadence(str, Enum):
    Daily = 'Daily'
    Weekly = 'Weekly'
    Biweekly = 'Biweekly'
    Monthly = 'Monthly'
    Ad_hoc = 'Ad hoc'
    Seasonal = 'Seasonal'
    Inactive = 'Inactive'

class ShowStatus(str, Enum):
    Active = 'Active'
    Inactive = 'Inactive'
    No_longer_on_network = 'No longer on network'

class RankingCategory(str, Enum):
    Level_1 = '1'
    Level_2 = '2'
    Level_3 = '3'
    Level_4 = '4'
    Level_5 = '5'

class Role(str, Enum):
    admin = 'admin'
    partner = 'partner'
    internal_full_access = 'internal_full_access'
    internal_show_access = 'internal_show_access'

class GenreName(str, Enum):
    Arts = "Arts"
    Business = "Business"
    Comedy = "Comedy"
    Education = "Education"
    Fiction = "Fiction"
    Government = "Government"
    Health_Fitness = "Health & Fitness"
    History = "History"
    Kids_Family = "Kids & Family"
    Leisure = "Leisure"
    Music = "Music"
    News = "News"
    Religion_Spirituality = "Religion & Spirituality"
    Science = "Science"
    Society_Culture = "Society & Culture"
    Sports = "Sports"
    Technology = "Technology"
    True_Crime = "True Crime"
    TV_Film = "TV & Film"

class AgeDemographic(str, Enum):
    A18_24 = "18-24"
    A25_34 = "25-34"
    A35_44 = "35-44"
    A45_54 = "45-54"
    A55_plus = "55+"

class ContractLink(BaseModel):
    url: str
    label: Optional[str] = None

class Demographic(BaseModel):
    show_id: Optional[str] = None
    age_demographic: Optional[AgeDemographic] = None
    gender: Optional[str] = None

class ShowHostContactRead(BaseModel):
    """Host contact on API read paths; no format validation so legacy DB rows still serialize."""
    position: int = Field(..., ge=1, le=MAX_SHOW_HOSTS)
    contact_name: Optional[str] = None
    contact_address: Optional[str] = None
    contact_phone: Optional[str] = None
    contact_email: Optional[str] = None


class ShowHostContact(ShowHostContactRead):
    """Host contact on create/update; validates email and phone formats."""

    @field_validator("contact_email", mode="before")
    @classmethod
    def _v_contact_email(cls, v):
        return validate_email_field(v, "Show Host Contact Email")

    @field_validator("contact_phone", mode="before")
    @classmethod
    def _v_contact_phone(cls, v):
        return validate_phone_field(v, "Show Host Contact Phone")


def _host_contact_has_value(data: dict, prefix: str) -> bool:
    return any(data.get(f"{prefix}{suffix}") for suffix in ("name", "address", "phone", "email"))


def _build_host_from_legacy_prefix(data: dict, position: int, prefix: str) -> Optional[dict]:
    if not _host_contact_has_value(data, prefix):
        return None
    return {
        "position": position,
        "contact_name": data.pop(f"{prefix}name", None),
        "contact_address": data.pop(f"{prefix}address", None),
        "contact_phone": data.pop(f"{prefix}phone", None),
        "contact_email": data.pop(f"{prefix}email", None),
    }


def _parse_legacy_host_blob(blob: Optional[str]) -> dict:
    if not blob or blob in ("Internal", "-"):
        return {}
    parts = [p.strip() for p in str(blob).split(", ")]
    if len(parts) < 2:
        return {"contact_name": parts[0] if parts else None}
    if len(parts) == 2:
        return {"contact_name": parts[0], "contact_phone": parts[1]}
    if len(parts) == 3:
        return {"contact_name": parts[0], "contact_phone": parts[1], "contact_email": parts[2]}
    return {
        "contact_name": parts[0],
        "contact_address": ", ".join(parts[1:-2]) or None,
        "contact_phone": parts[-2],
        "contact_email": parts[-1],
    }


def _coerce_show_hosts_from_legacy(data: dict) -> dict:
    """Merge legacy host_contact_* / numbered host columns into hosts[]."""
    if not isinstance(data, dict):
        return data
    if data.get("hosts"):
        for legacy_key in (
            "host_contact_name", "host_contact_address", "host_contact_phone", "host_contact_email",
            "show_host_contact",
        ):
            data.pop(legacy_key, None)
        for position in range(1, MAX_SHOW_HOSTS + 1):
            for suffix in ("name", "address", "phone", "email"):
                data.pop(f"host_{position}_contact_{suffix}", None)
        return data

    hosts: List[dict] = []
    legacy_host = _build_host_from_legacy_prefix(data, 1, "host_contact_")
    if legacy_host:
        hosts.append(legacy_host)

    for position in range(1, MAX_SHOW_HOSTS + 1):
        if position == 1 and legacy_host:
            continue
        numbered = _build_host_from_legacy_prefix(data, position, f"host_{position}_contact_")
        if numbered and not any(h.get("position") == position for h in hosts):
            hosts.append(numbered)

    if not hosts and data.get("show_host_contact"):
        parsed = _parse_legacy_host_blob(data.get("show_host_contact"))
        if parsed:
            hosts.append({"position": 1, **parsed})

    if hosts:
        hosts.sort(key=lambda h: h["position"])
        data["hosts"] = hosts

    for legacy_key in (
        "host_contact_name", "host_contact_address", "host_contact_phone", "host_contact_email",
        "show_host_contact",
    ):
        data.pop(legacy_key, None)
    for position in range(1, MAX_SHOW_HOSTS + 1):
        for suffix in ("name", "address", "phone", "email"):
            data.pop(f"host_{position}_contact_{suffix}", None)
    return data

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
    minimum_guarantee: Optional[bool] = None
    annual_usd: Optional[dict[str, float]] = None
    subnetwork_id: Optional[str] = None
    media_type: Optional[MediaType] = None
    is_rate_card: bool = Field(False, alias='rate_card') # Using alias for DB compatibility (Rate Card field)
    relationship_level: Optional[RelationshipLevel] = None
    show_type: Optional[ShowType] = None
    ranking_category: Optional[RankingCategory] = None
    evergreen_ownership_pct: Optional[float] = None
    has_sponsorship_revenue: Optional[bool] = False
    has_non_evergreen_revenue: Optional[bool] = False
    has_myco_ledger_access: Optional[bool] = False
    has_flightpath_access: Optional[bool] = False
    has_branded_revenue: Optional[bool] = False
    has_marketing_revenue: Optional[bool] = False
    has_web_mgmt_revenue: Optional[bool] = False
    genre_name: Optional[str] = None
    is_original: Optional[bool] = False
    cadence: Optional[Cadence] = None
    base_cpm_usd: Optional[float] = None
    span_cpm_usd: Optional[float] = None
    pre_roll_ad_slots: Optional[int] = None
    mid_roll_ad_slots: Optional[int] = None
    post_roll_ad_slots: Optional[int] = None
    avg_show_length_mins: Optional[int] = None
    first_episode_date: Optional[date] = None
    us_listeners_pct: Optional[float] = None
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
    show_producer_contact: Optional[str] = None
    show_host_contact: Optional[str] = None
    primary_show_contact: Optional[str] = None
    hosts: Optional[List[ShowHostContact]] = None
    primary_contact_name: Optional[str] = None
    primary_contact_address: Optional[str] = None
    primary_contact_phone: Optional[str] = None
    primary_contact_email: Optional[str] = None
    producer_contact_name: Optional[str] = None
    producer_contact_address: Optional[str] = None
    producer_contact_phone: Optional[str] = None
    producer_contact_email: Optional[str] = None
    contract_links: Optional[List[ContractLink]] = None
    age_demographic: Optional[AgeDemographic] = None
    gender: Optional[str] = None
    is_undersized: Optional[bool] = False
    show_status: Optional[ShowStatus] = ShowStatus.Active
    qbo_show_id: Optional[int] = None
    qbo_show_name: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def _merge_legacy_hosts(cls, data):
        return _coerce_show_hosts_from_legacy(data)

    @field_validator("hosts", mode="before")
    @classmethod
    def _v_hosts(cls, v):
        if v in (None, ""):
            return None
        if not isinstance(v, list):
            return v
        cleaned = []
        seen = set()
        for item in v:
            if isinstance(item, dict):
                pos = item.get("position")
                if pos is None:
                    continue
                if pos in seen:
                    raise ValueError("Duplicate show host position in hosts list.")
                seen.add(pos)
                if not any(item.get(k) for k in ("contact_name", "contact_address", "contact_phone", "contact_email")):
                    continue
                cleaned.append(item)
            else:
                cleaned.append(item)
        return cleaned or None

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
        raise ValueError("Invalid value for 'show_type'. Must be one of: Branded, Original, Partner, Hybrid.")

    @field_validator("ranking_category", mode="before")
    def _v_ranking_category(cls, v):
        if v in (None, ""):
            return None
        key = _norm_key(v)
        if key in RANKING_CATEGORY_MAP:
            return RankingCategory(RANKING_CATEGORY_MAP[key])
        raise ValueError("Invalid value for 'ranking_category'. Must be one of: 1, 2, 3, 4, 5.")

    @field_validator("cadence", mode="before")
    def _v_cadence(cls, v):
        if v in (None, ""):
            return None
        key = _norm_key(v)
        if key in CADENCE_MAP:
            return Cadence(CADENCE_MAP[key])
        raise ValueError(
            "Invalid value for 'cadence'. Must be one of: Daily, Weekly, Biweekly, Monthly, Ad hoc, Seasonal, Inactive."
        )

    @field_validator("subnetwork_id", mode="before")
    def _v_subnetwork_id(cls, v):
        if v in (None, ""):
            return None
        key = _norm_key(v)
        if key in SUBNETWORK_MAP:
            return SUBNETWORK_MAP[key]
        raise ValueError(
            f"Invalid value for 'subnetwork_id'. Must be one of: {', '.join(SUBNETWORKS)}."
        )

    @field_validator("show_status", mode="before")
    def _v_show_status(cls, v):
        if v in (None, ""):
            return ShowStatus.Active
        key = _norm_key(v)
        if key in SHOW_STATUS_MAP:
            return ShowStatus(SHOW_STATUS_MAP[key])
        raise ValueError(
            f"Invalid value for 'show_status'. Must be one of: {', '.join(SHOW_STATUSES)}."
        )

    @field_validator("contract_links", mode="before")
    def _v_contract_links(cls, v):
        if v in (None, ""):
            return None
        if isinstance(v, str):
            import json
            try:
                v = json.loads(v)
            except json.JSONDecodeError:
                raise ValueError("contract_links must be valid JSON.")
        if not isinstance(v, list):
            raise ValueError("contract_links must be a list.")
        if len(v) > 10:
            raise ValueError("contract_links supports at most 10 entries.")
        return v

    @field_validator("us_listeners_pct", mode="before")
    def _v_us_listeners_pct(cls, v):
        if v in (None, ""):
            return None
        try:
            f = float(v)
        except (TypeError, ValueError):
            raise ValueError("us_listeners_pct must be a number between 0 and 100.")
        if f < 0 or f > 100:
            raise ValueError("us_listeners_pct must be between 0 and 100.")
        return f

    @field_validator("genre_name", mode="before")
    def _v_genre_name(cls, v):
        if v in (None, ""):
            return None
        key = _norm_key(v)
        if key in GENRE_MAP:
            return GENRE_MAP[key]
        raise ValueError(f"Invalid value for 'genre_name': '{v}'. Not in allowed list. See Import Guide for allowed genres.")
    @field_validator("first_episode_date", mode="before")
    @classmethod
    def _normalize_first_episode_date(cls, v):
        return normalize_mysql_date(v) if v is not None else None

    @field_validator("age_demographic", mode="before")
    def _v_age_demographic(cls, v):
        return validate_age_demographic(v)

    @field_validator("gender", mode="before")
    def _v_gender(cls, v):
        return validate_gender(v)

    @field_validator("primary_contact_email", mode="before")
    def _v_primary_contact_email(cls, v):
        return validate_email_field(v, "Primary Show Contact Email")

    @field_validator("producer_contact_email", mode="before")
    def _v_producer_contact_email(cls, v):
        return validate_email_field(v, "Show Producer Contact Email")

    @field_validator("primary_contact_phone", mode="before")
    def _v_primary_contact_phone(cls, v):
        return validate_phone_field(v, "Primary Show Contact Phone")

    @field_validator("producer_contact_phone", mode="before")
    def _v_producer_contact_phone(cls, v):
        return validate_phone_field(v, "Show Producer Contact Phone")


class Show(BaseModel):
    id: str
    title: Optional[str] = None
    minimum_guarantee: Optional[bool] = None
    annual_usd: Optional[dict[str, float]] = None
    subnetwork_id: Optional[str] = None
    media_type: Optional[MediaType] = None
    is_rate_card: bool = Field(False, alias='rate_card')
    relationship_level: Optional[RelationshipLevel] = None
    show_type: Optional[ShowType] = None
    ranking_category: Optional[RankingCategory] = None
    evergreen_ownership_pct: Optional[float] = None
    has_sponsorship_revenue: Optional[bool] = None
    has_non_evergreen_revenue: Optional[bool] = None
    has_myco_ledger_access: Optional[bool] = None
    has_flightpath_access: Optional[bool] = None
    has_branded_revenue: Optional[bool] = None
    has_marketing_revenue: Optional[bool] = None
    has_web_mgmt_revenue: Optional[bool] = None
    genre_name: Optional[str] = None
    is_original: Optional[bool] = None
    cadence: Optional[Cadence] = None
    base_cpm_usd: Optional[float] = None
    span_cpm_usd: Optional[float] = None
    pre_roll_ad_slots: Optional[int] = None
    mid_roll_ad_slots: Optional[int] = None
    post_roll_ad_slots: Optional[int] = None
    avg_show_length_mins: Optional[int] = None
    first_episode_date: Optional[date] = None
    us_listeners_pct: Optional[float] = None
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
    show_producer_contact: Optional[str] = None
    show_host_contact: Optional[str] = None
    primary_show_contact: Optional[str] = None
    hosts: List[ShowHostContactRead] = Field(default_factory=list)
    primary_contact_name: Optional[str] = None
    primary_contact_address: Optional[str] = None
    primary_contact_phone: Optional[str] = None
    primary_contact_email: Optional[str] = None
    producer_contact_name: Optional[str] = None
    producer_contact_address: Optional[str] = None
    producer_contact_phone: Optional[str] = None
    producer_contact_email: Optional[str] = None
    contract_links: Optional[List[ContractLink]] = None
    age_demographic: Optional[AgeDemographic] = None
    gender: Optional[str] = None
    # Archive fields
    is_archived: bool = Field(False, alias='is_archived')
    archived_at: Optional[datetime] = None
    archived_by: Optional[str] = None
    archived_by_id: Optional[str] = None
    # Creation fields
    created_at: Optional[datetime] = None
    created_by: Optional[str] = None
    created_by_id: Optional[str] = None
    is_undersized: Optional[bool] = None
    show_status: Optional[ShowStatus] = None
    qbo_show_id: Optional[int] = None
    qbo_show_name: Optional[str] = None

    @field_validator("age_demographic", mode="before")
    @classmethod
    def _v_age_demographic_read(cls, v):
        if v in (None, ""):
            return None
        try:
            return validate_age_demographic(v)
        except ValueError:
            return None


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
    mapped_vendor_qbo_id: Optional[int] = None
    mapped_vendor_name: Optional[str] = None
    settings: Optional[dict] = None  # JSON field for user preferences/settings


class UserSettingsUpdate(BaseModel):
    """Model for updating user settings"""
    settings: dict

class ShowUpdate(BaseModel):    
    title: Optional[str] = None
    minimum_guarantee: Optional[bool] = None
    annual_usd: Optional[dict[str, float]] = None
    subnetwork_id: Optional[str] = None
    media_type: Optional[MediaType] = None
    is_rate_card: Optional[bool] = Field(None, alias='rate_card')
    relationship_level: Optional[RelationshipLevel] = None
    show_type: Optional[ShowType] = None
    ranking_category: Optional[RankingCategory] = None
    evergreen_ownership_pct: Optional[float] = None
    has_sponsorship_revenue: Optional[bool] = None
    has_non_evergreen_revenue: Optional[bool] = None
    has_myco_ledger_access: Optional[bool] = None
    has_flightpath_access: Optional[bool] = None
    has_branded_revenue: Optional[bool] = None
    has_marketing_revenue: Optional[bool] = None
    has_web_mgmt_revenue: Optional[bool] = None
    genre_name: Optional[str] = None
    is_original: Optional[bool] = None
    cadence: Optional[Cadence] = None
    base_cpm_usd: Optional[float] = None
    span_cpm_usd: Optional[float] = None
    pre_roll_ad_slots: Optional[int] = None
    mid_roll_ad_slots: Optional[int] = None
    post_roll_ad_slots: Optional[int] = None
    avg_show_length_mins: Optional[int] = None
    first_episode_date: Optional[date] = None
    us_listeners_pct: Optional[float] = None
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
    show_producer_contact: Optional[str] = None
    show_host_contact: Optional[str] = None
    primary_show_contact: Optional[str] = None
    hosts: Optional[List[ShowHostContact]] = None
    primary_contact_name: Optional[str] = None
    primary_contact_address: Optional[str] = None
    primary_contact_phone: Optional[str] = None
    primary_contact_email: Optional[str] = None
    producer_contact_name: Optional[str] = None
    producer_contact_address: Optional[str] = None
    producer_contact_phone: Optional[str] = None
    producer_contact_email: Optional[str] = None
    contract_links: Optional[List[ContractLink]] = None
    age_demographic: Optional[AgeDemographic] = None
    gender: Optional[str] = None
    is_undersized: Optional[bool] = None
    show_status: Optional[ShowStatus] = None
    qbo_show_id: Optional[int] = None
    qbo_show_name: Optional[str] = None
    # Archive fields
    is_archived: Optional[bool] = None
        
    @model_validator(mode="before")
    @classmethod
    def _merge_legacy_hosts_update(cls, data):
        return _coerce_show_hosts_from_legacy(data)

    @field_validator("hosts", mode="before")
    @classmethod
    def _v_hosts_update(cls, v):
        if v in (None, ""):
            return None
        if not isinstance(v, list):
            return v
        cleaned = []
        seen = set()
        for item in v:
            if isinstance(item, dict):
                pos = item.get("position")
                if pos is None:
                    continue
                if pos in seen:
                    raise ValueError("Duplicate show host position in hosts list.")
                seen.add(pos)
                if not any(item.get(k) for k in ("contact_name", "contact_address", "contact_phone", "contact_email")):
                    continue
                cleaned.append(item)
            else:
                cleaned.append(item)
        return cleaned or None

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
        raise ValueError("Invalid value for 'show_type'. Must be one of: Branded, Original, Partner, Hybrid.")

    @field_validator("ranking_category", mode="before")
    def _v_ranking_category(cls, v):
        if v in (None, ""):
            return None
        key = _norm_key(v)
        if key in RANKING_CATEGORY_MAP:
            return RankingCategory(RANKING_CATEGORY_MAP[key])
        raise ValueError("Invalid value for 'ranking_category'. Must be one of: 1, 2, 3, 4, 5.")

    @field_validator("cadence", mode="before")
    def _v_cadence(cls, v):
        if v in (None, ""):
            return None
        key = _norm_key(v)
        if key in CADENCE_MAP:
            return Cadence(CADENCE_MAP[key])
        raise ValueError(
            "Invalid value for 'cadence'. Must be one of: Daily, Weekly, Biweekly, Monthly, Ad hoc, Seasonal, Inactive."
        )

    @field_validator("subnetwork_id", mode="before")
    def _v_subnetwork_id_update(cls, v):
        if v in (None, ""):
            return None
        key = _norm_key(v)
        if key in SUBNETWORK_MAP:
            return SUBNETWORK_MAP[key]
        raise ValueError(
            f"Invalid value for 'subnetwork_id'. Must be one of: {', '.join(SUBNETWORKS)}."
        )

    @field_validator("show_status", mode="before")
    def _v_show_status_update(cls, v):
        if v in (None, ""):
            return None
        key = _norm_key(v)
        if key in SHOW_STATUS_MAP:
            return ShowStatus(SHOW_STATUS_MAP[key])
        raise ValueError(
            f"Invalid value for 'show_status'. Must be one of: {', '.join(SHOW_STATUSES)}."
        )

    @field_validator("contract_links", mode="before")
    def _v_contract_links_update(cls, v):
        if v in (None, ""):
            return None
        if isinstance(v, str):
            import json
            try:
                v = json.loads(v)
            except json.JSONDecodeError:
                raise ValueError("contract_links must be valid JSON.")
        if not isinstance(v, list):
            raise ValueError("contract_links must be a list.")
        if len(v) > 10:
            raise ValueError("contract_links supports at most 10 entries.")
        return v

    @field_validator("us_listeners_pct", mode="before")
    def _v_us_listeners_pct_update(cls, v):
        if v in (None, ""):
            return None
        try:
            f = float(v)
        except (TypeError, ValueError):
            raise ValueError("us_listeners_pct must be a number between 0 and 100.")
        if f < 0 or f > 100:
            raise ValueError("us_listeners_pct must be between 0 and 100.")
        return f

    @field_validator("genre_name", mode="before")
    def _v_genre_name(cls, v):
        if v in (None, ""):
            return None
        key = _norm_key(v)
        if key in GENRE_MAP:
            return GENRE_MAP[key]
        raise ValueError(f"Invalid value for 'genre_name': '{v}'. Not in allowed list. See Import Guide for allowed genres.")

    @field_validator("first_episode_date", mode="before")
    @classmethod
    def _normalize_first_episode_date_update(cls, v):
        return normalize_mysql_date(v) if v is not None else None

    @field_validator("age_demographic", mode="before")
    def _v_age_demographic_update(cls, v):
        return validate_age_demographic(v)

    @field_validator("gender", mode="before")
    def _v_gender_update(cls, v):
        return validate_gender(v)

    @field_validator("primary_contact_email", mode="before")
    def _v_primary_contact_email_update(cls, v):
        return validate_email_field(v, "Primary Show Contact Email")

    @field_validator("producer_contact_email", mode="before")
    def _v_producer_contact_email_update(cls, v):
        return validate_email_field(v, "Show Producer Contact Email")

    @field_validator("primary_contact_phone", mode="before")
    def _v_primary_contact_phone_update(cls, v):
        return validate_phone_field(v, "Primary Show Contact Phone")

    @field_validator("producer_contact_phone", mode="before")
    def _v_producer_contact_phone_update(cls, v):
        return validate_phone_field(v, "Show Producer Contact Phone")



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
    ranking_category: Optional[str] = None
    first_episode_date: Optional[str] = None
    minimum_guarantee: Optional[bool] = None
    evergreen_ownership_pct: Optional[float] = None
    genre_name: Optional[str] = None
    subnetwork_id: Optional[str] = None
    rate_card: Optional[bool] = None
    is_original: Optional[bool] = None
    revenue_2023: Optional[float] = None
    revenue_2024: Optional[float] = None
    revenue_2025: Optional[float] = None
    show_status: Optional[str] = None
    is_undersized: Optional[bool] = None
    standard_ads_percent: Optional[float] = None
    programmatic_ads_span_percent: Optional[float] = None
    has_sponsorship_revenue: Optional[bool] = None
    has_non_evergreen_revenue: Optional[bool] = None
    has_myco_ledger_access: Optional[bool] = None
    has_branded_revenue: Optional[bool] = None
    has_marketing_revenue: Optional[bool] = None
    has_web_mgmt_revenue: Optional[bool] = None

    @field_validator("ranking_category", mode="before")
    def _v_ranking_category(cls, v):
        if v in (None, ""):
            return None
        key = _norm_key(v)
        if key in RANKING_CATEGORY_MAP:
            return RANKING_CATEGORY_MAP[key]
        raise ValueError("Invalid value for 'ranking_category'. Must be one of: 1, 2, 3, 4, 5.")

    @field_validator("first_episode_date", mode="before")
    @classmethod
    def _normalize_first_episode_date_podcast(cls, v):
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
    role: Optional[Role] = None
    mapped_vendor_qbo_id: Optional[int] = None

# ===== NEW (additions for feedback feature) =====

class FeedbackType(str, Enum):
    new_feature = "New Feature"
    general_feedback = "General Feedback"

class FeedbackStatus(str, Enum):
    open = "Open"
    in_progress = "In Progress"
    completed = "Completed"

class FeedbackCreate(BaseModel):
    title: constr(min_length=1, max_length=100)
    type: FeedbackType
    description: constr(min_length=10, max_length=1000)

class FeedbackListItem(BaseModel):
    id: str
    title: str
    type: FeedbackType
    created_by: str
    created_at: datetime
    createdByName: str
    status: FeedbackStatus = FeedbackStatus.open
    completed_at: Optional[datetime] = None
    completed_by: Optional[str] = None
    completedByName: Optional[str] = None
    updated_at: Optional[datetime] = None

class Feedback(BaseModel):
    id: str
    title: str
    type: FeedbackType
    description: str
    created_by: str
    created_at: datetime
    createdByName: str
    status: FeedbackStatus = FeedbackStatus.open
    completed_at: Optional[datetime] = None
    completed_by: Optional[str] = None
    completedByName: Optional[str] = None
    resolution_note: Optional[str] = None
    updated_at: Optional[datetime] = None

class FeedbackStatusUpdate(BaseModel):
    status: FeedbackStatus
    resolution_note: Optional[constr(max_length=2000)] = None

class FeedbackResolutionUpdate(BaseModel):
    resolution_note: Optional[constr(max_length=2000)] = None

# ===== NEW (additions for username availability check) =====

class UsernameCheckRequest(BaseModel):
    username: EmailStr

class UsernameCheckResponse(BaseModel):
    available: bool

# ===== MYCO Notices =====

class NoticeType(str, Enum):
    host_read_ads = "host_read_ads"
    sponsorship_vetting = "sponsorship_vetting"

class AdType(str, Enum):
    endorsement = "endorsement"
    personal_experience = "personal_experience"

class NoticeStatus(str, Enum):
    active = "active"
    complete = "complete"
    cancelled = "cancelled"
    expired = "expired"

class ContactSource(str, Enum):
    auto_primary = "auto_primary"
    manual = "manual"

class DeliveryChannel(str, Enum):
    email = "email"
    text = "text"
    myco = "myco"

class DeliveryStatus(str, Enum):
    sent = "sent"
    failed = "failed"
    skipped = "skipped"

class UserNotificationType(str, Enum):
    notice_delivery = "notice_delivery"
    delivery_failure = "delivery_failure"
    system = "system"

InboxMessageType = UserNotificationType

class NoticeContactInput(BaseModel):
    contact_name: constr(min_length=1, max_length=255)
    contact_email: Optional[EmailStr] = None
    contact_phone: Optional[str] = None
    contact_source: ContactSource = ContactSource.manual
    channel_email: bool = False
    channel_text: bool = False
    channel_myco: bool = False


class NoticeContact(BaseModel):
    id: Optional[str] = None
    notice_id: Optional[str] = None
    position: int
    contact_name: str
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None
    contact_source: ContactSource = ContactSource.manual
    myco_user_id: Optional[str] = None
    myco_user_name: Optional[str] = None
    channel_email: bool = False
    channel_text: bool = False
    channel_myco: bool = False


class NoticeCommunicationSettings(BaseModel):
    channel_email: bool = False
    channel_text: bool = False
    channel_myco: bool = False
    frequency_hours: int = Field(default=24, ge=1, le=168)

class HostReadAdsNoticeCreate(BaseModel):
    show_id: str
    brand_name: constr(min_length=1, max_length=255)
    ad_copy_link: constr(min_length=1, max_length=2048)
    due_date: date
    notes: Optional[constr(max_length=400)] = None
    contacts: List[NoticeContactInput] = Field(..., min_length=1, max_length=3)
    channel_email: bool = False
    channel_text: bool = False
    channel_myco: bool = False
    frequency_hours: int = Field(default=24, ge=1, le=168)

class SponsorshipVettingNoticeCreate(BaseModel):
    show_id: str
    brand_name: constr(min_length=1, max_length=255)
    brand_overview: constr(min_length=1, max_length=400)
    ad_type: AdType
    due_date: date
    notes: Optional[constr(max_length=400)] = None
    contacts: List[NoticeContactInput] = Field(..., min_length=1, max_length=3)
    channel_email: bool = False
    channel_text: bool = False
    channel_myco: bool = False
    frequency_hours: int = Field(default=24, ge=1, le=168)

class NoticeNotesUpdate(BaseModel):
    notes: Optional[constr(max_length=400)] = None

class NoticeUpdate(BaseModel):
    brand_name: Optional[constr(min_length=1, max_length=255)] = None
    ad_copy_link: Optional[constr(max_length=2048)] = None
    brand_overview: Optional[constr(max_length=400)] = None
    ad_type: Optional[AdType] = None
    due_date: Optional[date] = None
    notes: Optional[constr(max_length=400)] = None
    contacts: Optional[List[NoticeContactInput]] = Field(default=None, min_length=1, max_length=3)
    channel_email: Optional[bool] = None
    channel_text: Optional[bool] = None
    channel_myco: Optional[bool] = None
    frequency_hours: Optional[int] = Field(default=None, ge=1, le=168)

class NoticeDelivery(BaseModel):
    id: str
    notice_id: str
    channel: DeliveryChannel
    status: DeliveryStatus
    recipient: Optional[str] = None
    contact_position: Optional[int] = Field(default=None, ge=1, le=3)
    error_message: Optional[str] = None
    external_id: Optional[str] = None
    external_status: Optional[str] = None
    external_status_at: Optional[datetime] = None
    is_reminder: bool = False
    sent_at: datetime

class NoticeChannelDeliveryRequest(BaseModel):
    channel: DeliveryChannel
    is_reminder: Optional[bool] = None
    contact_positions: Optional[List[int]] = Field(default=None, min_length=1, max_length=3)

class NoticeChannelDeliveryResponse(BaseModel):
    notice_id: str
    channel: DeliveryChannel
    delivery: NoticeDelivery
    deliveries: Optional[List[NoticeDelivery]] = None

class NoticeListItem(BaseModel):
    id: str
    notice_type: NoticeType
    show_id: str
    show_title: Optional[str] = None
    brand_name: str
    due_date: date
    status: NoticeStatus
    contacts: List[NoticeContact] = Field(default_factory=list)
    channel_email: bool
    channel_text: bool
    channel_myco: bool
    created_by: str
    created_by_name: Optional[str] = None
    created_at: datetime

class Notice(BaseModel):
    id: str
    notice_type: NoticeType
    show_id: str
    show_title: Optional[str] = None
    brand_name: str
    ad_copy_link: Optional[str] = None
    brand_overview: Optional[str] = None
    ad_type: Optional[AdType] = None
    due_date: date
    notes: Optional[str] = None
    contacts: List[NoticeContact] = Field(default_factory=list)
    channel_email: bool
    channel_text: bool
    channel_myco: bool
    frequency_hours: int
    reminder_window_days: int
    reminder_started_at: datetime
    status: NoticeStatus
    last_sent_at: Optional[datetime] = None
    next_send_at: datetime
    send_count: int
    created_by: str
    created_by_name: Optional[str] = None
    created_by_email: Optional[str] = None
    completed_by: Optional[str] = None
    completed_by_name: Optional[str] = None
    completed_at: Optional[datetime] = None
    cancelled_by: Optional[str] = None
    cancelled_by_name: Optional[str] = None
    cancelled_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    deliveries: Optional[List["NoticeDelivery"]] = None

class ShowContactPreview(BaseModel):
    contact_name: str = ""
    contact_email: str = ""
    contact_phone: str = ""
    contact_source: ContactSource = ContactSource.manual


class MycoUserLookup(BaseModel):
    email: str
    matched: bool
    user_id: Optional[str] = None
    user_name: Optional[str] = None

class UserNotification(BaseModel):
    id: str
    user_id: str
    type: UserNotificationType
    title: str
    body: str
    notice_id: Optional[str] = None
    metadata: Optional[dict] = None
    read_at: Optional[datetime] = None
    pinned_at: Optional[datetime] = None
    created_at: datetime


class InboxMessage(UserNotification):
    """Inbox service message (alias with inbox-oriented naming)."""


class InboxMessageListItem(BaseModel):
    id: str
    user_id: str
    type: UserNotificationType
    title: str
    body_preview: str
    notice_id: Optional[str] = None
    metadata: Optional[dict] = None
    read_at: Optional[datetime] = None
    pinned_at: Optional[datetime] = None
    created_at: datetime


class InboxMessageListResponse(BaseModel):
    items: List[InboxMessageListItem]
    has_more: bool
    total_count: int
    filtered_total_count: int


class InboxBulkRequest(BaseModel):
    message_ids: List[str] = []
    action: str
    select_all: bool = False
    inbox_filter: str = "all"
    unread_only: bool = False
    search: Optional[str] = None


class InboxBulkResponse(BaseModel):
    updated: int
    unread_delta: int = 0


class InboxSettingsResponse(BaseModel):
    inbox_retention_days: int
    inbox_retention_label: str


class SystemSettingsResponse(InboxSettingsResponse):
    pass


class SystemSettingsPatch(BaseModel):
    inbox_retention_days: int

# ---- Staff Directory ----

STAFF_LINK_ELIGIBLE_ROLES = ("admin", "internal_full_access", "internal_show_access")
PARTNER_STAFF_EMAIL_BLOCK_MSG = "Partner Account - This Email cannot be used for Staff"


class StaffDepartment(str, Enum):
    finance = "finance"
    sales = "sales"
    production = "production"
    marketing = "marketing"
    operations = "operations"


class StaffEmailLinkStatus(str, Enum):
    linked = "linked"
    unlinked = "unlinked"
    partner_blocked = "partner_blocked"
    already_exists = "already_exists"


class StaffMemberBase(BaseModel):
    name: constr(strip_whitespace=True, min_length=1, max_length=255)
    title: constr(strip_whitespace=True, min_length=1, max_length=255)
    department: StaffDepartment
    email: EmailStr
    pronouns: Optional[str] = None
    is_supervisor: bool = False
    supervisor_id: Optional[str] = None
    google_voice_number: Optional[str] = None
    personal_phone: Optional[str] = None
    linkedin_url: Optional[str] = None

    @field_validator("pronouns", mode="before")
    @classmethod
    def _v_pronouns(cls, v):
        if v in (None, ""):
            return None
        s = str(v).strip()
        return s or None

    @field_validator("supervisor_id", mode="before")
    @classmethod
    def _v_supervisor_id(cls, v):
        if v in (None, ""):
            return None
        s = str(v).strip()
        return s or None

    @field_validator("google_voice_number", mode="before")
    @classmethod
    def _v_google_voice(cls, v):
        return validate_phone_field(v, "Google Voice Number")

    @field_validator("personal_phone", mode="before")
    @classmethod
    def _v_personal_phone(cls, v):
        return validate_phone_field(v, "Personal Phone")

    @field_validator("linkedin_url", mode="before")
    @classmethod
    def _v_linkedin(cls, v):
        return validate_linkedin_url_field(v, "LinkedIn URL")


class StaffMemberCreate(StaffMemberBase):
    pass


class StaffMemberUpdate(StaffMemberBase):
    pass


class StaffMemberRead(StaffMemberBase):
    id: str
    user_id: Optional[str] = None
    linked_user_name: Optional[str] = None
    linked_user_role: Optional[str] = None
    supervisor_name: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    created_by_name: Optional[str] = None
    updated_by_name: Optional[str] = None


class StaffSupervisorOption(BaseModel):
    id: str
    name: str
    title: str
    department: StaffDepartment


class StaffEmailLinkCheckResponse(BaseModel):
    status: StaffEmailLinkStatus
    user_name: Optional[str] = None
    user_role: Optional[str] = None
