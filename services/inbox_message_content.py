"""Build inbox message body and metadata from notice payloads."""
from __future__ import annotations

import os
from datetime import date, datetime
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from services.myco_client_url import resolve_client_base_url

NOTICE_DESCRIPTION_SEPARATOR = " | "
NOTICE_BODY_HEADER_SEPARATOR = " · "


def _join_notice_description_parts(*parts: str) -> str:
    return NOTICE_DESCRIPTION_SEPARATOR.join(part.strip() for part in parts if part and part.strip())


def _notice_type_label(notice_type: str) -> str:
    if notice_type == "host_read_ads":
        return "Host-Read Ads"
    if notice_type == "sponsorship_vetting":
        return "Sponsorship Vetting"
    return notice_type.replace("_", " ").title()


def _format_due_date(due: Any) -> str:
    if due is None:
        return "—"
    if hasattr(due, "isoformat"):
        return due.isoformat()
    return str(due)


def _format_due_date_display(due: Any) -> str:
    if due is None:
        return "—"
    if hasattr(due, "isoformat"):
        raw = due.isoformat()
    else:
        raw = str(due)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed.strftime("%b %d, %Y")
    except ValueError:
        return raw


def _due_urgency_list_label(due: Any) -> str:
    if due is None:
        return ""
    if hasattr(due, "isoformat"):
        raw = due.isoformat()
    else:
        raw = str(due)
    try:
        due_day = datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except ValueError:
        return ""
    diff = (due_day - date.today()).days
    if diff < 0:
        days = abs(diff)
        return "1 Day Overdue" if days == 1 else f"{days} Days Overdue"
    if diff == 0:
        return "Due Today"
    if diff == 1:
        return "Due in 1 Day"
    return f"Due in {diff} Days"


def _due_urgency_tone(due: Any) -> str:
    if due is None:
        return ""
    if hasattr(due, "isoformat"):
        raw = due.isoformat()
    else:
        raw = str(due)
    try:
        due_day = datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except ValueError:
        return ""
    diff = (due_day - date.today()).days
    if diff < 0:
        return "overdue"
    if diff <= 3:
        return "soon"
    return "ok"


def _due_date_header_detail(due: Any) -> str:
    """Card header detail: exact due date shown before the urgency pill, e.g. "Due on Jun 13, 2026"."""
    formatted = _format_due_date_display(due)
    if not formatted or formatted == "—":
        return ""
    return f"Due on {formatted}"


def build_notice_body_header_type_and_detail(notice: dict) -> tuple[str, str]:
    type_label = _notice_type_label(notice.get("notice_type", ""))
    return type_label, _due_date_header_detail(notice.get("due_date"))


def build_notice_body_header_badge(notice: dict) -> tuple[str, str]:
    """Returns (label, color) for overdue/soon pill badges in card headers."""
    urgency = _due_urgency_list_label(notice.get("due_date"))
    tone = _due_urgency_tone(notice.get("due_date"))
    if not urgency or tone == "ok":
        return "", ""
    if tone == "overdue":
        return urgency, "#b91c1c"
    return urgency, "#b45309"


def build_notice_list_title(notice: dict, *, is_reminder: bool) -> str:
    show = (notice.get("show_title") or "").strip() or "Show"
    if is_reminder:
        return f"Notice Reminder for {show}"
    return f"Notice Created for {show}"


def build_notice_list_description1(notice: dict, *, is_reminder: bool) -> str:
    label = _notice_type_label(notice.get("notice_type", ""))
    urgency = _due_urgency_list_label(notice.get("due_date"))
    parts = [label]
    if urgency:
        parts.append(urgency)
    return _join_notice_description_parts(*parts)


def build_notice_list_description2(notice: dict) -> str:
    brand = (notice.get("brand_name") or "").strip()
    return f"Brand: {brand}" if brand else ""


def build_notice_email_subject(notice: dict, *, is_reminder: bool) -> str:
    return build_notice_list_title(notice, is_reminder=is_reminder)


def build_notice_email_preview(notice: dict, *, is_reminder: bool) -> str:
    return _join_notice_description_parts(
        build_notice_list_description1(notice, is_reminder=is_reminder),
        build_notice_list_description2(notice),
    )


MYCO_EMAIL_LOGO_PATH = "/myco-beta-footer-logo.png"
MYCO_EMAIL_LOGO_DARK_PATH = "/myco-beta-footer-logo-light.png"
MYCO_PROD_CLIENT_URL = "https://myco.evergreenpodcasts.com"


def get_myco_client_base_url(notice: Optional[dict] = None) -> str:
    return resolve_client_base_url(notice=notice)


def build_myco_home_url(notice: Optional[dict] = None) -> str:
    base = get_myco_client_base_url(notice)
    return f"{base}/" if base else ""


def _myco_prod_client_base_url() -> str:
    return os.getenv("MYCO_PROD_CLIENT_URL", MYCO_PROD_CLIENT_URL).strip().rstrip("/")


def _build_myco_email_logo_url(path: str, override_env: str) -> str:
    override = os.getenv(override_env, "").strip()
    if override:
        return override
    base = _myco_prod_client_base_url()
    return f"{base}{path}" if base else ""


def build_myco_email_logo_url() -> str:
    """Dark logo for light backgrounds (default / light-theme inboxes)."""
    return _build_myco_email_logo_url(MYCO_EMAIL_LOGO_PATH, "MYCO_EMAIL_LOGO_URL")


def build_myco_email_logo_dark_url() -> str:
    """Light logo for dark backgrounds (dark-theme inboxes)."""
    return _build_myco_email_logo_url(MYCO_EMAIL_LOGO_DARK_PATH, "MYCO_EMAIL_LOGO_DARK_URL")


def build_myco_email_logo_home_url() -> str:
    """Prod Myco home page — logo in emails always links here."""
    base = _myco_prod_client_base_url()
    return f"{base}/" if base else ""


def build_notice_open_url(notice_id: object, notice: Optional[dict] = None) -> str:
    """Absolute URL to open a notice in Myco (for email deep links)."""
    notice_id_str = str(notice_id or "").strip()
    if not notice_id_str:
        return ""
    base = get_myco_client_base_url(notice)
    if not base:
        return ""
    return f"{base}/myco-notices?notice={notice_id_str}"


def get_ad_copy_link_hostname(href: object) -> str:
    """Hostname label for ad copy link buttons (mirrors client AdCopyLink)."""
    trimmed = str(href or "").strip()
    if not trimmed:
        return ""
    try:
        return urlparse(trimmed).hostname or trimmed
    except ValueError:
        return trimmed


def build_notice_inbox_metadata(
    notice: dict,
    *,
    is_reminder: bool,
    channel: str = "myco",
    recipient_name: Optional[str] = None,
) -> dict:
    due = notice.get("due_date")
    contacts = notice.get("contacts") or []
    primary = contacts[0] if contacts else {}
    metadata: Dict[str, Any] = {
        "notice_type": notice.get("notice_type"),
        "show_title": notice.get("show_title"),
        "brand_name": notice.get("brand_name"),
        "due_date": _format_due_date(due),
        "is_reminder": is_reminder,
        "channel": channel,
        "sender_name": notice.get("created_by_name"),
        "recipient_name": recipient_name
        or primary.get("myco_user_name")
        or primary.get("contact_name")
        or notice.get("myco_recipient_name"),
    }
    if notice.get("ad_copy_link"):
        metadata["ad_copy_link"] = notice.get("ad_copy_link")
    if notice.get("brand_overview"):
        metadata["brand_overview"] = notice.get("brand_overview")
    if notice.get("ad_type"):
        metadata["ad_type"] = notice.get("ad_type")
    if notice.get("notes"):
        metadata["notes"] = notice.get("notes")
    return metadata


def build_notice_inbox_body(notice: dict, *, is_reminder: bool) -> str:
    lines = []
    if is_reminder:
        lines.append("REMINDER")
        lines.append("")
    lines.append(f"Myco Notice — {_notice_type_label(notice.get('notice_type', ''))}")
    lines.append(f"Show: {notice.get('show_title') or '—'}")
    lines.append(f"Brand: {notice.get('brand_name') or '—'}")
    lines.append(f"Due date: {_format_due_date(notice.get('due_date'))}")
    if notice.get("ad_copy_link"):
        lines.append(f"Ad copy: {notice['ad_copy_link']}")
    if notice.get("brand_overview"):
        lines.append(f"Brand overview: {notice['brand_overview']}")
    if notice.get("ad_type"):
        ad_label = str(notice["ad_type"]).replace("_", " ").title()
        lines.append(f"Ad type: {ad_label}")
    if notice.get("notes"):
        lines.append(f"Notes: {notice['notes']}")
    return "\n".join(lines)


def build_notice_sms_body(notice: dict, *, is_reminder: bool) -> str:
    """Structured SMS body for Myco notice created and reminder sends."""
    show = (notice.get("show_title") or "").strip() or "Show"
    type_label = _notice_type_label(notice.get("notice_type", ""))
    urgency = _due_urgency_list_label(notice.get("due_date"))
    brand = (notice.get("brand_name") or "").strip()
    due_date = _format_due_date(notice.get("due_date"))

    headline = f"Myco Notice Reminder for {show}" if is_reminder else f"Myco Notice Created for {show}"
    lines = [headline]

    if urgency:
        lines.append(f"{type_label} - {urgency}")
    else:
        lines.append(type_label)

    if brand:
        lines.append(f"Brand: {brand}")

    if due_date and due_date != "—":
        lines.append(f"Due Date: {due_date}")

    if notice.get("notice_type") == "host_read_ads" and notice.get("ad_copy_link"):
        lines.append(f"Ad copy: {notice['ad_copy_link']}")

    open_notice_url = build_notice_open_url(notice.get("id"), notice)
    if open_notice_url:
        lines.append(f"Open notice: {open_notice_url}")

    return "\n".join(lines)[:1500]


def _trim_or_empty(value: Any) -> str:
    return str(value or "").strip()


def _notice_contact(notice: dict) -> Dict[str, str]:
    contacts = notice.get("contacts") or []
    if contacts:
        names = [_trim_or_empty(c.get("contact_name")) for c in contacts if _trim_or_empty(c.get("contact_name"))]
        emails = [_trim_or_empty(c.get("contact_email")) for c in contacts if _trim_or_empty(c.get("contact_email"))]
        phones = [_trim_or_empty(c.get("contact_phone")) for c in contacts if _trim_or_empty(c.get("contact_phone"))]
        return {
            "name": ", ".join(names),
            "email": ", ".join(emails),
            "phone": ", ".join(phones),
        }
    return {
        "name": _trim_or_empty(notice.get("contact_name")),
        "email": _trim_or_empty(notice.get("contact_email")),
        "phone": _trim_or_empty(notice.get("contact_phone")),
    }


def _notice_show_brand(notice: dict) -> Dict[str, str]:
    return {
        "show": _trim_or_empty(notice.get("show_title")),
        "brand": _trim_or_empty(notice.get("brand_name")),
    }


def _append_notice_contact_lines(lines: list, notice: dict) -> None:
    show_brand = _notice_show_brand(notice)
    contact = _notice_contact(notice)
    if show_brand["show"]:
        lines.append(f"Show: {show_brand['show']}")
    if show_brand["brand"]:
        lines.append(f"Brand: {show_brand['brand']}")
    if contact["name"]:
        lines.append(f"Contact: {contact['name']}")
    if contact["email"]:
        lines.append(f"Email: {contact['email']}")
    if contact["phone"]:
        lines.append(f"Phone: {contact['phone']}")


def build_delivery_failure_description1(notice: dict, failed_channels: list) -> str:
    labels = []
    for item in failed_channels:
        channel = str(item.get("channel", "unknown"))
        labels.append(channel[:1].upper() + channel[1:] if channel else "Unknown")
    if not labels:
        return ""
    channels = labels[0] if len(labels) == 1 else ", ".join(labels)
    recipient = _notice_contact(notice)["name"]
    if recipient:
        return f"{channels} Failed to send to {recipient}"
    return f"{channels} Failed to send"


def build_delivery_failure_body(notice: dict, failed_channels: list) -> str:
    notice_title = notice.get("show_title") or "Show"
    brand = notice.get("brand_name") or ""
    if brand:
        notice_title = f"Myco Notice — {notice_title} / {brand}"
    else:
        notice_title = f"Myco Notice — {notice_title}"
    preview = build_delivery_failure_description1(notice, failed_channels)
    lines = [preview, ""]
    _append_notice_contact_lines(lines, notice)
    if lines[-1] != "":
        lines.append("")
    lines.append(f"Failed channels for {notice_title}:")
    for item in failed_channels:
        channel = str(item.get("channel", "unknown")).title()
        error = item.get("error") or "Unknown error"
        lines.append(f"• {channel}: {error}")
    return "\n".join(lines)


def build_delivery_failure_metadata(
    notice: dict, failed_channels: list, *, is_reminder: bool
) -> dict:
    return {
        "failed_channels": failed_channels,
        "is_reminder": is_reminder,
        "show_title": notice.get("show_title"),
        "brand_name": notice.get("brand_name"),
        "contact_name": _notice_contact(notice)["name"],
        "contact_email": _notice_contact(notice)["email"],
        "contact_phone": _notice_contact(notice)["phone"],
        "sender_name": "Myco Notices",
        "recipient_name": notice.get("created_by_name"),
    }
