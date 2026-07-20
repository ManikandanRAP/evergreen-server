"""Send MYCO Notices SMS via Twilio."""
from __future__ import annotations

import os
from typing import Any, Dict, Optional, Tuple

from services.env_loader import load_local_env
from services.inbox_message_content import build_notice_sms_body

load_local_env()

TERMINAL_TWILIO_STATUSES = frozenset({"delivered", "undelivered", "failed", "canceled"})


def _twilio_configured() -> bool:
    return bool(
        os.getenv("TWILIO_ACCOUNT_SID")
        and os.getenv("TWILIO_API_KEY")
        and os.getenv("TWILIO_API_SECRET")
        and os.getenv("TWILIO_FROM_NUMBER")
    )


def get_twilio_client():
    from twilio.rest import Client

    return Client(
        os.getenv("TWILIO_API_KEY"),
        os.getenv("TWILIO_API_SECRET"),
        os.getenv("TWILIO_ACCOUNT_SID"),
    )


def _status_callback_url() -> Optional[str]:
    return (os.getenv("TWILIO_STATUS_CALLBACK_URL") or "").strip() or None


def fetch_twilio_message_status(message_sid: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not _twilio_configured():
        return None, "Twilio not configured"
    try:
        message = get_twilio_client().messages(message_sid).fetch()
        error_message = message.error_message
        if message.error_code and not error_message:
            error_message = f"Twilio error {message.error_code}"
        return {
            "status": (message.status or "").strip().lower() or None,
            "error_code": message.error_code,
            "error_message": error_message,
        }, None
    except Exception as exc:
        return None, str(exc)


def send_notice_sms(
    notice: dict,
    is_reminder: bool = False,
    to_phone: Optional[str] = None,
) -> Tuple[bool, Optional[str], Optional[str], Optional[str]]:
    """Return (ok, sid_or_error, to_phone, initial_twilio_status)."""
    phone = (to_phone or notice.get("contact_phone") or "").strip()
    if not phone:
        contacts = notice.get("contacts") or []
        for contact in contacts:
            candidate = (contact.get("contact_phone") or "").strip()
            if candidate:
                phone = candidate
                break
    to_phone = phone
    if not to_phone:
        return False, "No contact phone", None, None
    if not _twilio_configured():
        return False, "Twilio not configured", to_phone, None

    try:
        create_kwargs: Dict[str, Any] = {
            "body": build_notice_sms_body(notice, is_reminder=is_reminder),
            "from_": os.getenv("TWILIO_FROM_NUMBER"),
            "to": to_phone,
        }
        callback_url = _status_callback_url()
        if callback_url:
            create_kwargs["status_callback"] = callback_url

        message = get_twilio_client().messages.create(**create_kwargs)
        twilio_status = (message.status or "").strip().lower() or None
        return True, message.sid, to_phone, twilio_status
    except Exception as exc:
        return False, str(exc), to_phone, None
