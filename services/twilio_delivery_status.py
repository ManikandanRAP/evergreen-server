"""Sync Twilio SMS delivery status into Myco notice delivery records."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from myco_notices_db import MycoNoticesDb
from services.twilio_sms_service import TERMINAL_TWILIO_STATUSES, fetch_twilio_message_status


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _carrier_error_message(status_payload: Dict[str, Any]) -> Optional[str]:
    status = (status_payload.get("status") or "").lower()
    if status not in {"undelivered", "failed", "canceled"}:
        return None
    return status_payload.get("error_message") or f"SMS {status}"


def apply_twilio_status_update(
    message_sid: str,
    twilio_status: str,
    error_message: Optional[str] = None,
) -> Tuple[Optional[dict], Optional[str]]:
    db = MycoNoticesDb()
    normalized = (twilio_status or "").strip().lower()
    if not message_sid or not normalized:
        return None, "Missing message SID or status"
    return db.update_delivery_external_status_by_sid(
        message_sid,
        normalized,
        error_message if normalized in TERMINAL_TWILIO_STATUSES else None,
    )


def refresh_text_delivery_status(delivery: dict) -> dict:
    if delivery.get("channel") != "text" or delivery.get("status") != "sent":
        return delivery
    message_sid = (delivery.get("external_id") or "").strip()
    if not message_sid:
        return delivery

    current_status = (delivery.get("external_status") or "").strip().lower()
    if current_status in TERMINAL_TWILIO_STATUSES:
        return delivery

    payload, err = fetch_twilio_message_status(message_sid)
    if err or not payload or not payload.get("status"):
        return delivery

    updated, update_err = apply_twilio_status_update(
        message_sid,
        payload["status"],
        _carrier_error_message(payload),
    )
    if update_err or not updated:
        return delivery
    return updated


def refresh_text_delivery_statuses(deliveries: List[dict]) -> List[dict]:
    refreshed: List[dict] = []
    for delivery in deliveries:
        if delivery.get("channel") == "text":
            refreshed.append(refresh_text_delivery_status(delivery))
        else:
            refreshed.append(delivery)
    return refreshed
