"""Orchestrate multi-channel notice delivery."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from inbox_db import InboxDb
from myco_notices_db import MycoNoticesDb
from services.gmail_service import send_notice_email
from services.inbox_message_content import (
    build_delivery_failure_body,
    build_delivery_failure_metadata,
    build_notice_inbox_body,
    build_notice_inbox_metadata,
)
from services.myco_client_url import resolve_client_base_url
from services.twilio_sms_service import send_notice_sms


def _with_client_base_url(notice: dict, client_base_url: Optional[str] = None) -> dict:
    resolved = resolve_client_base_url(notice=notice, explicit=client_base_url)
    if not resolved:
        return notice
    enriched = dict(notice)
    enriched["client_base_url"] = resolved
    return enriched


def _notice_title(notice: dict) -> str:
    show = notice.get("show_title") or "Show"
    brand = notice.get("brand_name") or ""
    return f"Myco Notice — {show} / {brand}"


def _is_reminder_context(notice: dict) -> bool:
    return (notice.get("send_count") or 0) > 0


def _notice_contacts(notice: dict) -> List[dict]:
    contacts = notice.get("contacts") or []
    return [c for c in contacts if isinstance(c, dict)]


def _notify_delivery_failure(
    inbox_db: InboxDb,
    notice: dict,
    failed_channels: List[dict],
    is_reminder: bool,
) -> None:
    if not failed_channels:
        return
    creator_id = notice.get("created_by")
    if not creator_id:
        return
    failure_metadata = build_delivery_failure_metadata(notice, failed_channels, is_reminder=is_reminder)
    inbox_db.create_inbox_message(
        creator_id,
        "delivery_failure",
        "Notice delivery failed",
        build_delivery_failure_body(notice, failed_channels),
        notice.get("id"),
        failure_metadata,
    )


def _contact_position(contact: dict) -> Optional[int]:
    try:
        position = int(contact.get("position") or 0)
    except (TypeError, ValueError):
        return None
    return position if position in (1, 2, 3) else None


def _normalize_contact_positions(positions: Optional[List[int]]) -> Optional[List[int]]:
    if not positions:
        return None
    cleaned: List[int] = []
    seen = set()
    for raw in positions:
        try:
            position = int(raw)
        except (TypeError, ValueError):
            continue
        if position not in (1, 2, 3) or position in seen:
            continue
        seen.add(position)
        cleaned.append(position)
    return cleaned or None


def _notice_with_contact_positions(notice: dict, positions: Optional[List[int]]) -> dict:
    """Return a shallow copy of notice limited to the selected contact positions."""
    normalized = _normalize_contact_positions(positions)
    if not normalized:
        return notice
    allowed = set(normalized)
    filtered = dict(notice)
    filtered["contacts"] = [
        contact
        for contact in _notice_contacts(notice)
        if _contact_position(contact) in allowed
    ]
    return filtered


def _deliver_email(
    notice: dict,
    notice_id: str,
    is_reminder: bool,
    db: MycoNoticesDb,
) -> Tuple[List[dict], List[dict]]:
    deliveries: List[dict] = []
    failures: List[dict] = []
    emails = []
    seen = set()
    for contact in _notice_contacts(notice):
        if not contact.get("channel_email"):
            continue
        email = (contact.get("contact_email") or "").strip()
        if not email:
            continue
        key = email.lower()
        if key in seen:
            continue
        seen.add(key)
        emails.append({"email": email, "position": _contact_position(contact)})

    if not emails:
        failure = {"channel": "email", "error": "No contact email"}
        delivery, _ = db.log_delivery(
            notice_id, "email", "failed", None, "No contact email", None, is_reminder
        )
        return [delivery or {"channel": "email", "status": "failed"}], [failure]

    for entry in emails:
        email = entry["email"]
        ok, detail, recipient = send_notice_email(notice, is_reminder, to_email=email)
        status = "sent" if ok else "failed"
        if not ok:
            failures.append({"channel": "email", "error": detail or "Email delivery failed", "recipient": email})
        delivery, _ = db.log_delivery(
            notice_id,
            "email",
            status,
            recipient,
            None if ok else detail,
            detail if ok else None,
            is_reminder,
            contact_position=entry.get("position"),
        )
        deliveries.append(
            delivery
            or {
                "channel": "email",
                "status": status,
                "recipient": email,
                "contact_position": entry.get("position"),
            }
        )
    return deliveries, failures


def _deliver_text(
    notice: dict,
    notice_id: str,
    is_reminder: bool,
    db: MycoNoticesDb,
) -> Tuple[List[dict], List[dict]]:
    deliveries: List[dict] = []
    failures: List[dict] = []
    phones = []
    seen = set()
    for contact in _notice_contacts(notice):
        if not contact.get("channel_text"):
            continue
        phone = (contact.get("contact_phone") or "").strip()
        if not phone:
            continue
        if phone in seen:
            continue
        seen.add(phone)
        phones.append({"phone": phone, "position": _contact_position(contact)})

    if not phones:
        failure = {"channel": "text", "error": "No contact phone"}
        delivery, _ = db.log_delivery(
            notice_id, "text", "failed", None, "No contact phone", None, is_reminder
        )
        return [delivery or {"channel": "text", "status": "failed"}], [failure]

    for entry in phones:
        phone = entry["phone"]
        ok, detail, recipient, twilio_status = send_notice_sms(notice, is_reminder, to_phone=phone)
        status = "sent" if ok else "failed"
        if not ok:
            failures.append({"channel": "text", "error": detail or "SMS delivery failed", "recipient": phone})
        delivery, _ = db.log_delivery(
            notice_id,
            "text",
            status,
            recipient,
            None if ok else detail,
            detail if ok else None,
            is_reminder,
            external_status=twilio_status if ok else None,
            contact_position=entry.get("position"),
        )
        if ok and delivery and twilio_status:
            from services.twilio_delivery_status import refresh_text_delivery_status

            delivery = refresh_text_delivery_status(delivery)
        deliveries.append(
            delivery
            or {
                "channel": "text",
                "status": status,
                "recipient": phone,
                "contact_position": entry.get("position"),
            }
        )
    return deliveries, failures


def _deliver_myco(
    notice: dict,
    notice_id: str,
    is_reminder: bool,
    db: MycoNoticesDb,
    inbox_db: InboxDb,
) -> Tuple[List[dict], List[dict]]:
    deliveries: List[dict] = []
    failures: List[dict] = []
    recipients = []
    seen = set()
    for contact in _notice_contacts(notice):
        if not contact.get("channel_myco"):
            continue
        recipient_id = str(contact.get("myco_user_id") or "").strip()
        if not recipient_id or recipient_id in seen:
            continue
        seen.add(recipient_id)
        recipients.append(
            {
                "id": recipient_id,
                "name": contact.get("myco_user_name") or contact.get("contact_name"),
                "position": _contact_position(contact),
            }
        )

    if not recipients:
        failure = {"channel": "myco", "error": "No MYCO recipient user"}
        delivery, _ = db.log_delivery(
            notice_id, "myco", "failed", None, "No MYCO recipient user", None, is_reminder
        )
        return [delivery or {"channel": "myco", "status": "failed"}], [failure]

    title = _notice_title(notice)
    body = build_notice_inbox_body(notice, is_reminder=is_reminder)

    for recipient in recipients:
        recipient_id = recipient["id"]
        metadata = build_notice_inbox_metadata(
            notice,
            is_reminder=is_reminder,
            channel="myco",
            recipient_name=recipient.get("name"),
        )
        message, merr = inbox_db.create_inbox_message(
            recipient_id, "notice_delivery", title, body, notice_id, metadata
        )
        ok = message is not None and not merr
        status = "sent" if ok else "failed"
        if not ok:
            failures.append(
                {
                    "channel": "myco",
                    "error": merr or "Myco inbox delivery failed",
                    "recipient": recipient_id,
                }
            )
        delivery, _ = db.log_delivery(
            notice_id,
            "myco",
            status,
            recipient_id,
            merr if not ok else None,
            message.get("id") if ok and message else None,
            is_reminder,
            contact_position=recipient.get("position"),
        )
        deliveries.append(
            delivery
            or {
                "channel": "myco",
                "status": status,
                "recipient": recipient_id,
                "contact_position": recipient.get("position"),
            }
        )
    return deliveries, failures


def send_notice_channel(
    notice_id: str,
    channel: str,
    is_reminder: Optional[bool] = None,
    client_base_url: Optional[str] = None,
    contact_positions: Optional[List[int]] = None,
) -> Tuple[Dict[str, Any], Optional[str]]:
    """Manually deliver a single channel without advancing the notice schedule."""
    db = MycoNoticesDb()
    inbox_db = InboxDb(db.client)
    notice, err = db.get_notice_by_id(notice_id)
    if err:
        return {}, err
    notice = _with_client_base_url(notice, client_base_url)
    if notice.get("status") != "active":
        return {}, "Notice is not active"

    requested_positions = _normalize_contact_positions(contact_positions)
    if contact_positions is not None and not requested_positions:
        return {}, "Select at least one valid contact (1–3)"
    if requested_positions:
        available = {
            pos
            for pos in (_contact_position(c) for c in _notice_contacts(notice))
            if pos is not None
        }
        missing = [pos for pos in requested_positions if pos not in available]
        if missing:
            return {}, f"Contact {missing[0]} is not on this notice"
        notice = _notice_with_contact_positions(notice, requested_positions)

    channel_enabled = {
        "email": notice.get("channel_email"),
        "text": notice.get("channel_text"),
        "myco": notice.get("channel_myco"),
    }
    if channel not in channel_enabled:
        return {}, f"Invalid channel: {channel}"
    if not channel_enabled[channel]:
        return {}, f"{channel.title()} channel is not enabled for this notice"

    contacts = _notice_contacts(notice)
    if not contacts:
        return {}, "Select at least one contact"
    if channel == "email" and not any(
        c.get("channel_email") and (c.get("contact_email") or "").strip() for c in contacts
    ):
        return {}, "Email channel requires a selected contact with Email enabled"
    if channel == "text" and not any(
        c.get("channel_text") and (c.get("contact_phone") or "").strip() for c in contacts
    ):
        return {}, "Text channel requires a selected contact with Text enabled"
    if channel == "myco" and not any(
        c.get("channel_myco") and (c.get("myco_user_id") or "").strip() for c in contacts
    ):
        return {}, "MYCO channel requires a selected contact with MYCO enabled and a matching account"

    reminder = _is_reminder_context(notice) if is_reminder is None else is_reminder

    if channel == "email":
        deliveries, failures = _deliver_email(notice, notice_id, reminder, db)
    elif channel == "text":
        deliveries, failures = _deliver_text(notice, notice_id, reminder, db)
    else:
        deliveries, failures = _deliver_myco(notice, notice_id, reminder, db, inbox_db)

    if failures:
        _notify_delivery_failure(inbox_db, notice, failures, reminder)

    # API response historically returned a single delivery; keep last for compatibility.
    return {
        "notice_id": notice_id,
        "channel": channel,
        "delivery": deliveries[-1] if deliveries else {"channel": channel, "status": "failed"},
        "deliveries": deliveries,
    }, None


def send_notice(
    notice_id: str,
    is_reminder: bool = False,
    client_base_url: Optional[str] = None,
) -> Tuple[Dict[str, Any], Optional[str]]:
    db = MycoNoticesDb()
    inbox_db = InboxDb(db.client)
    notice, err = db.get_notice_by_id(notice_id)
    if err:
        return {}, err
    notice = _with_client_base_url(notice, client_base_url)
    if notice.get("status") != "active":
        return {}, "Notice is not active"

    claimed, claim_err = db.claim_notice_for_send(notice_id, int(notice.get("frequency_hours") or 24))
    if claim_err:
        return {}, claim_err
    if not claimed:
        return {"notice_id": notice_id, "skipped": True, "deliveries": [], "any_sent": False}, None

    results: List[dict] = []
    any_sent = False
    failed_channels: List[dict] = []

    if notice.get("channel_email"):
        deliveries, failures = _deliver_email(notice, notice_id, is_reminder, db)
        results.extend(deliveries)
        failed_channels.extend(failures)
        if any(d.get("status") == "sent" for d in deliveries):
            any_sent = True

    if notice.get("channel_text"):
        deliveries, failures = _deliver_text(notice, notice_id, is_reminder, db)
        results.extend(deliveries)
        failed_channels.extend(failures)
        if any(d.get("status") == "sent" for d in deliveries):
            any_sent = True

    if notice.get("channel_myco"):
        deliveries, failures = _deliver_myco(notice, notice_id, is_reminder, db, inbox_db)
        results.extend(deliveries)
        failed_channels.extend(failures)
        if any(d.get("status") == "sent" for d in deliveries):
            any_sent = True

    if failed_channels:
        _notify_delivery_failure(inbox_db, notice, failed_channels, is_reminder)

    return {"notice_id": notice_id, "deliveries": results, "any_sent": any_sent}, None
