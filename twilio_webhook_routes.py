"""Twilio webhook endpoints for Myco Notices."""
from __future__ import annotations

import os

from fastapi import APIRouter, HTTPException, Request

from services.twilio_delivery_status import apply_twilio_status_update

router = APIRouter()


def _validate_twilio_signature(request: Request, form_data: dict) -> bool:
    auth_token = (os.getenv("TWILIO_AUTH_TOKEN") or "").strip()
    if not auth_token:
        return os.getenv("ENVIRONMENT", "").lower() == "development"

    signature = request.headers.get("X-Twilio-Signature", "")
    if not signature:
        return False

    try:
        from twilio.request_validator import RequestValidator

        url = str(request.url)
        return RequestValidator(auth_token).validate(url, form_data, signature)
    except Exception:
        return False


@router.post("/webhooks/twilio/sms-status")
async def twilio_sms_status_webhook(request: Request):
    form = await request.form()
    form_data = {key: form.get(key) for key in form.keys()}

    if not _validate_twilio_signature(request, form_data):
        raise HTTPException(status_code=403, detail="Invalid Twilio signature")

    message_sid = (form_data.get("MessageSid") or form_data.get("SmsSid") or "").strip()
    message_status = (form_data.get("MessageStatus") or form_data.get("SmsStatus") or "").strip()
    error_message = (form_data.get("ErrorMessage") or "").strip() or None

    if not message_sid or not message_status:
        raise HTTPException(status_code=400, detail="Missing MessageSid or MessageStatus")

    _, err = apply_twilio_status_update(message_sid, message_status, error_message)
    if err and "not found" not in err.lower():
        raise HTTPException(status_code=500, detail=err)

    return {"ok": True}
