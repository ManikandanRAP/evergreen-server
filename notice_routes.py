"""MYCO Notices API routes."""
from __future__ import annotations

import csv
import io
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse

from models import (
    HostReadAdsNoticeCreate,
    MycoUserLookup,
    Notice,
    NoticeChannelDeliveryRequest,
    NoticeChannelDeliveryResponse,
    NoticeListItem,
    NoticeNotesUpdate,
    NoticeStatus,
    NoticeType,
    NoticeUpdate,
    ShowContactPreview,
    SponsorshipVettingNoticeCreate,
)
from myco_notices_db import MycoNoticesDb, normalize_contacts_payload, user_is_assigned_to_notice
from services.myco_client_url import resolve_client_base_url_from_request
from services.notice_delivery import send_notice, send_notice_channel

router = APIRouter()

NOTICES_MANAGER_ROLES = ("admin", "internal_full_access")


def _user_field(user, field: str):
    if isinstance(user, dict):
        return user.get(field)
    return getattr(user, field, None)


def _user_role(user) -> str:
    role = _user_field(user, "role")
    if role is None:
        return ""
    if hasattr(role, "value"):
        return str(role.value)
    return str(role)


def _can_manage_notices(user) -> bool:
    return _user_role(user) in NOTICES_MANAGER_ROLES


def _resolve_notice_user(user) -> tuple[str, str]:
    user_id = _user_field(user, "id")
    email = _user_field(user, "email")
    if not user_id:
        raise HTTPException(status_code=401, detail="Could not resolve user identity")
    return str(user_id), str(email or "").strip()


def _user_can_access_notice(notice: dict, user) -> bool:
    if _can_manage_notices(user):
        return True
    user_id, user_email = _resolve_notice_user(user)
    return user_is_assigned_to_notice(notice, user_id, user_email)


def _contacts_from_payload(contacts) -> list:
    return [
        {
            "contact_name": c.contact_name,
            "contact_email": str(c.contact_email) if c.contact_email else None,
            "contact_phone": c.contact_phone,
            "contact_source": c.contact_source.value if hasattr(c.contact_source, "value") else c.contact_source,
            "channel_email": bool(c.channel_email),
            "channel_text": bool(c.channel_text),
            "channel_myco": bool(c.channel_myco),
        }
        for c in contacts
    ]


def _validate_channels(data: dict) -> None:
    contacts = data.get("contacts") or []
    cleaned, cerr = normalize_contacts_payload(contacts)
    if cerr:
        raise HTTPException(status_code=400, detail=cerr)

    # Soft-resolve Myco: keep channel_myco only when a matching Myco user exists.
    db = MycoNoticesDb()
    for contact in cleaned:
        if contact.get("channel_myco"):
            contact["channel_myco"] = bool(db.resolve_myco_user_id(contact.get("contact_email")))

    if not any(
        c.get("channel_email") or c.get("channel_text") or c.get("channel_myco") for c in cleaned
    ):
        raise HTTPException(
            status_code=400,
            detail="At least one communication channel must be selected for a contact",
        )

    data["contacts"] = cleaned
    data["channel_email"] = any(c.get("channel_email") for c in cleaned)
    data["channel_text"] = any(c.get("channel_text") for c in cleaned)
    data["channel_myco"] = any(c.get("channel_myco") for c in cleaned)


def _create_payload_from_host(payload: HostReadAdsNoticeCreate) -> dict:
    return {
        "notice_type": NoticeType.host_read_ads.value,
        "show_id": payload.show_id,
        "brand_name": payload.brand_name,
        "ad_copy_link": payload.ad_copy_link,
        "brand_overview": None,
        "ad_type": None,
        "due_date": payload.due_date,
        "notes": payload.notes,
        "contacts": _contacts_from_payload(payload.contacts),
        "channel_email": payload.channel_email,
        "channel_text": payload.channel_text,
        "channel_myco": payload.channel_myco,
        "frequency_hours": payload.frequency_hours,
        "reminder_window_days": 7,
    }


def _create_payload_from_vetting(payload: SponsorshipVettingNoticeCreate) -> dict:
    return {
        "notice_type": NoticeType.sponsorship_vetting.value,
        "show_id": payload.show_id,
        "brand_name": payload.brand_name,
        "ad_copy_link": None,
        "brand_overview": payload.brand_overview,
        "ad_type": payload.ad_type.value,
        "due_date": payload.due_date,
        "notes": payload.notes,
        "contacts": _contacts_from_payload(payload.contacts),
        "channel_email": payload.channel_email,
        "channel_text": payload.channel_text,
        "channel_myco": payload.channel_myco,
        "frequency_hours": payload.frequency_hours,
        "reminder_window_days": 7,
    }


def _format_contacts_csv(contacts: list) -> tuple[str, str, str]:
    names = []
    emails = []
    phones = []
    for contact in contacts or []:
        if contact.get("contact_name"):
            names.append(contact["contact_name"])
        if contact.get("contact_email"):
            emails.append(contact["contact_email"])
        if contact.get("contact_phone"):
            phones.append(contact["contact_phone"])
    return "; ".join(names), "; ".join(emails), "; ".join(phones)


def register_notice_routes(app, get_notices_manager, get_current_active_user):
    @router.get("/notices/shows/{show_id}/contact-preview", response_model=ShowContactPreview)
    def contact_preview(show_id: str, _user=Depends(get_current_active_user)):
        db = MycoNoticesDb()
        contact, err = db.get_show_primary_contact(show_id)
        if err:
            raise HTTPException(status_code=404, detail=err)
        has_name = bool(contact.get("contact_name"))
        return ShowContactPreview(
            contact_name=contact.get("contact_name") or "",
            contact_email=contact.get("contact_email") or "",
            contact_phone=contact.get("contact_phone") or "",
            contact_source="auto_primary" if has_name else "manual",
        )

    @router.get("/notices/resolve-myco-user", response_model=MycoUserLookup)
    def resolve_myco_user(
        email: str = Query(..., min_length=3),
        _user=Depends(get_current_active_user),
    ):
        db = MycoNoticesDb()
        matched = db.resolve_myco_user(email)
        if not matched:
            return MycoUserLookup(email=email.strip(), matched=False)
        return MycoUserLookup(
            email=email.strip(),
            matched=True,
            user_id=matched.get("id"),
            user_name=matched.get("name"),
        )

    @router.post("/notices/host-read-ads", response_model=Notice, status_code=201)
    def create_host_read_ads(
        payload: HostReadAdsNoticeCreate,
        background_tasks: BackgroundTasks,
        request: Request,
        user=Depends(get_notices_manager),
    ):
        data = _create_payload_from_host(payload)
        _validate_channels(data)
        client_base_url = resolve_client_base_url_from_request(request)
        if client_base_url:
            data["client_base_url"] = client_base_url
        db = MycoNoticesDb()
        notice, err = db.create_notice(data, user.get("id"))
        if err:
            raise HTTPException(status_code=400, detail=err)
        background_tasks.add_task(send_notice, notice["id"], False, client_base_url or None)
        return notice

    @router.post("/notices/sponsorship-vetting", response_model=Notice, status_code=201)
    def create_sponsorship_vetting(
        payload: SponsorshipVettingNoticeCreate,
        background_tasks: BackgroundTasks,
        request: Request,
        user=Depends(get_notices_manager),
    ):
        data = _create_payload_from_vetting(payload)
        _validate_channels(data)
        client_base_url = resolve_client_base_url_from_request(request)
        if client_base_url:
            data["client_base_url"] = client_base_url
        db = MycoNoticesDb()
        notice, err = db.create_notice(data, user.get("id"))
        if err:
            raise HTTPException(status_code=400, detail=err)
        background_tasks.add_task(send_notice, notice["id"], False, client_base_url or None)
        return notice

    @router.get("/notices", response_model=list[NoticeListItem])
    def list_notices(
        status: Optional[NoticeStatus] = None,
        notice_type: Optional[NoticeType] = None,
        show_id: Optional[str] = None,
        search: Optional[str] = None,
        user=Depends(get_current_active_user),
    ):
        db = MycoNoticesDb()
        restrict_to_assigned_user = not _can_manage_notices(user)
        assigned_user_id = None
        assigned_user_email = None
        if restrict_to_assigned_user:
            assigned_user_id, assigned_user_email = _resolve_notice_user(user)
        notices, err = db.list_notices(
            status=status.value if status else None,
            notice_type=notice_type.value if notice_type else None,
            show_id=show_id,
            search=search,
            summary=True,
            restrict_to_assigned_user=restrict_to_assigned_user,
            assigned_user_id=assigned_user_id,
            assigned_user_email=assigned_user_email,
        )
        if err:
            raise HTTPException(status_code=500, detail=err)
        return notices

    @router.get("/notices/export")
    def export_notices_csv(
        status: Optional[NoticeStatus] = None,
        notice_type: Optional[NoticeType] = None,
        show_id: Optional[str] = None,
        search: Optional[str] = None,
        _user=Depends(get_notices_manager),
    ):
        db = MycoNoticesDb()
        notices, err = db.list_notices(
            status=status.value if status else None,
            notice_type=notice_type.value if notice_type else None,
            show_id=show_id,
            search=search,
        )
        if err:
            raise HTTPException(status_code=500, detail=err)

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "id", "type", "show", "brand", "status", "due_date", "contact_names",
            "contact_emails", "contact_phones", "channels", "frequency_hours",
            "last_sent_at", "next_send_at", "send_count", "created_at",
        ])
        for n in notices:
            channels = ",".join(
                c for c, on in [
                    ("email", n.get("channel_email")),
                    ("text", n.get("channel_text")),
                    ("myco", n.get("channel_myco")),
                ] if on
            )
            names, emails, phones = _format_contacts_csv(n.get("contacts") or [])
            writer.writerow([
                n.get("id"),
                n.get("notice_type"),
                n.get("show_title"),
                n.get("brand_name"),
                n.get("status"),
                n.get("due_date"),
                names,
                emails,
                phones,
                channels,
                n.get("frequency_hours"),
                n.get("last_sent_at"),
                n.get("next_send_at"),
                n.get("send_count"),
                n.get("created_at"),
            ])
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=myco-notices-export.csv"},
        )

    @router.get("/notices/{notice_id}", response_model=Notice)
    def get_notice(notice_id: str, user=Depends(get_current_active_user)):
        db = MycoNoticesDb()
        notice, err = db.get_notice_by_id(notice_id, include_deliveries=True)
        if err:
            code = 404 if "not found" in err.lower() else 500
            raise HTTPException(status_code=code, detail=err)
        if not _user_can_access_notice(notice, user):
            raise HTTPException(status_code=403, detail="Not enough permissions")
        return notice

    @router.patch("/notices/{notice_id}/notes", response_model=Notice)
    def update_notice_notes(
        notice_id: str,
        payload: NoticeNotesUpdate,
        user=Depends(get_current_active_user),
    ):
        db = MycoNoticesDb()
        notice, err = db.get_notice_by_id(notice_id)
        if err:
            code = 404 if "not found" in err.lower() else 500
            raise HTTPException(status_code=code, detail=err)
        if not _user_can_access_notice(notice, user):
            raise HTTPException(status_code=403, detail="Not enough permissions")
        if _can_manage_notices(user):
            raise HTTPException(
                status_code=403,
                detail="Use the full notice editor to update notes",
            )
        updated, err = db.update_notice_notes(notice_id, payload.notes)
        if err:
            code = 400 if "edit" in err.lower() else 500
            raise HTTPException(status_code=code, detail=err)
        return updated

    @router.put("/notices/{notice_id}", response_model=Notice)
    def update_notice(notice_id: str, payload: NoticeUpdate, _user=Depends(get_notices_manager)):
        db = MycoNoticesDb()
        updates = payload.model_dump(exclude_unset=True)
        if "ad_type" in updates and updates["ad_type"] is not None:
            updates["ad_type"] = updates["ad_type"].value
        if "contacts" in updates and updates["contacts"] is not None:
            updates["contacts"] = [
                {
                    "contact_name": c["contact_name"],
                    "contact_email": str(c["contact_email"]) if c.get("contact_email") else None,
                    "contact_phone": c.get("contact_phone"),
                    "contact_source": c.get("contact_source") or "manual",
                    "channel_email": bool(c.get("channel_email")),
                    "channel_text": bool(c.get("channel_text")),
                    "channel_myco": bool(c.get("channel_myco")),
                }
                for c in updates["contacts"]
            ]

        notice, err = db.get_notice_by_id(notice_id)
        if err:
            raise HTTPException(status_code=404, detail=err)

        def _require_non_empty(field: str, label: str) -> None:
            if field in updates and not (updates[field] or "").strip():
                raise HTTPException(status_code=400, detail=f"{label} cannot be empty")

        _require_non_empty("brand_name", "Brand name")
        if notice.get("notice_type") == "host_read_ads":
            _require_non_empty("ad_copy_link", "Ad copy link")
        else:
            _require_non_empty("brand_overview", "Brand overview")

        merged_check = {
            "channel_email": updates.get("channel_email", notice.get("channel_email")),
            "channel_text": updates.get("channel_text", notice.get("channel_text")),
            "channel_myco": updates.get("channel_myco", notice.get("channel_myco")),
            "contacts": updates.get("contacts")
            or [
                {
                    "contact_name": c.get("contact_name"),
                    "contact_email": c.get("contact_email"),
                    "contact_phone": c.get("contact_phone"),
                    "contact_source": c.get("contact_source") or "manual",
                    "channel_email": bool(c.get("channel_email")),
                    "channel_text": bool(c.get("channel_text")),
                    "channel_myco": bool(c.get("channel_myco")),
                }
                for c in (notice.get("contacts") or [])
            ],
        }
        _validate_channels(merged_check)
        updates["contacts"] = merged_check["contacts"]
        updates["channel_email"] = merged_check["channel_email"]
        updates["channel_text"] = merged_check["channel_text"]
        updates["channel_myco"] = merged_check["channel_myco"]

        updated, err = db.update_notice(notice_id, updates)
        if err:
            code = 400 if "edit" in err.lower() or "contact" in err.lower() else 500
            raise HTTPException(status_code=code, detail=err)
        return updated

    @router.delete("/notices/{notice_id}", status_code=204)
    def delete_notice(notice_id: str, _user=Depends(get_notices_manager)):
        db = MycoNoticesDb()
        ok, err = db.delete_notice(notice_id)
        if not ok:
            code = 404 if err and "not found" in err.lower() else 500
            raise HTTPException(status_code=code, detail=err or "Delete failed")
        return Response(status_code=204)

    @router.patch("/notices/{notice_id}/complete", response_model=Notice)
    def complete_notice(notice_id: str, user=Depends(get_current_active_user)):
        db = MycoNoticesDb()
        notice, err = db.get_notice_by_id(notice_id)
        if err:
            code = 404 if "not found" in err.lower() else 500
            raise HTTPException(status_code=code, detail=err)
        if not _user_can_access_notice(notice, user):
            raise HTTPException(status_code=403, detail="Not enough permissions")
        completed, err = db.set_notice_status(notice_id, "complete", _resolve_notice_user(user)[0])
        if err:
            code = 404 if "not found" in err.lower() else 400
            raise HTTPException(status_code=code, detail=err)
        return completed

    @router.patch("/notices/{notice_id}/cancel", response_model=Notice)
    def cancel_notice(notice_id: str, user=Depends(get_notices_manager)):
        db = MycoNoticesDb()
        notice, err = db.set_notice_status(notice_id, "cancelled", user.get("id"))
        if err:
            code = 404 if "not found" in err.lower() else 400
            raise HTTPException(status_code=code, detail=err)
        return notice

    @router.post("/notices/{notice_id}/deliver", response_model=NoticeChannelDeliveryResponse)
    def deliver_notice_channel(
        notice_id: str,
        payload: NoticeChannelDeliveryRequest,
        request: Request,
        user=Depends(get_notices_manager),
    ):
        client_base_url = resolve_client_base_url_from_request(request)
        result, err = send_notice_channel(
            notice_id,
            payload.channel.value,
            payload.is_reminder,
            client_base_url or None,
            payload.contact_positions,
        )
        if err:
            code = 404 if "not found" in err.lower() else 400
            raise HTTPException(status_code=code, detail=err)
        return result

    app.include_router(router)
