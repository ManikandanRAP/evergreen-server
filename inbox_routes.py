"""Inbox messages API routes."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from inbox_constants import retention_label
from inbox_db import InboxDb
from inbox_events import inbox_sse_generator, notify_inbox_unread_changed, start_inbox_event_listener
from models import (
    InboxBulkRequest,
    InboxBulkResponse,
    InboxMessage,
    InboxMessageListResponse,
    InboxSettingsResponse,
    SystemSettingsPatch,
    SystemSettingsResponse,
)

router = APIRouter()


def register_inbox_routes(app, get_current_active_user, get_admin_user):
    @app.on_event("startup")
    def _start_inbox_events() -> None:
        start_inbox_event_listener()

    @router.get("/inbox/events")
    async def inbox_events_stream(user=Depends(get_current_active_user)):
        user_id = user.get("id")
        if not user_id:
            raise HTTPException(status_code=401, detail="Could not resolve user identity")
        return StreamingResponse(
            inbox_sse_generator(str(user_id)),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    @router.get("/inbox/settings", response_model=InboxSettingsResponse)
    def inbox_settings(_user=Depends(get_current_active_user)):
        db = InboxDb()
        days, err = db.get_inbox_retention_days()
        if err:
            raise HTTPException(status_code=500, detail=err)
        return InboxSettingsResponse(
            inbox_retention_days=days,
            inbox_retention_label=retention_label(days),
        )

    @router.get("/inbox/messages", response_model=InboxMessageListResponse)
    def list_inbox_messages(
        inbox_filter: str = Query("all"),
        unread_only: bool = Query(False),
        search: Optional[str] = Query(None),
        sort: str = Query("date_desc"),
        limit: int = Query(50, ge=1, le=100),
        before: Optional[datetime] = Query(None),
        before_id: Optional[str] = Query(None),
        before_unread: Optional[int] = Query(None, ge=0, le=1),
        after: Optional[datetime] = Query(None),
        after_id: Optional[str] = Query(None),
        user=Depends(get_current_active_user),
    ):
        db = InboxDb()
        items, has_more, err = db.list_inbox_messages(
            user.get("id"),
            inbox_filter=inbox_filter,
            unread_only=unread_only,
            search=search,
            sort=sort,
            limit=limit,
            before=before,
            before_id=before_id,
            before_unread=before_unread,
            after=after,
            after_id=after_id,
        )
        if err:
            raise HTTPException(status_code=500, detail=err)
        total_count, count_err = db.inbox_total_count(user.get("id"))
        if count_err:
            raise HTTPException(status_code=500, detail=count_err)
        filtered_total_count, filtered_err = db.inbox_filtered_count(
            user.get("id"),
            inbox_filter=inbox_filter,
            unread_only=unread_only,
            search=search,
        )
        if filtered_err:
            raise HTTPException(status_code=500, detail=filtered_err)
        return InboxMessageListResponse(
            items=items,
            has_more=has_more,
            total_count=total_count,
            filtered_total_count=filtered_total_count,
        )

    @router.get("/inbox/messages/unread-count")
    def inbox_unread_count(user=Depends(get_current_active_user)):
        db = InboxDb()
        count, err = db.inbox_unread_count(user.get("id"))
        if err:
            raise HTTPException(status_code=500, detail=err)
        return {"count": count}

    @router.get("/inbox/messages/{message_id}", response_model=InboxMessage)
    def get_inbox_message(message_id: str, user=Depends(get_current_active_user)):
        db = InboxDb()
        item, err = db.get_inbox_message(message_id, user.get("id"))
        if err:
            raise HTTPException(status_code=404, detail=err)
        return item

    @router.patch("/inbox/messages/{message_id}/read", response_model=InboxMessage)
    def mark_inbox_message_read(message_id: str, user=Depends(get_current_active_user)):
        db = InboxDb()
        item, err = db.mark_inbox_message_read(message_id, user.get("id"))
        if err:
            raise HTTPException(status_code=404, detail=err)
        notify_inbox_unread_changed(user.get("id"))
        return item

    @router.patch("/inbox/messages/read-all")
    def mark_all_inbox_messages_read(user=Depends(get_current_active_user)):
        db = InboxDb()
        count, err = db.mark_all_inbox_messages_read(user.get("id"))
        if err:
            raise HTTPException(status_code=500, detail=err)
        notify_inbox_unread_changed(user.get("id"), 0)
        return {"updated": count}

    @router.patch("/inbox/messages/{message_id}/unread", response_model=InboxMessage)
    def mark_inbox_message_unread(message_id: str, user=Depends(get_current_active_user)):
        db = InboxDb()
        item, err = db.mark_inbox_message_unread(message_id, user.get("id"))
        if err:
            raise HTTPException(status_code=404, detail=err)
        notify_inbox_unread_changed(user.get("id"))
        return item

    @router.patch("/inbox/messages/{message_id}/pin", response_model=InboxMessage)
    def pin_inbox_message(message_id: str, user=Depends(get_current_active_user)):
        db = InboxDb()
        item, err = db.set_inbox_message_pinned(message_id, user.get("id"), pinned=True)
        if err:
            status = 404 if err == "Message not found" else 400
            raise HTTPException(status_code=status, detail=err)
        return item

    @router.patch("/inbox/messages/{message_id}/unpin", response_model=InboxMessage)
    def unpin_inbox_message(message_id: str, user=Depends(get_current_active_user)):
        db = InboxDb()
        item, err = db.set_inbox_message_pinned(message_id, user.get("id"), pinned=False)
        if err:
            status = 404 if err == "Message not found" else 400
            raise HTTPException(status_code=status, detail=err)
        return item

    @router.post("/inbox/messages/bulk", response_model=InboxBulkResponse)
    def bulk_inbox_messages(payload: InboxBulkRequest, user=Depends(get_current_active_user)):
        db = InboxDb()
        if payload.select_all:
            result, err = db.bulk_inbox_action_by_query(
                user.get("id"),
                payload.action,
                inbox_filter=payload.inbox_filter,
                unread_only=payload.unread_only,
                search=payload.search,
            )
        else:
            if not payload.message_ids:
                raise HTTPException(status_code=400, detail="message_ids required")
            result, err = db.bulk_inbox_action(
                user.get("id"), payload.message_ids, payload.action
            )
        if err:
            status = 400 if err in {"Invalid action", "Pin not supported"} else 500
            raise HTTPException(status_code=status, detail=err)
        if result.get("unread_delta"):
            notify_inbox_unread_changed(user.get("id"))
        return InboxBulkResponse(**result)

    @router.delete("/inbox/messages/{message_id}")
    def delete_inbox_message(message_id: str, user=Depends(get_current_active_user)):
        db = InboxDb()
        deleted, was_unread, err = db.delete_inbox_message(message_id, user.get("id"))
        if err:
            status = 404 if err == "Message not found" else 500
            raise HTTPException(status_code=status, detail=err)
        if was_unread:
            notify_inbox_unread_changed(user.get("id"))
        return {"deleted": deleted, "was_unread": was_unread}

    @router.get("/admin/system-settings", response_model=SystemSettingsResponse)
    def get_system_settings(_admin=Depends(get_admin_user)):
        db = InboxDb()
        days, err = db.get_inbox_retention_days()
        if err:
            raise HTTPException(status_code=500, detail=err)
        return SystemSettingsResponse(
            inbox_retention_days=days,
            inbox_retention_label=retention_label(days),
        )

    @router.patch("/admin/system-settings", response_model=SystemSettingsResponse)
    def patch_system_settings(payload: SystemSettingsPatch, _admin=Depends(get_admin_user)):
        db = InboxDb()
        ok, err = db.set_inbox_retention_days(payload.inbox_retention_days)
        if not ok:
            raise HTTPException(status_code=400, detail=err or "Invalid retention setting")
        days, get_err = db.get_inbox_retention_days()
        if get_err:
            raise HTTPException(status_code=500, detail=get_err)
        return SystemSettingsResponse(
            inbox_retention_days=days,
            inbox_retention_label=retention_label(days),
        )

    app.include_router(router)
