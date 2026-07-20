"""Staff Directory API routes."""
from __future__ import annotations

import csv
import io
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from models import (
    PARTNER_STAFF_EMAIL_BLOCK_MSG,
    StaffDepartment,
    StaffEmailLinkCheckResponse,
    StaffEmailLinkStatus,
    StaffMemberCreate,
    StaffMemberRead,
    StaffMemberUpdate,
    StaffSupervisorOption,
)
from staff_directory_db import StaffDirectoryDb

router = APIRouter()


def _user_field(user, field: str):
    if isinstance(user, dict):
        return user.get(field)
    return getattr(user, field, None)


def _payload_dict(payload) -> dict:
    data = payload.model_dump()
    if hasattr(data.get("department"), "value"):
        data["department"] = data["department"].value
    elif data.get("department") is not None:
        data["department"] = str(data["department"])
    data["email"] = str(data["email"]).strip()
    return data


def _resolve_link_or_raise(db: StaffDirectoryDb, email: str, exclude_staff_id: Optional[str] = None):
    link, err = db.resolve_email_link(email)
    if err:
        raise HTTPException(status_code=500, detail=err)
    status = link.get("status")
    if status == "partner_blocked":
        raise HTTPException(status_code=400, detail=PARTNER_STAFF_EMAIL_BLOCK_MSG)
    user_id = link.get("user_id") if status == "linked" else None
    if user_id:
        taken, err = db.user_id_taken(user_id, exclude_id=exclude_staff_id)
        if err:
            raise HTTPException(status_code=500, detail=err)
        if taken:
            raise HTTPException(
                status_code=409,
                detail="This Myco account is already linked to another staff member.",
            )
    return user_id


def _validate_supervisor(db: StaffDirectoryDb, data: dict, staff_id: Optional[str] = None):
    supervisor_id = data.get("supervisor_id")
    if not supervisor_id:
        return
    if staff_id and supervisor_id == staff_id:
        raise HTTPException(status_code=400, detail="A staff member cannot be their own supervisor.")
    supervisor, err = db.get_supervisor(supervisor_id)
    if err:
        raise HTTPException(status_code=500, detail=err)
    if not supervisor:
        raise HTTPException(status_code=400, detail="Selected supervisor was not found.")
    if not supervisor.get("is_supervisor"):
        raise HTTPException(status_code=400, detail="Selected person is not marked as a supervisor.")


def register_staff_directory_routes(app, get_admin_or_internal_user, get_admin_user):
    @router.get("/staff-directory/check-email-link", response_model=StaffEmailLinkCheckResponse)
    def check_email_link(
        email: str = Query(..., min_length=3),
        exclude_id: Optional[str] = Query(None),
        _admin=Depends(get_admin_user),
    ):
        db = StaffDirectoryDb()
        existing, err = db.find_staff_by_email(email, exclude_id=exclude_id)
        if err:
            raise HTTPException(status_code=500, detail=err)
        if existing:
            return StaffEmailLinkCheckResponse(
                status=StaffEmailLinkStatus.already_exists,
                user_name=existing.get("name") or None,
            )
        link, err = db.resolve_email_link(email)
        if err:
            raise HTTPException(status_code=500, detail=err)
        return StaffEmailLinkCheckResponse(
            status=StaffEmailLinkStatus(link.get("status") or "unlinked"),
            user_name=link.get("user_name"),
            user_role=link.get("user_role"),
        )

    @router.get("/staff-directory/supervisors", response_model=List[StaffSupervisorOption])
    def list_supervisors(
        exclude_id: Optional[str] = Query(None),
        _user=Depends(get_admin_or_internal_user),
    ):
        db = StaffDirectoryDb()
        rows, err = db.list_supervisors(exclude_id=exclude_id)
        if err:
            raise HTTPException(status_code=500, detail=err)
        return rows

    @router.get("/staff-directory/export")
    def export_staff_directory(_user=Depends(get_admin_or_internal_user)):
        db = StaffDirectoryDb()
        rows, err = db.list_staff()
        if err:
            raise HTTPException(status_code=500, detail=err)

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(
            [
                "Name",
                "Pronouns",
                "Title",
                "Department",
                "Is Supervisor",
                "Supervisor",
                "Email",
                "Google Voice",
                "Personal Phone",
                "LinkedIn",
                "Linked Myco User",
                "Linked Myco Role",
            ]
        )
        for r in rows:
            writer.writerow(
                [
                    r.get("name") or "",
                    r.get("pronouns") or "",
                    r.get("title") or "",
                    r.get("department") or "",
                    "Yes" if r.get("is_supervisor") else "No",
                    r.get("supervisor_name") or "",
                    r.get("email") or "",
                    r.get("google_voice_number") or "",
                    r.get("personal_phone") or "",
                    r.get("linkedin_url") or "",
                    r.get("linked_user_name") or "",
                    r.get("linked_user_role") or "",
                ]
            )
        buf.seek(0)
        return StreamingResponse(
            iter([buf.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=staff-directory.csv"},
        )

    @router.get("/staff-directory", response_model=List[StaffMemberRead])
    def list_staff_directory(
        department: Optional[StaffDepartment] = Query(None),
        is_supervisor: Optional[bool] = Query(None),
        _user=Depends(get_admin_or_internal_user),
    ):
        db = StaffDirectoryDb()
        dept = department.value if department else None
        rows, err = db.list_staff(department=dept, is_supervisor=is_supervisor)
        if err:
            raise HTTPException(status_code=500, detail=err)
        return rows

    @router.get("/staff-directory/{staff_id}", response_model=StaffMemberRead)
    def get_staff_member(staff_id: str, _user=Depends(get_admin_or_internal_user)):
        db = StaffDirectoryDb()
        row, err = db.get_staff_by_id(staff_id)
        if err:
            raise HTTPException(status_code=500, detail=err)
        if not row:
            raise HTTPException(status_code=404, detail="Staff member not found")
        return row

    @router.post("/staff-directory", response_model=StaffMemberRead, status_code=201)
    def create_staff_member(payload: StaffMemberCreate, admin=Depends(get_admin_user)):
        db = StaffDirectoryDb()
        data = _payload_dict(payload)
        taken, err = db.email_taken(data["email"])
        if err:
            raise HTTPException(status_code=500, detail=err)
        if taken:
            raise HTTPException(status_code=409, detail="A staff member with this email already exists.")
        _validate_supervisor(db, data)
        user_id = _resolve_link_or_raise(db, data["email"])
        admin_id = _user_field(admin, "id")
        if not admin_id:
            raise HTTPException(status_code=401, detail="Could not resolve user identity")
        row, err = db.create_staff(data=data, user_id=user_id, created_by=str(admin_id))
        if err:
            raise HTTPException(status_code=500, detail=err)
        return row

    @router.put("/staff-directory/{staff_id}", response_model=StaffMemberRead)
    def update_staff_member(
        staff_id: str,
        payload: StaffMemberUpdate,
        admin=Depends(get_admin_user),
    ):
        db = StaffDirectoryDb()
        existing, err = db.get_staff_by_id(staff_id)
        if err:
            raise HTTPException(status_code=500, detail=err)
        if not existing:
            raise HTTPException(status_code=404, detail="Staff member not found")

        data = _payload_dict(payload)
        taken, err = db.email_taken(data["email"], exclude_id=staff_id)
        if err:
            raise HTTPException(status_code=500, detail=err)
        if taken:
            raise HTTPException(status_code=409, detail="A staff member with this email already exists.")

        was_supervisor = bool(existing.get("is_supervisor"))
        will_be_supervisor = bool(data.get("is_supervisor"))
        if was_supervisor and not will_be_supervisor:
            reports, err = db.count_reports(staff_id)
            if err:
                raise HTTPException(status_code=500, detail=err)
            if reports > 0:
                raise HTTPException(
                    status_code=409,
                    detail="Cannot remove supervisor status while other staff still report to this person. Reassign them first.",
                )

        _validate_supervisor(db, data, staff_id=staff_id)
        user_id = _resolve_link_or_raise(db, data["email"], exclude_staff_id=staff_id)
        admin_id = _user_field(admin, "id")
        if not admin_id:
            raise HTTPException(status_code=401, detail="Could not resolve user identity")
        row, err = db.update_staff(
            staff_id, data=data, user_id=user_id, updated_by=str(admin_id)
        )
        if err:
            raise HTTPException(status_code=500, detail=err)
        return row

    @router.delete("/staff-directory/{staff_id}")
    def delete_staff_member(staff_id: str, _admin=Depends(get_admin_user)):
        db = StaffDirectoryDb()
        existing, err = db.get_staff_by_id(staff_id)
        if err:
            raise HTTPException(status_code=500, detail=err)
        if not existing:
            raise HTTPException(status_code=404, detail="Staff member not found")
        reports, err = db.count_reports(staff_id)
        if err:
            raise HTTPException(status_code=500, detail=err)
        if reports > 0:
            raise HTTPException(
                status_code=409,
                detail="Cannot delete this supervisor while other staff still report to them. Reassign them first.",
            )
        ok, err = db.delete_staff(staff_id)
        if not ok:
            raise HTTPException(status_code=500, detail=err or "Delete failed")
        return {"success": True}

    app.include_router(router)
