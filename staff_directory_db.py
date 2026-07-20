"""Database operations for the Staff Directory module."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from models import STAFF_LINK_ELIGIBLE_ROLES
from sqlclient import SqlClient

PARTNER_ROLE = "partner"

_STAFF_SELECT = """
SELECT
  s.id,
  s.name,
  s.pronouns,
  s.title,
  s.department,
  s.is_supervisor,
  s.supervisor_id,
  s.email,
  s.user_id,
  s.google_voice_number,
  s.personal_phone,
  s.linkedin_url,
  s.created_by,
  s.updated_by,
  s.created_at,
  s.updated_at,
  sup.name AS supervisor_name,
  lu.name AS linked_user_name,
  lu.role AS linked_user_role,
  cb.name AS created_by_name,
  ub.name AS updated_by_name
FROM staff_members s
LEFT JOIN staff_members sup ON sup.id = s.supervisor_id
LEFT JOIN users lu ON lu.id = s.user_id
LEFT JOIN users cb ON cb.id = s.created_by
LEFT JOIN users ub ON ub.id = s.updated_by
"""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value in (1, "1", b"\x01"):
        return True
    return bool(value)


def _row_to_staff(row: Optional[dict]) -> Optional[dict]:
    if not row:
        return None
    out = dict(row)
    out["is_supervisor"] = _normalize_bool(out.get("is_supervisor"))
    for key in (
        "id",
        "supervisor_id",
        "user_id",
        "created_by",
        "updated_by",
    ):
        if out.get(key) is not None:
            out[key] = str(out[key])
    if out.get("department") is not None:
        out["department"] = str(out["department"])
    if out.get("linked_user_role") is not None:
        out["linked_user_role"] = str(out["linked_user_role"])
    return out


class StaffDirectoryDb:
    def __init__(self, client: Optional[SqlClient] = None):
        self.client = client or SqlClient()

    def lookup_user_by_email(self, email: str) -> Tuple[Optional[dict], Optional[str]]:
        sql = "SELECT id, name, email, role FROM users WHERE LOWER(email) = LOWER(%s) LIMIT 1"
        row, _, err = self.client._execute_query(sql, (email.strip(),), fetch="one")
        if err:
            return None, str(err)
        if not row:
            return None, None
        return {
            "id": str(row["id"]),
            "name": row.get("name"),
            "email": row.get("email"),
            "role": str(row.get("role") or ""),
        }, None

    def resolve_email_link(self, email: str) -> Tuple[dict, Optional[str]]:
        """Return {status, user_id?, user_name?, user_role?} for an email."""
        user, err = self.lookup_user_by_email(email)
        if err:
            return {}, err
        if not user:
            return {"status": "unlinked"}, None
        role = user.get("role") or ""
        if role == PARTNER_ROLE:
            return {
                "status": "partner_blocked",
                "user_name": user.get("name"),
                "user_role": role,
            }, None
        if role in STAFF_LINK_ELIGIBLE_ROLES:
            return {
                "status": "linked",
                "user_id": user["id"],
                "user_name": user.get("name"),
                "user_role": role,
            }, None
        return {"status": "unlinked"}, None

    def get_staff_by_id(self, staff_id: str) -> Tuple[Optional[dict], Optional[str]]:
        sql = _STAFF_SELECT + " WHERE s.id = %s"
        row, _, err = self.client._execute_query(sql, (staff_id,), fetch="one")
        if err:
            return None, str(err)
        return _row_to_staff(row), None

    def list_staff(
        self,
        department: Optional[str] = None,
        is_supervisor: Optional[bool] = None,
    ) -> Tuple[List[dict], Optional[str]]:
        clauses = []
        params: List[Any] = []
        if department:
            clauses.append("s.department = %s")
            params.append(department)
        if is_supervisor is not None:
            clauses.append("s.is_supervisor = %s")
            params.append(1 if is_supervisor else 0)
        sql = _STAFF_SELECT
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY s.name ASC"
        rows, _, err = self.client._execute_query(sql, tuple(params) if params else None, fetch="all")
        if err:
            return [], str(err)
        return [_row_to_staff(r) for r in (rows or [])], None

    def list_supervisors(self, exclude_id: Optional[str] = None) -> Tuple[List[dict], Optional[str]]:
        sql = """
        SELECT id, name, title, department
        FROM staff_members
        WHERE is_supervisor = 1
        """
        params: List[Any] = []
        if exclude_id:
            sql += " AND id <> %s"
            params.append(exclude_id)
        sql += " ORDER BY name ASC"
        rows, _, err = self.client._execute_query(sql, tuple(params) if params else None, fetch="all")
        if err:
            return [], str(err)
        result = []
        for r in rows or []:
            result.append(
                {
                    "id": str(r["id"]),
                    "name": r.get("name") or "",
                    "title": r.get("title") or "",
                    "department": str(r.get("department")),
                }
            )
        return result, None

    def email_taken(self, email: str, exclude_id: Optional[str] = None) -> Tuple[bool, Optional[str]]:
        sql = "SELECT id FROM staff_members WHERE LOWER(email) = LOWER(%s)"
        params: List[Any] = [email.strip()]
        if exclude_id:
            sql += " AND id <> %s"
            params.append(exclude_id)
        sql += " LIMIT 1"
        row, _, err = self.client._execute_query(sql, tuple(params), fetch="one")
        if err:
            return False, str(err)
        return bool(row), None

    def find_staff_by_email(
        self, email: str, exclude_id: Optional[str] = None
    ) -> Tuple[Optional[dict], Optional[str]]:
        sql = "SELECT id, name, email FROM staff_members WHERE LOWER(email) = LOWER(%s)"
        params: List[Any] = [email.strip()]
        if exclude_id:
            sql += " AND id <> %s"
            params.append(exclude_id)
        sql += " LIMIT 1"
        row, _, err = self.client._execute_query(sql, tuple(params), fetch="one")
        if err:
            return None, str(err)
        if not row:
            return None, None
        return {
            "id": str(row["id"]),
            "name": row.get("name") or "",
            "email": row.get("email") or "",
        }, None

    def user_id_taken(self, user_id: str, exclude_id: Optional[str] = None) -> Tuple[bool, Optional[str]]:
        sql = "SELECT id FROM staff_members WHERE user_id = %s"
        params: List[Any] = [user_id]
        if exclude_id:
            sql += " AND id <> %s"
            params.append(exclude_id)
        sql += " LIMIT 1"
        row, _, err = self.client._execute_query(sql, tuple(params), fetch="one")
        if err:
            return False, str(err)
        return bool(row), None

    def get_supervisor(self, supervisor_id: str) -> Tuple[Optional[dict], Optional[str]]:
        sql = "SELECT id, name, is_supervisor FROM staff_members WHERE id = %s"
        row, _, err = self.client._execute_query(sql, (supervisor_id,), fetch="one")
        if err:
            return None, str(err)
        if not row:
            return None, None
        return {
            "id": str(row["id"]),
            "name": row.get("name"),
            "is_supervisor": _normalize_bool(row.get("is_supervisor")),
        }, None

    def count_reports(self, supervisor_id: str) -> Tuple[int, Optional[str]]:
        sql = "SELECT COUNT(*) AS cnt FROM staff_members WHERE supervisor_id = %s"
        row, _, err = self.client._execute_query(sql, (supervisor_id,), fetch="one")
        if err:
            return 0, str(err)
        return int((row or {}).get("cnt") or 0), None

    def create_staff(
        self,
        *,
        data: Dict[str, Any],
        user_id: Optional[str],
        created_by: str,
    ) -> Tuple[Optional[dict], Optional[str]]:
        staff_id = str(uuid.uuid4())
        now = _utcnow()
        sql = """
        INSERT INTO staff_members (
          id, name, pronouns, title, department, is_supervisor, supervisor_id,
          email, user_id, google_voice_number, personal_phone, linkedin_url,
          created_by, updated_by, created_at, updated_at
        ) VALUES (
          %s, %s, %s, %s, %s, %s, %s,
          %s, %s, %s, %s, %s,
          %s, %s, %s, %s
        )
        """
        values = (
            staff_id,
            data["name"],
            data.get("pronouns"),
            data["title"],
            data["department"],
            1 if data.get("is_supervisor") else 0,
            data.get("supervisor_id"),
            str(data["email"]).strip(),
            user_id,
            data.get("google_voice_number"),
            data.get("personal_phone"),
            data.get("linkedin_url"),
            created_by,
            created_by,
            now,
            now,
        )
        _, _, err = self.client._execute_query(sql, values, is_transaction=True)
        if err:
            return None, str(err)
        return self.get_staff_by_id(staff_id)

    def update_staff(
        self,
        staff_id: str,
        *,
        data: Dict[str, Any],
        user_id: Optional[str],
        updated_by: str,
    ) -> Tuple[Optional[dict], Optional[str]]:
        now = _utcnow()
        sql = """
        UPDATE staff_members SET
          name = %s,
          pronouns = %s,
          title = %s,
          department = %s,
          is_supervisor = %s,
          supervisor_id = %s,
          email = %s,
          user_id = %s,
          google_voice_number = %s,
          personal_phone = %s,
          linkedin_url = %s,
          updated_by = %s,
          updated_at = %s
        WHERE id = %s
        """
        values = (
            data["name"],
            data.get("pronouns"),
            data["title"],
            data["department"],
            1 if data.get("is_supervisor") else 0,
            data.get("supervisor_id"),
            str(data["email"]).strip(),
            user_id,
            data.get("google_voice_number"),
            data.get("personal_phone"),
            data.get("linkedin_url"),
            updated_by,
            now,
            staff_id,
        )
        _, _, err = self.client._execute_query(sql, values, is_transaction=True)
        if err:
            return None, str(err)
        return self.get_staff_by_id(staff_id)

    def delete_staff(self, staff_id: str) -> Tuple[bool, Optional[str]]:
        sql = "DELETE FROM staff_members WHERE id = %s"
        _, affected, err = self.client._execute_query(sql, (staff_id,), is_transaction=True)
        if err:
            return False, str(err)
        if not affected:
            return False, "Staff member not found"
        return True, None

    def link_staff_by_user_email(self, user_id: str, email: str) -> Tuple[bool, Optional[str]]:
        """Link staff row matching email to this user (admin/internal only caller)."""
        if not email or not user_id:
            return True, None
        # Clear any previous link for this user on a different email
        clear_sql = "UPDATE staff_members SET user_id = NULL WHERE user_id = %s AND LOWER(email) <> LOWER(%s)"
        _, _, err = self.client._execute_query(clear_sql, (user_id, email.strip()), is_transaction=True)
        if err:
            return False, str(err)

        # If another staff already has this user_id with same email, nothing to do
        taken, err = self.user_id_taken(user_id)
        if err:
            return False, err
        if taken:
            # Already linked to some staff — if it's the matching email, OK
            sql = "SELECT id, email FROM staff_members WHERE user_id = %s LIMIT 1"
            row, _, err = self.client._execute_query(sql, (user_id,), fetch="one")
            if err:
                return False, str(err)
            if row and str(row.get("email") or "").lower() == email.strip().lower():
                return True, None
            return True, None

        sql = """
        UPDATE staff_members
        SET user_id = %s, updated_at = %s
        WHERE LOWER(email) = LOWER(%s) AND (user_id IS NULL OR user_id = %s)
        """
        _, _, err = self.client._execute_query(
            sql, (user_id, _utcnow(), email.strip(), user_id), is_transaction=True
        )
        if err:
            return False, str(err)
        return True, None

    def clear_staff_link_for_user(self, user_id: str) -> Tuple[bool, Optional[str]]:
        sql = "UPDATE staff_members SET user_id = NULL, updated_at = %s WHERE user_id = %s"
        _, _, err = self.client._execute_query(sql, (_utcnow(), user_id), is_transaction=True)
        if err:
            return False, str(err)
        return True, None
