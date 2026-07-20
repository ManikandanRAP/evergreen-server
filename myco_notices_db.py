"""Database operations for MYCO Notices."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlclient import SqlClient

NOTICE_SELECT = """
SELECT
    n.*,
    s.title AS show_title,
    cu.name AS created_by_name,
    cu.email AS created_by_email,
    comp.name AS completed_by_name,
    canc.name AS cancelled_by_name
FROM myco_notices n
LEFT JOIN shows s ON n.show_id = s.id
LEFT JOIN users cu ON n.created_by = cu.id
LEFT JOIN users comp ON n.completed_by = comp.id
LEFT JOIN users canc ON n.cancelled_by = canc.id
"""

NOTICE_LIST_SELECT = """
SELECT
    n.id,
    n.notice_type,
    n.show_id,
    s.title AS show_title,
    n.brand_name,
    n.due_date,
    n.status,
    n.channel_email,
    n.channel_text,
    n.channel_myco,
    n.created_by,
    cu.name AS created_by_name,
    n.created_at
FROM myco_notices n
LEFT JOIN shows s ON n.show_id = s.id
LEFT JOIN users cu ON n.created_by = cu.id
"""

CONTACT_SELECT = """
SELECT
    c.id,
    c.notice_id,
    c.position,
    c.contact_name,
    c.contact_email,
    c.contact_phone,
    c.contact_source,
    c.myco_user_id,
    u.name AS myco_user_name,
    c.channel_email,
    c.channel_text,
    c.channel_myco,
    c.created_at,
    c.updated_at
FROM myco_notice_contacts c
LEFT JOIN users u ON c.myco_user_id = u.id
"""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _row_to_notice(row: dict) -> dict:
    if not row:
        return row
    row["channel_email"] = bool(row.get("channel_email"))
    row["channel_text"] = bool(row.get("channel_text"))
    row["channel_myco"] = bool(row.get("channel_myco"))
    row["is_reminder"] = bool(row.get("is_reminder", False))
    return row


def _row_to_contact(row: dict) -> dict:
    if not row:
        return row
    return {
        "id": row.get("id"),
        "notice_id": row.get("notice_id"),
        "position": int(row.get("position") or 0),
        "contact_name": row.get("contact_name") or "",
        "contact_email": row.get("contact_email"),
        "contact_phone": row.get("contact_phone"),
        "contact_source": row.get("contact_source") or "manual",
        "myco_user_id": row.get("myco_user_id"),
        "myco_user_name": row.get("myco_user_name"),
        "channel_email": bool(row.get("channel_email")),
        "channel_text": bool(row.get("channel_text")),
        "channel_myco": bool(row.get("channel_myco")),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }


def _normalize_one_contact(raw: dict, index: int) -> Tuple[Optional[dict], Optional[str]]:
    name = (raw.get("contact_name") or "").strip()
    email = (raw.get("contact_email") or "").strip() or None
    phone = (raw.get("contact_phone") or "").strip() or None
    source = (raw.get("contact_source") or "manual").strip() or "manual"
    if source not in ("auto_primary", "manual"):
        source = "manual"
    channel_email = bool(raw.get("channel_email"))
    channel_text = bool(raw.get("channel_text"))
    channel_myco = bool(raw.get("channel_myco"))

    label = f"Contact {index + 1}"
    if channel_email and not email:
        return None, f"{label}: Email channel requires an email address"
    if channel_text and not phone:
        return None, f"{label}: Text channel requires a phone number"

    return {
        "contact_name": name,
        "contact_email": email,
        "contact_phone": phone,
        "contact_source": "manual" if index > 0 else source,
        "channel_email": channel_email,
        "channel_text": channel_text,
        "channel_myco": channel_myco,
    }, None


def normalize_contacts_payload(contacts: Optional[List[dict]]) -> Tuple[List[dict], Optional[str]]:
    """Validate and normalize 1–3 contacts for create/update."""
    if not contacts:
        return [], "At least one contact is required"
    if len(contacts) > 3:
        return [], "A notice can have at most 3 contacts"

    cleaned: List[dict] = []
    for index, raw in enumerate(contacts):
        if not isinstance(raw, dict):
            return [], f"Contact {index + 1} is invalid"
        name = (raw.get("contact_name") or "").strip()
        email = (raw.get("contact_email") or "").strip() or None
        phone = (raw.get("contact_phone") or "").strip() or None
        has_any = bool(name or email or phone or raw.get("channel_email") or raw.get("channel_text") or raw.get("channel_myco"))

        if index == 0:
            if not name:
                return [], "Contact 1 name is required"
            item, err = _normalize_one_contact(raw, index)
            if err:
                return [], err
            cleaned.append(item)
            continue

        if not has_any:
            continue
        if not name:
            return [], f"Contact {index + 1} name is required when other fields are set"
        item, err = _normalize_one_contact(raw, index)
        if err:
            return [], err
        cleaned.append(item)

    if not cleaned:
        return [], "At least one contact is required"
    if len(cleaned) > 3:
        return [], "A notice can have at most 3 contacts"
    if not any(
        c.get("channel_email") or c.get("channel_text") or c.get("channel_myco") for c in cleaned
    ):
        return [], "At least one communication channel must be selected for a contact"
    return cleaned, None


def user_is_assigned_to_notice(
    notice: dict,
    user_id: str,
    user_email: Optional[str] = None,
) -> bool:
    user_id = str(user_id or "").strip()
    user_email_norm = (user_email or "").strip().lower()
    if not user_id and not user_email_norm:
        return False

    contacts = notice.get("contacts") or []
    for contact in contacts:
        if user_id and str(contact.get("myco_user_id") or "").strip() == user_id:
            return True
        contact_email = (contact.get("contact_email") or "").strip().lower()
        if contact_email and user_email_norm and contact_email == user_email_norm:
            return True
    return False


def assigned_notice_sql_clause() -> str:
    return """
        AND EXISTS (
            SELECT 1 FROM myco_notice_contacts c
            WHERE c.notice_id = n.id
              AND (
                    (c.myco_user_id IS NOT NULL AND c.myco_user_id = %s)
                 OR (
                        c.contact_email IS NOT NULL
                    AND TRIM(c.contact_email) <> ''
                    AND LOWER(TRIM(c.contact_email)) = LOWER(TRIM(%s))
                 )
              )
        )
    """


class MycoNoticesDb:
    def __init__(self, client: Optional[SqlClient] = None):
        self.client = client or SqlClient()

    def user_is_assigned_to_notice(
        self, notice: dict, user_id: str, user_email: Optional[str] = None
    ) -> bool:
        return user_is_assigned_to_notice(notice, user_id, user_email)

    def get_show_primary_contact(self, show_id: str) -> Tuple[Optional[dict], Optional[str]]:
        show, err = self.client.get_podcast_by_id(show_id)
        if err:
            return None, err
        if not show:
            return None, "Show not found"
        return {
            "contact_name": (show.get("primary_contact_name") or "").strip(),
            "contact_email": (show.get("primary_contact_email") or "").strip(),
            "contact_phone": (show.get("primary_contact_phone") or "").strip(),
            "contact_source": "auto_primary" if show.get("primary_contact_name") else "manual",
        }, None

    def resolve_myco_user(self, contact_email: Optional[str]) -> Optional[dict]:
        if not contact_email or not str(contact_email).strip():
            return None
        sql = "SELECT id, name, email FROM users WHERE LOWER(email) = LOWER(%s) LIMIT 1"
        row, _, err = self.client._execute_query(sql, (contact_email.strip(),), fetch="one")
        if err or not row:
            return None
        return {"id": row.get("id"), "name": row.get("name"), "email": row.get("email")}

    def resolve_myco_user_id(self, contact_email: Optional[str]) -> Optional[str]:
        user = self.resolve_myco_user(contact_email)
        return user.get("id") if user else None

    def _get_contacts_for_notice_ids(self, notice_ids: List[str]) -> Dict[str, List[dict]]:
        if not notice_ids:
            return {}
        placeholders = ", ".join(["%s"] * len(notice_ids))
        sql = f"""
        {CONTACT_SELECT}
        WHERE c.notice_id IN ({placeholders})
        ORDER BY c.notice_id, c.position ASC
        """
        rows, _, err = self.client._execute_query(sql, tuple(notice_ids), fetch="all")
        if err or not rows:
            return {}
        grouped: Dict[str, List[dict]] = {}
        for row in rows:
            contact = _row_to_contact(dict(row))
            grouped.setdefault(contact["notice_id"], []).append(contact)
        return grouped

    def get_contacts_for_notice(self, notice_id: str) -> Tuple[List[dict], Optional[str]]:
        grouped = self._get_contacts_for_notice_ids([notice_id])
        return grouped.get(notice_id, []), None

    def _attach_contacts(self, notices: List[dict]) -> List[dict]:
        ids = [n.get("id") for n in notices if n.get("id")]
        grouped = self._get_contacts_for_notice_ids(ids)
        for notice in notices:
            notice["contacts"] = grouped.get(notice.get("id"), [])
        return notices

    def _prepare_contacts_for_write(
        self, contacts: List[dict]
    ) -> Tuple[List[dict], Dict[str, bool], Optional[str]]:
        cleaned, err = normalize_contacts_payload(contacts)
        if err:
            return [], {}, err
        prepared: List[dict] = []
        for position, contact in enumerate(cleaned, start=1):
            myco_user = self.resolve_myco_user(contact.get("contact_email"))
            myco_user_id = myco_user.get("id") if myco_user else None
            channel_email = bool(contact.get("channel_email")) and bool(contact.get("contact_email"))
            channel_text = bool(contact.get("channel_text")) and bool(contact.get("contact_phone"))
            # Myco only when an account exists for that contact email.
            channel_myco = bool(contact.get("channel_myco")) and bool(myco_user_id)
            prepared.append(
                {
                    "position": position,
                    "contact_name": contact["contact_name"],
                    "contact_email": contact.get("contact_email"),
                    "contact_phone": contact.get("contact_phone"),
                    "contact_source": contact.get("contact_source") or "manual",
                    "myco_user_id": myco_user_id,
                    "myco_user_name": myco_user.get("name") if myco_user else None,
                    "channel_email": channel_email,
                    "channel_text": channel_text,
                    "channel_myco": channel_myco,
                }
            )
        if not any(
            c.get("channel_email") or c.get("channel_text") or c.get("channel_myco") for c in prepared
        ):
            return [], {}, "At least one communication channel must be selected for a contact"
        notice_channels = {
            "channel_email": any(c.get("channel_email") for c in prepared),
            "channel_text": any(c.get("channel_text") for c in prepared),
            "channel_myco": any(c.get("channel_myco") for c in prepared),
        }
        return prepared, notice_channels, None

    def _insert_contacts(self, notice_id: str, contacts: List[dict]) -> Optional[str]:
        now = _utcnow()
        sql = """
        INSERT INTO myco_notice_contacts (
            id, notice_id, position, contact_name, contact_email, contact_phone,
            contact_source, myco_user_id, channel_email, channel_text, channel_myco,
            created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for contact in contacts:
            params = (
                str(uuid.uuid4()),
                notice_id,
                contact["position"],
                contact["contact_name"],
                contact.get("contact_email"),
                contact.get("contact_phone"),
                contact.get("contact_source") or "manual",
                contact.get("myco_user_id"),
                bool(contact.get("channel_email")),
                bool(contact.get("channel_text")),
                bool(contact.get("channel_myco")),
                now,
                now,
            )
            _, _, err = self.client._execute_query(sql, params, is_transaction=True)
            if err:
                return str(err)
        return None

    def _replace_contacts(self, notice_id: str, contacts: List[dict]) -> Optional[str]:
        _, _, del_err = self.client._execute_query(
            "DELETE FROM myco_notice_contacts WHERE notice_id = %s",
            (notice_id,),
            is_transaction=True,
        )
        if del_err:
            return str(del_err)
        return self._insert_contacts(notice_id, contacts)

    def create_notice(self, data: dict, created_by: str) -> Tuple[Optional[dict], Optional[str]]:
        contacts, notice_channels, cerr = self._prepare_contacts_for_write(data.get("contacts") or [])
        if cerr:
            return None, cerr

        notice_id = str(uuid.uuid4())
        now = _utcnow()

        sql = """
        INSERT INTO myco_notices (
            id, notice_type, show_id, brand_name, ad_copy_link, brand_overview, ad_type,
            due_date, notes,
            channel_email, channel_text, channel_myco, frequency_hours, reminder_window_days,
            reminder_started_at, status, last_sent_at, next_send_at, send_count,
            client_base_url, created_by, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s, %s,
            %s, 'active', NULL, %s, 0,
            %s, %s, %s, %s
        )
        """
        params = (
            notice_id,
            data["notice_type"],
            data["show_id"],
            data["brand_name"],
            data.get("ad_copy_link"),
            data.get("brand_overview"),
            data.get("ad_type"),
            data["due_date"],
            data.get("notes"),
            bool(notice_channels.get("channel_email")),
            bool(notice_channels.get("channel_text")),
            bool(notice_channels.get("channel_myco")),
            int(data.get("frequency_hours", 24)),
            int(data.get("reminder_window_days", 7)),
            now,
            now,
            data.get("client_base_url"),
            created_by,
            now,
            now,
        )
        _, _, err = self.client._execute_query(sql, params, is_transaction=True)
        if err:
            return None, str(err)

        contact_err = self._insert_contacts(notice_id, contacts)
        if contact_err:
            self.client._execute_query(
                "DELETE FROM myco_notices WHERE id = %s", (notice_id,), is_transaction=True
            )
            return None, contact_err

        return self.get_notice_by_id(notice_id)

    def get_notice_by_id(self, notice_id: str, include_deliveries: bool = False) -> Tuple[Optional[dict], Optional[str]]:
        sql = f"{NOTICE_SELECT} WHERE n.id = %s"
        row, _, err = self.client._execute_query(sql, (notice_id,), fetch="one")
        if err:
            return None, str(err)
        if not row:
            return None, "Notice not found"
        notice = _row_to_notice(dict(row))
        contacts, cerr = self.get_contacts_for_notice(notice_id)
        if cerr:
            return None, cerr
        notice["contacts"] = contacts
        if include_deliveries:
            deliveries, derr = self.get_deliveries_for_notice(notice_id)
            if derr:
                return None, derr
            notice["deliveries"] = deliveries
        return notice, None

    def list_notices(
        self,
        status: Optional[str] = None,
        notice_type: Optional[str] = None,
        show_id: Optional[str] = None,
        search: Optional[str] = None,
        *,
        summary: bool = False,
        restrict_to_assigned_user: bool = False,
        assigned_user_id: Optional[str] = None,
        assigned_user_email: Optional[str] = None,
    ) -> Tuple[List[dict], Optional[str]]:
        sql = f"{NOTICE_LIST_SELECT if summary else NOTICE_SELECT} WHERE 1=1"
        params: list = []
        if status:
            sql += " AND n.status = %s"
            params.append(status)
        if notice_type:
            sql += " AND n.notice_type = %s"
            params.append(notice_type)
        if show_id:
            sql += " AND n.show_id = %s"
            params.append(show_id)
        if search:
            sql += """
            AND (
                n.brand_name LIKE %s
                OR s.title LIKE %s
                OR EXISTS (
                    SELECT 1 FROM myco_notice_contacts c
                    WHERE c.notice_id = n.id AND c.contact_name LIKE %s
                )
            )
            """
            like = f"%{search}%"
            params.extend([like, like, like])
        if restrict_to_assigned_user:
            if not assigned_user_id:
                return [], "Assigned user id is required"
            sql += assigned_notice_sql_clause()
            params.extend([assigned_user_id, assigned_user_email or ""])
        sql += " ORDER BY n.created_at DESC"
        rows, _, err = self.client._execute_query(sql, tuple(params) if params else None, fetch="all")
        if err:
            return [], str(err)
        notices = [_row_to_notice(dict(r)) for r in (rows or [])]
        return self._attach_contacts(notices), None

    def update_notice(self, notice_id: str, updates: dict) -> Tuple[Optional[dict], Optional[str]]:
        existing, err = self.get_notice_by_id(notice_id)
        if err:
            return None, err
        if existing.get("status") not in ("active",):
            return None, "Only active notices can be edited"

        allowed = {
            "brand_name",
            "ad_copy_link",
            "brand_overview",
            "ad_type",
            "due_date",
            "notes",
            "channel_email",
            "channel_text",
            "frequency_hours",
        }
        set_parts = ["updated_at = %s"]
        values: list = [_utcnow()]
        for key, val in updates.items():
            if key in allowed and val is not None:
                set_parts.append(f"{key} = %s")
                values.append(val)

        contacts_update = updates.get("contacts")
        prepared_contacts = None
        if contacts_update is not None:
            prepared_contacts, notice_channels, cerr = self._prepare_contacts_for_write(contacts_update)
            if cerr:
                return None, cerr
            for key in ("channel_email", "channel_text", "channel_myco"):
                set_parts.append(f"{key} = %s")
                values.append(bool(notice_channels.get(key)))

        if len(set_parts) == 1 and prepared_contacts is None:
            return existing, None

        if len(set_parts) > 1:
            sql = f"UPDATE myco_notices SET {', '.join(set_parts)} WHERE id = %s"
            values.append(notice_id)
            _, affected, err = self.client._execute_query(sql, tuple(values), is_transaction=True)
            if err:
                return None, str(err)
            if affected == 0:
                return None, "Notice not found"

        if prepared_contacts is not None:
            replace_err = self._replace_contacts(notice_id, prepared_contacts)
            if replace_err:
                return None, replace_err

        return self.get_notice_by_id(notice_id)

    def update_notice_notes(
        self, notice_id: str, notes: Optional[str]
    ) -> Tuple[Optional[dict], Optional[str]]:
        existing, err = self.get_notice_by_id(notice_id)
        if err:
            return None, err
        if existing.get("status") not in ("active",):
            return None, "Only active notices can be edited"

        now = _utcnow()
        sql = "UPDATE myco_notices SET notes = %s, updated_at = %s WHERE id = %s"
        _, affected, err = self.client._execute_query(
            sql, (notes, now, notice_id), is_transaction=True
        )
        if err:
            return None, str(err)
        if affected == 0:
            return None, "Notice not found"
        return self.get_notice_by_id(notice_id)

    def delete_notice(self, notice_id: str) -> Tuple[bool, Optional[str]]:
        sql = "DELETE FROM myco_notices WHERE id = %s"
        _, affected, err = self.client._execute_query(sql, (notice_id,), is_transaction=True)
        if err:
            return False, str(err)
        if affected == 0:
            return False, "Notice not found"
        return True, None

    def set_notice_status(
        self, notice_id: str, status: str, user_id: str
    ) -> Tuple[Optional[dict], Optional[str]]:
        now = _utcnow()
        if status == "complete":
            sql = """
            UPDATE myco_notices
            SET status = 'complete', completed_by = %s, completed_at = %s, updated_at = %s
            WHERE id = %s AND status = 'active'
            """
            params = (user_id, now, now, notice_id)
        elif status == "cancelled":
            sql = """
            UPDATE myco_notices
            SET status = 'cancelled', cancelled_by = %s, cancelled_at = %s, updated_at = %s
            WHERE id = %s AND status = 'active'
            """
            params = (user_id, now, now, notice_id)
        else:
            return None, "Invalid status transition"

        _, affected, err = self.client._execute_query(sql, params, is_transaction=True)
        if err:
            return None, str(err)
        if affected == 0:
            return None, "Notice not found or not active"
        return self.get_notice_by_id(notice_id)

    def cancel_notices_for_show(self, show_id: str, user_id: Optional[str] = None) -> Tuple[int, Optional[str]]:
        now = _utcnow()
        sql = """
        UPDATE myco_notices
        SET status = 'cancelled', cancelled_at = %s, cancelled_by = %s, updated_at = %s
        WHERE show_id = %s AND status = 'active'
        """
        _, affected, err = self.client._execute_query(
            sql, (now, user_id, now, show_id), is_transaction=True
        )
        if err:
            return 0, str(err)
        return affected or 0, None

    def log_delivery(
        self,
        notice_id: str,
        channel: str,
        status: str,
        recipient: Optional[str],
        error_message: Optional[str],
        external_id: Optional[str],
        is_reminder: bool,
        external_status: Optional[str] = None,
        contact_position: Optional[int] = None,
    ) -> Tuple[Optional[dict], Optional[str]]:
        delivery_id = str(uuid.uuid4())
        now = _utcnow()
        position = int(contact_position) if contact_position in (1, 2, 3) else None
        sql = """
        INSERT INTO myco_notice_deliveries
        (id, notice_id, channel, status, recipient, contact_position, error_message, external_id,
         external_status, external_status_at, is_reminder, sent_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        external_status_at = now if external_status else None
        params = (
            delivery_id,
            notice_id,
            channel,
            status,
            recipient,
            position,
            error_message,
            external_id,
            external_status,
            external_status_at,
            is_reminder,
            now,
        )
        _, _, err = self.client._execute_query(sql, params, is_transaction=True)
        if err:
            return None, str(err)
        return {
            "id": delivery_id,
            "notice_id": notice_id,
            "channel": channel,
            "status": status,
            "recipient": recipient,
            "contact_position": position,
            "error_message": error_message,
            "external_id": external_id,
            "external_status": external_status,
            "external_status_at": external_status_at,
            "is_reminder": is_reminder,
            "sent_at": now,
        }, None

    def update_delivery_external_status_by_sid(
        self,
        external_id: str,
        external_status: str,
        error_message: Optional[str] = None,
    ) -> Tuple[Optional[dict], Optional[str]]:
        now = _utcnow()
        sql = """
        UPDATE myco_notice_deliveries
        SET external_status = %s,
            external_status_at = %s,
            error_message = COALESCE(%s, error_message)
        WHERE external_id = %s
        """
        _, affected, err = self.client._execute_query(
            sql,
            (external_status, now, error_message, external_id),
            is_transaction=True,
        )
        if err:
            return None, str(err)
        if not affected:
            return None, "Delivery not found for message SID"

        fetch_sql = """
        SELECT * FROM myco_notice_deliveries
        WHERE external_id = %s
        ORDER BY sent_at DESC
        LIMIT 1
        """
        rows, _, fetch_err = self.client._execute_query(fetch_sql, (external_id,), fetch="one")
        if fetch_err:
            return None, str(fetch_err)
        if not rows:
            return None, "Delivery not found for message SID"
        return _row_to_notice(dict(rows)), None

    def get_deliveries_for_notice(self, notice_id: str) -> Tuple[List[dict], Optional[str]]:
        sql = """
        SELECT * FROM myco_notice_deliveries
        WHERE notice_id = %s
        ORDER BY sent_at DESC
        """
        rows, _, err = self.client._execute_query(sql, (notice_id,), fetch="all")
        if err:
            return [], str(err)
        deliveries = [_row_to_notice(dict(r)) for r in (rows or [])]
        from services.twilio_delivery_status import refresh_text_delivery_statuses

        return refresh_text_delivery_statuses(deliveries), None

    def claim_notice_for_send(self, notice_id: str, frequency_hours: int) -> Tuple[bool, Optional[str]]:
        """Atomically claim a due, active notice for sending."""
        now = _utcnow()
        next_send = now + timedelta(hours=frequency_hours)
        sql = """
        UPDATE myco_notices
        SET last_sent_at = %s, next_send_at = %s, send_count = send_count + 1, updated_at = %s
        WHERE id = %s AND status = 'active' AND next_send_at <= %s
        """
        _, affected, err = self.client._execute_query(
            sql, (now, next_send, now, notice_id, now), is_transaction=True
        )
        if err:
            return False, str(err)
        return (affected or 0) > 0, None

    def expire_notice(self, notice_id: str) -> Tuple[Optional[dict], Optional[str]]:
        now = _utcnow()
        sql = "UPDATE myco_notices SET status = 'expired', updated_at = %s WHERE id = %s AND status = 'active'"
        _, _, err = self.client._execute_query(sql, (now, notice_id), is_transaction=True)
        if err:
            return None, str(err)
        return self.get_notice_by_id(notice_id)

    def get_due_notices(self) -> Tuple[List[dict], Optional[str]]:
        sql = f"""
        {NOTICE_SELECT}
        WHERE n.status = 'active'
          AND n.next_send_at <= UTC_TIMESTAMP()
          AND UTC_DATE() <= DATE_ADD(n.due_date, INTERVAL n.reminder_window_days DAY)
        """
        rows, _, err = self.client._execute_query(sql, fetch="all")
        if err:
            return [], str(err)
        notices = [_row_to_notice(dict(r)) for r in (rows or [])]
        return self._attach_contacts(notices), None

    def expire_overdue_window_notices(self) -> Tuple[int, Optional[str]]:
        sql = """
        UPDATE myco_notices
        SET status = 'expired', updated_at = UTC_TIMESTAMP()
        WHERE status = 'active'
          AND UTC_DATE() > DATE_ADD(due_date, INTERVAL reminder_window_days DAY)
        """
        _, affected, err = self.client._execute_query(sql, is_transaction=True)
        if err:
            return 0, str(err)
        return affected or 0, None
