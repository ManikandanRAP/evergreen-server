"""Database operations for the inbox message service."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from inbox_constants import (
    DEFAULT_INBOX_RETENTION_DAYS,
    inbox_filter_flags,
    normalize_inbox_list_filter,
    normalize_inbox_list_sort,
    normalize_retention_days,
)
from sqlclient import SqlClient

SETTING_INBOX_RETENTION = "inbox_retention_days"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_metadata(raw: Any) -> Optional[dict]:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def _row_to_inbox_message(row: dict) -> dict:
    if not row:
        return row
    row = dict(row)
    row["metadata"] = _parse_metadata(row.get("metadata"))
    return row


def _row_to_inbox_list_item(row: dict) -> dict:
    if not row:
        return row
    row = dict(row)
    preview = (row.get("body_preview") or "").strip()
    row["body_preview"] = preview
    if "metadata" in row:
        row["metadata"] = _parse_metadata(row.get("metadata"))
    return row


class InboxDb:
    def __init__(self, client: Optional[SqlClient] = None):
        self.client = client or SqlClient()
        self._messages_table_cache: Optional[str] = None
        self._metadata_column_cache: Optional[bool] = None
        self._pinned_column_cache: Optional[bool] = None

    def _messages_table(self) -> str:
        if self._messages_table_cache:
            return self._messages_table_cache
        inbox_type, _ = self.client._get_table_type("inbox_messages")
        if inbox_type == "BASE TABLE":
            self._messages_table_cache = "inbox_messages"
            return self._messages_table_cache
        legacy_type, _ = self.client._get_table_type("user_notifications")
        if legacy_type == "BASE TABLE":
            self._messages_table_cache = "user_notifications"
            return self._messages_table_cache
        self._messages_table_cache = "inbox_messages"
        return self._messages_table_cache

    def _has_metadata_column(self) -> bool:
        if self._metadata_column_cache is not None:
            return self._metadata_column_cache
        table = self._messages_table()
        self._metadata_column_cache = self.client._column_exists(table, "metadata")
        return self._metadata_column_cache

    def _has_pinned_column(self) -> bool:
        if self._pinned_column_cache is not None:
            return self._pinned_column_cache
        table = self._messages_table()
        self._pinned_column_cache = self.client._column_exists(table, "pinned_at")
        return self._pinned_column_cache

    def _list_order_clause(self, sort: str = "date_desc", *, pinned_only: bool = False) -> str:
        sort = normalize_inbox_list_sort(sort)
        has_pin = self._has_pinned_column()
        if sort == "date_asc":
            if pinned_only and has_pin:
                return "ORDER BY pinned_at DESC, created_at ASC, id ASC"
            return "ORDER BY created_at ASC, id ASC"
        if sort == "unread_first":
            if has_pin and not pinned_only:
                return (
                    "ORDER BY read_at IS NULL DESC, pinned_at IS NULL, "
                    "pinned_at DESC, created_at DESC, id DESC"
                )
            return "ORDER BY read_at IS NULL DESC, created_at DESC, id DESC"
        if has_pin and not pinned_only:
            return "ORDER BY pinned_at IS NULL, pinned_at DESC, created_at DESC, id DESC"
        if pinned_only and has_pin:
            return "ORDER BY pinned_at DESC, created_at DESC, id DESC"
        return "ORDER BY created_at DESC, id DESC"

    def _inbox_list_filter_clause(
        self,
        *,
        inbox_filter: str = "all",
        search: Optional[str] = None,
    ) -> Tuple[str, list]:
        sql = ""
        params: list = []
        unread_only, pinned_only = inbox_filter_flags(inbox_filter)
        if unread_only:
            sql += " AND read_at IS NULL"
        if pinned_only and self._has_pinned_column():
            sql += " AND pinned_at IS NOT NULL"
        if search and search.strip():
            term = f"%{search.strip()}%"
            sql += " AND (title LIKE %s OR body LIKE %s)"
            params.extend([term, term])
        return sql, params

    def _inbox_list_pagination_clause(
        self,
        sort: str,
        *,
        before: Optional[datetime] = None,
        before_id: Optional[str] = None,
        before_unread: Optional[int] = None,
        after: Optional[datetime] = None,
        after_id: Optional[str] = None,
    ) -> Tuple[str, list]:
        sort = normalize_inbox_list_sort(sort)
        sql = ""
        params: list = []

        if sort == "date_asc":
            if after is not None:
                sql += " AND (created_at > %s OR (created_at = %s AND id > %s))"
                params.extend([after, after, after_id or ""])
            return sql, params

        if sort == "unread_first" and before is not None:
            unread_flag = 1 if before_unread else 0
            sql += """
            AND (
              (CASE WHEN read_at IS NULL THEN 1 ELSE 0 END) < %s
              OR (
                (CASE WHEN read_at IS NULL THEN 1 ELSE 0 END) = %s
                AND (
                  created_at < %s
                  OR (created_at = %s AND id < %s)
                )
              )
            )
            """
            params.extend(
                [unread_flag, unread_flag, before, before, before_id or ""]
            )
            return sql, params

        if before is not None:
            sql += " AND (created_at < %s OR (created_at = %s AND id < %s))"
            params.extend([before, before, before_id or ""])
        return sql, params

    def _inbox_retention_where(self, user_id: str, retention_days: int) -> Tuple[str, list]:
        return (
            "user_id = %s AND created_at >= UTC_TIMESTAMP() - INTERVAL %s DAY",
            [user_id, retention_days],
        )

    def get_inbox_retention_days(self) -> Tuple[int, Optional[str]]:
        sql = "SELECT setting_value FROM system_settings WHERE setting_key = %s LIMIT 1"
        row, _, err = self.client._execute_query(sql, (SETTING_INBOX_RETENTION,), fetch="one")
        if err:
            return DEFAULT_INBOX_RETENTION_DAYS, str(err)
        if not row:
            return DEFAULT_INBOX_RETENTION_DAYS, None
        try:
            days = int(row.get("setting_value", DEFAULT_INBOX_RETENTION_DAYS))
            return normalize_retention_days(days), None
        except ValueError:
            return DEFAULT_INBOX_RETENTION_DAYS, None

    def set_inbox_retention_days(self, days: int) -> Tuple[bool, Optional[str]]:
        try:
            normalized = normalize_retention_days(days)
        except ValueError as exc:
            return False, str(exc)
        now = _utcnow()
        sql = """
        INSERT INTO system_settings (setting_key, setting_value, updated_at)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value), updated_at = VALUES(updated_at)
        """
        _, _, err = self.client._execute_query(
            sql, (SETTING_INBOX_RETENTION, str(normalized), now), is_transaction=True
        )
        if err:
            return False, str(err)
        return True, None

    def create_inbox_message(
        self,
        user_id: str,
        mtype: str,
        title: str,
        body: str,
        notice_id: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> Tuple[Optional[dict], Optional[str]]:
        message_id = str(uuid.uuid4())
        now = _utcnow()
        metadata_json = json.dumps(metadata) if metadata else None
        table = self._messages_table()
        if self._has_metadata_column():
            sql = f"""
            INSERT INTO {table} (id, user_id, type, title, body, notice_id, metadata, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = (message_id, user_id, mtype, title, body, notice_id, metadata_json, now)
        else:
            sql = f"""
            INSERT INTO {table} (id, user_id, type, title, body, notice_id, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            params = (message_id, user_id, mtype, title, body, notice_id, now)
        _, _, err = self.client._execute_query(sql, params, is_transaction=True)
        if err:
            return None, str(err)
        try:
            from inbox_events import notify_inbox_unread_changed

            notify_inbox_unread_changed(user_id)
        except Exception as exc:
            print(f"WARNING: Inbox event notify failed after create: {exc}")
        return {
            "id": message_id,
            "user_id": user_id,
            "type": mtype,
            "title": title,
            "body": body,
            "notice_id": notice_id,
            "metadata": metadata,
            "read_at": None,
            "created_at": now,
        }, None

    def list_inbox_messages(
        self,
        user_id: str,
        *,
        inbox_filter: str = "all",
        search: Optional[str] = None,
        sort: str = "date_desc",
        limit: int = 50,
        before: Optional[datetime] = None,
        before_id: Optional[str] = None,
        before_unread: Optional[int] = None,
        after: Optional[datetime] = None,
        after_id: Optional[str] = None,
        retention_days: Optional[int] = None,
        unread_only: bool = False,
    ) -> Tuple[List[dict], bool, Optional[str]]:
        effective_filter = normalize_inbox_list_filter(inbox_filter, unread_only=unread_only)
        effective_sort = normalize_inbox_list_sort(sort)
        if retention_days is None:
            retention_days, _ = self.get_inbox_retention_days()
        safe_limit = max(1, min(int(limit or 50), 100))
        table = self._messages_table()
        metadata_select = ", metadata" if self._has_metadata_column() else ""
        sql = f"""
        SELECT id, user_id, type, title,
               LEFT(body, 200) AS body_preview,
               notice_id, read_at, pinned_at, created_at{metadata_select}
        FROM {table}
        WHERE user_id = %s
          AND created_at >= UTC_TIMESTAMP() - INTERVAL %s DAY
        """
        params: list = [user_id, retention_days]
        filter_sql, filter_params = self._inbox_list_filter_clause(
            inbox_filter=effective_filter, search=search
        )
        sql += filter_sql
        params.extend(filter_params)
        _, pinned_only = inbox_filter_flags(effective_filter)
        page_sql, page_params = self._inbox_list_pagination_clause(
            effective_sort,
            before=before,
            before_id=before_id,
            before_unread=before_unread,
            after=after,
            after_id=after_id,
        )
        sql += page_sql
        params.extend(page_params)
        sql += f" {self._list_order_clause(effective_sort, pinned_only=pinned_only)} LIMIT %s"
        params.append(safe_limit + 1)
        rows, _, err = self.client._execute_query(sql, tuple(params), fetch="all")
        if err:
            return [], False, str(err)
        items = [_row_to_inbox_list_item(dict(r)) for r in (rows or [])]
        has_more = len(items) > safe_limit
        if has_more:
            items = items[:safe_limit]
        return items, has_more, None

    def get_inbox_message(
        self, message_id: str, user_id: str, *, retention_days: Optional[int] = None
    ) -> Tuple[Optional[dict], Optional[str]]:
        if retention_days is None:
            retention_days, _ = self.get_inbox_retention_days()
        table = self._messages_table()
        sql = f"""
        SELECT * FROM {table}
        WHERE id = %s AND user_id = %s
          AND created_at >= UTC_TIMESTAMP() - INTERVAL %s DAY
        LIMIT 1
        """
        row, _, err = self.client._execute_query(
            sql, (message_id, user_id, retention_days), fetch="one"
        )
        if err:
            return None, str(err)
        if not row:
            return None, "Message not found"
        return _row_to_inbox_message(dict(row)), None

    def inbox_unread_count(self, user_id: str, *, retention_days: Optional[int] = None) -> Tuple[int, Optional[str]]:
        if retention_days is None:
            retention_days, _ = self.get_inbox_retention_days()
        table = self._messages_table()
        sql = f"""
        SELECT COUNT(*) AS cnt FROM {table}
        WHERE user_id = %s AND read_at IS NULL
          AND created_at >= UTC_TIMESTAMP() - INTERVAL %s DAY
        """
        row, _, err = self.client._execute_query(sql, (user_id, retention_days), fetch="one")
        if err:
            return 0, str(err)
        return int(row.get("cnt", 0) if row else 0), None

    def inbox_total_count(self, user_id: str, *, retention_days: Optional[int] = None) -> Tuple[int, Optional[str]]:
        if retention_days is None:
            retention_days, _ = self.get_inbox_retention_days()
        table = self._messages_table()
        base_where, base_params = self._inbox_retention_where(user_id, retention_days)
        sql = f"SELECT COUNT(*) AS cnt FROM {table} WHERE {base_where}"
        row, _, err = self.client._execute_query(sql, tuple(base_params), fetch="one")
        if err:
            return 0, str(err)
        return int(row.get("cnt", 0) if row else 0), None

    def inbox_filtered_count(
        self,
        user_id: str,
        *,
        inbox_filter: str = "all",
        search: Optional[str] = None,
        retention_days: Optional[int] = None,
        unread_only: bool = False,
    ) -> Tuple[int, Optional[str]]:
        effective_filter = normalize_inbox_list_filter(inbox_filter, unread_only=unread_only)
        if retention_days is None:
            retention_days, _ = self.get_inbox_retention_days()
        table = self._messages_table()
        base_where, base_params = self._inbox_retention_where(user_id, retention_days)
        filter_sql, filter_params = self._inbox_list_filter_clause(
            inbox_filter=effective_filter, search=search
        )
        sql = f"SELECT COUNT(*) AS cnt FROM {table} WHERE {base_where}{filter_sql}"
        row, _, err = self.client._execute_query(
            sql, tuple([*base_params, *filter_params]), fetch="one"
        )
        if err:
            return 0, str(err)
        return int(row.get("cnt", 0) if row else 0), None

    def mark_inbox_message_read(
        self, message_id: str, user_id: str
    ) -> Tuple[Optional[dict], Optional[str]]:
        now = _utcnow()
        table = self._messages_table()
        sql = f"""
        UPDATE {table} SET read_at = %s
        WHERE id = %s AND user_id = %s AND read_at IS NULL
        """
        _, affected, err = self.client._execute_query(
            sql, (now, message_id, user_id), is_transaction=True
        )
        if err:
            return None, str(err)
        if affected == 0:
            existing, get_err = self.get_inbox_message(message_id, user_id)
            if get_err or not existing:
                return None, "Message not found"
            return existing, None
        item, get_err = self.get_inbox_message(message_id, user_id)
        if get_err:
            return None, get_err
        return item, None

    def mark_all_inbox_messages_read(self, user_id: str) -> Tuple[int, Optional[str]]:
        retention_days, _ = self.get_inbox_retention_days()
        now = _utcnow()
        table = self._messages_table()
        sql = f"""
        UPDATE {table} SET read_at = %s
        WHERE user_id = %s AND read_at IS NULL
          AND created_at >= UTC_TIMESTAMP() - INTERVAL %s DAY
        """
        _, affected, err = self.client._execute_query(
            sql, (now, user_id, retention_days), is_transaction=True
        )
        if err:
            return 0, str(err)
        return affected or 0, None

    def delete_inbox_message(self, message_id: str, user_id: str) -> Tuple[bool, bool, Optional[str]]:
        """Returns (deleted, was_unread, error)."""
        table = self._messages_table()
        check_sql = f"SELECT read_at FROM {table} WHERE id = %s AND user_id = %s LIMIT 1"
        row, _, err = self.client._execute_query(check_sql, (message_id, user_id), fetch="one")
        if err:
            return False, False, str(err)
        if not row:
            return False, False, "Message not found"
        was_unread = row.get("read_at") is None
        delete_sql = f"DELETE FROM {table} WHERE id = %s AND user_id = %s"
        _, affected, err = self.client._execute_query(
            delete_sql, (message_id, user_id), is_transaction=True
        )
        if err:
            return False, was_unread, str(err)
        if not affected:
            return False, was_unread, "Message not found"
        return True, was_unread, None

    def mark_inbox_message_unread(
        self, message_id: str, user_id: str
    ) -> Tuple[Optional[dict], Optional[str]]:
        table = self._messages_table()
        sql = f"""
        UPDATE {table} SET read_at = NULL
        WHERE id = %s AND user_id = %s AND read_at IS NOT NULL
        """
        _, affected, err = self.client._execute_query(
            sql, (message_id, user_id), is_transaction=True
        )
        if err:
            return None, str(err)
        if affected == 0:
            existing, get_err = self.get_inbox_message(message_id, user_id)
            if get_err or not existing:
                return None, "Message not found"
            return existing, None
        item, get_err = self.get_inbox_message(message_id, user_id)
        if get_err:
            return None, get_err
        return item, None

    def set_inbox_message_pinned(
        self, message_id: str, user_id: str, pinned: bool
    ) -> Tuple[Optional[dict], Optional[str]]:
        if not self._has_pinned_column():
            return None, "Pin not supported"
        table = self._messages_table()
        pinned_at = _utcnow() if pinned else None
        sql = f"""
        UPDATE {table} SET pinned_at = %s
        WHERE id = %s AND user_id = %s
        """
        _, affected, err = self.client._execute_query(
            sql, (pinned_at, message_id, user_id), is_transaction=True
        )
        if err:
            return None, str(err)
        if not affected:
            return None, "Message not found"
        item, get_err = self.get_inbox_message(message_id, user_id)
        if get_err:
            return None, get_err
        return item, None

    def bulk_inbox_action(
        self, user_id: str, message_ids: List[str], action: str
    ) -> Tuple[Dict[str, int], Optional[str]]:
        ids = [mid for mid in message_ids if mid]
        if not ids:
            return {"updated": 0, "unread_delta": 0}, None
        if action not in {"read", "unread", "delete", "pin", "unpin"}:
            return {"updated": 0, "unread_delta": 0}, "Invalid action"

        table = self._messages_table()
        retention_days, _ = self.get_inbox_retention_days()
        placeholders = ", ".join(["%s"] * len(ids))
        base_where = (
            f"id IN ({placeholders}) AND user_id = %s "
            f"AND created_at >= UTC_TIMESTAMP() - INTERVAL %s DAY"
        )
        base_params: list = [*ids, user_id, retention_days]

        if action == "delete":
            count_sql = f"""
            SELECT COUNT(*) AS cnt FROM {table}
            WHERE {base_where} AND read_at IS NULL
            """
            row, _, err = self.client._execute_query(count_sql, tuple(base_params), fetch="one")
            if err:
                return {"updated": 0, "unread_delta": 0}, str(err)
            unread_count = int(row.get("cnt", 0) if row else 0)
            delete_sql = f"DELETE FROM {table} WHERE {base_where}"
            _, affected, err = self.client._execute_query(
                delete_sql, tuple(base_params), is_transaction=True
            )
            if err:
                return {"updated": 0, "unread_delta": 0}, str(err)
            return {"updated": affected or 0, "unread_delta": -(unread_count or 0)}, None

        if action == "read":
            now = _utcnow()
            count_sql = f"""
            SELECT COUNT(*) AS cnt FROM {table}
            WHERE {base_where} AND read_at IS NULL
            """
            row, _, err = self.client._execute_query(count_sql, tuple(base_params), fetch="one")
            if err:
                return {"updated": 0, "unread_delta": 0}, str(err)
            unread_count = int(row.get("cnt", 0) if row else 0)
            sql = f"UPDATE {table} SET read_at = %s WHERE {base_where} AND read_at IS NULL"
            _, affected, err = self.client._execute_query(
                sql, (now, *base_params), is_transaction=True
            )
            if err:
                return {"updated": 0, "unread_delta": 0}, str(err)
            return {"updated": affected or 0, "unread_delta": -(unread_count or 0)}, None

        if action == "unread":
            count_sql = f"""
            SELECT COUNT(*) AS cnt FROM {table}
            WHERE {base_where} AND read_at IS NOT NULL
            """
            row, _, err = self.client._execute_query(count_sql, tuple(base_params), fetch="one")
            if err:
                return {"updated": 0, "unread_delta": 0}, str(err)
            read_count = int(row.get("cnt", 0) if row else 0)
            sql = f"UPDATE {table} SET read_at = NULL WHERE {base_where} AND read_at IS NOT NULL"
            _, affected, err = self.client._execute_query(
                sql, tuple(base_params), is_transaction=True
            )
            if err:
                return {"updated": 0, "unread_delta": 0}, str(err)
            return {"updated": affected or 0, "unread_delta": read_count or 0}, None

        if action in {"pin", "unpin"}:
            if not self._has_pinned_column():
                return {"updated": 0, "unread_delta": 0}, "Pin not supported"
            pinned_at = _utcnow() if action == "pin" else None
            if action == "pin":
                sql = f"UPDATE {table} SET pinned_at = %s WHERE {base_where}"
                params = (pinned_at, *base_params)
            else:
                sql = f"UPDATE {table} SET pinned_at = NULL WHERE {base_where} AND pinned_at IS NOT NULL"
                params = tuple(base_params)
            _, affected, err = self.client._execute_query(sql, params, is_transaction=True)
            if err:
                return {"updated": 0, "unread_delta": 0}, str(err)
            return {"updated": affected or 0, "unread_delta": 0}, None

        return {"updated": 0, "unread_delta": 0}, "Invalid action"

    def bulk_inbox_action_by_query(
        self,
        user_id: str,
        action: str,
        *,
        inbox_filter: str = "all",
        search: Optional[str] = None,
        unread_only: bool = False,
    ) -> Tuple[Dict[str, int], Optional[str]]:
        if action not in {"read", "unread", "delete", "pin", "unpin"}:
            return {"updated": 0, "unread_delta": 0}, "Invalid action"

        effective_filter = normalize_inbox_list_filter(inbox_filter, unread_only=unread_only)
        table = self._messages_table()
        retention_days, _ = self.get_inbox_retention_days()
        base_where, base_params = self._inbox_retention_where(user_id, retention_days)
        filter_sql, filter_params = self._inbox_list_filter_clause(
            inbox_filter=effective_filter, search=search
        )
        base_where = f"{base_where}{filter_sql}"
        base_params = [*base_params, *filter_params]

        if action == "delete":
            count_sql = f"""
            SELECT COUNT(*) AS cnt FROM {table}
            WHERE {base_where} AND read_at IS NULL
            """
            row, _, err = self.client._execute_query(
                count_sql, tuple(base_params), fetch="one"
            )
            if err:
                return {"updated": 0, "unread_delta": 0}, str(err)
            unread_count = int(row.get("cnt", 0) if row else 0)
            delete_sql = f"DELETE FROM {table} WHERE {base_where}"
            _, affected, err = self.client._execute_query(
                delete_sql, tuple(base_params), is_transaction=True
            )
            if err:
                return {"updated": 0, "unread_delta": 0}, str(err)
            return {"updated": affected or 0, "unread_delta": -(unread_count or 0)}, None

        if action == "read":
            now = _utcnow()
            count_sql = f"""
            SELECT COUNT(*) AS cnt FROM {table}
            WHERE {base_where} AND read_at IS NULL
            """
            row, _, err = self.client._execute_query(
                count_sql, tuple(base_params), fetch="one"
            )
            if err:
                return {"updated": 0, "unread_delta": 0}, str(err)
            unread_count = int(row.get("cnt", 0) if row else 0)
            sql = f"UPDATE {table} SET read_at = %s WHERE {base_where} AND read_at IS NULL"
            _, affected, err = self.client._execute_query(
                sql, (now, *base_params), is_transaction=True
            )
            if err:
                return {"updated": 0, "unread_delta": 0}, str(err)
            return {"updated": affected or 0, "unread_delta": -(unread_count or 0)}, None

        if action == "unread":
            count_sql = f"""
            SELECT COUNT(*) AS cnt FROM {table}
            WHERE {base_where} AND read_at IS NOT NULL
            """
            row, _, err = self.client._execute_query(
                count_sql, tuple(base_params), fetch="one"
            )
            if err:
                return {"updated": 0, "unread_delta": 0}, str(err)
            read_count = int(row.get("cnt", 0) if row else 0)
            sql = f"UPDATE {table} SET read_at = NULL WHERE {base_where} AND read_at IS NOT NULL"
            _, affected, err = self.client._execute_query(
                sql, tuple(base_params), is_transaction=True
            )
            if err:
                return {"updated": 0, "unread_delta": 0}, str(err)
            return {"updated": affected or 0, "unread_delta": read_count or 0}, None

        if action in {"pin", "unpin"}:
            if not self._has_pinned_column():
                return {"updated": 0, "unread_delta": 0}, "Pin not supported"
            pinned_at = _utcnow() if action == "pin" else None
            if action == "pin":
                sql = f"UPDATE {table} SET pinned_at = %s WHERE {base_where}"
                params = (pinned_at, *base_params)
            else:
                sql = (
                    f"UPDATE {table} SET pinned_at = NULL WHERE {base_where} "
                    "AND pinned_at IS NOT NULL"
                )
                params = tuple(base_params)
            _, affected, err = self.client._execute_query(sql, params, is_transaction=True)
            if err:
                return {"updated": 0, "unread_delta": 0}, str(err)
            return {"updated": affected or 0, "unread_delta": 0}, None

        return {"updated": 0, "unread_delta": 0}, "Invalid action"

    def purge_expired_inbox_messages(
        self, retention_days: Optional[int] = None, batch_size: int = 10000
    ) -> Tuple[int, Optional[str]]:
        if retention_days is None:
            retention_days, _ = self.get_inbox_retention_days()
        table = self._messages_table()
        sql = f"""
        DELETE FROM {table}
        WHERE created_at < UTC_TIMESTAMP() - INTERVAL %s DAY
        LIMIT %s
        """
        _, affected, err = self.client._execute_query(
            sql, (retention_days, batch_size), is_transaction=True
        )
        if err:
            return 0, str(err)
        return affected or 0, None
