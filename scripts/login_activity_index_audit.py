#!/usr/bin/env python3
"""
Audit expected indexes for user_login_activity.

Exits non-zero if required indexes are missing.
"""

import os
import sys
from datetime import datetime, timezone

import pymysql


REQUIRED_INDEXES = {
    "PRIMARY",
    "uq_user_login_activity_event_uuid",
    "idx_user_login_activity_time_id",
    "idx_user_login_activity_action_status_time_id",
    "idx_user_login_activity_email_time_id",
    "idx_user_login_activity_name_time_id",
    "idx_user_login_activity_request_id_time",
}


def connect_db():
    return pymysql.connect(
        host=os.environ.get("DB_HOST", "127.0.0.1"),
        port=int(os.environ.get("DB_PORT", "3306")),
        user=os.environ.get("DB_USER", "root"),
        password=os.environ.get("DB_PASSWORD", "rootpassword"),
        database=os.environ.get("DB_NAME", "evergreen"),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def main():
    try:
        conn = connect_db()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT index_name
                FROM information_schema.statistics
                WHERE table_schema = DATABASE()
                  AND table_name = 'user_login_activity'
                """
            )
            existing = {row["index_name"] for row in (cur.fetchall() or [])}
        conn.close()
    except Exception as e:
        print(f"INDEX_AUDIT_ERROR: {type(e).__name__}: {e}")
        sys.exit(2)

    missing = sorted(REQUIRED_INDEXES - existing)
    extra = sorted(existing - REQUIRED_INDEXES)

    print("LOGIN_ACTIVITY_INDEX_AUDIT_REPORT")
    print(f"executed_at_utc: {datetime.now(timezone.utc).isoformat()}")
    print(f"missing_required_indexes: {missing}")
    print(f"extra_indexes: {extra}")

    if missing:
        print("LOGIN_ACTIVITY_INDEX_AUDIT_FAILED")
        sys.exit(1)

    print("LOGIN_ACTIVITY_INDEX_AUDIT_OK")
    sys.exit(0)


if __name__ == "__main__":
    main()
