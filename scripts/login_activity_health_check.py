#!/usr/bin/env python3
"""
Periodic health check for user_login_activity performance.

Checks:
1) Basic table accessibility
2) Fast-path query latency for keyset list
3) Count query latency (background/hybrid metric)
4) Slow query log rough signal from performance_schema (best effort)

Exits non-zero when thresholds are breached.
"""

import os
import sys
import time
from datetime import datetime, timezone

import pymysql


def env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except Exception:
        return default


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


def timed_query(cur, sql: str, params=None):
    started = time.perf_counter()
    cur.execute(sql, params or ())
    rows = cur.fetchall()
    elapsed_ms = (time.perf_counter() - started) * 1000
    return rows, elapsed_ms


def main():
    read_p95_budget_ms = env_int("LOGIN_ACTIVITY_READ_BUDGET_MS", 300)
    count_budget_ms = env_int("LOGIN_ACTIVITY_COUNT_BUDGET_MS", 1500)
    row_limit = env_int("LOGIN_ACTIVITY_PROBE_LIMIT", 25)

    failures = []
    report = {
        "executed_at_utc": datetime.now(timezone.utc).isoformat(),
        "budgets_ms": {
            "read_budget": read_p95_budget_ms,
            "count_budget": count_budget_ms,
        },
        "metrics_ms": {},
    }

    try:
        conn = connect_db()
        with conn.cursor() as cur:
            # 1) Table accessible
            cur.execute("SELECT 1 FROM user_login_activity LIMIT 1")
            _ = cur.fetchall()

            # 2) Fast read probe
            _, read_ms = timed_query(
                cur,
                """
                SELECT id, occurred_at_utc, user_email, action, status
                FROM user_login_activity
                ORDER BY occurred_at_utc DESC, id DESC
                LIMIT %s
                """,
                (row_limit,),
            )
            report["metrics_ms"]["keyset_list_probe"] = round(read_ms, 2)
            if read_ms > read_p95_budget_ms:
                failures.append(
                    f"Read probe too slow: {read_ms:.2f}ms > budget {read_p95_budget_ms}ms"
                )

            # 3) Count probe (hybrid total metric)
            _, count_ms = timed_query(
                cur,
                "SELECT COUNT(*) AS total FROM user_login_activity",
            )
            report["metrics_ms"]["count_probe"] = round(count_ms, 2)
            if count_ms > count_budget_ms:
                failures.append(
                    f"Count probe too slow: {count_ms:.2f}ms > budget {count_budget_ms}ms"
                )

            # 4) best-effort slow log signal (may be disabled)
            try:
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM performance_schema.events_statements_summary_by_digest
                    WHERE digest_text LIKE 'SELECT id, occurred_at_utc, user_email, action, status FROM user_login_activity%'
                    """
                )
                row = cur.fetchone() or {}
                report["signals"] = {"digest_rows": int(row.get("cnt") or 0)}
            except Exception:
                report["signals"] = {"digest_rows": "unavailable"}

        conn.close()
    except Exception as e:
        print(f"HEALTH_CHECK_ERROR: {type(e).__name__}: {e}")
        sys.exit(2)

    print("LOGIN_ACTIVITY_HEALTH_CHECK_REPORT")
    for key, val in report.items():
        print(f"{key}: {val}")

    if failures:
        print("LOGIN_ACTIVITY_HEALTH_CHECK_FAILED")
        for f in failures:
            print(f"- {f}")
        sys.exit(1)

    print("LOGIN_ACTIVITY_HEALTH_CHECK_OK")
    sys.exit(0)


if __name__ == "__main__":
    main()
