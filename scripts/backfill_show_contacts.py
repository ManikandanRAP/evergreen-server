#!/usr/bin/env python3
"""One-time backfill: legacy show_*_contact TEXT -> structured columns.

Run after myco_show_may28_contacts_structured.sql and before drop legacy columns.

  python scripts/backfill_show_contacts.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlclient import SqlClient  # noqa: E402


def parse_contact(s: str | None) -> tuple[str | None, str | None, str | None, str | None]:
    if not s or s.strip() in ("-", "Internal"):
        name = s.strip() if s else None
        return (name, None, None, None)
    parts = [p.strip() for p in s.split(", ") if p.strip()]
    if len(parts) < 2:
        return (parts[0] if parts else None, None, None, None)
    if len(parts) == 2:
        return (parts[0], None, parts[1], None)
    if len(parts) == 3:
        return (parts[0], None, parts[1], parts[2])
    return (parts[0], ", ".join(parts[1:-2]), parts[-2], parts[-1])


def main() -> None:
    client = SqlClient()
    shows, _, err = client._execute_query("SELECT * FROM shows", fetch="all")
    if err:
        raise SystemExit(err)

    updated = 0
    for show in shows or []:
        host = parse_contact(show.get("show_host_contact"))
        primary = parse_contact(show.get("primary_show_contact"))
        producer = parse_contact(show.get("show_producer_contact"))

        sql = """
        UPDATE shows SET
          host_contact_name=%s, host_contact_address=%s, host_contact_phone=%s, host_contact_email=%s,
          primary_contact_name=%s, primary_contact_address=%s, primary_contact_phone=%s, primary_contact_email=%s,
          producer_contact_name=%s, producer_contact_address=%s, producer_contact_phone=%s, producer_contact_email=%s
        WHERE id=%s
        """
        vals = (*host, *primary, *producer, show["id"])
        _, _, e = client._execute_query(sql, vals, is_transaction=True)
        if e:
            print(f"Failed {show.get('id')}: {e}")
        else:
            updated += 1

    print(f"Backfilled {updated} shows.")


if __name__ == "__main__":
    main()
