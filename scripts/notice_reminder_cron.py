#!/usr/bin/env python3
"""Send due MYCO notice reminders and expire notices past the reminder window."""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from inbox_db import InboxDb
from myco_notices_db import MycoNoticesDb
from services.notice_delivery import send_notice


def main() -> int:
    inbox_db = InboxDb()
    purged, purge_err = inbox_db.purge_expired_inbox_messages()
    if purge_err:
        print(f"purge_expired_inbox_messages failed: {purge_err}", file=sys.stderr)
        return 1
    if purged:
        print(f"Purged {purged} expired inbox message(s)")

    db = MycoNoticesDb()
    expired, err = db.expire_overdue_window_notices()
    if err:
        print(f"expire_overdue_window_notices failed: {err}", file=sys.stderr)
        return 1
    if expired:
        print(f"Expired {expired} notice(s) past reminder window")

    due_notices, err = db.get_due_notices()
    if err:
        print(f"get_due_notices failed: {err}", file=sys.stderr)
        return 1

    sent = 0
    skipped = 0
    failed = 0
    for notice in due_notices:
        is_reminder = int(notice.get("send_count") or 0) > 0
        try:
            result, send_err = send_notice(notice["id"], is_reminder)
        except Exception as exc:  # one bad notice must not abort the whole run
            result, send_err = {}, str(exc)
        if send_err:
            print(f"Notice {notice['id']}: {send_err}", file=sys.stderr)
            failed += 1
        elif result.get("skipped"):
            skipped += 1
            print(f"Notice {notice['id']}: skipped (already claimed by another sender)")
        else:
            sent += 1
            print(f"Notice {notice['id']}: delivered ({'reminder' if is_reminder else 'initial'})")

    print(f"Processed {len(due_notices)} due notice(s): {sent} ok, {skipped} skipped, {failed} failed")
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
