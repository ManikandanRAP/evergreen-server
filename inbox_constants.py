"""Inbox service constants — retention presets and validation."""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

DEFAULT_INBOX_RETENTION_DAYS = 365

INBOX_RETENTION_PRESETS: List[Tuple[int, str]] = [
    (1, "1 day"),
    (7, "1 week"),
    (30, "1 month"),
    (90, "3 months"),
    (180, "6 months"),
    (365, "12 months"),
]

ALLOWED_INBOX_RETENTION_DAYS = {days for days, _ in INBOX_RETENTION_PRESETS}

_RETENTION_LABEL_BY_DAYS: Dict[int, str] = dict(INBOX_RETENTION_PRESETS)


def retention_label(days: int) -> str:
    return _RETENTION_LABEL_BY_DAYS.get(days, f"{days} days")


def normalize_retention_days(days: int) -> int:
    if days not in ALLOWED_INBOX_RETENTION_DAYS:
        raise ValueError(
            f"inbox_retention_days must be one of {sorted(ALLOWED_INBOX_RETENTION_DAYS)}"
        )
    return days


INBOX_LIST_FILTERS = frozenset({"all", "unread", "pinned"})
INBOX_LIST_SORTS = frozenset({"date_desc", "date_asc", "unread_first"})


def normalize_inbox_list_filter(value: Optional[str], *, unread_only: bool = False) -> str:
    if value and value in INBOX_LIST_FILTERS:
        return value
    if unread_only:
        return "unread"
    return "all"


def normalize_inbox_list_sort(value: Optional[str]) -> str:
    if value and value in INBOX_LIST_SORTS:
        return value
    return "date_desc"


def inbox_filter_flags(inbox_filter: str) -> tuple[bool, bool]:
    """Returns (unread_only, pinned_only)."""
    if inbox_filter == "unread":
        return True, False
    if inbox_filter == "pinned":
        return False, True
    return False, False
