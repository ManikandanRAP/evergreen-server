# utils/date_normalizer.py
import re
from datetime import datetime
from typing import Optional  # <-- for Python < 3.10

_NULLS = {"", "none", "null", "na", "n/a", "nan", "-"}

def _y2k(yy: int, pivot: int = 68) -> int:
    """00..pivot -> 2000..20pivot; else 1900..1999."""
    return 2000 + yy if yy <= pivot else 1900 + yy

def _fmt(yyyy: int, mm: int, dd: int) -> str:
    return datetime(yyyy, mm, dd).strftime("%Y-%m-%d")

def normalize_mysql_date(
    value,
    *,
    pivot: int = 68,  # 2-digit year cutoff (e.g., 68 -> 00..68 as 2000..2068)
) -> Optional[str]:
    """
    Accepts:
      - YYYY-MM-DD
      - MM{/, -, ,}DD{same}YY or MM{/, -, ,}DD{same}YYYY (month-first)
      - DD{/, -, ,}MM{same}YYYY when month-first is invalid (e.g. 17,09,2025 → 2025-09-17)
    Returns 'YYYY-MM-DD' or None for null-like inputs.
    Raises ValueError for unsupported/invalid inputs.
    """
    if value is None:
        return None
    s = str(value).strip()
    if s.lower() in _NULLS:
        return None

    # 1) Pass-through: strict MySQL DATE
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        try:
            yyyy, mm, dd = map(int, s.split("-"))
            return _fmt(yyyy, mm, dd)
        except ValueError as e:
            raise ValueError(f"Invalid date (YYYY-MM-DD): {s}") from e

    # 2) Two numeric parts + year with unified separator (/, -, ,)
    # Try month-first (MM/DD/YYYY, common in US / export template), then day-first (DD/MM/YYYY) when the first
    # parse is impossible (e.g. month 17) or invalid calendar — so "17,09,2025" → 2025-09-17.
    m = re.fullmatch(r"\s*(\d{1,2})\s*([,/-])\s*(\d{1,2})\s*\2\s*(\d{2}|\d{4})\s*", s)
    if m:
        a_str, sep, b_str, yy_str = m.groups()
        a, b = int(a_str), int(b_str)

        if len(yy_str) == 2:
            yyyy = _y2k(int(yy_str), pivot)
        else:
            yyyy = int(yy_str)

        def _try_yyyy_mm_dd(mm: int, dd: int) -> Optional[str]:
            if not (1 <= mm <= 12) or not (1 <= dd <= 31):
                return None
            try:
                return _fmt(yyyy, mm, dd)
            except ValueError:
                return None

        out = _try_yyyy_mm_dd(a, b)  # month-first: a=MM, b=DD
        if out is not None:
            return out
        out = _try_yyyy_mm_dd(b, a)  # day-first: a=DD, b=MM
        if out is not None:
            return out

        raise ValueError(
            f"Invalid date (tried MM{sep}DD{sep}{'YYYY' if len(yy_str) == 4 else 'YY'} "
            f"and DD{sep}MM{sep}{'YYYY' if len(yy_str) == 4 else 'YY'}): {s}"
        )

    # 3) Everything else is unsupported by spec
    raise ValueError(f"Unsupported date format: {s}")
