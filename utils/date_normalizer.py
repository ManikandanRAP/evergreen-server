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
    Accepts only:
      - YYYY-MM-DD
      - MM{/, -, ,}DD{same}YY
      - MM{/, -, ,}DD{same}YYYY
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

    # 2) Month-first with unified separator (/, -, ,)
    m = re.fullmatch(r"\s*(\d{1,2})\s*([,/-])\s*(\d{1,2})\s*\2\s*(\d{2}|\d{4})\s*", s)
    if m:
        mm_str, sep, dd_str, yy_str = m.groups()
        mm, dd = int(mm_str), int(dd_str)

        if not (1 <= mm <= 12):
            raise ValueError(f"Invalid month in MM{sep}DD{sep}{'YYYY' if len(yy_str)==4 else 'YY'}: {s}")
        if not (1 <= dd <= 31):
            raise ValueError(f"Invalid day in MM{sep}DD{sep}{'YYYY' if len(yy_str)==4 else 'YY'}: {s}")

        if len(yy_str) == 2:
            yyyy = _y2k(int(yy_str), pivot)
        else:  # len == 4
            yyyy = int(yy_str)

        try:
            return _fmt(yyyy, mm, dd)
        except ValueError as e:
            # Catches invalid dates like 02/30/25, 04-31-2025, etc.
            raise ValueError(f"Invalid date (MM{sep}DD{sep}{'YYYY' if len(yy_str)==4 else 'YY'}): {s}") from e

    # 3) Everything else is unsupported by spec
    raise ValueError(f"Unsupported date format: {s}")
