"""Load evergreen-server/.env for local runs (uvicorn, cron scripts)."""
from __future__ import annotations

import os

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


def load_local_env() -> None:
    if load_dotenv is None:
        return
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(root, ".env"), override=False)
