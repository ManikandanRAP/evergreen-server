"""Resolve Myco web app base URL for notice emails (per-request + stored on notice)."""
from __future__ import annotations

import os
from typing import Any, Mapping, Optional
from urllib.parse import urlparse

_HOST_SUFFIX_ALLOWLIST = (
    ".evergreenpodcasts.com",
)
_STATIC_ALLOWED_HOSTS = frozenset({"localhost", "127.0.0.1"})


def _extra_allowed_hosts() -> set[str]:
    raw = os.getenv("MYCO_CLIENT_URL_ALLOWLIST", "")
    return {part.strip().lower() for part in raw.split(",") if part.strip()}


def normalize_client_base_url(raw: object) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    if not text.startswith(("http://", "https://")):
        text = f"https://{text}"
    parsed = urlparse(text)
    if not parsed.scheme or not parsed.netloc:
        return ""
    return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")


def is_allowed_client_host(hostname: object) -> bool:
    host = str(hostname or "").strip().lower()
    if not host:
        return False
    if host in _STATIC_ALLOWED_HOSTS:
        return True
    if host in _extra_allowed_hosts():
        return True
    for suffix in _HOST_SUFFIX_ALLOWLIST:
        if host.endswith(suffix):
            return True
    return False


def is_allowed_client_base_url(url: object) -> bool:
    normalized = normalize_client_base_url(url)
    if not normalized:
        return False
    hostname = urlparse(normalized).hostname
    return is_allowed_client_host(hostname)


def resolve_client_base_url(
    *,
    notice: Optional[Mapping[str, Any]] = None,
    explicit: object = None,
) -> str:
    """Pick the best client base URL for email links (request/stored notice, then env fallback)."""
    candidates: list[object] = []
    if explicit:
        candidates.append(explicit)
    if notice:
        candidates.append(notice.get("client_base_url"))
    candidates.append(os.getenv("MYCO_CLIENT_URL", ""))

    for candidate in candidates:
        normalized = normalize_client_base_url(candidate)
        if normalized and is_allowed_client_base_url(normalized):
            return normalized
    return ""


def _origin_from_referer(referer: str) -> str:
    parsed = urlparse(referer.strip())
    if not parsed.scheme or not parsed.netloc:
        return ""
    return f"{parsed.scheme}://{parsed.netloc}"


def resolve_client_base_url_from_headers(headers: Mapping[str, str]) -> str:
    """Resolve from API request headers (X-Myco-Client-Url, Origin, Referer)."""
    lowered = {str(k).lower(): v for k, v in headers.items()}
    for key in ("x-myco-client-url", "origin", "referer"):
        raw = str(lowered.get(key) or "").strip()
        if not raw:
            continue
        candidate = raw if key != "referer" else _origin_from_referer(raw)
        normalized = normalize_client_base_url(candidate)
        if normalized and is_allowed_client_base_url(normalized):
            return normalized
    return ""


def resolve_client_base_url_from_request(request: Any) -> str:
    headers = getattr(request, "headers", None)
    if headers is None:
        return ""
    return resolve_client_base_url_from_headers(headers)
