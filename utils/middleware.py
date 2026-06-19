"""
utils/middleware.py

Request-level helpers used by Flask route handlers.

Exports:
    rate_limited(key, limit, window)  — sliding-window rate limiter
    client_ip()                       — best-effort client IP from headers
"""

from __future__ import annotations

import threading
import time

from flask import request

_rate_limits: dict[str, list[float]] = {}
_rate_lock = threading.Lock()


def rate_limited(key: str, limit: int = 60, window: int = 60) -> bool:
    """
    Return True if *key* has exceeded *limit* hits within the last *window*
    seconds, False otherwise.  Thread-safe via a module-level lock.
    """
    now = time.time()
    with _rate_lock:
        hits = [t for t in _rate_limits.get(key, []) if now - t < window]
        if len(hits) >= limit:
            _rate_limits[key] = hits
            return True
        hits.append(now)
        _rate_limits[key] = hits
    return False


def client_ip() -> str:
    """
    Return the best-effort client IP, honouring common reverse-proxy headers
    set by nginx (X-Real-IP, X-Forwarded-For) before falling back to the
    raw socket address.
    """
    return (
        request.headers.get("X-Real-IP")
        or request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or request.remote_addr
        or "unknown"
    )
