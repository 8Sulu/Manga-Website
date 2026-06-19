"""
utils/format_utils.py

Display-layer formatting helpers.  No Flask or DB dependency — safe to
import from anywhere.

Exports:
    fmt_scraped_at(dt)  — human-readable "2d ago / today / 3w ago" label
"""

from __future__ import annotations

from datetime import datetime, timezone


def fmt_scraped_at(dt) -> str | None:
    """
    Format a ScrapedAt datetime or ISO string into a compact human label.

    Returns None when dt is None or unparseable so callers can omit the
    label entirely rather than display a broken string.

    Examples: 'today', 'yesterday', '3d ago', '2w ago', '1mo ago'
    """
    if dt is None:
        return None
    try:
        if isinstance(dt, str):
            dt = datetime.fromisoformat(dt)
        now = datetime.now(timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        days = (now - dt).days
        if days == 0:
            return 'today'
        if days == 1:
            return 'yesterday'
        if days < 7:
            return f'{days}d ago'
        if days < 31:
            return f'{days // 7}w ago'
        return f'{days // 30}mo ago'
    except Exception:
        return None
