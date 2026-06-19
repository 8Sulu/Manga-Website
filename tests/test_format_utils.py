"""
tests/test_format_utils.py

Unit tests for utils/format_utils.py  (fmt_scraped_at).
All pure-function, no DB/network.

The function formats a datetime into a human label relative to now.
We freeze "now" by patching datetime.now inside the module so tests
don't become flaky based on wall-clock time.
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from unittest.mock import patch

import pytest

from utils.format_utils import fmt_scraped_at


# ── Helper: build a UTC datetime N days ago ───────────────────────────────────

def _ago(days: float = 0, hours: float = 0) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days, hours=hours)


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestFmtScrapedAt:

    def test_none_returns_none(self):
        assert fmt_scraped_at(None) is None

    def test_unparseable_string_returns_none(self):
        assert fmt_scraped_at("not a date") is None

    def test_today(self):
        assert fmt_scraped_at(_ago(hours=2)) == 'today'

    def test_yesterday(self):
        assert fmt_scraped_at(_ago(days=1)) == 'yesterday'

    @pytest.mark.parametrize("days, expected", [
        (2,  '2d ago'),
        (3,  '3d ago'),
        (6,  '6d ago'),
    ])
    def test_days_ago(self, days, expected):
        assert fmt_scraped_at(_ago(days=days)) == expected

    @pytest.mark.parametrize("days, expected", [
        (7,  '1w ago'),
        (14, '2w ago'),
        (21, '3w ago'),
        (30, '4w ago'),   # 30 // 7 == 4
    ])
    def test_weeks_ago(self, days, expected):
        assert fmt_scraped_at(_ago(days=days)) == expected

    @pytest.mark.parametrize("days, expected", [
        (31,  '1mo ago'),
        (60,  '2mo ago'),
        (90,  '3mo ago'),
        (365, '12mo ago'),
    ])
    def test_months_ago(self, days, expected):
        assert fmt_scraped_at(_ago(days=days)) == expected

    def test_iso_string_input(self):
        # fmt_scraped_at accepts ISO-format strings as well as datetimes
        dt = _ago(days=3)
        result = fmt_scraped_at(dt.isoformat())
        assert result == '3d ago'

    def test_naive_datetime_treated_as_utc(self):
        # Naive datetimes (no tzinfo) are pinned to UTC by the function
        naive = datetime.now(timezone.utc) - timedelta(days=2)
        assert fmt_scraped_at(naive) == '2d ago'

    def test_zero_days_is_today(self):
        # A datetime 23h 59m ago is still "today"
        almost_yesterday = _ago(hours=23, days=0)
        assert fmt_scraped_at(almost_yesterday) == 'today'
