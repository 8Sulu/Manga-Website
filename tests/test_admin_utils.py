"""
tests/test_admin_utils.py

Unit tests for utils/admin_utils.py.

parse_range_str is the only pure function here — insert_csv talks to the DB
and is integration-tested separately (or skipped in unit suite).
"""
import pytest
from utils.admin_utils import parse_range_str


class TestParseRangeStr:
    """
    parse_range_str converts admin-panel range strings into (lo, hi) tuples.

    Contract:
        ""        → (1, 1)
        "50"      → (1, 50)          — single number means "first N"
        "1-50"    → (1, 50)
        "10-"     → (10, max_titles) — open-ended upper bound
        "-50"     → (1, 50)          — open-ended lower bound
        "bad"     → (1, 1)           — invalid input falls back to (1,1)

    Em-dash (–) and en-dash (—) are normalised to '-' before parsing
    because copy-paste from the UI often smuggles them in.
    """

    # ── Canonical forms ───────────────────────────────────────────────────────

    def test_empty_string(self):
        assert parse_range_str('') == (1, 1)

    def test_single_number_gives_1_to_n(self):
        assert parse_range_str('50') == (1, 50)

    def test_single_number_1(self):
        assert parse_range_str('1') == (1, 1)

    def test_explicit_range(self):
        assert parse_range_str('1-50') == (1, 50)

    def test_explicit_range_not_starting_at_1(self):
        assert parse_range_str('10-100') == (10, 100)

    # ── Open-ended bounds ─────────────────────────────────────────────────────

    def test_open_upper_bound(self):
        lo, hi = parse_range_str('10-', max_titles=500)
        assert lo == 10
        assert hi == 500

    def test_open_lower_bound(self):
        lo, hi = parse_range_str('-50')
        assert lo == 1
        assert hi == 50

    def test_open_upper_bound_uses_default_max(self):
        # Default max_titles is 9999
        lo, hi = parse_range_str('10-')
        assert lo == 10
        assert hi == 9999

    # ── Unicode dash normalisation ─────────────────────────────────────────────

    def test_en_dash_normalised(self):
        # U+2013 EN DASH — common copy-paste artifact from browsers
        assert parse_range_str('1\u201350') == (1, 50)

    def test_em_dash_normalised(self):
        # U+2014 EM DASH
        assert parse_range_str('1\u201450') == (1, 50)

    # ── Invalid / garbage input ───────────────────────────────────────────────

    def test_non_numeric_falls_back(self):
        assert parse_range_str('abc') == (1, 1)

    def test_whitespace_stripped(self):
        assert parse_range_str('  10-50  ') == (10, 50)

    # ── max_titles propagation ────────────────────────────────────────────────

    @pytest.mark.parametrize("s, max_t, expected", [
        ('1-50',  200, (1, 50)),
        ('50',    200, (1, 50)),
        ('10-',   200, (10, 200)),
    ])
    def test_max_titles_respected(self, s, max_t, expected):
        assert parse_range_str(s, max_t) == expected
