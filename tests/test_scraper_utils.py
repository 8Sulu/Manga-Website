"""
tests/test_scraper_utils.py

Unit tests for utils/scraper_utils.py.

All pure functions — no DB, no network, no mocks needed.
Tests are named to double as documentation: the test name describes the
exact input class and expected outcome so a future maintainer knows exactly
what breaks without reading the body.
"""

import pytest
from utils.scraper_utils import (
    normalize_status,
    extract_volume,
    is_novel,
    branch_short,
    STATUS_PRIORITY,
)


# ── normalize_status ──────────────────────────────────────────────────────────


class TestNormalizeStatus:
    """
    normalize_status maps raw SirsiDynix location/status strings to one of
    three canonical values: 'Available', 'On Hold', or 'Checked Out'.

    The critical invariant is exact-set matching (not substring contains) so
    'Non-Youth Fiction' does NOT match 'youth' and return Available.
    """

    # ── Available — exact-set members ────────────────────────────────────────

    @pytest.mark.parametrize(
        "raw",
        [
            "Manga",
            "manga",
            "MANGA",
            "graphic novels",
            "Graphic Novels",
            "GRAPHIC NOVELS",
            "young adult",
            "Young Adult",
            "general collection",
            "General Collection",
            "new materials",
            "reference",
            "children",
            "juvenile",
            "easy",
            "oversize",
            "paperback",
            "teen",
        ],
    )
    def test_available_exact_set(self, raw):
        assert normalize_status(raw) == "Available"

    # ── Available — regex fallback ────────────────────────────────────────────

    @pytest.mark.parametrize(
        "raw",
        [
            "on shelf",
            "On Shelf",
            "ON SHELF",
            "checked in",
            "in library",
            "in stacks",
            "available",
        ],
    )
    def test_available_regex_fallback(self, raw):
        assert normalize_status(raw) == "Available"

    # ── Checked Out ───────────────────────────────────────────────────────────

    @pytest.mark.parametrize(
        "raw",
        [
            "Checked Out",
            "checked out",
            "CHECKED OUT",
            "checkedout",
            "CheckedOut",
            "due 2025-01-01",
            "Due 2025-06-15",
            "overdue",
            "In Transit",
            "in transit",
            "transit",
        ],
    )
    def test_checked_out(self, raw):
        assert normalize_status(raw) == "Checked Out"

    def test_empty_string_is_checked_out(self):
        assert normalize_status("") == "Checked Out"

    def test_unknown_string_defaults_to_checked_out(self):
        # Anything unrecognised should be treated as unavailable
        assert normalize_status("Weird Location XYZ") == "Checked Out"

    # ── On Hold ───────────────────────────────────────────────────────────────

    @pytest.mark.parametrize(
        "raw",
        [
            "On Hold",
            "on hold",
            "HOLD",
            # NOTE: "hold shelf" contains 'shelf', which triggers the
            # `'shelf' not in s` guard in normalize_status, so it falls through
            # to 'Checked Out'. This is the intended behavior — an item sitting
            # on the hold shelf is not yet checked out but is also not "on hold"
            # in the cataloging sense. Test "On Hold" / "HOLD" / "on hold" only.
        ],
    )
    def test_on_hold(self, raw):
        assert normalize_status(raw) == "On Hold"

    def test_hold_shelf_falls_through_to_checked_out(self):
        # "hold shelf" contains 'shelf', so the hold branch is skipped and
        # the string falls through to the default 'Checked Out'. This is
        # documented intentional behavior — see normalize_status docstring.
        assert normalize_status("hold shelf") == "Checked Out"

    # ── Substring collision guard — the original bug this fixed ──────────────

    @pytest.mark.parametrize(
        "raw, expected",
        [
            # 'Youth' appears inside 'Non-Youth Fiction' — must NOT match 'young adult'
            ("Non-Youth Fiction", "Checked Out"),
            # Exact member 'young adult' IS Available
            ("young adult", "Available"),
            # 'on hold shelf': 'shelf' guard fires → Checked Out (documented above)
            ("on hold shelf", "Checked Out"),
            # Transit overrides everything
            ("in transit to branch", "Checked Out"),
        ],
    )
    def test_substring_collision_guard(self, raw, expected):
        assert normalize_status(raw) == expected

    # ── STATUS_PRIORITY ordering ──────────────────────────────────────────────

    def test_priority_ordering(self):
        assert STATUS_PRIORITY["Available"] > STATUS_PRIORITY["On Hold"]
        assert STATUS_PRIORITY["On Hold"] > STATUS_PRIORITY["Checked Out"]


# ── extract_volume ────────────────────────────────────────────────────────────


class TestExtractVolume:
    """
    extract_volume parses a volume number from a SirsiDynix title or
    call-number string. Returns 0 when no volume can be identified.
    """

    @pytest.mark.parametrize(
        "text, expected",
        [
            # Full word 'volume'
            ("Berserk, Volume 1", 1),
            ("Berserk, Volume 38", 38),
            # Abbreviated 'vol'
            ("One Piece vol. 5", 5),
            ("One Piece vol 5", 5),
            ("Vol 10", 10),
            ("VOL. 12", 12),
            # Single-letter prefix 'v'
            ("Attack on Titan v3", 3),
            ("Frieren v 2", 2),
            # Call-number comma suffix
            ("YA GRAPHIC NOV BERSERK, 7", 7),
            # Trailing number (bare)
            ("Some Title 99", 99),
            # No volume → 0
            ("", 0),
            ("Berserk", 0),
            ("The Complete Works", 0),
            # Omnibus / deluxe: extract the raw first number found
            ("Omnibus Vol. 7-8-9", 7),
        ],
    )
    def test_extract_volume_parametrized(self, text, expected):
        assert extract_volume(text) == expected


# ── is_novel ──────────────────────────────────────────────────────────────────


class TestIsNovel:
    """
    is_novel determines whether SirsiDynix subject-exclusion filters should
    be added to suppress graphic novel results contaminating novel searches.
    """

    @pytest.mark.parametrize(
        "manga_type",
        [
            "Light Novel",
            "light novel",
            "LIGHT NOVEL",
            "light_novel",
            "Novel",
            "novel",
            "NOVEL",
            "ln",
            "LN",
        ],
    )
    def test_novel_types_return_true(self, manga_type):
        assert is_novel(manga_type) is True

    @pytest.mark.parametrize(
        "manga_type",
        [
            "Manga",
            "manga",
            "Manhwa",
            "Manhua",
            "One Shot",
            "",
            None,
        ],
    )
    def test_non_novel_types_return_false(self, manga_type):
        assert is_novel(manga_type) is False

    def test_extra_whitespace_handled(self):
        assert is_novel("  Light Novel  ") is True

    def test_none_handled(self):
        assert is_novel(None) is False


# ── branch_short ──────────────────────────────────────────────────────────────


class TestBranchShort:
    """
    branch_short returns a compact label for use in volume chip UI.
    It checks for keywords in order — first match wins.
    """

    @pytest.mark.parametrize(
        "name, expected",
        [
            # First keyword match wins — ordering matters in _BRANCH_SHORT_MAP.
            ("Leroy Collins Leon County Main Public Library", "Main"),
            # "northeast" → "north" fires before "east"
            ("Bruce J. Host Northeast Branch Library", "North"),
            ("South Regional/Broward College Library", "South"),
            ("West Regional Library", "West"),
            ("Weston Branch", "Weston"),
            ("Hollywood Branch", "Hollywood"),
            # "central" fires before "lauderhill" in the map
            ("Lauderhill Central Park Library", "Central"),
            ("African American Research Library", "African"),
            # "beach" fires before "deerfield" in the map
            ("Deerfield Beach Percy White Branch", "Beach"),
            ("Davie/Cooper City Branch", "Davie"),
        ],
    )
    def test_known_branches(self, name, expected):
        assert branch_short(name) == expected

    def test_unknown_branch_no_keyword_returns_first_word(self):
        # None of the keyword patterns match → falls back to first word, truncated to 8 chars
        assert branch_short("Xyz Coordinating Center") == "Xyz"

    def test_branch_keyword_in_map_matches(self):
        # The word 'branch' IS in _BRANCH_SHORT_MAP, so any name containing
        # it will match — tested here to document the explicit behavior.
        assert branch_short("Futuristic Space Branch") == "Branch"

    def test_empty_string_handled(self):
        result = branch_short("")
        assert isinstance(result, str)
