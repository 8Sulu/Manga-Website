"""
utils/scraper_utils.py

Shared utilities for leon_scraper.py and broward_scraper.py.

Includes _normalize_status() with exact-match ON_SHELF set (#7).
"""
from __future__ import annotations

import re
from typing import NamedTuple

from utils.database_utils import execute_query          # clean import, no sys.path


# ── Status normalisation ───────────────────────────────────────────────────────

# Exact lowercase strings that mean "on shelf".
# Previously used any(kw in raw …) which allowed substring false-positives
# (e.g. "Youth Fiction" matching "Non-Youth Fiction").  Exact set is safe.
_ON_SHELF_EXACT: frozenset[str] = frozenset({
    "general collection",
    "new materials",
    "reference",
    "graphic novels",
    "young adult",
    "children",
    "juvenile",
    "easy",
    "oversize",
    "paperback",
    "manga",
    "teen",
})

# Fallback: word-boundary regex for less common but recognisable on-shelf phrases.
# Only used when the exact set doesn't match.
_ON_SHELF_RE = re.compile(
    r'\b(on\s+shelf|available|checked\s+in|in\s+library|in\s+stacks)\b',
    re.IGNORECASE,
)


def normalize_status(raw: str) -> str:
    """
    Map a raw SirsiDynix status/location string to one of three canonical
    values: "Available", "On Hold", or "Checked Out".

    Uses exact-match set membership (not substring contains) so partial-word
    collisions (e.g. 'Non-Youth Fiction' matching 'Youth') are impossible.
    """
    s = raw.strip().lower()

    # Fast path: direct exact match
    if s in _ON_SHELF_EXACT:
        return "Available"

    # Checked-out / due-date indicators
    if any(kw in s for kw in ("checked out", "checkedout", "due ", "overdue")):
        return "Checked Out"

    if "hold" in s and "shelf" not in s:
        return "On Hold"

    if "transit" in s:
        return "Checked Out"      # in-transit → treat as unavailable

    # Secondary regex for less common "on shelf" phrasings
    if _ON_SHELF_RE.search(raw):
        return "Available"

    # Default: treat as unavailable if we can't positively identify it
    return "Checked Out"


# ── Status priority (for best-status-wins aggregation) ───────────────────────

STATUS_PRIORITY: dict[str, int] = {
    "Available":   2,
    "On Hold":     1,
    "Checked Out": 0,
}


# ── Volume extraction ──────────────────────────────────────────────────────────

_VOL_RE = re.compile(
    r'(?:vol(?:ume)?\.?\s*|v\.?\s*)(\d+)'
    r'|,\s*(\d+)$'                          # trailing ", N"
    r'|\s(\d+)\s*$',                        # trailing space N
    re.IGNORECASE,
)


def extract_volume(text: str) -> int:
    """
    Extract a volume number from a title or call-number string.
    Returns 0 if no volume can be identified (treated as series-level).
    """
    if not text:
        return 0
    m = _VOL_RE.search(text)
    if m:
        val = next(g for g in m.groups() if g is not None)
        return int(val)
    return 0


# ── Branch name shortening ────────────────────────────────────────────────────

# Maps substrings in a branch name (lowercase) to a short display label.
# Checked in order — first match wins.
_BRANCH_SHORT_MAP: list[tuple[str, str]] = [
    ("main",        "Main"),
    ("central",     "Central"),
    ("downtown",    "Downtown"),
    ("north",       "North"),
    ("south",       "South"),
    ("east",        "East"),
    ("west",        "West"),
    ("beach",       "Beach"),
    ("airport",     "Airport"),
    ("carver",      "Carver"),
    ("african",     "African"),
    ("lauderdale",  "Laud."),
    ("pompano",     "Pompano"),
    ("deerfield",   "Deerfield"),
    ("tamarac",     "Tamarac"),
    ("plantation",  "Plant."),
    ("davie",       "Davie"),
    ("cooper",      "Cooper"),
    ("weston",      "Weston"),
    ("miramar",     "Miramar"),
    ("hallandale",  "Halland."),
    ("hollywood",   "Hollywood"),
    ("dania",       "Dania"),
    ("pembroke",    "Pembroke"),
    ("margate",     "Margate"),
    ("coconut",     "Coconut"),
    ("sunrise",     "Sunrise"),
    ("lauderhill",  "L.Hill"),
    ("imperial",    "Imperial"),
    ("lakes",       "Lakes"),
    ("regional",    "Regional"),
    ("branch",      "Branch"),
]


def branch_short(name: str) -> str:
    """
    Return a short display label for a branch name, for use in compact vol chips.
    Falls back to the first word of the name if nothing matches.
    """
    lower = name.strip().lower()
    for key, label in _BRANCH_SHORT_MAP:
        if key in lower:
            return label
    # Fallback: first word, title-cased, max 8 chars
    first = name.strip().split()[0] if name.strip() else name
    return first[:8]


# ── Novel detection ───────────────────────────────────────────────────────────

_NOVEL_TYPES: frozenset[str] = frozenset({
    "light novel", "light_novel", "novel", "ln",
})


def is_novel(manga_type: str) -> bool:
    return manga_type.strip().lower() in _NOVEL_TYPES


# ── DB title/author map ────────────────────────────────────────────────────────

class TitleRow(NamedTuple):
    idx:        int
    title:      str
    author:     str
    manga_id:   int
    manga_type: str


def load_title_author_map() -> list[TitleRow]:
    """
    Return every manga row ordered by MangaID.
    Each element is a TitleRow(idx, title, author, manga_id, manga_type).
    """
    rows = execute_query(
        'SELECT MangaID, Title, Author, Type FROM manga ORDER BY MangaID'
    )
    return [
        TitleRow(
            idx        = i + 1,
            title      = r['Title'],
            author     = r['Author'] or '',
            manga_id   = r['MangaID'],
            manga_type = r['Type'] or '',
        )
        for i, r in enumerate(rows)
    ]
