"""
utils/scraper_utils.py

Shared utilities for leon_scraper.py and broward_scraper.py.

Exports:
    TitleRow            — NamedTuple for rows from load_title_author_map()
    STATUS_PRIORITY     — dict ranking availability statuses (best = highest)
    normalize_status()  — map raw catalog status string → Available/On Hold/Checked Out
    branch_short()      — compact display name for a branch
    is_novel()          — True for Light Novel / Novel manga types
    extract_volume()    — parse a volume number from any title/call-number string
    load_title_author_map() — ordered list of TitleRow from the manga table
"""
from __future__ import annotations

import re
from typing import NamedTuple

from utils.database_utils import execute_query


# ── Status priority ───────────────────────────────────────────────────────────

STATUS_PRIORITY: dict[str, int] = {
    'Available':   2,
    'On Hold':     1,
    'Checked Out': 0,
}


# ── Status normalisation (FIX #4: exact-match set, no substring collisions) ───
#
# Substring matching on raw location strings causes false positives:
# "Non-Youth Fiction" matches "Youth", "Young Adult Non-Fiction" matches
# "Adult".  Exact matching against the lowercased string prevents this.

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

# Fallback regex for less common but recognisable on-shelf phrasings.
# Only reached when the exact set doesn't match.
_ON_SHELF_RE = re.compile(
    r'\b(on\s+shelf|available|checked\s+in|in\s+library|in\s+stacks)\b',
    re.IGNORECASE,
)


def normalize_status(raw: str) -> str:
    """
    Map a raw SirsiDynix status/location string to one of three canonical
    values: 'Available', 'On Hold', or 'Checked Out'.

    Uses exact-match set membership (not substring contains) so partial-word
    collisions (e.g. 'Non-Youth Fiction' matching 'Youth') are impossible.
    Falls back to a word-boundary regex for less common on-shelf phrasings.
    """
    if not raw:
        return 'Checked Out'

    s = raw.strip().lower()

    # Fast path: direct exact match
    if s in _ON_SHELF_EXACT:
        return 'Available'

    # Checked-out / due-date indicators
    if any(kw in s for kw in ('checked out', 'checkedout', 'due ', 'overdue')):
        return 'Checked Out'

    if 'hold' in s and 'shelf' not in s:
        return 'On Hold'

    if 'transit' in s:
        return 'Checked Out'      # in-transit → treat as unavailable

    # Secondary regex for less common "on shelf" phrasings
    if _ON_SHELF_RE.search(raw):
        return 'Available'

    # Default: treat as unavailable if we can't positively identify it
    return 'Checked Out'


# ── Branch display names ──────────────────────────────────────────────────────

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


def branch_short(name: str, library_id: int | None = None,
                 broward_library_id: int | None = None) -> str:
    """
    Return a short display label for a branch name, for use in compact vol chips.

    Falls back to the first word of the name if nothing in the map matches.
    """
    lower = name.strip().lower()
    for key, label in _BRANCH_SHORT_MAP:
        if key in lower:
            return label
    first = name.strip().split()[0] if name.strip() else name
    return first[:8]


# ── Novel detection ───────────────────────────────────────────────────────────

_NOVEL_TYPES: frozenset[str] = frozenset({
    "light novel", "light_novel", "novel", "ln",
})


def is_novel(manga_type: str) -> bool:
    return (manga_type or '').strip().lower() in _NOVEL_TYPES


# ── TitleRow — shared return type for load_title_author_map() ─────────────────
# FIX #5: TitleRow is kept so external callers can import it if needed,
# and the return type annotation is accurate.

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


# ── Volume number extraction ──────────────────────────────────────────────────

_VOL_RE = re.compile(
    r'(?:vol(?:ume)?\.?\s*|v\.?\s*)(\d+)'
    r'|,\s*(\d+)$'
    r'|\s(\d+)\s*$',
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
