"""
utils/scraper_utils.py

Shared helpers used by both the LCPL and Broward County scrapers.
Centralises logic that would otherwise live in parallel across two scripts.

Exports:
    STATUS_PRIORITY         – dict ranking availability statuses (best = highest)
    ON_SHELF                – frozenset of status keywords meaning Available
    is_novel()              – True for Light Novel / Novel manga types
    normalize_status()      – map raw catalog status string → Available/On Hold/Checked Out
    branch_short()          – compact display name for a branch (used in results UI)
    load_title_author_map() – ordered list of (index, title, author, id, type)
    extract_volume()        – parse a volume number from any title/call-number string
    fmt_scraped_at()        – re-exported from format_utils for scraper convenience
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))


# ── Availability status priority ──────────────────────────────────────────────
# Higher = better.  Used when collapsing multiple copies or branches into one
# representative status.

STATUS_PRIORITY: dict[str, int] = {
    'Available':   2,
    'On Hold':     1,
    'Checked Out': 0,
}


# ── Status normalisation ──────────────────────────────────────────────────────
# Catalog systems return verbose, inconsistent status strings.  Everything maps
# to one of the three canonical values above.

ON_SHELF: frozenset[str] = frozenset({
    'Graphic Novel',
    'Youth Fiction',
    'Adult Non-Fiction',
    'Available',
    'General Collection',
    'New Materials',
})


def normalize_status(raw: str) -> str:
    """
    Map a raw catalog status string to 'Available', 'On Hold', or 'Checked Out'.

    The ON_SHELF set uses substring matching, so 'Graphic Novel - YA Fiction'
    correctly resolves to 'Available'.
    """
    if not raw:
        return 'Checked Out'
    if any(kw in raw for kw in ON_SHELF):
        return 'Available'
    if 'hold' in raw.lower():
        return 'On Hold'
    return 'Checked Out'


# ── Branch display names ──────────────────────────────────────────────────────

def branch_short(name: str, library_id: int, broward_library_id: int) -> str:
    """
    Return a compact display label for a branch, used in the results grid.

    LCPL and Broward branches are matched by keyword in their full DB name.
    The broward_library_id parameter distinguishes which lookup table to use.
    """
    if library_id != broward_library_id:
        for keyword, short in (
            ('Main',       'Main'),        ('Leroy',      'Main'),
            ('Northeast',  'NE Branch'),   ('Bruce',      'NE Branch'),
            ('Eastside',   'Eastside'),    ('Perry',      'BL Perry'),
            ('Jackson',    'Lk Jackson'),  ('Braden',     'Ft Braden'),
            ('Fort',       'Ft Braden'),   ('Woodville',  'Woodville'),
        ):
            if keyword in name:
                return short
        return name.split(' ')[0]

    for keyword, short in (
        ('Northwest Regional',      'NW Regional'),
        ('North Regional',          'N Regional'),
        ('South Regional',          'S Regional'),
        ('Southwest Regional',      'SW Regional'),
        ('West Regional',           'W Regional'),
        ('Main Library',            'Main'),
        ('African American',        'AARLCC'),
        ('Hollywood Beach',         'Hollywood Bch'),
        ('Hollywood',               'Hollywood'),
        ('Lauderhill Central',      'Lauderhill CP'),
        ('Lauderhill Towne',        'Lauderhill TC'),
        ('Lauderdale Lakes',        'Laud. Lakes'),
        ('Pompano Beach',           'Pompano Bch'),
        ('Pembroke Pines',          'Pemb. Pines'),
        ('Miramar',                 'Miramar'),
        ('Weston',                  'Weston'),
        ('Tamarac',                 'Tamarac'),
        ('Sunrise',                 'Sunrise'),
        ('Margate',                 'Margate'),
        ('Deerfield Beach',         'Deerfield Bch'),
        ('Dania Beach',             'Dania Bch'),
        ('Hallandale',              'Hallandale'),
        ('Carver Ranches',          'Carver Ranch'),
        ('Century Plaza',           'Century Plz'),
        ('North Lauderdale',        'N. Laud.'),
        ('Imperial Point',          'Imperial Pt'),
        ('Riverland',               'Riverland'),
        ('Davie',                   'Davie/CC'),
        ('Beach Branch',            'Beach'),
        ('Jan Moran',               'Jan Moran'),
        ('Galt Ocean',              'Galt Ocean'),
        ('Fort Lauderdale Reading', 'FTL Reading'),
        ('Tyrone Bryant',           'Tyrone Bryant'),
        ('Northwest Branch',        'NW Branch'),
        ('Nova Southeastern',       'NSU'),
    ):
        if keyword in name:
            return short
    return name.split(' ')[0]


# ── Media-type helpers ────────────────────────────────────────────────────────

def is_novel(manga_type: str) -> bool:
    """Return True for Light Novel / Novel titles (handles spaces and hyphens)."""
    return (manga_type or '').lower().replace(' ', '-') in ('light-novel', 'novel')


# ── Title/author source of truth ──────────────────────────────────────────────

def load_title_author_map() -> list[tuple[int, str, str, int, str]]:
    """
    Query the manga table and return a list of tuples:
        (1-based-index, title, author, manga_id, manga_type)
    ordered by MangaID — the canonical ordering shared by both scrapers.
    """
    from config.settings import DB_CONFIG
    import mysql.connector

    conn   = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute('SELECT MangaID, Title, Author, Type FROM manga ORDER BY MangaID')
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    return [
        (i + 1, r['Title'], r['Author'] or '', r['MangaID'], r['Type'] or '')
        for i, r in enumerate(rows)
    ]


# ── Volume number extraction ──────────────────────────────────────────────────
# Handles titles like "Berserk Vol. 3", call numbers like "GN V.7",
# omnibus ranges like "Vol. 7-9" (→ 7), and bare trailing digits.

_OMNIBUS_RE = re.compile(r'(\d+)\s*[-–]\s*\d+')

_VOL_PATTERNS: list[re.Pattern] = [
    re.compile(r'\bvol(?:ume)?\.?\s*(\d+)',    re.I),   # Vol. 3 / Volume 3
    re.compile(r'\bv\.?\s*(\d+)\b',            re.I),   # V.3 / v3
    re.compile(r'#\s*(\d+)\b'),                          # #3
    re.compile(r'\bno\.?\s*(\d+)\b',           re.I),   # No. 3
    re.compile(r'\bpart\s*(\d+)\b',            re.I),   # Part 3
    re.compile(r'\bbook\s*(\d+)\b',            re.I),   # Book 3
    re.compile(r'\bep(?:isode)?\.?\s*(\d+)\b', re.I),   # Ep. 3 / Episode 3
]


def extract_volume(text: str) -> int:
    """
    Return the volume number embedded in *text*, or 0 if none is found.

    Checks omnibus ranges first ("Vol. 7-9" → 7), then each pattern
    in priority order, then bare trailing digits as a last resort.
    """
    if not text:
        return 0

    m = _OMNIBUS_RE.search(text)
    if m:
        return int(m.group(1))

    for pat in _VOL_PATTERNS:
        m = pat.search(text)
        if m:
            return int(m.group(1))

    m = re.search(r'[.,]\s*(\d+)\s*$', text)
    if m:
        return int(m.group(1))

    m = re.search(r'\b(\d+)\s*$', text)
    if m:
        return int(m.group(1))

    return 0
