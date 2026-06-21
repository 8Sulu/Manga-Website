"""
utils/admin_utils.py

Constants and helpers used exclusively by the admin routes in backend.py:
  - SCHEMA               — DDL string for a full DB reset
  - INSERT_OPS           — (csv_filename, INSERT query) pairs for re-seeding
  - insert_csv()         — seed one CSV file into the DB
  - parse_range_str()    — parse "1-50" / "20" range strings
  - stamp_alembic_head() — keep Alembic's bookkeeping in sync after a reset

SCHEMA / ALEMBIC SYNC — READ BEFORE EDITING SCHEMA:
  Schema *changes* should go through a new file in migrations/versions/
  (see migrations/env.py and the "Database Migrations" section of
  README.md) — NOT a hand-edit here. SCHEMA below is kept ONLY for the
  admin "Reset Database" button, which still does a full drop/reseed for
  fast dev resets and disaster recovery. It's hand-synced to match
  whatever the head Alembic migration produces (currently: the original
  five tables, plus the FULLTEXT index added in
  migrations/versions/0002_add_fulltext_search.py) so that the schema you
  get from clicking "Reset Database" and the schema you'd get from
  `alembic upgrade head` against an empty DB are identical. If you add a
  migration that changes the schema, update SCHEMA to match in the same
  commit, or the two paths will silently drift.
"""

import csv

from config.settings import BASE_DIR, DATA_DIR
from utils.database_utils import get_db_connection


# ── DB schema ────────────────────────────────────────────────────────────────
# Kept in sync with migrations/versions/ — see the module docstring above.

SCHEMA = """
SET FOREIGN_KEY_CHECKS=0;
DROP TABLE IF EXISTS branch_availability_status;
DROP TABLE IF EXISTS availability;
DROP TABLE IF EXISTS branch;
DROP TABLE IF EXISTS manga;
DROP TABLE IF EXISTS library;
SET FOREIGN_KEY_CHECKS=1;
CREATE TABLE manga (
    MangaID INT PRIMARY KEY, Title VARCHAR(255) NOT NULL, `Type` VARCHAR(50),
    Volumes INT, Members INT, Score DECIMAL(4,2), Author VARCHAR(255),
    CoverMedium VARCHAR(512), CoverLarge VARCHAR(512),
    FULLTEXT INDEX ft_manga_title (Title)
);
CREATE TABLE library (
    LibraryID INT PRIMARY KEY AUTO_INCREMENT,
    LibraryName VARCHAR(255) NOT NULL, `URL` VARCHAR(255) NOT NULL
);
CREATE TABLE branch (
    BranchID INT PRIMARY KEY AUTO_INCREMENT, BranchName VARCHAR(255) NOT NULL,
    `Address` VARCHAR(255), LibraryID INT NOT NULL,
    FOREIGN KEY (LibraryID) REFERENCES library(LibraryID) ON DELETE CASCADE
);
CREATE TABLE availability (
    AvailabilityID INT AUTO_INCREMENT PRIMARY KEY,
    MangaID INT NOT NULL, Volume INT NOT NULL,
    ScrapedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (MangaID) REFERENCES manga(MangaID) ON DELETE CASCADE
);
CREATE TABLE branch_availability_status (
    BranchStatusID INT AUTO_INCREMENT PRIMARY KEY,
    AvailabilityID INT NOT NULL, BranchID INT NOT NULL, `Status` VARCHAR(100) NOT NULL,
    FOREIGN KEY (AvailabilityID) REFERENCES availability(AvailabilityID) ON DELETE CASCADE,
    FOREIGN KEY (BranchID) REFERENCES branch(BranchID) ON DELETE CASCADE
);
"""

INSERT_OPS = [
    (
        "manga.csv",
        "INSERT INTO manga (MangaID, Title, Type, Volumes, Members, Score, Author, CoverMedium, CoverLarge) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
    ),
    ("libraries.csv", "INSERT INTO library (LibraryName, `URL`) VALUES (%s,%s)"),
    ("branches.csv", "INSERT INTO branch (BranchName, `Address`, LibraryID) VALUES (%s,%s,%s)"),
]


# ── CSV seeder ─────────────────────────────────────────────────────────────────


def insert_csv(filename: str, query: str) -> str:
    filepath = DATA_DIR / filename
    try:
        with open(filepath, encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)
            with get_db_connection() as conn:
                cursor = conn.cursor()
                for row in reader:
                    row = [None if v.strip().upper() in ("NULL", "") else v for v in row]
                    cursor.execute(query, tuple(row))
                conn.commit()
        return f"✓ {filename}"
    except Exception as e:
        return f"✗ {filename}: {e}"


# ── Alembic bookkeeping ─────────────────────────────────────────────────────


def stamp_alembic_head() -> str:
    """
    Mark Alembic's version table as being at the latest migration.

    "Reset Database" drops and recreates every table from SCHEMA above —
    completely bypassing migrations/versions/. SCHEMA is hand-synced with
    the head migration (see the module docstring) specifically so the live
    schema and "head" are identical the instant after a reset finishes.
    The one thing a CREATE-TABLE-from-SCHEMA reset can't do on its own is
    update Alembic's own bookkeeping table (`alembic_version`) — that
    table isn't in SCHEMA's DROP list, so it survives a reset untouched.

    Without this call: a reset performed before `alembic upgrade head` has
    ever run leaves no alembic_version row at all, and the next `alembic
    upgrade head` doesn't know it's already at head — it tries to re-run
    "CREATE TABLE manga" against a table that already exists and fails.
    `alembic stamp head` isn't a real migration run (no SQL from any
    upgrade() executes) — it's a bookkeeping write that keeps that table
    truthful.
    """
    try:
        from alembic import command
        from alembic.config import Config

        cfg = Config(str(BASE_DIR / "alembic.ini"))
        cfg.set_main_option("script_location", str(BASE_DIR / "migrations"))
        command.stamp(cfg, "head")
        return "✓ alembic stamped at head"
    except Exception as e:
        return f"✗ alembic stamp failed: {e}"


# ── Range parser ───────────────────────────────────────────────────────────────


def parse_range_str(s: str, max_titles: int = 9999) -> tuple[int, int]:
    """
    Parse a range string into (lo, hi).
      "20"     → (1, 20)
      "1-50"   → (1, 50)
      "10-"    → (10, max_titles)
      ""       → (1, 1)
    """
    import re

    s = s.strip().replace("\u2013", "-").replace("\u2014", "-")
    if not s:
        return 1, 1
    if s.isdigit():
        return 1, int(s)
    m = re.match(r"^(\d*)-(\d*)$", s)
    if m:
        lo = int(m.group(1)) if m.group(1) else 1
        hi = int(m.group(2)) if m.group(2) else max_titles
        return lo, hi
    return 1, 1
