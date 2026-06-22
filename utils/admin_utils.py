"""
utils/admin_utils.py

Helpers used by the admin routes in backend.py: schema reset, CSV
re-seeding, range-string parsing, and Alembic bookkeeping.

SCHEMA below is for the "Reset Database" button's full drop/reseed path
only. Real schema changes belong in migrations/versions/; if you add a
migration, update SCHEMA to match in the same commit or the two paths
will drift.
"""

import csv

from config.settings import BASE_DIR, DATA_DIR
from utils.database_utils import get_db_connection

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


def insert_csv(filename: str, query: str) -> str:
    """Seed one CSV file into the DB, returning a ✓/✗ summary line."""
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


def stamp_alembic_head() -> str:
    """
    Mark Alembic's version table as being at the latest migration.

    "Reset Database" recreates tables directly from SCHEMA, bypassing
    migrations/ entirely — so it never touches alembic_version. Without
    this call, the next `alembic upgrade head` doesn't know the DB is
    already current and tries to re-run CREATE TABLE against tables that
    already exist.
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


def parse_range_str(s: str, max_titles: int = 9999) -> tuple[int, int]:
    """
    Parse a range string into (lo, hi).
      "20"     → (1, 20)
      "1-50"   → (1, 50)
      "10-"    → (10, max_titles)
      ""       → (1, 1)

    Em-dash/en-dash are normalised to '-' since copy-pasted ranges from
    the UI sometimes carry them in.
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
