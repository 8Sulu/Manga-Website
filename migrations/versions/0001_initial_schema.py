"""Initial schema — manga, library, branch, availability, branch_availability_status

Reproduces the original utils/admin_utils.SCHEMA DDL exactly, just
versioned. Revision IDs are plain "0001"/"0002" rather than Alembic's
default random hex — there's no autogenerate to worry about colliding
with, and a sequential, readable ID matches the filename for a project
this size.

Revision ID: 0001
Revises:
Create Date: 2026-06-20 00:00:00.000000
"""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE manga (
            MangaID INT PRIMARY KEY, Title VARCHAR(255) NOT NULL, `Type` VARCHAR(50),
            Volumes INT, Members INT, Score DECIMAL(4,2), Author VARCHAR(255),
            CoverMedium VARCHAR(512), CoverLarge VARCHAR(512)
        )
    """)
    op.execute("""
        CREATE TABLE library (
            LibraryID INT PRIMARY KEY AUTO_INCREMENT,
            LibraryName VARCHAR(255) NOT NULL, `URL` VARCHAR(255) NOT NULL
        )
    """)
    op.execute("""
        CREATE TABLE branch (
            BranchID INT PRIMARY KEY AUTO_INCREMENT, BranchName VARCHAR(255) NOT NULL,
            `Address` VARCHAR(255), LibraryID INT NOT NULL,
            FOREIGN KEY (LibraryID) REFERENCES library(LibraryID) ON DELETE CASCADE
        )
    """)
    op.execute("""
        CREATE TABLE availability (
            AvailabilityID INT AUTO_INCREMENT PRIMARY KEY,
            MangaID INT NOT NULL, Volume INT NOT NULL,
            ScrapedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (MangaID) REFERENCES manga(MangaID) ON DELETE CASCADE
        )
    """)
    op.execute("""
        CREATE TABLE branch_availability_status (
            BranchStatusID INT AUTO_INCREMENT PRIMARY KEY,
            AvailabilityID INT NOT NULL, BranchID INT NOT NULL, `Status` VARCHAR(100) NOT NULL,
            FOREIGN KEY (AvailabilityID) REFERENCES availability(AvailabilityID) ON DELETE CASCADE,
            FOREIGN KEY (BranchID) REFERENCES branch(BranchID) ON DELETE CASCADE
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS branch_availability_status")
    op.execute("DROP TABLE IF EXISTS availability")
    op.execute("DROP TABLE IF EXISTS branch")
    op.execute("DROP TABLE IF EXISTS library")
    op.execute("DROP TABLE IF EXISTS manga")
