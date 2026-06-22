"""Add FULLTEXT index on manga.Title for full-text search

Revision ID: 0002
Revises: 0001
Create Date: 2026-06-20 00:00:00.000000
"""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE manga ADD FULLTEXT INDEX ft_manga_title (Title)")


def downgrade() -> None:
    op.execute("ALTER TABLE manga DROP INDEX ft_manga_title")
