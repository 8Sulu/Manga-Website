"""
migrations/env.py

Alembic environment script.

WHY NO ORM MODELS / target_metadata = None:
The rest of the app talks to MySQL via raw mysql.connector throughout (see
utils/database_utils.py) — there's no SQLAlchemy model layer anywhere else
in the codebase. Alembic still requires SQLAlchemy under the hood (it's a
hard dependency of the alembic package itself, and it's what actually
opens the DB connection below), but rather than bolt on an ORM layer just
to satisfy Alembic, every migration in migrations/versions/ uses
op.execute() with raw SQL — the exact same DDL style utils/admin_utils.py's
SCHEMA string always used, just versioned instead of hand-edited in place.
Since there's no Base.metadata to diff against, `alembic revision
--autogenerate` won't work here — migrations are written by hand, same as
SCHEMA always was, just incremental instead of requiring a full
drop/reseed.

WHY THE DB URL IS BUILT FROM config.settings.DB_CONFIG, NOT alembic.ini:
So this never drifts from whatever utils/database_utils.py is actually
connecting to — one source of truth (.env via config/settings.py) for both
the app and migrations, same as every other module in this codebase.
"""

from __future__ import annotations

import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlalchemy.engine import URL

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import DB_CONFIG  # noqa: E402

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = None


def _build_db_url() -> str:
    """Build a SQLAlchemy URL from the same DB_CONFIG the rest of the app uses."""
    return URL.create(
        drivername="mysql+mysqlconnector",
        username=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        database=DB_CONFIG["database"],
    ).render_as_string(hide_password=False)


config.set_main_option("sqlalchemy.url", _build_db_url())


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
