"""
utils/database_utils.py

Database access helpers used throughout the application.

Context managers (get_db_connection, get_db_cursor) are the preferred interface
for Flask route handlers.  Scrapers that need direct cursor/transaction control
(e.g. bulk-insert loops with manual commit) should use get_connection().

Exports:
    get_connection()              — raw connection (scrapers)
    get_db_connection()           — context manager, auto-rollback/close
    get_db_cursor()                — context manager, dict cursor, auto-commit
    execute_query(sql, params)    — SELECT → list or single row
    execute_update(sql, params)   — INSERT/UPDATE/DELETE → rowcount
    get_library_ids()             — (lcpl_id, broward_id) with process-lifetime cache
    invalidate_library_id_cache() — call after a DB reset
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING

import mysql.connector

from config.settings import DB_CONFIG

if TYPE_CHECKING:
    # Submodule-level imports — needed only for the type checker.
    from mysql.connector.abstracts import MySQLConnectionAbstract
    from mysql.connector.pooling import PooledMySQLConnection

log = logging.getLogger(__name__)

# ── Raw connection (scrapers) ──────────────────────────────────────────────────


def get_connection() -> PooledMySQLConnection | MySQLConnectionAbstract:
    """
    Return a raw mysql.connector connection.

    The caller is responsible for commit(), rollback(), and close().
    Use the context managers below for Flask route handlers instead.
    """
    return mysql.connector.connect(**DB_CONFIG)


def reconnect(conn, cursor):
    """
    Ping a connection and reopen it if it's gone away (e.g. MySQL's
    wait_timeout closing an idle connection mid-scrape). Returns (conn,
    cursor) — callers must use the returned pair, never the originals.

    Shared by leon_scraper.py and broward_scraper.py, which used to each
    carry an identical standalone copy — same drift risk as the
    ON_SHELF_STATUSES bug, caught here before it had the chance to diverge.
    """
    try:
        conn.ping(reconnect=True, attempts=3, delay=5)
        cursor.close()
        return conn, conn.cursor()
    except Exception as e:
        log.warning(f"DB ping failed, opening fresh connection: {e}")
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        conn = get_connection()
        cursor = conn.cursor()
        return conn, cursor


# ── Context managers (Flask routes) ───────────────────────────────────────────


@contextmanager
def get_db_connection():
    """Yield a connection; auto-rollback on exception, always close cleanly."""
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        yield conn
        # If no exception occurred, commit any outstanding operations
        if conn and conn.is_connected() and not conn.autocommit:
            conn.commit()
    except Exception as err:
        if conn and conn.is_connected():
            try:
                conn.rollback()
            except Exception:
                pass
        raise err
    finally:
        if conn and conn.is_connected():
            try:
                # Defensive rollback to ensure no open transaction views leak into the pool
                if not conn.autocommit:
                    conn.rollback()
            except Exception:
                pass
            conn.close()


@contextmanager
def get_db_cursor():
    """Yield a dict cursor inside a managed transaction; auto-commit on success."""
    with get_db_connection() as conn:
        cursor = conn.cursor(dictionary=True)
        try:
            yield cursor
            if not conn.autocommit:
                conn.commit()
        except Exception:
            if not conn.autocommit:
                conn.rollback()
            raise
        finally:
            cursor.close()


# ── One-shot helpers ───────────────────────────────────────────────────────────


def execute_query(query: str, params=None, fetch_all: bool = True):
    """Execute a SELECT and return all rows (or one row if fetch_all=False)."""
    with get_db_cursor() as cursor:
        cursor.execute(query, params or ())
        return cursor.fetchall() if fetch_all else cursor.fetchone()


def execute_update(query: str, params=None) -> int:
    """Execute an INSERT / UPDATE / DELETE and return the affected row count."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query, params or ())
            if not conn.autocommit:
                conn.commit()
            return cursor.rowcount
        except Exception as err:
            if not conn.autocommit:
                conn.rollback()
            raise err
        finally:
            cursor.close()


# ── Library ID cache ───────────────────────────────────────────────────────────

_library_id_cache: tuple[int, int] | None = None


def invalidate_library_id_cache() -> None:
    """
    Clear the cached library IDs.  Must be called after a DB reset so the
    next request re-reads the newly seeded library rows.
    """
    global _library_id_cache
    _library_id_cache = None


def get_library_ids() -> tuple[int, int]:
    global _library_id_cache
    if _library_id_cache is not None:
        return _library_id_cache

    try:
        rows = execute_query("SELECT LibraryID, LibraryName FROM library")
        lcpl = broward = None
        for r in rows:
            name = r["LibraryName"] or ""
            if "Leon" in name or "LeRoy" in name or "LCPL" in name:
                lcpl = r["LibraryID"]
            elif "Broward" in name:
                broward = r["LibraryID"]
        if lcpl is not None and broward is not None:
            _library_id_cache = (lcpl, broward)
            return _library_id_cache
        log.warning("get_library_ids: name match incomplete (lcpl=%s, broward=%s)", lcpl, broward)
    except Exception as e:
        log.warning("get_library_ids: name-match query failed: %s", e)

    try:
        rows = execute_query("SELECT LibraryID FROM library ORDER BY LibraryID LIMIT 2")
        if len(rows) >= 2:
            _library_id_cache = (rows[0]["LibraryID"], rows[1]["LibraryID"])
            log.warning("get_library_ids: using insertion-order fallback %s", _library_id_cache)
            return _library_id_cache
    except Exception as e:
        log.error("get_library_ids: insertion-order fallback failed: %s", e)

    log.error(
        "get_library_ids: all lookups failed — hardcoding (1, 2), which is known "
        "wrong for this DB (Broward is 15, not 2). Fix the library table."
    )
    return 1, 2
