import mysql.connector
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.pooling import PooledMySQLConnection
from contextlib import contextmanager

from config.settings import DB_CONFIG


# ── Raw connection (scrapers) ──────────────────────────────────────────────────

def get_connection() -> PooledMySQLConnection | MySQLConnectionAbstract:
    """
    Return a raw mysql.connector connection.

    The caller is responsible for commit(), rollback(), and close().
    Use the context managers below for Flask route handlers instead.
    """
    return mysql.connector.connect(**DB_CONFIG)


# ── Context managers (Flask routes) ───────────────────────────────────────────


@contextmanager
def get_db_connection():
    """Yield a connection; auto-rollback on exception, always close."""
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        yield conn
    except mysql.connector.Error as err:
        if conn:
            conn.rollback()
        raise err
    finally:
        if conn and conn.is_connected():
            conn.close()


@contextmanager
def get_db_cursor():
    """Yield a dict cursor inside a managed transaction; auto-commit on success."""
    with get_db_connection() as conn:
        cursor = conn.cursor(dictionary=True)
        try:
            yield cursor
            conn.commit()
        except Exception:
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
            conn.commit()
            return cursor.rowcount
        except mysql.connector.Error as err:
            conn.rollback()
            raise err
        finally:
            cursor.close()


# ── Library ID cache ───────────────────────────────────────────────────────────
# Library IDs are stable for the lifetime of the process (they only change
# after a DB reset).  We cache the (lcpl_id, broward_id) pair on first lookup
# and expose invalidate_library_id_cache() for the reset route to call.

_library_id_cache: tuple[int, int] | None = None


def invalidate_library_id_cache() -> None:
    """
    Clear the cached library IDs.  Must be called after a DB reset so the
    next request re-reads the newly seeded library rows.
    """
    global _library_id_cache
    _library_id_cache = None


def get_library_ids() -> tuple[int, int]:
    """
    Return (lcpl_library_id, broward_library_id).

    Result is cached for the lifetime of the process.  Falls back to (1, 2)
    if the library table is empty or inaccessible so the rest of the app
    degrades gracefully rather than crashing.
    """
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
    except Exception:
        pass

    # Fallback: assume insertion order 1, 2 (matches libraries.csv seed)
    try:
        rows = execute_query("SELECT LibraryID FROM library ORDER BY LibraryID LIMIT 2")
        if len(rows) >= 2:
            _library_id_cache = (rows[0]["LibraryID"], rows[1]["LibraryID"])
            return _library_id_cache
    except Exception:
        pass

    return 1, 2
