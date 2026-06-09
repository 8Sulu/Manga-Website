"""
utils/database_utils.py

Database access helpers used throughout the application.

Context managers (get_db_connection, get_db_cursor) are the preferred interface
for Flask route handlers.  Scrapers that need direct cursor/transaction control
(e.g. bulk-insert loops with manual commit) should use get_connection().
"""
import mysql.connector
from contextlib import contextmanager
from config.settings import DB_CONFIG


# ── Raw connection (scrapers) ──────────────────────────────────────────────────

def get_connection() -> mysql.connector.MySQLConnection:
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
