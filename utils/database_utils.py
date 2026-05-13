import mysql.connector
from contextlib import contextmanager
from config.settings import DB_CONFIG

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
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
    """Context manager for database cursors"""
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

def execute_query(query, params=None, fetch_all=True):
    """Execute a database query and return results"""
    with get_db_cursor() as cursor:
        cursor.execute(query, params or ())
        if fetch_all:
            return cursor.fetchall()
        else:
            return cursor.fetchone()

def execute_update(query, params=None):
    """Execute an update/insert/delete query"""
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
