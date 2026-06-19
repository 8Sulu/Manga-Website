"""
tests/conftest.py

Fixtures shared across all test modules.

Key design decisions:
  - No real DB or network calls anywhere. Every function that touches MySQL or
    an external URL is patched at the module level before the module under
    test is imported, or patched per-test via monkeypatch / unittest.mock.

  - `app` fixture creates the Flask test client with SESSION_COOKIE_SECURE=False
    (required for plain-HTTP test requests) and a temporary session directory
    so tests don't pollute the repo's data/ folder.

  - `admin_client` gives an already-authenticated client so individual admin-
    route tests don't have to repeat the login dance.

  - `csrf_client` is a helper that reads the CSRF token from the session and
    injects it into every mutating request automatically.
"""
from __future__ import annotations

import os
import sys
import json
import secrets
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

# ── Make the project root importable when running `pytest` from any cwd ──────
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ── Stub out mysql.connector *before* any app module imports it ──────────────
# This prevents "No module named mysql" errors on CI machines without MySQL.

class _FakeCursor:
    def __init__(self, rows=None, dictionary=False):
        self._rows = rows or []
        self.lastrowid = 1
        self.rowcount  = len(self._rows)
    def execute(self, q, p=None): pass
    def fetchall(self): return list(self._rows)
    def fetchone(self): return self._rows[0] if self._rows else None
    def close(self): pass
    def __enter__(self): return self
    def __exit__(self, *a): pass

class _FakeConn:
    def cursor(self, dictionary=False): return _FakeCursor()
    def commit(self): pass
    def rollback(self): pass
    def close(self): pass
    def is_connected(self): return True
    def ping(self, **kw): pass
    def __enter__(self): return self
    def __exit__(self, *a): pass

_fake_connector = MagicMock()
_fake_connector.connect.return_value = _FakeConn()

# Patch before any project import touches mysql
sys.modules.setdefault('mysql', MagicMock())
sys.modules.setdefault('mysql.connector', _fake_connector)

# Stub dotenv so mal_client doesn't crash if python-dotenv isn't installed
_dotenv_stub = MagicMock()
_dotenv_stub.load_dotenv = lambda *a, **kw: None
sys.modules.setdefault('dotenv', _dotenv_stub)


# ── Flask app fixture ─────────────────────────────────────────────────────────

@pytest.fixture(scope='session')
def _tmp_data(tmp_path_factory):
    """A single temporary data dir reused across the whole session."""
    return tmp_path_factory.mktemp('data')


@pytest.fixture()
def app(_tmp_data):
    """
    A fresh Flask test app per test function.

    Patches:
      - SESSION_COOKIE_SECURE = False  (browsers reject Secure on plain HTTP)
      - SESSION_FILE_DIR → temp dir   (no real data/ writes)
      - execute_query / execute_update → no-op stubs
      - get_library_ids → (1, 2)
    """
    # Patch DB helpers before importing backend so the module-level
    # Session() init doesn't try to connect.
    with patch('utils.database_utils.mysql.connector.connect', return_value=_FakeConn()), \
         patch('utils.database_utils.execute_query', return_value=[]), \
         patch('utils.database_utils.execute_update', return_value=0), \
         patch('utils.database_utils.get_library_ids', return_value=(1, 2)):

        from web.backend import app as flask_app

        flask_app.config.update(
            TESTING                = True,
            SESSION_COOKIE_SECURE  = False,
            SESSION_FILE_DIR       = str(_tmp_data / 'sessions'),
            SECRET_KEY             = 'test-secret',
            WTF_CSRF_ENABLED       = False,
        )
        Path(flask_app.config['SESSION_FILE_DIR']).mkdir(parents=True, exist_ok=True)

        yield flask_app


@pytest.fixture()
def client(app):
    return app.test_client()


@pytest.fixture()
def admin_client(app):
    """Test client with an active admin session."""
    with app.test_client() as c:
        with app.test_request_context():
            pass
        # Inject admin flag directly into the session
        with c.session_transaction() as sess:
            sess['admin'] = True
            sess['csrf_token'] = 'test-csrf-token'
        yield c


# ── CSRF helper ───────────────────────────────────────────────────────────────

def post_json(client, url, data: dict, csrf: str = 'test-csrf-token'):
    """POST JSON with the CSRF token in the header."""
    return client.post(
        url,
        json={**data, 'csrf_token': csrf},
        headers={'X-CSRF-Token': csrf, 'Content-Type': 'application/json'},
    )


# ── DB row factories ──────────────────────────────────────────────────────────

def make_row(
    manga_id: int = 1,
    title: str    = 'Berserk',
    volume: int   = 1,
    volumes: int  = 40,
    type_: str    = 'Manga',
    score: float  = 9.4,
    author: str   = 'Miura',
    cover: str    = '',
    branch_id: int   = 1,
    branch_name: str = 'Main Library',
    status: str      = 'Available',
    library_id: int  = 1,
    library_name: str = 'Leon County Public Library',
    scraped_at        = None,
) -> dict:
    """Return a single DB row dict as returned by the /search SQL query."""
    return {
        'MangaID':     manga_id,
        'Title':       title,
        'Volume':      volume,
        'Volumes':     volumes,
        'Type':        type_,
        'Members':     10000,
        'Score':       score,
        'Author':      author,
        'CoverMedium': cover,
        'BranchName':  branch_name,
        'BranchID':    branch_id,
        'Status':      status,
        'LibraryID':   library_id,
        'LibraryName': library_name,
        'ScrapedAt':   scraped_at,
    }
