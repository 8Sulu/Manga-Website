"""
tests/test_job_runner.py

Unit tests for utils/job_runner.py.

The job runner launches real subprocesses, so we test it two ways:
  1. Unit-level: patch subprocess.Popen and verify state transitions,
     history writes, on_complete callbacks, and stop-flag mechanics.
  2. Lightweight integration: run a real trivial subprocess (python -c ...)
     and verify the job reaches 'done' state.

No DB, no Flask, no network.
"""
from __future__ import annotations

import sys
import time
import threading
from io import StringIO
from unittest.mock import patch, MagicMock, call

import pytest


# ── Import isolation ──────────────────────────────────────────────────────────
# job_runner uses module-level dicts. Reset them between tests so state
# from one test doesn't leak into the next.

from utils import job_runner
from utils.job_runner import (
    start_job, stop_job, get_job, JOB_NAMES,
    read_job_history, append_job_history,
    _jobs, _jobs_lock, _history, _history_lock,
)


@pytest.fixture(autouse=True)
def _reset_job_state():
    """Clear in-process job and history state before each test."""
    with _jobs_lock:
        _jobs.clear()
    with _history_lock:
        _history.clear()
    job_runner._history_loaded = False
    job_runner._history_path   = None
    yield
    with _jobs_lock:
        _jobs.clear()
    with _history_lock:
        _history.clear()


# ── JOB_NAMES ─────────────────────────────────────────────────────────────────

class TestJobNames:
    def test_expected_jobs_present(self):
        assert 'scrape_leon'   in JOB_NAMES
        assert 'scrape_broward' in JOB_NAMES
        assert 'get_manga'     in JOB_NAMES

    def test_arbitrary_name_not_in_job_names(self):
        assert 'drop_tables' not in JOB_NAMES


# ── start_job / get_job ───────────────────────────────────────────────────────

class TestStartJob:

    def _fake_popen(self, lines: list[str], returncode: int = 0):
        """Build a mock Popen that yields `lines` from stdout."""
        mock_proc             = MagicMock()
        mock_proc.stdout      = iter(line + '\n' for line in lines)
        mock_proc.returncode  = returncode
        mock_proc.poll.return_value = None
        mock_proc.wait.return_value = returncode
        return mock_proc

    def test_start_job_returns_200_ok(self):
        with patch('subprocess.Popen', return_value=self._fake_popen(['done'])):
            ok, status, msg = start_job('scrape_leon', ['echo', 'hi'])
        assert ok     is True
        assert status == 200

    def test_start_duplicate_job_returns_409(self):
        with _jobs_lock:
            _jobs['scrape_leon'] = {'running': True, 'progress': 0, 'message': ''}
        ok, status, msg = start_job('scrape_leon', ['echo', 'hi'])
        assert ok     is False
        assert status == 409
        assert 'already running' in msg

    def test_get_job_unknown_name_returns_none(self):
        assert get_job('no_such_job') is None

    def test_get_job_before_start_returns_none(self):
        assert get_job('scrape_leon') is None

    def test_job_completes_and_is_not_running(self):
        """Run a real trivial subprocess and confirm the job finishes."""
        ok, _, _ = start_job('scrape_leon', [sys.executable, '-c', 'print("done")'])
        assert ok is True
        # Poll until done, max 5s
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            job = get_job('scrape_leon')
            if job and not job['running']:
                break
            time.sleep(0.05)
        job = get_job('scrape_leon')
        assert job is not None
        assert job['running'] is False

    def test_job_failure_records_error_in_history(self):
        """A subprocess that exits non-zero should produce an 'error' history entry."""
        ok, _, _ = start_job('scrape_leon', [sys.executable, '-c', 'raise SystemExit(1)'])
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            job = get_job('scrape_leon')
            if job and not job['running']:
                break
            time.sleep(0.05)
        history = read_job_history()
        entries = [e for e in history if e['job'] == 'scrape_leon']
        assert entries
        assert entries[-1]['status'] == 'error'

    def test_progress_parsed_from_bracket_notation(self):
        """
        Lines like '[5/10] Processing …' should advance the progress counter.
        We verify by inspecting the job dict mid-run via a tight poll.
        """
        script = (
            'import time\n'
            'for i in range(1, 4):\n'
            '    print(f"[{i}/3] item", flush=True)\n'
            '    time.sleep(0.05)\n'
        )
        start_job('scrape_leon', [sys.executable, '-c', script])
        deadline = time.monotonic() + 5
        max_progress = 0
        while time.monotonic() < deadline:
            job = get_job('scrape_leon')
            if job:
                max_progress = max(max_progress, job['progress'])
            if job and not job['running']:
                break
            time.sleep(0.05)
        # 3/3 = 100%
        assert max_progress == 100


# ── stop_job ──────────────────────────────────────────────────────────────────

class TestStopJob:

    def test_stop_not_running_returns_false(self):
        assert stop_job('scrape_leon') is False

    def test_stop_sets_stop_requested_flag(self):
        with _jobs_lock:
            _jobs['scrape_leon'] = {
                'running': True, 'progress': 0, 'message': '', 'stop_requested': False
            }
        result = stop_job('scrape_leon')
        assert result is True
        with _jobs_lock:
            assert _jobs['scrape_leon']['stop_requested'] is True


# ── on_complete callback ───────────────────────────────────────────────────────

class TestOnComplete:

    def test_on_complete_called_when_job_finishes(self):
        callback_results = []

        def on_done(name, ok):
            callback_results.append((name, ok))

        start_job('scrape_leon', [sys.executable, '-c', 'print("ok")'],
                  on_complete=on_done)

        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if callback_results:
                break
            time.sleep(0.05)

        assert len(callback_results) == 1
        assert callback_results[0] == ('scrape_leon', True)

    def test_on_complete_called_with_false_on_error(self):
        callback_results = []

        def on_done(name, ok):
            callback_results.append((name, ok))

        start_job('scrape_leon', [sys.executable, '-c', 'raise SystemExit(1)'],
                  on_complete=on_done)

        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if callback_results:
                break
            time.sleep(0.05)

        assert callback_results[0][1] is False


# ── Job history ────────────────────────────────────────────────────────────────

class TestJobHistory:

    def test_append_then_read(self):
        sentinel = f'inserted-sentinel-{id(self)}'
        append_job_history('scrape_leon', 'done', sentinel)
        history = read_job_history()
        match = [e for e in history if e['message'] == sentinel]
        assert len(match) == 1
        assert match[0]['job']    == 'scrape_leon'
        assert match[0]['status'] == 'done'
        assert 'at' in match[0]

    def test_history_capped_at_200(self):
        for i in range(210):
            append_job_history('scrape_leon', 'done', f'run {i}')
        assert len(read_job_history()) == 200

    def test_history_persisted_to_file(self, tmp_path):
        history_file = tmp_path / 'job_history.json'
        job_runner._history_path = history_file

        append_job_history('get_manga', 'done', 'added 5 titles')

        import json
        written = json.loads(history_file.read_text())
        assert any(e['job'] == 'get_manga' for e in written)

    def test_history_loaded_from_file_on_first_read(self, tmp_path):
        import json
        history_file = tmp_path / 'job_history.json'
        history_file.write_text(json.dumps([
            {'job': 'reset', 'status': 'done', 'message': 'ok', 'at': '2024-01-01'}
        ]))
        job_runner._history_path   = history_file
        job_runner._history_loaded = False

        history = read_job_history()
        assert any(e['job'] == 'reset' for e in history)
