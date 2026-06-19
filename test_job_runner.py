"""
tests/test_job_runner.py

Unit tests for utils/job_runner.py.

The job runner launches real subprocesses, so we test it two ways:
  1. Unit-level: patch subprocess.Popen and verify state transitions,
     history writes, on_complete callbacks, and stop-flag mechanics.
  2. Lightweight integration: run a real trivial subprocess (python -c ...)
     and verify the job reaches 'done' state.

No DB, no Flask, no network.

IMPORTANT — thread/fixture interaction:
`start_job` spawns a daemon thread and returns immediately; it does not wait
for that thread to finish. `_run_subprocess` indexes `_jobs[job_name]`
directly (not `.get()`) inside its `for line in proc.stdout:` loop. If a
test returns without waiting for its job to actually finish, the
`_reset_job_state` autouse fixture below can clear `_jobs` while that
thread is still mid-loop, producing a `KeyError` inside the daemon thread —
surfaced by pytest as a `PytestUnhandledThreadExceptionWarning`, not a
clean test failure, which makes it easy to miss.

Every test that calls `start_job` therefore waits for the job to reach
`running: False` (via `_wait_until_done`) before the test function returns.
This guarantees the thread has exited the stdout loop — the only place
`_jobs[job_name]` is indexed in a way that's unsafe against a concurrent
`.clear()` — before the next test's fixture can run.

This matters even for tests that mock `subprocess.Popen`: the `with
patch(...)` block can exit before the background thread gets around to
calling `Popen`, so the thread may end up calling the *real* Popen instead
of the mock. Waiting for completion *inside* the patch context (not after
it) keeps the mock active for the thread's actual lifetime.
"""
from __future__ import annotations

import sys
import time
import uuid
from unittest.mock import patch, MagicMock

import pytest


from utils import job_runner
from utils.job_runner import (
    start_job, stop_job, get_job, JOB_NAMES,
    read_job_history, append_job_history,
    _jobs, _jobs_lock, _history, _history_lock,
)


@pytest.fixture(autouse=True)
def _reset_job_state():
    """Clear in-process job and history state before and after each test."""
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


@pytest.fixture()
def job_name():
    """
    Unique job name per test.

    start_job/stop_job/get_job don't validate against JOB_NAMES (that
    check only happens at the Flask route layer), so any string works.
    Uniqueness is defense-in-depth on top of _wait_until_done — it means
    two tests can never collide on the same _jobs key even if a wait were
    ever accidentally skipped.
    """
    return f"test_job_{uuid.uuid4().hex[:8]}"


def _wait_until_done(name: str, timeout: float = 5.0) -> dict | None:
    """
    Poll get_job(name) until running is False (or timeout).

    Returns the final job dict, or None if the job never appeared at all.
    Every test that calls start_job MUST call this before returning — see
    the module docstring for why.
    """
    deadline = time.monotonic() + timeout
    job = None
    while time.monotonic() < deadline:
        job = get_job(name)
        if job is not None and not job['running']:
            return job
        time.sleep(0.02)
    return job


# ── JOB_NAMES ─────────────────────────────────────────────────────────────────

class TestJobNames:
    def test_expected_jobs_present(self):
        assert 'scrape_leon'    in JOB_NAMES
        assert 'scrape_broward' in JOB_NAMES
        assert 'get_manga'      in JOB_NAMES

    def test_arbitrary_name_not_in_job_names(self):
        assert 'drop_tables' not in JOB_NAMES


# ── start_job / get_job ───────────────────────────────────────────────────────

class TestStartJob:

    def _fake_popen(self, lines: list[str], returncode: int = 0):
        """Build a mock Popen that yields `lines` from stdout."""
        mock_proc                    = MagicMock()
        mock_proc.stdout             = iter(line + '\n' for line in lines)
        mock_proc.returncode         = returncode
        mock_proc.poll.return_value  = None
        mock_proc.wait.return_value  = returncode
        return mock_proc

    def test_start_job_returns_200_ok(self, job_name):
        with patch('subprocess.Popen', return_value=self._fake_popen(['done'])):
            ok, status, msg = start_job(job_name, ['echo', 'hi'])
            assert ok     is True
            assert status == 200
            # Wait for completion *inside* the patch context — the
            # background thread may not call Popen until after this
            # point, and the mock must still be active when it does.
            _wait_until_done(job_name)

    def test_start_duplicate_job_returns_409(self, job_name):
        with _jobs_lock:
            _jobs[job_name] = {'running': True, 'progress': 0, 'message': ''}
        ok, status, msg = start_job(job_name, ['echo', 'hi'])
        assert ok     is False
        assert status == 409
        assert 'already running' in msg
        # No real thread was spawned for the duplicate request (start_job
        # returns early), and the fake "running" entry has no backing
        # process — clear it manually so teardown doesn't trip on it.
        with _jobs_lock:
            _jobs.pop(job_name, None)

    def test_get_job_unknown_name_returns_none(self):
        assert get_job('no_such_job_at_all') is None

    def test_get_job_before_start_returns_none(self, job_name):
        assert get_job(job_name) is None

    def test_job_completes_and_is_not_running(self, job_name):
        """Run a real trivial subprocess and confirm the job finishes."""
        ok, _, _ = start_job(job_name, [sys.executable, '-c', 'print("done")'])
        assert ok is True
        job = _wait_until_done(job_name)
        assert job is not None
        assert job['running'] is False

    def test_job_failure_records_error_in_history(self, job_name):
        """A subprocess that exits non-zero should produce an 'error' history entry."""
        start_job(job_name, [sys.executable, '-c', 'raise SystemExit(1)'])
        _wait_until_done(job_name)
        history = read_job_history()
        entries = [e for e in history if e['job'] == job_name]
        assert entries
        assert entries[-1]['status'] == 'error'

    def test_progress_parsed_from_bracket_notation(self, job_name):
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
        start_job(job_name, [sys.executable, '-c', script])
        deadline = time.monotonic() + 5
        max_progress = 0
        while time.monotonic() < deadline:
            job = get_job(job_name)
            if job:
                max_progress = max(max_progress, job['progress'])
            if job and not job['running']:
                break
            time.sleep(0.02)
        # 3/3 = 100%
        assert max_progress == 100


# ── stop_job ──────────────────────────────────────────────────────────────────

class TestStopJob:

    def test_stop_not_running_returns_false(self, job_name):
        assert stop_job(job_name) is False

    def test_stop_sets_stop_requested_flag(self, job_name):
        with _jobs_lock:
            _jobs[job_name] = {
                'running': True, 'progress': 0, 'message': '', 'stop_requested': False
            }
        result = stop_job(job_name)
        assert result is True
        with _jobs_lock:
            assert _jobs[job_name]['stop_requested'] is True
            _jobs.pop(job_name, None)   # no backing thread — clean up manually

    def test_stop_actually_terminates_running_process(self, job_name):
        """End-to-end: start a long-running job, stop it, confirm it stops."""
        script = (
            'import time\n'
            'for i in range(200):\n'
            '    print(f"[{i}/200] tick", flush=True)\n'
            '    time.sleep(0.02)\n'
        )
        start_job(job_name, [sys.executable, '-c', script])
        time.sleep(0.1)   # let it actually start producing output
        stopped = stop_job(job_name)
        assert stopped is True
        job = _wait_until_done(job_name)
        assert job is not None
        assert job['running'] is False
        assert job['message'] == 'stopped'


# ── on_complete callback ───────────────────────────────────────────────────────

class TestOnComplete:

    def test_on_complete_called_when_job_finishes(self, job_name):
        callback_results = []

        def on_done(name, ok):
            callback_results.append((name, ok))

        start_job(job_name, [sys.executable, '-c', 'print("ok")'], on_complete=on_done)
        _wait_until_done(job_name)

        assert len(callback_results) == 1
        assert callback_results[0] == (job_name, True)

    def test_on_complete_called_with_false_on_error(self, job_name):
        callback_results = []

        def on_done(name, ok):
            callback_results.append((name, ok))

        start_job(job_name, [sys.executable, '-c', 'raise SystemExit(1)'], on_complete=on_done)
        _wait_until_done(job_name)

        assert callback_results[0][1] is False


# ── Job history ────────────────────────────────────────────────────────────────

class TestJobHistory:

    def test_append_then_read(self):
        sentinel = f'inserted-sentinel-{uuid.uuid4().hex[:8]}'
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
