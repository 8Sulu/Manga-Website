"""
tests/test_job_runner.py

Unit tests for utils/job_runner.py (Redis/RQ-backed job queue).

No real `rq worker` process is started for these tests. Instead, the
autouse fixture below points job_runner at a dedicated Redis DB and swaps in
an `is_async=False` Queue, so `start_job()` runs `execute_job()` inline in
the calling thread/process — the exact same function a real `rq worker`
would invoke, just without a second process. This is the standard RQ
testing pattern and keeps these tests fast and hermetic while still
exercising the real code path.

Requires a real Redis reachable at REDIS_URL (defaults to
redis://localhost:6379, here pinned to db 15 so it never collides with dev
data). No mocking of subprocess.Popen — tests spawn tiny real `python -c`
processes, same as the old suite's "lightweight integration" tests.
"""

from __future__ import annotations

import sys
import threading
import time
import uuid

import pytest
import redis as redis_module
from rq import Queue

from utils import job_runner
from utils.job_runner import (
    JOB_NAMES,
    append_job_history,
    execute_job,
    get_job,
    read_job_history,
    start_job,
    stop_job,
)

TEST_REDIS_URL = "redis://localhost:6379/15"


@pytest.fixture(autouse=True)
def _redis_test_env():
    """
    Point job_runner at a clean, dedicated Redis DB for each test and use a
    synchronous Queue so start_job() executes inline. Flushes before and
    after so tests never see leftover state from a previous run.
    """
    conn = redis_module.Redis.from_url(TEST_REDIS_URL, decode_responses=True)
    conn.flushdb()

    job_runner._redis_conn = conn
    job_runner._queue = Queue(job_runner.QUEUE_NAME, connection=conn, is_async=False)

    yield

    conn.flushdb()
    job_runner._redis_conn = None
    job_runner._queue = None


@pytest.fixture()
def job_name():
    """Unique job name per test so tests can never collide on Redis keys."""
    return f"test_job_{uuid.uuid4().hex[:8]}"


def _wait_until_done(name: str, timeout: float = 5.0) -> dict | None:
    """Poll get_job(name) until running is False (or timeout)."""
    deadline = time.monotonic() + timeout
    job = None
    while time.monotonic() < deadline:
        job = get_job(name)
        if job is not None and not job["running"]:
            return job
        time.sleep(0.02)
    return job


# ── JOB_NAMES ───────────────────────────────────────────────────────────────────


class TestJobNames:
    def test_expected_jobs_present(self):
        assert "scrape_leon" in JOB_NAMES
        assert "scrape_broward" in JOB_NAMES
        assert "get_manga" in JOB_NAMES

    def test_arbitrary_name_not_in_job_names(self):
        assert "drop_tables" not in JOB_NAMES


# ── start_job / get_job ───────────────────────────────────────────────────────


class TestStartJob:
    def test_start_job_returns_200_ok(self, job_name):
        ok, status, msg = start_job(job_name, [sys.executable, "-c", 'print("done")'])
        assert ok is True
        assert status == 200
        # The test Queue is synchronous, so by the time start_job() returns
        # the job has already run to completion.
        job = get_job(job_name)
        assert job is not None
        assert job["running"] is False

    def test_start_duplicate_job_returns_409(self, job_name):
        r = job_runner._get_redis()
        r.hset(job_runner._state_key(job_name), "running", "1")
        ok, status, msg = start_job(job_name, ["echo", "hi"])
        assert ok is False
        assert status == 409
        assert "already running" in msg

    def test_get_job_unknown_name_returns_none(self):
        assert get_job("no_such_job_at_all") is None

    def test_get_job_before_start_returns_none(self, job_name):
        assert get_job(job_name) is None

    def test_job_completes_and_is_not_running(self, job_name):
        """Run a real trivial subprocess and confirm the job finishes."""
        ok, _, _ = start_job(job_name, [sys.executable, "-c", 'print("done")'])
        assert ok is True
        job = _wait_until_done(job_name)
        assert job is not None
        assert job["running"] is False

    def test_job_failure_records_error_in_history(self, job_name):
        """A subprocess that exits non-zero should produce an 'error' history entry."""
        start_job(job_name, [sys.executable, "-c", "raise SystemExit(1)"])
        history = read_job_history()
        entries = [e for e in history if e["job"] == job_name]
        assert entries
        assert entries[-1]["status"] == "error"

    def test_rq_job_id_stored_in_state(self, job_name):
        start_job(job_name, [sys.executable, "-c", 'print("done")'])
        r = job_runner._get_redis()
        assert r.hget(job_runner._state_key(job_name), "rq_job_id")

    def test_concurrent_start_calls_only_one_succeeds(self, job_name):
        """
        Two Gunicorn workers hitting /admin for the same job_name at nearly
        the same time should not both win — this is exactly the race the old
        in-process `_jobs_lock` prevented, now enforced via Redis WATCH/MULTI
        instead of a Python lock (since multiple *processes*, not just
        threads, can call start_job concurrently in production).
        """
        script = "import time; time.sleep(0.3)"
        results: list[tuple[bool, int, str]] = []
        lock = threading.Lock()

        def runner():
            res = start_job(job_name, [sys.executable, "-c", script])
            with lock:
                results.append(res)

        threads = [threading.Thread(target=runner) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        oks = [r for r in results if r[0] is True]
        conflicts = [r for r in results if r[0] is False and r[1] == 409]
        assert len(oks) == 1
        assert len(conflicts) == 2


# ── execute_job — the function the RQ worker actually runs ───────────────────


class TestExecuteJob:
    def test_progress_parsed_from_bracket_notation(self, job_name):
        """
        Lines like '[5/10] Processing …' should advance the progress counter.
        execute_job is run directly in a background thread (mirroring how a
        real `rq worker` would invoke it) so the test can poll progress
        while the subprocess is still mid-run.
        """
        script = (
            "import time\n"
            "for i in range(1, 4):\n"
            '    print(f"[{i}/3] item", flush=True)\n'
            "    time.sleep(0.05)\n"
        )
        t = threading.Thread(target=execute_job, args=(job_name, [sys.executable, "-c", script]))
        t.start()

        deadline = time.monotonic() + 5
        max_progress = 0
        while time.monotonic() < deadline:
            job = get_job(job_name)
            if job:
                max_progress = max(max_progress, job["progress"])
                if not job["running"] and not t.is_alive():
                    break
            time.sleep(0.02)

        t.join(timeout=5)
        assert max_progress == 100  # 3/3 = 100%

    def test_stop_actually_terminates_running_process(self, job_name):
        """End-to-end: run a long job, stop it, confirm it stops."""
        script = (
            "import time\n"
            "for i in range(200):\n"
            '    print(f"[{i}/200] tick", flush=True)\n'
            "    time.sleep(0.02)\n"
        )
        t = threading.Thread(target=execute_job, args=(job_name, [sys.executable, "-c", script]))
        t.start()
        time.sleep(0.2)  # let it actually start producing output

        stopped = stop_job(job_name)
        assert stopped is True

        t.join(timeout=5)
        job = get_job(job_name)
        assert job is not None
        assert job["running"] is False
        assert job["message"] == "stopped"


# ── stop_job ──────────────────────────────────────────────────────────────────


class TestStopJob:
    def test_stop_not_running_returns_false(self, job_name):
        assert stop_job(job_name) is False

    def test_stop_sets_stop_requested_flag(self, job_name):
        r = job_runner._get_redis()
        state_key = job_runner._state_key(job_name)
        r.hset(state_key, mapping={"running": "1", "progress": "0", "message": ""})

        result = stop_job(job_name)

        assert result is True
        assert r.hget(state_key, "stop_requested") == "1"


# ── Job history ────────────────────────────────────────────────────────────────


class TestJobHistory:
    def test_append_then_read(self):
        sentinel = f"inserted-sentinel-{uuid.uuid4().hex[:8]}"
        append_job_history("scrape_leon", "done", sentinel)
        history = read_job_history()
        match = [e for e in history if e["message"] == sentinel]
        assert len(match) == 1
        assert match[0]["job"] == "scrape_leon"
        assert match[0]["status"] == "done"
        assert "at" in match[0]

    def test_history_capped_at_200(self):
        for i in range(210):
            append_job_history("scrape_leon", "done", f"run {i}")
        assert len(read_job_history()) == 200

    def test_history_ordered_oldest_first(self):
        append_job_history("scrape_leon", "done", "first-entry")
        append_job_history("scrape_leon", "done", "second-entry")
        history = [e["message"] for e in read_job_history()]
        assert history.index("first-entry") < history.index("second-entry")

    def test_history_capped_keeps_most_recent(self):
        for i in range(210):
            append_job_history("scrape_leon", "done", f"run {i}")
        history = read_job_history()
        # Oldest 10 runs (0-9) should have been trimmed away; the most
        # recent entry (run 209) must be the last element.
        assert history[-1]["message"] == "run 209"
        assert all(e["message"] != "run 0" for e in history)
