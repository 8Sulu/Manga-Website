"""
utils/job_runner.py

Redis-backed job queue, built on RQ (Redis Queue).

WHY THIS EXISTS — MIGRATION FROM THE IN-PROCESS VERSION
─────────────────────────────────────────────────────────
The previous implementation kept a plain `_jobs: dict` in process memory,
guarded by a `threading.Lock`. That works *only* if there is exactly one
Gunicorn worker process, because `/api/job/<name>` has to land on the same
process that started the job to see its progress — see the old comment in
gunicorn.conf.py ("WHY 1 WORKER"). It's a hard ceiling on scaling: you can't
add a second worker (or a second container replica) without job status
silently going stale on whichever worker didn't run the job.

This version moves all job state into Redis:
    job:state:{job_name}  → HASH  {running, progress, message, stop_requested, rq_job_id}
    job:history           → LIST  of JSON-encoded {job, status, message, at}

Redis is shared by every process that can reach it, so:
  - Any Gunicorn worker can answer `/api/job/<name>` correctly.
  - The actual subprocess (scraper / MAL fetch) runs inside a separate
    `rq worker manga-jobs` process — started independently (see
    manga-worker.service / docker-compose's `worker` service) — so a long
    scrape no longer ties up a web request thread or worker slot at all.
  - `stop_job()` works across processes: it just flips a Redis flag that
    the worker process polls.

Public API is intentionally unchanged from the old module so callers
(web/backend.py) didn't need to change shape — only the `on_complete`
callback parameter was dropped (see note on `start_job` below).

Public API
──────────
    start_job(name, cmd)            → (ok, http_status, message)
    stop_job(name)                  → bool
    get_job(name)                   → dict | None
    JOB_NAMES                       — frozenset of recognised job identifiers
    read_job_history()              → list[dict]   (oldest → newest)
    append_job_history(job, s, m)  → None
    execute_job(name, cmd)          — the function the RQ worker actually
                                       runs; not normally called directly,
                                       but it's a plain top-level function
                                       (not a closure) so RQ/tests can
                                       import and invoke it directly.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import threading
import time
from datetime import datetime, timezone

import redis
from rq import Queue

# ── Valid job identifiers ──────────────────────────────────────────────────────

JOB_NAMES: frozenset[str] = frozenset({"scrape_leon", "scrape_broward", "get_manga"})

# How long (seconds) with no stdout output before the watchdog kills the job.
# Import errors / early crashes typically produce nothing then the process dies,
# but a truly silent hang (e.g. waiting on a socket) should also be caught.
STDOUT_TIMEOUT: int = 300

# ── Redis / RQ wiring ──────────────────────────────────────────────────────────

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
QUEUE_NAME = "manga-jobs"

# Ceiling on how long a single enqueued job may run before RQ kills it.
# Generous on purpose — bulk scrapes are slow — the STDOUT_TIMEOUT watchdog
# above is what catches a *silently hung* job long before this fires.
JOB_TIMEOUT = "6h"
RESULT_TTL = 600
FAILURE_TTL = 600

HISTORY_KEY = "job:history"
HISTORY_MAX = 200

# Lazily-created singletons. Tests assign directly to these module globals
# (e.g. `job_runner._redis_conn = fake_redis_instance`) to inject a fake/test
# Redis + a synchronous (`is_async=False`) Queue without touching real infra.
_redis_conn: redis.Redis | None = None
_queue: Queue | None = None


def _get_redis() -> redis.Redis:
    global _redis_conn
    if _redis_conn is None:
        _redis_conn = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    return _redis_conn


def _get_queue() -> Queue:
    global _queue
    if _queue is None:
        _queue = Queue(QUEUE_NAME, connection=_get_redis())
    return _queue


def _state_key(job_name: str) -> str:
    return f"job:state:{job_name}"


# ── Job history (Redis list, capped at HISTORY_MAX) ────────────────────────────


def read_job_history() -> list[dict]:
    """Return job history, oldest first (mirrors the old in-memory contract)."""
    r = _get_redis()
    raw = r.lrange(HISTORY_KEY, 0, -1)
    out: list[dict] = []
    for item in raw:
        try:
            out.append(json.loads(item))
        except (TypeError, ValueError):
            continue
    return out


def append_job_history(job: str, status: str, message: str) -> None:
    entry = {
        "job": job,
        "status": status,
        "message": message,
        "at": datetime.now(timezone.utc).isoformat(),
    }
    r = _get_redis()
    # RPUSH (append) + LTRIM to the last HISTORY_MAX keeps history oldest→newest
    # without a read-modify-write race between concurrent appenders.
    r.rpush(HISTORY_KEY, json.dumps(entry, ensure_ascii=False))
    r.ltrim(HISTORY_KEY, -HISTORY_MAX, -1)


# ── Subprocess execution (runs inside the `rq worker` process) ────────────────

_RESULT_RE = re.compile(
    r"inserted|updated|done|complete|stopped|failed|error|no books|no new|"
    r"All runs|✓|✗|\[\*\]|\[-\]|\[\+\]",
    re.IGNORECASE,
)
_LOG_PREFIX_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")


def execute_job(job_name: str, cmd: list[str]) -> None:
    """
    Entry point invoked by `rq worker manga-jobs` (see manga-worker.service /
    docker-compose's `worker` service). NOT normally called directly from
    request handlers — `start_job()` enqueues this; a separate worker process
    dequeues and runs it.

    Streams the subprocess's stdout into Redis-backed job state (so every
    Gunicorn worker sees live progress), watches a Redis stop flag (so
    `stop_job()` works regardless of which process called it), and runs a
    watchdog thread that kills the process if it goes silent for too long.
    """
    r = _get_redis()
    state_key = _state_key(job_name)

    r.hset(
        state_key,
        mapping={"running": "1", "progress": "0", "message": "starting…", "stop_requested": "0"},
    )

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    # ── Watchdog — kills proc if stdout goes silent for STDOUT_TIMEOUT s ──────
    _last_output_ts: list[float] = [time.monotonic()]  # mutable cell for closure
    _watchdog_msg: list[str] = [""]

    def _watchdog() -> None:
        while True:
            time.sleep(5)
            if proc.poll() is not None:
                return  # process already exited
            silent_for = time.monotonic() - _last_output_ts[0]
            if silent_for > STDOUT_TIMEOUT:
                proc.terminate()
                _watchdog_msg[0] = f"watchdog: no output for {STDOUT_TIMEOUT}s — process killed"
                r.hset(state_key, "message", _watchdog_msg[0])
                return

    watchdog_thread = threading.Thread(target=_watchdog, daemon=True)
    watchdog_thread.start()

    last_msg = history_msg = ""
    progress = 0
    stopped = False

    for line in proc.stdout:
        line = line.rstrip()
        if not line:
            continue

        _last_output_ts[0] = time.monotonic()  # reset watchdog timer
        last_msg = line[:120]

        m = re.search(r"\[(\d+)/(\d+)\]", line)
        if m:
            n, total = int(m.group(1)), int(m.group(2))
            progress = int(n / total * 100) if total else 0

        if _RESULT_RE.search(line) and not _LOG_PREFIX_RE.match(line):
            history_msg = line[:200]

        # Cross-process stop signal — set by stop_job() from any web worker.
        if r.hget(state_key, "stop_requested") == "1":
            proc.terminate()
            stopped = True
            r.hset(state_key, mapping={"progress": progress, "message": last_msg})
            break

        r.hset(state_key, mapping={"progress": progress, "message": last_msg})

    proc.wait()
    ok = proc.returncode == 0

    watchdog_msg = _watchdog_msg[0]

    if stopped:
        final_display = history_msg = "stopped"
    elif not ok and not last_msg and watchdog_msg.startswith("watchdog"):
        final_display = history_msg = watchdog_msg
    else:
        final_display = last_msg if ok else f"error: exited {proc.returncode}"

    if not history_msg:
        history_msg = last_msg or ("done" if ok else f"exited {proc.returncode}")

    r.hset(
        state_key,
        mapping={
            "running": "0",
            "progress": 100 if ok else progress,
            "message": final_display,
        },
    )

    append_job_history(job_name, "done" if ok else "error", history_msg)


# ── Public API ─────────────────────────────────────────────────────────────────


def start_job(job_name: str, cmd: list[str]) -> tuple[bool, int, str]:
    """
    Enqueue `cmd` to run under the name `job_name` and return immediately.

    NOTE on the dropped `on_complete` parameter: the old in-process version
    accepted an `on_complete(job_name, ok)` callback (backend.py used it to
    invalidate an in-memory cache). That doesn't translate to a distributed
    queue — `execute_job` above runs in a *different process* than the
    Gunicorn worker that called `start_job`, so a Python closure captured
    here would either fail to serialize or, if it somehow ran, would mutate
    state in the wrong process. Call sites that need "do X after this job"
    should poll `get_job()` / rely on a short cache TTL instead (see
    `_missing_manga_ids` in backend.py).
    """
    r = _get_redis()
    state_key = _state_key(job_name)

    # Optimistic-lock loop: only one concurrent start_job() call for the same
    # job_name should win the "claim". This is the distributed equivalent of
    # the old `_jobs_lock` — necessary now that multiple Gunicorn workers can
    # call start_job() for the same job_name at (almost) the same time.
    with r.pipeline() as pipe:
        while True:
            try:
                pipe.watch(state_key)
                if pipe.hget(state_key, "running") == "1":
                    pipe.unwatch()
                    return False, 409, f"{job_name} is already running"
                pipe.multi()
                pipe.hset(
                    state_key,
                    mapping={
                        "running": "1",
                        "progress": "0",
                        "message": "queued…",
                        "stop_requested": "0",
                    },
                )
                pipe.execute()
                break
            except redis.WatchError:
                continue  # state changed under us — re-check and retry

    queue = _get_queue()
    job = queue.enqueue(
        execute_job,
        job_name,
        cmd,
        job_timeout=JOB_TIMEOUT,
        result_ttl=RESULT_TTL,
        failure_ttl=FAILURE_TTL,
    )
    r.hset(state_key, "rq_job_id", job.id)
    return True, 200, f"{job_name} started"


def stop_job(job_name: str) -> bool:
    r = _get_redis()
    state_key = _state_key(job_name)
    if r.hget(state_key, "running") != "1":
        return False
    r.hset(state_key, "stop_requested", "1")
    return True


def get_job(job_name: str) -> dict | None:
    r = _get_redis()
    data = r.hgetall(_state_key(job_name))
    if not data:
        return None
    return {
        "running": data.get("running") == "1",
        "progress": int(data.get("progress") or 0),
        "message": data.get("message", ""),
    }
