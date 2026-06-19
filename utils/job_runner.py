"""
utils/job_runner.py

In-process thread-based job runner with subprocess watchdog (#3).

Design notes
────────────
• One gunicorn worker → _jobs dict is safe in process memory.
  Future path to multi-worker: swap start_job / stop_job / get_job for
  a Redis-backed Celery/RQ task queue without changing callers.

• Each job runs as a subprocess launched inside a daemon thread.
  A watchdog thread monitors the worker thread; if the process has not
  produced any stdout within STDOUT_TIMEOUT seconds (catches silent
  crashes — import errors, segfaults, etc.) it sends SIGTERM and marks
  the job failed.

Public API
──────────
    start_job(name, cmd, on_complete=None) → (ok, http_status, message)
    stop_job(name)   → bool
    get_job(name)    → dict | None
    JOB_NAMES        — frozenset of recognised job identifiers
    read_job_history()              → list[dict]   (in-memory, #16)
    append_job_history(job, s, m)  → None
"""

from __future__ import annotations

import json
import re
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable


# ── Valid job identifiers ──────────────────────────────────────────────────────

JOB_NAMES: frozenset[str] = frozenset({"scrape_leon", "scrape_broward", "get_manga"})

# How long (seconds) with no stdout output before the watchdog kills the job.
# Import errors / early crashes typically produce nothing then the process dies,
# but a truly silent hang (e.g. waiting on a socket) should also be caught.
STDOUT_TIMEOUT: int = 300


# ── In-memory job state ────────────────────────────────────────────────────────

_jobs: dict[str, dict] = {}
_jobs_lock: threading.Lock = threading.Lock()


# ── In-memory history cache (#16) ─────────────────────────────────────────────

_history: list[dict] = []
_history_lock: threading.Lock = threading.Lock()
_history_loaded: bool = False


# ── History file path ──────────────────────────────────────────────────────────

_history_path: Path | None = None


def _get_history_path() -> Path:
    global _history_path
    if _history_path is None:
        from config.settings import DATA_DIR  # clean import, no sys.path hack

        _history_path = DATA_DIR / "job_history.json"
    return _history_path


def _ensure_history_loaded() -> None:
    global _history_loaded
    if _history_loaded:
        return
    try:
        p = _get_history_path()
        if p.exists():
            data = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(data, list):
                with _history_lock:
                    _history.clear()
                    _history.extend(data)
    except Exception:
        pass
    _history_loaded = True


def read_job_history() -> list[dict]:
    _ensure_history_loaded()
    with _history_lock:
        return list(_history)


def append_job_history(job: str, status: str, message: str) -> None:
    _ensure_history_loaded()
    entry = {
        "job": job,
        "status": status,
        "message": message,
        "at": datetime.now(timezone.utc).isoformat(),
    }
    with _history_lock:
        _history.append(entry)
        if len(_history) > 200:
            del _history[:-200]
        snapshot = list(_history)

    try:
        from config.settings import DATA_DIR

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        _get_history_path().write_text(
            json.dumps(snapshot, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception:
        pass


# ── Subprocess runner ──────────────────────────────────────────────────────────

_RESULT_RE = re.compile(
    r"inserted|updated|done|complete|stopped|failed|error|no books|no new|"
    r"All runs|✓|✗|\[\*\]|\[-\]|\[\+\]",
    re.IGNORECASE,
)
_LOG_PREFIX_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")


def _run_subprocess(job_name: str, cmd: list[str]) -> None:
    with _jobs_lock:
        _jobs[job_name].update(running=True, progress=0, message="starting…")
        on_complete: Callable | None = _jobs[job_name].get("on_complete")

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    # ── #3: Watchdog — kills proc if stdout goes silent for STDOUT_TIMEOUT s ──
    _last_output_ts: list[float] = [time.monotonic()]  # mutable cell for closure

    def _watchdog() -> None:
        while True:
            time.sleep(5)
            with _jobs_lock:
                still_running = _jobs.get(job_name, {}).get("running", False)
            if not still_running:
                return
            if proc.poll() is not None:
                return  # process already exited
            silent_for = time.monotonic() - _last_output_ts[0]
            if silent_for > STDOUT_TIMEOUT:
                proc.terminate()
                with _jobs_lock:
                    _jobs[job_name]["message"] = (
                        f"watchdog: no output for {STDOUT_TIMEOUT}s — process killed"
                    )
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

        with _jobs_lock:
            job = _jobs.get(job_name)
            if job and job.get("stop_requested"):
                proc.terminate()
                stopped = True
                break
            _jobs[job_name].update(progress=progress, message=last_msg)

    proc.wait()
    ok = proc.returncode == 0

    # Watchdog may have set a message; preserve it if we have nothing better
    with _jobs_lock:
        watchdog_msg = _jobs[job_name].get("message", "")

    if stopped:
        final_display = history_msg = "stopped"
    elif not ok and not last_msg and watchdog_msg.startswith("watchdog"):
        final_display = history_msg = watchdog_msg
    else:
        final_display = last_msg if ok else f"error: exited {proc.returncode}"

    if not history_msg:
        history_msg = last_msg or ("done" if ok else f"exited {proc.returncode}")

    with _jobs_lock:
        _jobs[job_name].update(
            running=False,
            progress=100 if ok else progress,
            message=final_display,
        )

    append_job_history(job_name, "done" if ok else "error", history_msg)

    if on_complete:
        try:
            on_complete(job_name, ok)
        except Exception:
            pass


# ── Public API ─────────────────────────────────────────────────────────────────


def start_job(
    job_name: str,
    cmd: list[str],
    on_complete: Callable | None = None,
) -> tuple[bool, int, str]:
    with _jobs_lock:
        if _jobs.get(job_name, {}).get("running"):
            return False, 409, f"{job_name} is already running"
        _jobs[job_name] = {
            "running": False,
            "progress": 0,
            "message": "",
            "stop_requested": False,
            "on_complete": on_complete,
        }

    t = threading.Thread(
        target=_run_subprocess,
        args=(job_name, cmd),
        daemon=True,
    )
    with _jobs_lock:
        _jobs[job_name]["thread"] = t
    t.start()
    return True, 200, f"{job_name} started"


def stop_job(job_name: str) -> bool:
    with _jobs_lock:
        job = _jobs.get(job_name)
        if not job or not job.get("running"):
            return False
        job["stop_requested"] = True
    return True


def get_job(job_name: str) -> dict | None:
    with _jobs_lock:
        job = _jobs.get(job_name)
        if not job:
            return None
        return {
            "running": job.get("running", False),
            "progress": job.get("progress", 0),
            "message": job.get("message", ""),
        }
