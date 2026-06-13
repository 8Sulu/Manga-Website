"""
utils/job_runner.py

In-process thread-based job runner and persistent job history log.

Each job is a subprocess launched in a daemon thread.  Progress is tracked
by parsing [n/total] markers from stdout.  State lives in the module-level
_jobs dict; because gunicorn runs with a single worker this is safe —
all threads share the same process memory.

Public API
──────────
    start_job(name, cmd)  → (ok: bool, http_status: int, message: str)
    stop_job(name)        → bool
    get_job(name)         → dict | None
    JOB_NAMES             — frozenset of recognised job identifiers
    read_job_history()    → list[dict]          (from in-memory cache)
    append_job_history(job, status, message) → None
"""
from __future__ import annotations

import json
import re
import subprocess
import threading
from datetime import datetime, timezone
from pathlib import Path


# ── Valid job identifiers ──────────────────────────────────────────────────────

JOB_NAMES: frozenset[str] = frozenset({'scrape', 'scrape_broward', 'get_manga'})


# ── In-memory job state ────────────────────────────────────────────────────────

_jobs:      dict[str, dict] = {}
_jobs_lock: threading.Lock  = threading.Lock()


# ── In-memory history cache (#16) ─────────────────────────────────────────────
# Source of truth is the in-memory list; disk is only written on append.
# This eliminates the constant file reads that happened on every 1-second poll.

_history:      list[dict]      = []          # newest last
_history_lock: threading.Lock  = threading.Lock()
_history_loaded:               bool = False  # loaded from disk on first access


# ── History file path ──────────────────────────────────────────────────────────

_history_path: Path | None = None


def _get_history_path() -> Path:
    global _history_path
    if _history_path is None:
        from config.settings import DATA_DIR
        _history_path = DATA_DIR / 'job_history.json'
    return _history_path


def _ensure_history_loaded() -> None:
    """Load history from disk once, on first access."""
    global _history_loaded
    if _history_loaded:
        return
    try:
        p = _get_history_path()
        if p.exists():
            data = json.loads(p.read_text(encoding='utf-8'))
            if isinstance(data, list):
                with _history_lock:
                    _history.clear()
                    _history.extend(data)
    except Exception:
        pass
    _history_loaded = True


# ── Job history ────────────────────────────────────────────────────────────────

def read_job_history() -> list[dict]:
    """Return the job history list (newest last).  Reads from in-memory cache."""
    _ensure_history_loaded()
    with _history_lock:
        return list(_history)


def append_job_history(job: str, status: str, message: str) -> None:
    """Append one entry to the in-memory history and flush to disk (capped at 200)."""
    _ensure_history_loaded()
    entry = {
        'job':     job,
        'status':  status,
        'message': message,
        'at':      datetime.now(timezone.utc).isoformat(),
    }
    with _history_lock:
        _history.append(entry)
        # Keep only the last 200 entries
        if len(_history) > 200:
            del _history[:-200]
        snapshot = list(_history)

    # Write to disk outside the lock so we don't block readers
    try:
        from config.settings import DATA_DIR
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        _get_history_path().write_text(
            json.dumps(snapshot, indent=2, ensure_ascii=False),
            encoding='utf-8',
        )
    except Exception:
        pass


# ── Subprocess runner (runs in a daemon thread) ────────────────────────────────

_RESULT_RE = re.compile(
    r'inserted|updated|done|complete|stopped|failed|error|no books|no new|'
    r'All runs|✓|✗|\[\*\]|\[-\]|\[\+\]',
    re.IGNORECASE,
)
_LOG_PREFIX_RE = re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')


def _run_subprocess(job_name: str, cmd: list[str]) -> None:
    with _jobs_lock:
        _jobs[job_name].update(running=True, progress=0, message='starting…')
        on_complete = _jobs[job_name].get('on_complete')

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    last_msg = history_msg = ''
    progress = 0
    stopped  = False

    for line in proc.stdout:
        line = line.rstrip()
        if not line:
            continue

        last_msg = line[:120]

        m = re.search(r'\[(\d+)/(\d+)\]', line)
        if m:
            n, total = int(m.group(1)), int(m.group(2))
            progress = int(n / total * 100) if total else 0

        if _RESULT_RE.search(line) and not _LOG_PREFIX_RE.match(line):
            history_msg = line[:200]

        with _jobs_lock:
            if _jobs[job_name].get('stop_requested'):
                proc.terminate()
                stopped = True
                break
            _jobs[job_name].update(progress=progress, message=last_msg)

    proc.wait()
    ok = proc.returncode == 0

    if stopped:
        final_display = history_msg = 'stopped'
    else:
        final_display = last_msg if ok else f'error: exited {proc.returncode}'

    if not history_msg:
        history_msg = last_msg or ('done' if ok else f'exited {proc.returncode}')

    with _jobs_lock:
        _jobs[job_name].update(
            running=False,
            progress=100 if ok else progress,
            message=final_display,
        )

    append_job_history(job_name, 'done' if ok else 'error', history_msg)

    # Fire optional on_complete callback (e.g. cache invalidation in backend)
    if on_complete:
        try:
            on_complete(job_name, ok)
        except Exception:
            pass


# ── Public API ─────────────────────────────────────────────────────────────────

def start_job(
    job_name: str,
    cmd: list[str],
    on_complete=None,          # optional callable(job_name: str, ok: bool)
) -> tuple[bool, int, str]:
    with _jobs_lock:
        if _jobs.get(job_name, {}).get('running'):
            return False, 409, f'{job_name} is already running'
        _jobs[job_name] = {
            'running':        False,
            'progress':       0,
            'message':        '',
            'stop_requested': False,
            'on_complete':    on_complete,
        }

    t = threading.Thread(
        target=_run_subprocess,
        args=(job_name, cmd),
        daemon=True,
    )
    with _jobs_lock:
        _jobs[job_name]['thread'] = t
    t.start()
    return True, 200, f'{job_name} started'


def stop_job(job_name: str) -> bool:
    with _jobs_lock:
        job = _jobs.get(job_name)
        if not job or not job.get('running'):
            return False
        job['stop_requested'] = True
    return True


def get_job(job_name: str) -> dict | None:
    with _jobs_lock:
        job = _jobs.get(job_name)
        if not job:
            return None
        return {
            'running':  job.get('running',  False),
            'progress': job.get('progress', 0),
            'message':  job.get('message',  ''),
        }
