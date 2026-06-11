"""
utils/job_runner.py

In-process thread-based job runner and persistent job history log.

Each job is a subprocess (leon_scraper.py, broward_scraper.py, get_manga.py)
launched in a daemon thread.  Progress is tracked by parsing [n/total]
markers from stdout.  State lives in the module-level _jobs dict; because
gunicorn runs with a single worker (see gunicorn.conf.py) this is safe —
all threads share the same process memory.

Public API
──────────
    start_job(name, cmd)  → (ok: bool, http_status: int, message: str)
    stop_job(name)        → bool
    get_job(name)         → dict | None
    JOB_NAMES             — frozenset of recognised job identifiers
    read_job_history()    → list[dict]
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


# ── History file path (resolved lazily to avoid import-time side effects) ─────

_history_path: Path | None = None


def _get_history_path() -> Path:
    global _history_path
    if _history_path is None:
        from config.settings import DATA_DIR
        _history_path = DATA_DIR / 'job_history.json'
    return _history_path


# ── Job history ────────────────────────────────────────────────────────────────

def read_job_history() -> list[dict]:
    """Return the persisted job history list (newest last), or [] on error."""
    try:
        p = _get_history_path()
        if p.exists():
            return json.loads(p.read_text(encoding='utf-8'))
    except Exception:
        pass
    return []


def append_job_history(job: str, status: str, message: str) -> None:
    """Append one entry to the job history JSON file (capped at 200 entries)."""
    try:
        from config.settings import DATA_DIR
        DATA_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    history = read_job_history()
    history.append({
        'job':     job,
        'status':  status,
        'message': message,
        'at':      datetime.now(timezone.utc).isoformat(),
    })
    try:
        _get_history_path().write_text(
            json.dumps(history[-200:], indent=2, ensure_ascii=False),
            encoding='utf-8',
        )
    except Exception:
        pass


# ── Subprocess runner (runs in a daemon thread) ────────────────────────────────

# Matches lines that contain a meaningful final result worth storing in history
_RESULT_RE = re.compile(
    r'inserted|updated|done|complete|stopped|failed|error|no books|no new|'
    r'All runs|✓|✗|\[\*\]|\[-\]|\[\+\]',
    re.IGNORECASE,
)
# Lines that start with a log timestamp are informational, not result lines
_LOG_PREFIX_RE = re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')


def _run_subprocess(job_name: str, cmd: list[str]) -> None:
    """Read stdout from *cmd*, update job state, then persist the result."""
    with _jobs_lock:
        _jobs[job_name].update(running=True, progress=0, message='starting…')

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

        # Advance progress bar from [n/total] markers
        m = re.search(r'\[(\d+)/(\d+)\]', line)
        if m:
            n, total = int(m.group(1)), int(m.group(2))
            progress = int(n / total * 100) if total else 0

        # Keep the most informative line for job history
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


# ── Public API ─────────────────────────────────────────────────────────────────

def start_job(job_name: str, cmd: list[str]) -> tuple[bool, int, str]:
    """
    Launch *cmd* in a background daemon thread under *job_name*.

    Returns (ok, http_status, message).  Returns (False, 409, …) if the
    job is already running.
    """
    with _jobs_lock:
        if _jobs.get(job_name, {}).get('running'):
            return False, 409, f'{job_name} is already running'
        _jobs[job_name] = {
            'running':        False,
            'progress':       0,
            'message':        '',
            'stop_requested': False,
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
    """
    Signal *job_name* to stop by setting stop_requested.
    Returns True if the job was running, False otherwise.
    """
    with _jobs_lock:
        job = _jobs.get(job_name)
        if not job or not job.get('running'):
            return False
        job['stop_requested'] = True
    return True


def get_job(job_name: str) -> dict | None:
    """
    Return a snapshot of the current job state, or None if the job has
    never been started.  The returned dict is a copy — safe to read outside
    the lock.
    """
    with _jobs_lock:
        job = _jobs.get(job_name)
        if not job:
            return None
        return {
            'running':  job.get('running',  False),
            'progress': job.get('progress', 0),
            'message':  job.get('message',  ''),
        }
