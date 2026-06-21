"""
utils/job_logging.py

Structured logging for scripts that run as job_runner subprocesses
(leon_scraper.py, broward_scraper.py — see scripts/).

WHY TWO LOGGERS PER SCRIPT, NOT ONE
────────────────────────────────────
job_runner deliberately treats any line starting with a
"YYYY-MM-DD HH:MM:SS" timestamp as log noise — NOT a candidate for the job
history message (see utils/job_runner.py's _LOG_PREFIX_RE) — so verbose
diagnostic logging doesn't clobber the one-line human-readable summary an
admin sees in the job history table. That means the two kinds of output
need two different formats:

  get_logger(name)            -> verbose, timestamped, level-tagged.
                                  Excluded from job history by design.
                                  Use for diagnostics: retries, per-branch
                                  detail, warnings — anything a developer
                                  debugging a failed scrape would want but
                                  an admin glancing at the dashboard
                                  wouldn't.

  get_progress_logger(name)   -> bare message, no timestamp/level prefix.
                                  Direct replacement for the old
                                  print(..., flush=True) calls:
                                  "[12/50] Berserk", DB commit summaries,
                                  final completion lines. These ARE meant
                                  to be picked up as the job history
                                  message and shown as live progress text.

Both write to sys.stdout. WHY THIS STILL SATISFIES THE OLD
"flush=True is critical" LEARNING:
logging.StreamHandler.emit() calls self.flush() after every record —
unlike a bare print() to a non-TTY stream, which Python block-buffers by
default unless explicitly flushed. That buffering was the actual cause of
the watchdog-false-positive risk this codebase documented: an unflushed
print() can sit in a buffer indefinitely on a piped stdout, making an
actively-running job *look* hung to job_runner's STDOUT_TIMEOUT watchdog.
Switching to logging.StreamHandler removes the need to thread flush=True
through every call site by hand.
"""

from __future__ import annotations

import logging
import sys

_TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S"


class _BareFormatter(logging.Formatter):
    """No timestamp, no level tag — just the message, exactly like print()."""

    def format(self, record: logging.LogRecord) -> str:
        return record.getMessage()


def get_logger(name: str) -> logging.Logger:
    """Verbose diagnostic logger — timestamped, excluded from job history."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(fmt="%(asctime)s %(levelname)s %(message)s", datefmt=_TIMESTAMP_FMT)
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def get_progress_logger(name: str) -> logging.Logger:
    """
    Operator-facing progress logger — replaces the old print(flush=True)
    calls. No timestamp prefix, so these lines still flow through
    job_runner's progress-bracket regex and job-history capture exactly as
    they did when they were raw print() output.
    """
    logger = logging.getLogger(f"{name}.progress")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_BareFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger
