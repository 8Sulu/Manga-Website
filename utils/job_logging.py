"""
utils/job_logging.py

Structured logging for scraper subprocesses run via job_runner.

Provides two loggers per script:
  get_logger(name)          -> timestamped, level-tagged diagnostic output.
                                job_runner's history capture skips any line
                                starting with a timestamp, so this is for
                                developer-facing detail only.
  get_progress_logger(name) -> bare message, no prefix. Lines like
                                "[12/50] Berserk" are picked up by job_runner
                                as live progress and the job-history summary.

Both use logging.StreamHandler instead of print(), which auto-flushes after
every record. An unflushed print() on a piped stdout can sit in a buffer
indefinitely, making an actively-running job look hung to job_runner's
STDOUT_TIMEOUT watchdog.
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
    """Operator-facing progress logger — feeds job_runner's progress parser."""
    logger = logging.getLogger(f"{name}.progress")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_BareFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger
