# gunicorn.conf.py  (place next to backend.py in web/)
#
# Safe to run multiple workers: job state lives in Redis (see
# utils/job_runner.py), and the scrape/MAL-fetch subprocess itself runs
# inside a separate `rq worker manga-jobs` process (manga-worker.service),
# never inside a Gunicorn worker. Any worker here can correctly answer
# /api/job/<name> regardless of which one (if any) called start_job().
#
# Bump WORKERS via env var if you have the CPU cores to spare; a common
# starting point is (2 x cpu_cores) + 1. THREADS still help with ordinary
# request concurrency (search, polling) within each worker process.

import os

workers = int(os.getenv("GUNICORN_WORKERS", "3"))
threads = int(os.getenv("GUNICORN_THREADS", "4"))
worker_class = "gthread"

# Unix socket — nginx talks to gunicorn over this, no port needed
bind = "unix:/run/manga/gunicorn.sock"

# Point gunicorn at the Flask app
chdir = "/opt/manga/web"  # adjust to your project path
wsgi_app = "backend:app"

# Logging — goes to journald via systemd; use `journalctl -u manga` to read
accesslog = "-"
errorlog = "-"
loglevel = "info"

# Per-request timeout. Scrapes no longer run inside a request at all (they're
# enqueued to manga-worker.service and the request returns immediately) — this
# is just a generous safety net for any one HTTP request, not a scrape-duration
# ceiling. See utils/job_runner.py's JOB_TIMEOUT for the actual scrape-duration
# limit.
timeout = 300

# Graceful shutdown timeout
graceful_timeout = 30
