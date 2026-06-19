# gunicorn.conf.py  (place next to backend.py in web/)
#
# WHY 1 WORKER:
#   The job-tracking dict (_jobs) lives in process memory.  Multiple workers
#   would each have their own copy, so /api/job/<name> would return stale
#   data depending on which worker handled the request.
#   1 worker + threads gives safe concurrency without needing Redis.
#
# THREADS:
#   4 threads handle up to 4 simultaneous requests (search, poll, etc.).
#   Raise to 8 if you have ≥4 CPU cores and notice sluggish search under load.

workers = 1
threads = 4
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

# Restart a worker if a single request takes longer than 5 minutes
# (long scrapes run in background threads, not in the request itself)
timeout = 300

# Graceful shutdown timeout
graceful_timeout = 30
