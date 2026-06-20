# web/gunicorn.docker.conf.py
#
# Container counterpart to gunicorn.conf.py (used by the systemd/bare-metal
# deployment). Differences:
#
#   BIND: TCP (0.0.0.0:8000) instead of a unix socket — nginx talks to this
#   container over the docker network, not a shared filesystem socket.
#
#   CHDIR: none. The Dockerfile's WORKDIR (/app) already puts us in the
#   right place, and wsgi_app is given as a full package path
#   ("web.backend:app") so it resolves correctly regardless of cwd.
#
#   WORKERS: as of the Redis/RQ job-queue migration (see
#   utils/job_runner.py's module docstring), job state lives in Redis and
#   scrapes run in a separate `rq worker` container — not inside a Gunicorn
#   worker at all — so it's now safe to run more than one worker per
#   replica. Bump GUNICORN_WORKERS in .env if you have the CPU to spare.

import os

workers = int(os.getenv("GUNICORN_WORKERS", "2"))
threads = int(os.getenv("GUNICORN_THREADS", "4"))
worker_class = "gthread"

bind = f"0.0.0.0:{os.getenv('PORT', '8000')}"

wsgi_app = "web.backend:app"

accesslog = "-"
errorlog = "-"
loglevel = os.getenv("GUNICORN_LOG_LEVEL", "info")

# Per-request timeout — see the note in gunicorn.conf.py; scrapes run
# entirely outside the request cycle now, this is just a generous safety net.
timeout = 300
graceful_timeout = 30
