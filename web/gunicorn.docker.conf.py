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
#   WORKERS: still 1, for the same reason as the bare-metal config — the
#   job-tracking _jobs dict in utils/job_runner.py lives in process memory.
#   See gunicorn.conf.py's header comment; the same migration path (Redis/
#   RQ) applies here before scaling past one worker/container replica.

import os

workers = 1
threads = int(os.getenv("GUNICORN_THREADS", "4"))
worker_class = "gthread"

bind = f"0.0.0.0:{os.getenv('PORT', '8000')}"

wsgi_app = "web.backend:app"

accesslog = "-"
errorlog = "-"
loglevel = os.getenv("GUNICORN_LOG_LEVEL", "info")

# Restart a worker if a single request takes longer than 5 minutes
# (long scrapes run in background threads, not in the request itself)
timeout = 300
graceful_timeout = 30
