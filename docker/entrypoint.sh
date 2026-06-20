#!/usr/bin/env sh
# docker/entrypoint.sh
#
# Shared ENTRYPOINT for both the `app` (gunicorn) and `worker` (rq worker)
# containers — see docker-compose.yml. Waits for MySQL and Redis to accept
# connections before handing off to the real CMD, using mysql-connector-
# python and redis-py — both already project dependencies — instead of
# pulling in netcat or a wait-for-it binary.
#
# This is belt-and-suspenders: docker-compose.yml already uses
# `depends_on: condition: service_healthy` for both db and redis, but this
# guards anyone who runs a container directly (docker run) without compose.
set -e

python <<'PYEOF'
import os
import sys
import time

import mysql.connector

host     = os.getenv("DB_HOST", "db")
port     = int(os.getenv("DB_PORT", "3306"))
user     = os.getenv("DB_USER", "root")
password = os.getenv("DB_PASSWORD", "")
deadline = time.time() + 60

print(f"[entrypoint] waiting for MySQL at {host}:{port} as '{user}'...", flush=True)

while True:
    try:
        conn = mysql.connector.connect(
            host=host, port=port, user=user, password=password, connection_timeout=3,
        )
        conn.close()
        print("[entrypoint] MySQL is reachable", flush=True)
        break
    except Exception as e:
        if time.time() > deadline:
            print(f"[entrypoint] MySQL never became reachable: {e}", file=sys.stderr)
            sys.exit(1)
        time.sleep(2)
PYEOF

python <<'PYEOF'
import os
import sys
import time

import redis

redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
deadline  = time.time() + 60

print(f"[entrypoint] waiting for Redis at {redis_url}...", flush=True)

while True:
    try:
        client = redis.Redis.from_url(redis_url, socket_connect_timeout=3)
        client.ping()
        client.close()
        print("[entrypoint] Redis is reachable", flush=True)
        break
    except Exception as e:
        if time.time() > deadline:
            print(f"[entrypoint] Redis never became reachable: {e}", file=sys.stderr)
            sys.exit(1)
        time.sleep(2)
PYEOF

exec "$@"
