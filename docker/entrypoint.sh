#!/usr/bin/env sh
# docker/entrypoint.sh
#
# Waits for MySQL to accept connections before handing off to the real
# CMD (gunicorn). Uses mysql-connector-python — already a project
# dependency — instead of pulling in netcat or a wait-for-it binary.
#
# This is a belt-and-suspenders check: docker-compose.yml already uses
# `depends_on: condition: service_healthy`, but this guards anyone who
# runs the app container directly (docker run) without compose.
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

exec "$@"
