"""
services/mal_client.py

Thin wrapper around the MyAnimeList OAuth2 API: signs requests with the
current access token, and transparently refreshes it on a 401/invalid_token
response.

WHY REFRESHED TOKENS ARE PERSISTED TO data/mal_tokens.json, NOT WRITTEN
BACK TO .env:
MAL rotates the refresh token on every successful refresh — each call to
refresh_tokens() below returns a brand-new access_token AND a brand-new
refresh_token, and the old refresh_token stops being honoured shortly
after (see "Refreshing an access token" in MAL's OAuth docs). The
original implementation wrote the refreshed pair back into a relative
"./.env" path, which never actually persists anywhere durable:

  - Docker: .env is deliberately excluded from the image (.dockerignore)
    and is never bind-mounted into the container — docker-compose's
    `env_file: .env` only reads it on the HOST to populate environment
    variables at container start. There is no .env file inside the
    running `app`/`worker` containers for a relative path to resolve to,
    so the "write" silently created a throwaway file wherever the
    process's cwd happened to be, gone the instant the container was
    recreated.
  - Bare metal (systemd): manga.service sets
    `WorkingDirectory=/opt/manga/web` (gunicorn.conf.py's `chdir`), but
    the real .env this app actually loads from lives one directory up,
    at /opt/manga/.env (config.settings.ENV_FILE). A relative ".env"
    path here resolved to /opt/manga/web/.env — a file nothing else
    ever reads.

Either way, a successful refresh only ever updated *that one process's*
os.environ. The next deploy / restart / container recreate reloaded the
*original*, by-then-rotated-out refresh token from the real .env, and
the next refresh attempt was rejected by MAL — "token expired and
refresh failed", even though the refresh logic itself had worked fine
moments before the restart.

This is the same class of bug as the Flask-Session / job-tracking races
fixed elsewhere in this app: os.environ updates are per-process, so a
refresh performed by one Gunicorn worker (or the separate `rq worker`
container, which also calls into this module via get_manga.py) was
invisible to the others, which would go on to retry with the
already-rotated-out token from their own stale environment.

data/ is already this app's persistence boundary for exactly this kind
of shared, mutable runtime state (manga.csv, flask_sessions/,
job_history) and is bind-mounted for both the `app` and `worker`
containers (docker-compose's `./data:/app/data`) as well as being the
bare-metal DATA_DIR — so the refreshed pair is written there instead, as
a small JSON file, and read back in preference to the .env-sourced env
vars on every call. .env's MAL_ACCESS_TOKEN/MAL_REFRESH_TOKEN are now
only the *bootstrap* values, used until the first refresh happens.
"""

import json
import os

import requests
from dotenv import load_dotenv

from config.settings import DATA_DIR, REQUEST_TIMEOUT

# Load original environment variables
load_dotenv()

TOKENS_FILE = DATA_DIR / "mal_tokens.json"


def _load_persisted_tokens() -> dict:
    """Read the most recently refreshed token pair, if a refresh has ever saved one."""
    if TOKENS_FILE.exists():
        try:
            return json.loads(TOKENS_FILE.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            pass  # corrupt/unreadable — fall back to .env-sourced env vars below
    return {}


def _save_persisted_tokens(access_token: str, refresh_token: str) -> None:
    """
    Persist the refreshed pair to DATA_DIR (survives container recreation
    and restarts — see module docstring) and mirror them into this
    process's os.environ so callers in the *current* process pick them up
    immediately without a re-read.

    Written via a temp-file + rename so a concurrent reader in another
    Gunicorn worker (or the rq worker container) never sees a half-written
    file — Path.replace() is atomic on POSIX as long as both paths are on
    the same filesystem, which they are here (same directory).
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = TOKENS_FILE.parent / f"{TOKENS_FILE.name}.tmp"
    tmp_path.write_text(
        json.dumps({"access_token": access_token, "refresh_token": refresh_token}),
        encoding="utf-8",
    )
    tmp_path.replace(TOKENS_FILE)
    os.environ["MAL_ACCESS_TOKEN"] = access_token
    os.environ["MAL_REFRESH_TOKEN"] = refresh_token


def persist_tokens(access_token: str, refresh_token: str) -> None:
    """
    Public entry point for scripts/mal_authorize.py to call after a brand
    new authorization-code exchange (as opposed to refresh_tokens() below,
    which calls _save_persisted_tokens() directly after exchanging an
    already-issued refresh_token). Thin wrapper so the one-time auth
    script doesn't need to reach into this module's private helper.
    """
    _save_persisted_tokens(access_token, refresh_token)


def current_access_token() -> str:
    """The freshest known access token — a persisted refresh wins over .env/env."""
    return _load_persisted_tokens().get("access_token") or os.getenv("MAL_ACCESS_TOKEN", "")


def current_refresh_token() -> str:
    """The freshest known refresh token — a persisted refresh wins over .env/env."""
    return _load_persisted_tokens().get("refresh_token") or os.getenv("MAL_REFRESH_TOKEN", "")


def refresh_tokens():
    """Hits the MAL OAuth2 endpoint to exchange the refresh token for new access/refresh tokens."""
    print("[*] Access token expired. Attempting to refresh tokens...")
    url = "https://myanimelist.net/v1/oauth2/token"

    data = {
        "client_id": os.getenv("MAL_CLIENT_ID"),
        "client_secret": os.getenv("MAL_CLIENT_SECRET"),
        "grant_type": "refresh_token",
        "refresh_token": current_refresh_token(),
    }

    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    try:
        response = requests.post(url, data=data, headers=headers, timeout=REQUEST_TIMEOUT)
    except requests.exceptions.RequestException as e:
        print(f"[-] Critical: Token refresh request failed (network error): {e}")
        return None

    if response.status_code == 200:
        tokens = response.json()
        new_access = tokens["access_token"]
        new_refresh = tokens["refresh_token"]

        _save_persisted_tokens(new_access, new_refresh)
        print(f"[+] Tokens successfully refreshed and persisted to {TOKENS_FILE}")
        return new_access
    else:
        print(f"[-] Critical: Failed to refresh token. Server returned {response.status_code}")
        print(response.text)
        return None


def authenticated_request(url, method="GET", **kwargs):
    """Wrapper that signs requests with Bearer tokens and handles automatic retries on 401s."""
    access_token = current_access_token()

    headers = kwargs.get("headers", {})
    headers["Authorization"] = f"Bearer {access_token}"
    kwargs["headers"] = headers

    response = requests.request(method, url, **kwargs)

    if response.status_code == 401:
        try:
            error_data = response.json()
            if error_data.get("error") == "invalid_token":
                new_token = refresh_tokens()
                if new_token:
                    # Retry once with the brand new access token
                    headers["Authorization"] = f"Bearer {new_token}"
                    kwargs["headers"] = headers
                    return requests.request(method, url, **kwargs)
        except ValueError:
            # Response wasn't valid JSON, return original 401 error
            pass

    return response
