"""
scripts/mal_authorize.py

One-time interactive helper that runs the MyAnimeList Authorization Code +
PKCE flow end-to-end and persists the resulting access/refresh token pair
to data/mal_tokens.json — the same file services/mal_client.py reads from
and writes to on every subsequent automatic refresh (see that module's
docstring for why .env is never the long-term store).

You only need to run this:
  - once, to bootstrap a working refresh token after this fix is deployed
    (the old refresh token in .env was already silently rotated out by a
    refresh that never persisted anywhere durable — see mal_client.py), or
  - if a refresh token is ever revoked/expired again in the future, which
    shouldn't happen anymore now that refreshes actually persist.

Usage:
    python scripts/mal_authorize.py

Requires MAL_CLIENT_ID and MAL_CLIENT_SECRET in .env. If you don't already
know the redirect URI registered for this client, check/set one at
https://myanimelist.net/apiconfig — it must match EXACTLY what you enter
when this script asks for it.
"""

from __future__ import annotations

import os
import secrets
import string
import sys
import webbrowser
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

import requests

sys.path.append(str(Path(__file__).parent.parent))

from config.settings import REQUEST_TIMEOUT  # noqa: E402  (also loads .env as a side effect)
from services.mal_client import persist_tokens  # noqa: E402

AUTH_URL = "https://myanimelist.net/v1/oauth2/authorize"
TOKEN_URL = "https://myanimelist.net/v1/oauth2/token"


def _make_code_verifier(length: int = 128) -> str:
    """
    MAL only supports the 'plain' PKCE method (Step 1 of their OAuth docs),
    so code_challenge == code_verifier — no S256 hashing needed. RFC 7636
    allows [A-Z] [a-z] [0-9] - . _ ~ , length 43-128; we use the max.
    """
    alphabet = string.ascii_letters + string.digits + "-._~"
    return "".join(secrets.choice(alphabet) for _ in range(length))


def main() -> None:
    client_id = os.getenv("MAL_CLIENT_ID", "")
    client_secret = os.getenv("MAL_CLIENT_SECRET", "")

    if not client_id or not client_secret:
        print("[-] MAL_CLIENT_ID / MAL_CLIENT_SECRET missing from .env — set those first.")
        sys.exit(1)

    redirect_uri = os.getenv("MAL_REDIRECT_URI", "").strip()
    if not redirect_uri:
        redirect_uri = input(
            "Enter the redirect URI registered for this client "
            "(check/set at https://myanimelist.net/apiconfig): "
        ).strip()
    if not redirect_uri:
        print("[-] A redirect_uri is required.")
        sys.exit(1)

    code_verifier = _make_code_verifier()
    state = secrets.token_hex(8)

    params = {
        "response_type": "code",
        "client_id": client_id,
        "state": state,
        "redirect_uri": redirect_uri,
        "code_challenge": code_verifier,  # plain method: challenge == verifier
        "code_challenge_method": "plain",
    }
    auth_url = f"{AUTH_URL}?{urlencode(params)}"

    print("\n[1/3] Open this URL, log into MAL, and click Allow:\n")
    print(f"      {auth_url}\n")
    try:
        webbrowser.open(auth_url)
    except Exception:
        pass  # fine if this is a headless server — the printed URL still works

    print("[2/3] MAL then redirects you to your redirect_uri with ?code=...&state=...")
    print("      in the address bar. That's expected even if the page itself 404s or")
    print("      refuses to load — just copy the FULL URL from your browser's address bar.\n")
    pasted = input("      Paste the full redirect URL (or just the code) here: ").strip()

    if pasted.startswith("http"):
        qs = parse_qs(urlparse(pasted).query)
        code = (qs.get("code") or [""])[0]
        returned_state = (qs.get("state") or [""])[0]
        if returned_state and returned_state != state:
            print("[-] WARNING: returned state does not match what was sent — aborting.")
            sys.exit(1)
    else:
        code = pasted

    if not code:
        print("[-] No authorization code found in that input.")
        sys.exit(1)

    print("\n[3/3] Exchanging authorization code for tokens...")
    try:
        resp = requests.post(
            TOKEN_URL,
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
                "code_verifier": code_verifier,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=REQUEST_TIMEOUT,
        )
    except requests.exceptions.RequestException as e:
        print(f"[-] Token exchange request failed (network error): {e}")
        sys.exit(1)

    if resp.status_code != 200:
        print(f"[-] Token exchange failed: HTTP {resp.status_code}")
        print(resp.text)
        sys.exit(1)

    tokens = resp.json()
    access_token = tokens["access_token"]
    refresh_token = tokens["refresh_token"]

    persist_tokens(access_token, refresh_token)

    print("\n[+] Success — tokens persisted to data/mal_tokens.json")
    print("[+] No restart needed: mal_client.py reads this file fresh on every call,")
    print("    in every process (Gunicorn workers + the rq worker), so it's live now.")
    print("\n    For reference, the same values (in case you want them as .env bootstrap):")
    print(f"    MAL_ACCESS_TOKEN={access_token}")
    print(f"    MAL_REFRESH_TOKEN={refresh_token}")


if __name__ == "__main__":
    main()
