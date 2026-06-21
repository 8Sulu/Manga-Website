import functools
import os
import secrets
import sys
import threading
import time
from datetime import datetime, timezone

import redis
from cachelib.redis import RedisCache
from flask import Flask, render_template, request, jsonify, redirect, url_for, session
from flask_session import Session

from config.settings import SCRIPTS_DIR
from utils.database_utils import (
    get_db_connection,
    execute_query,
    execute_update,
    get_library_ids,
    invalidate_library_id_cache,
)
from utils.admin_utils import (
    SCHEMA,
    INSERT_OPS,
    insert_csv,
    parse_range_str,
    stamp_alembic_head,
)
from utils.fulltext import build_boolean_query
from utils.job_runner import (
    start_job,
    stop_job,
    get_job,
    JOB_NAMES,
    read_job_history,
    append_job_history,
)
from utils.middleware import rate_limited, client_ip
from utils.template_filters import register_filters
from utils.search_utils import build_results

# ── App setup ──────────────────────────────────────────────────────────────────

app = Flask(__name__, static_folder="static")
app.secret_key = os.getenv("FLASK_SECRET_KEY", secrets.token_hex(32))
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")

# ── Session storage — Redis-backed, not the filesystem ──────────────────────
#
# Was SESSION_TYPE="cachelib" backed by cachelib.FileSystemCache, writing one
# file per session under data/flask_sessions/. That was only ever safe with a
# single Gunicorn worker: FileSystemCache.set() periodically calls
# _remove_expired(), which globs the whole session directory and unlinks
# files it judges stale. Once GUNICORN_WORKERS > 1 (bumped as part of the
# Redis/RQ job-queue migration — see gunicorn.docker.conf.py — because job
# *state* no longer needed a single process), two worker processes sweep the
# same directory concurrently and one can unlink a file the instant before
# another tries to open it, raising a bare OSError that cachelib only logs
# and swallows. With enough accumulated session files (nothing was ever
# pruning them externally), that sweep could also run long enough to blow
# straight through nginx's 60s proxy_read_timeout — the 504s.
#
# Redis (already deployed for utils/job_runner.py's job queue) doesn't have
# this problem: GET/SET/DEL/EXPIRE are atomic server-side, so any number of
# Gunicorn workers — or container replicas — can share one session store
# safely. NOTE: this client deliberately does NOT use decode_responses=True
# (unlike job_runner's) — cachelib pickles session values to bytes, and
# decoding them as UTF-8 text on read would corrupt them.
_session_redis = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))

app.config.update(
    SESSION_TYPE="cachelib",
    SESSION_PERMANENT=False,
    SESSION_COOKIE_SECURE=os.getenv("SESSION_COOKIE_SECURE", "true").lower() == "true",
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
)
app.config["SESSION_CACHELIB"] = RedisCache(host=_session_redis, key_prefix="session:")
Session(app)
register_filters(app)

# ── Security headers ───────────────────────────────────────────────────────────


@app.after_request
def set_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
    if request.is_secure or request.headers.get("X-Forwarded-Proto") == "https":
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response


# ── CSRF protection ────────────────────────────────────────────────────────────


def _csrf_token() -> str:
    if "csrf_token" not in session:
        session["csrf_token"] = secrets.token_hex(32)
    return session["csrf_token"]


def csrf_protect(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if request.method in ("POST", "PUT", "DELETE", "PATCH"):
            token = (
                request.form.get("csrf_token")
                or (request.get_json(silent=True) or {}).get("csrf_token", "")
                or request.headers.get("X-CSRF-Token", "")
            )
            if not secrets.compare_digest(token, _csrf_token()):
                return jsonify({"ok": False, "message": "Invalid CSRF token"}), 403
        return f(*args, **kwargs)

    return decorated


app.jinja_env.globals["csrf_token"] = _csrf_token

# ── Auth ───────────────────────────────────────────────────────────────────────


def admin_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if session.get("admin"):
            return f(*args, **kwargs)
        if not ADMIN_PASSWORD and request.remote_addr in ("127.0.0.1", "::1"):
            return f(*args, **kwargs)
        if request.is_json or request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": "Authentication required"}), 401
        return redirect(url_for("admin_login", next=request.url))

    return decorated


@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if not ADMIN_PASSWORD:
        session["admin"] = True
        return redirect(request.args.get("next") or url_for("admin"))

    error = None
    if request.method == "POST":
        ip = client_ip()
        if rate_limited(f"login:{ip}", limit=10, window=300):
            error = "Too many login attempts. Please wait a few minutes."
        else:
            pw = request.form.get("password", "")
            if ADMIN_PASSWORD and secrets.compare_digest(pw, ADMIN_PASSWORD):
                session.clear()
                session["admin"] = True
                session["admin_time"] = datetime.now(timezone.utc).isoformat()
                session.permanent = False
                return redirect(request.args.get("next") or url_for("admin"))
            error = "Invalid password"

    return render_template("admin_login.html", error=error)


@app.route("/admin/logout")
def admin_logout():
    session.clear()
    return redirect("/")


# ── Admin dashboard ────────────────────────────────────────────────────────────


@app.route("/admin", methods=["GET", "POST"])
@admin_required
@csrf_protect
def admin():
    if request.method == "POST":
        return _handle_admin_post()

    try:
        manga_per_library = execute_query("""
            SELECT l.LibraryName, b.BranchName,
                   COUNT(DISTINCT CONCAT(a.MangaID, '-', a.Volume)) AS VolumeCount
            FROM branch b
            JOIN library l ON b.LibraryID = l.LibraryID
            JOIN branch_availability_status bas ON b.BranchID = bas.BranchID
            JOIN availability a ON bas.AvailabilityID = a.AvailabilityID
            GROUP BY l.LibraryName, b.BranchName
            HAVING COUNT(DISTINCT CONCAT(a.MangaID, '-', a.Volume)) > 0
            ORDER BY l.LibraryName, VolumeCount DESC
        """)
    except Exception:
        manga_per_library = []

    return render_template(
        "admin.html",
        manga_per_library=manga_per_library,
        job_history=list(reversed(read_job_history()))[:30],
    )


def _handle_admin_post():
    # FIX #2: parse JSON body once here, pass it down — no second get_json call in helpers
    json_body = request.get_json(silent=True) or {}
    action = request.form.get("action") or json_body.get("action")

    if action in ("scrape_leon", "scrape_broward"):
        return _start_scrape_job(action, json_body)

    if action == "get_manga":
        offset = json_body.get("offset", request.form.get("offset", "0"))
        try:
            offset = str(int(offset))
        except (ValueError, TypeError):
            return jsonify({"ok": False, "message": "Invalid offset"}), 400
        ok, status, msg = start_job(
            "get_manga",
            [sys.executable, str(SCRIPTS_DIR / "get_manga.py"), offset],
        )
        return jsonify({"ok": ok, "message": msg}), status

    return jsonify({"ok": False, "message": "Unknown action"}), 400


def _start_scrape_job(action: str, json_body: dict):
    # FIX #2: json_body is passed in, not re-parsed from request
    is_broward = action == "scrape_broward"
    script = "broward_scraper.py" if is_broward else "leon_scraper.py"
    lib_pattern = "%Broward%" if is_broward else "%Leon%"

    range_str = (request.form.get("range") or json_body.get("range", "")).strip()
    manga_id = (request.form.get("manga_id") or json_body.get("manga_id", "")).strip()
    only_new = (request.form.get("only_new") or json_body.get("only_new")) == "1"

    cmd = [sys.executable, str(SCRIPTS_DIR / script)]

    if manga_id:
        # Used exclusively by the "Find & Re-scrape Title" UI — no range needed
        cmd.extend(["--manga-ids", manga_id])

    elif range_str:
        lo, hi = parse_range_str(range_str, _titles_count())

        if only_new:
            missing_ids = _missing_manga_ids(lib_pattern)
            if not missing_ids:
                label = "Broward" if is_broward else "LCPL"
                return jsonify(
                    {"ok": False, "message": f"All titles already have {label} data"}
                ), 400

            all_ids = [
                r["MangaID"] for r in execute_query("SELECT MangaID FROM manga ORDER BY MangaID")
            ]
            valid_ids = set(all_ids[lo - 1 : hi])
            missing_ids = [m for m in missing_ids if m in valid_ids]

            if not missing_ids:
                return jsonify(
                    {"ok": False, "message": f"No new titles missing in range {range_str}"}
                ), 400

            cmd.extend(["--manga-ids", ",".join(map(str, missing_ids))])
        else:
            cmd.extend(["--range", f"{lo}-{hi}"])

    else:
        # A range is always required for bulk scrapes — never scrape everything blindly
        return jsonify({"ok": False, "message": "A range is required (e.g. 1-50)"}), 400

    # NOTE: the old in-process job runner accepted an on_complete callback
    # here to force-invalidate _missing_cache the instant a scrape finished.
    # That doesn't translate to the Redis/RQ queue: execute_job() now runs
    # inside a completely separate `rq worker` process (see
    # utils/job_runner.py's module docstring), so a closure captured in this
    # request handler has no way to run "after" a job executing somewhere
    # else. _missing_manga_ids()'s existing _MISSING_TTL (60s) means the
    # admin dashboard's missing-titles counts simply catch up within a
    # minute of a scrape finishing instead of instantly — an acceptable
    # trade-off for a dashboard stat, and one less fragile cross-process
    # callback to maintain.
    ok, status, msg = start_job(action, cmd)
    print(f"\n{cmd}\n")
    return jsonify({"ok": ok, "message": msg}), status


# ── Admin reset ────────────────────────────────────────────────────────────────


@app.route("/admin/reset", methods=["POST"])
@admin_required
@csrf_protect
def admin_reset():
    messages = []
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            for stmt in SCHEMA.strip().split(";\n"):
                stmt = stmt.strip()
                if stmt:
                    cursor.execute(stmt)
            conn.commit()
        messages.append("✓ Schema recreated")
    except Exception as e:
        return jsonify({"ok": False, "messages": [f"Schema error: {e}"]}), 500

    for filename, query in INSERT_OPS:
        messages.append(insert_csv(filename, query))

    # Keep Alembic's bookkeeping table truthful — see stamp_alembic_head()'s
    # docstring in utils/admin_utils.py for why this is necessary, not just
    # nice-to-have.
    messages.append(stamp_alembic_head())

    invalidate_library_id_cache()
    _invalidate_missing_cache()
    append_job_history("reset", "done", " · ".join(messages))
    return jsonify({"ok": True, "messages": messages})


# ── Public routes ──────────────────────────────────────────────────────────────


@app.route("/")
def home():
    lcpl_id, broward_id = get_library_ids()
    return render_template("index.html", LCPL_LIBRARY_ID=lcpl_id, BROWARD_LIBRARY_ID=broward_id)


@app.route("/api/docs")
def api_docs():
    """Swagger UI for web/static/openapi.yaml. Public/read-only — no auth needed."""
    return render_template("api_docs.html")


# ── API ────────────────────────────────────────────────────────────────────────


@app.route("/api/stats")
def api_stats():
    try:
        row = execute_query(
            """
            SELECT
              (SELECT COUNT(*) FROM availability) AS volumes,
              (SELECT COUNT(*) FROM manga)        AS titles
            """,
            fetch_all=False,
        )
        volumes = row["volumes"] if row else 0
        titles = row["titles"] if row else 0

        last_scraped = "Never scraped"
        for log in reversed(read_job_history()):
            if log["job"] in ("scrape_leon", "scrape_broward") and log["status"] == "done":
                try:
                    dt = datetime.fromisoformat(log["at"])
                    last_scraped = dt.strftime("Scraped %b %-d at %-I:%M %p")
                    break
                except ValueError:
                    pass

        return jsonify({"volumes": volumes, "titles": titles, "last_scraped": last_scraped})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/suggestions")
def api_suggestions():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])
    ip = client_ip()
    if rate_limited(f"suggest:{ip}", limit=120, window=60):
        return jsonify([]), 429
    try:
        bool_q = build_boolean_query(q)
        if bool_q:
            # BOOLEAN MODE + prefix truncation ('*'), ranked by relevance —
            # see utils/fulltext.py and README.md's "Full-Text Search"
            # section. Replaces a plain `Title LIKE '%q%'` scan, which
            # can't use any index and forces MySQL to examine every row on
            # every keystroke (item #8).
            rows = execute_query(
                "SELECT MangaID, Title, Type, Score, "
                "MATCH(Title) AGAINST (%s IN BOOLEAN MODE) AS relevance "
                "FROM manga WHERE MATCH(Title) AGAINST (%s IN BOOLEAN MODE) "
                "ORDER BY relevance DESC, Score DESC LIMIT 8",
                (bool_q, bool_q),
            )
        else:
            # q was nothing but short words / FULLTEXT operator characters
            # — FULLTEXT can't help here, fall back to the old LIKE scan.
            rows = execute_query(
                "SELECT MangaID, Title, Type, Score FROM manga "
                "WHERE Title LIKE %s ORDER BY Score DESC LIMIT 8",
                (f"%{q}%",),
            )
        return jsonify(
            [
                {
                    "manga_id": r["MangaID"],
                    "title": r["Title"],
                    "type": r["Type"],
                    "score": str(r["Score"] or ""),
                }
                for r in rows
            ]
        )
    except Exception:
        return jsonify([])


@app.route("/api/job/<name>")
@admin_required
def api_job_status(name):
    if name not in JOB_NAMES:
        return jsonify({"ok": False, "message": "unknown job"}), 400
    job = get_job(name)
    if not job:
        return jsonify({"running": False, "progress": 0, "message": ""})
    return jsonify(job)


@app.route("/api/job/stop/<name>", methods=["POST"])
@admin_required
@csrf_protect
def api_job_stop(name):
    if name not in JOB_NAMES:
        return jsonify({"ok": False, "message": "unknown job"}), 400
    stopped = stop_job(name)
    return jsonify({"ok": stopped, "message": "stop signal sent" if stopped else "job not running"})


@app.route("/api/job_history")
@admin_required
def api_job_history():
    return jsonify(list(reversed(read_job_history()))[:50])


# ── Missing-titles cache (60s TTL) ────────────────────────────────────────────

_missing_cache: dict = {}
_MISSING_TTL = 60


def _invalidate_missing_cache() -> None:
    _missing_cache.clear()


def _missing_manga_ids(library_pattern: str) -> list[int]:
    now = time.monotonic()
    cached = _missing_cache.get(library_pattern)
    if cached and (now - cached[0]) < _MISSING_TTL:
        return cached[1]

    try:
        manga_ids = [
            r["MangaID"] for r in execute_query("SELECT MangaID FROM manga ORDER BY MangaID")
        ]
        scraped = {
            r["MangaID"]
            for r in execute_query(
                """
            SELECT DISTINCT a.MangaID
            FROM availability a
            JOIN branch_availability_status bas ON a.AvailabilityID = bas.AvailabilityID
            JOIN branch b  ON bas.BranchID = b.BranchID
            JOIN library l ON b.LibraryID  = l.LibraryID
            WHERE l.LibraryName LIKE %s
        """,
                (library_pattern,),
            )
        }
        result = [mid for mid in manga_ids if mid not in scraped]
    except Exception:
        result = []

    _missing_cache[library_pattern] = (now, result)
    return result


@app.route("/api/missing_titles")
@admin_required
def api_missing_titles():
    total = _titles_count()
    return jsonify(
        {
            "count": len(_missing_manga_ids("%Leon%")),
            "broward_count": len(_missing_manga_ids("%Broward%")),
            "total_titles": total,
        }
    )


@app.route("/api/delete_title_results", methods=["POST"])
@admin_required
@csrf_protect
def api_delete_title_results():
    data = request.get_json() or {}
    manga_id = data.get("manga_id")
    lib_id = data.get("library")

    if not manga_id:
        return jsonify({"ok": False, "message": "No manga_id provided"}), 400

    try:
        if lib_id:
            execute_update(
                """
                DELETE bas FROM branch_availability_status bas
                JOIN availability a ON bas.AvailabilityID = a.AvailabilityID
                JOIN branch b ON bas.BranchID = b.BranchID
                WHERE a.MangaID = %s AND b.LibraryID = %s
            """,
                (manga_id, lib_id),
            )
            execute_update(
                """
                DELETE a FROM availability a
                LEFT JOIN branch_availability_status bas
                    ON a.AvailabilityID = bas.AvailabilityID
                WHERE a.MangaID = %s AND bas.AvailabilityID IS NULL
            """,
                (manga_id,),
            )
        else:
            execute_update("DELETE FROM availability WHERE MangaID = %s", (manga_id,))
        _invalidate_missing_cache()
        return jsonify(
            {
                "ok": True,
                "message": f"Cleared availability for ID: {manga_id}",
                "manga_id": manga_id,
            }
        )
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)}), 500


@app.route("/api/title_volumes/<int:manga_id>")
@admin_required
def api_title_volumes(manga_id):
    try:
        rows = execute_query(
            """
            SELECT a.Volume, a.AvailabilityID,
                   GROUP_CONCAT(
                       CONCAT(b.BranchName, ': ', bas.Status)
                       ORDER BY b.BranchName SEPARATOR ' | '
                   ) AS branches
            FROM availability a
            LEFT JOIN branch_availability_status bas ON a.AvailabilityID = bas.AvailabilityID
            LEFT JOIN branch b ON bas.BranchID = b.BranchID
            WHERE a.MangaID = %s
            GROUP BY a.AvailabilityID, a.Volume
            ORDER BY a.Volume
        """,
            (manga_id,),
        )
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── MAL proxy — background job so the request thread isn't blocked ─────────────

_mal_jobs: dict[str, dict] = {}
_mal_jobs_lock: threading.Lock = threading.Lock()


def _mal_fetch_worker(job_id: str, access_token_init: str) -> None:
    import requests as _req

    access_token = access_token_init
    all_statuses: dict[int, dict] = {}
    offset, limit, max_pages = 0, 1000, 10

    try:
        for _ in range(max_pages):
            url = (
                "https://api.myanimelist.net/v2/users/@me/mangalist"
                f"?fields=list_status&limit={limit}&offset={offset}"
            )
            try:
                resp = _req.get(
                    url,
                    headers={"Authorization": f"Bearer {access_token}"},
                    timeout=20,
                )
            except Exception as e:
                with _mal_jobs_lock:
                    _mal_jobs[job_id] = {
                        "status": "error",
                        "data": None,
                        "message": f"MAL API error: {e}",
                    }
                return

            if resp.status_code == 401:
                from services.mal_client import refresh_tokens

                new_token = refresh_tokens()
                if not new_token:
                    with _mal_jobs_lock:
                        _mal_jobs[job_id] = {
                            "status": "error",
                            "data": None,
                            "message": "MAL token expired and refresh failed",
                        }
                    return
                access_token = new_token
                resp = _req.get(
                    url, headers={"Authorization": f"Bearer {access_token}"}, timeout=20
                )

            if resp.status_code != 200:
                with _mal_jobs_lock:
                    _mal_jobs[job_id] = {
                        "status": "error",
                        "data": None,
                        "message": f"MAL API error {resp.status_code}",
                    }
                return

            data = resp.json()
            for item in data.get("data", []):
                node = item.get("node", {})
                mal_id = node.get("id")
                lst_status = item.get("list_status", {})
                if mal_id:
                    all_statuses[mal_id] = {
                        "status": lst_status.get("status", ""),
                        "score": lst_status.get("score", 0),
                        "num_volumes_read": lst_status.get("num_volumes_read", 0),
                    }

            if not data.get("paging", {}).get("next"):
                break
            offset += limit

        with _mal_jobs_lock:
            _mal_jobs[job_id] = {"status": "done", "data": all_statuses, "message": "ok"}

    except Exception as e:
        with _mal_jobs_lock:
            _mal_jobs[job_id] = {"status": "error", "data": None, "message": str(e)}


@app.route("/api/mal/mangalist")
def api_mal_mangalist():
    ip = client_ip()
    if rate_limited(f"mal:{ip}", limit=10, window=60):
        return jsonify({"ok": False, "message": "Rate limited"}), 429

    access_token = os.getenv("MAL_ACCESS_TOKEN", "")
    if not access_token:
        return jsonify({"ok": False, "message": "MAL access token not configured"}), 503

    job_id = secrets.token_hex(8)
    with _mal_jobs_lock:
        _mal_jobs[job_id] = {"status": "running", "data": None, "message": ""}

    t = threading.Thread(target=_mal_fetch_worker, args=(job_id, access_token), daemon=True)
    t.start()

    return jsonify({"ok": True, "job_id": job_id})


@app.route("/api/mal/mangalist/status/<job_id>")
def api_mal_mangalist_status(job_id: str):
    with _mal_jobs_lock:
        job = _mal_jobs.get(job_id)

    if not job:
        return jsonify({"ok": False, "status": "not_found"}), 404

    if job["status"] == "running":
        return jsonify({"ok": True, "status": "running"})

    if job["status"] == "error":
        with _mal_jobs_lock:
            _mal_jobs.pop(job_id, None)
        return jsonify({"ok": False, "status": "error", "message": job["message"]}), 502

    data = job["data"]
    with _mal_jobs_lock:
        _mal_jobs.pop(job_id, None)
    return jsonify({"ok": True, "status": "done", "data": data})


# FIX #11: MAL filter endpoints are session-mutating POSTs — protect with CSRF
@app.route("/api/mal/set_filter", methods=["POST"])
@csrf_protect
def api_mal_set_filter():
    ip = client_ip()
    if rate_limited(f"mal_filter:{ip}", limit=60, window=60):
        return jsonify({"ok": False, "message": "Rate limited"}), 429

    body = request.get_json(silent=True) or {}
    mal_data = body.get("data")
    mal_filters = body.get("filters")

    if mal_filters is not None:
        legal = {"", "include", "exclude"}
        valid_keys = {"reading", "completed", "on_hold", "dropped", "plan_to_read"}
        session["mal_filters"] = {
            k: v for k, v in mal_filters.items() if k in valid_keys and v in legal
        }

    if mal_data is not None:
        if not isinstance(mal_data, dict):
            return jsonify({"ok": False, "message": "Invalid data payload"}), 400
        session["mal_data"] = mal_data

    return jsonify({"ok": True})


# FIX #11: same — session-mutating POST needs CSRF protection
@app.route("/api/mal/clear_filter", methods=["POST"])
@csrf_protect
def api_mal_clear_filter():
    session.pop("mal_data", None)
    session.pop("mal_filters", None)
    return jsonify({"ok": True})


# ── Search ─────────────────────────────────────────────────────────────────────


@app.route("/search")
def search():
    ip = client_ip()
    if rate_limited(f"search:{ip}", limit=60, window=60):
        return "Too many requests", 429

    LCPL_LIBRARY_ID, BROWARD_LIBRARY_ID = get_library_ids()

    title = request.args.get("title", "").strip()
    type_ = request.args.get("type", "").strip()
    branch = request.args.get("branch", "").strip()
    volume = request.args.get("volume", "").strip()
    avail_filter = request.args.get("avail", "").strip()
    lib_filter = request.args.get("library", "").strip()
    no_vol1 = request.args.get("no_vol1", "").strip()
    sort_key = request.args.get("sort", "score").strip()

    conditions = ["b.BranchName IS NOT NULL", "b.BranchID IS NOT NULL"]
    params: list = []
    if title:
        bool_q = build_boolean_query(title)
        if bool_q:
            # FULLTEXT match runs as a subquery against `manga` alone (not
            # MATCH() directly in this multi-join WHERE clause) so MySQL
            # reliably uses the FULLTEXT index instead of leaving it to the
            # optimizer to push the condition through four joins. See
            # utils/fulltext.py and README.md's "Full-Text Search" section
            # (item #8).
            conditions.append(
                "m.MangaID IN ("
                "SELECT MangaID FROM manga WHERE MATCH(Title) AGAINST (%s IN BOOLEAN MODE)"
                ")"
            )
            params.append(bool_q)
        else:
            # Every word in `title` was shorter than the FULLTEXT index's
            # minimum indexed word length (or was nothing but BOOLEAN MODE
            # operator characters) — fall back to the old LIKE scan rather
            # than silently returning zero results.
            conditions.append("m.Title LIKE %s")
            params.append(f"%{title}%")
    if type_:
        conditions.append("m.Type = %s")
        params.append(type_)
    if volume:
        conditions.append("a.Volume = %s")
        params.append(volume)
    if branch:
        conditions.append("b.BranchName = %s")
        params.append(branch)
    if lib_filter:
        conditions.append("b.LibraryID = %s")
        params.append(int(lib_filter))

    sql = f"""
        SELECT m.MangaID, m.Title, a.Volume, m.Volumes, m.Type,
               m.Members, m.Score, m.Author, m.CoverMedium,
               b.BranchName, b.BranchID, bas.Status, b.LibraryID, l.LibraryName,
               a.ScrapedAt
        FROM manga m
        JOIN availability a                  ON m.MangaID = a.MangaID
        JOIN branch_availability_status bas  ON a.AvailabilityID = bas.AvailabilityID
        JOIN branch b                        ON bas.BranchID = b.BranchID
        JOIN library l                       ON b.LibraryID = l.LibraryID
        WHERE {" AND ".join(conditions)}
        ORDER BY m.Score DESC, a.Volume ASC, b.BranchName ASC
    """
    rows = execute_query(sql, params)

    grouped = build_results(
        rows,
        lcpl_library_id=LCPL_LIBRARY_ID,
        broward_library_id=BROWARD_LIBRARY_ID,
        avail_filter=avail_filter,
        no_vol1=no_vol1,
        mal_data=session.get("mal_data"),
        mal_filters=session.get("mal_filters"),
    )

    sort_dir = request.args.get("sort_dir", "desc").strip()
    reverse = sort_dir != "asc"

    if sort_key == "title":
        grouped.sort(key=lambda r: (r["Title"] or "").lower(), reverse=not reverse)
    elif sort_key == "avail":
        grouped.sort(key=lambda r: r["avail_count"], reverse=reverse)
    elif sort_key == "vols":
        grouped.sort(key=lambda r: r["vol_count"], reverse=reverse)
    else:
        grouped.sort(key=lambda r: float(r["Score"] or 0), reverse=reverse)

    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 50, type=int)

    total_count = len(grouped)
    total_pages = max(1, (total_count + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))

    filters = {
        "title": title,
        "type": type_,
        "branch": branch,
        "volume": volume,
        "avail": avail_filter,
        "library": lib_filter,
        "no_vol1": no_vol1,
    }
    return render_template(
        "results.html",
        results=grouped[(page - 1) * per_page : page * per_page],
        count=total_count,
        page=page,
        total_pages=total_pages,
        per_page=per_page,
        filters=filters,
        has_filters=any(v for v in filters.values()),
        LCPL_LIBRARY_ID=LCPL_LIBRARY_ID,
        BROWARD_LIBRARY_ID=BROWARD_LIBRARY_ID,
        mal_filters=session.get("mal_filters", {}),
        mal_active=bool(
            session.get("mal_filters") and any(v for v in session.get("mal_filters", {}).values())
        ),
        mal_loaded=bool(session.get("mal_data")),
        current_sort=sort_key,
        current_sort_dir=sort_dir,
    )


# ── Admin DB helpers ───────────────────────────────────────────────────────────


def _titles_count() -> int:
    try:
        res = execute_query("SELECT COUNT(*) AS n FROM manga", fetch_all=False)
        return res["n"] if res else 0
    except Exception:
        return 9999


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(debug=False, host="127.0.0.1", port=5000)
