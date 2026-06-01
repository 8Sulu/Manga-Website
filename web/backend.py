from flask import (Flask, render_template, request, jsonify, redirect,
                   url_for, session, make_response)
import csv, json, sys, threading, functools, os, hashlib, secrets, time
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import quote_plus as _quote_plus

sys.path.append(str(Path(__file__).parent.parent))
from config.settings import DB_CONFIG, DATA_DIR, SCRIPTS_DIR
from utils.database_utils import get_db_connection, execute_query, execute_update

app = Flask(__name__, static_folder='static')
app.secret_key = os.getenv('FLASK_SECRET_KEY', secrets.token_hex(32))
ADMIN_PASSWORD   = os.getenv('ADMIN_PASSWORD', '')
JOB_HISTORY_PATH = DATA_DIR / "job_history.json"

# ── Security headers ───────────────────────────────────────────────────────────

@app.after_request
def set_security_headers(response):
    response.headers['X-Content-Type-Options']  = 'nosniff'
    response.headers['X-Frame-Options']         = 'DENY'
    response.headers['X-XSS-Protection']        = '1; mode=block'
    response.headers['Referrer-Policy']         = 'strict-origin-when-cross-origin'
    response.headers['Permissions-Policy']      = 'geolocation=(), microphone=(), camera=()'
    # Only set HSTS when served over HTTPS (nginx handles TLS)
    if request.is_secure or request.headers.get('X-Forwarded-Proto') == 'https':
        response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
    return response

# ── Simple in-memory rate limiter ─────────────────────────────────────────────

_rate_limits: dict = {}          # ip -> [timestamp, ...]
_rate_lock = threading.Lock()

def _rate_limited(key: str, limit: int = 60, window: int = 60) -> bool:
    """Return True if this key has exceeded limit requests in the last window seconds."""
    now = time.time()
    with _rate_lock:
        hits = _rate_limits.get(key, [])
        hits = [t for t in hits if now - t < window]
        if len(hits) >= limit:
            _rate_limits[key] = hits
            return True
        hits.append(now)
        _rate_limits[key] = hits
    return False

def _client_ip() -> str:
    return request.headers.get('X-Real-IP') or \
           request.headers.get('X-Forwarded-For', '').split(',')[0].strip() or \
           request.remote_addr or 'unknown'

# ── CSRF protection (token in session, checked on state-changing POSTs) ────────

def _csrf_token() -> str:
    if 'csrf_token' not in session:
        session['csrf_token'] = secrets.token_hex(32)
    return session['csrf_token']

def csrf_protect(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if request.method in ('POST', 'PUT', 'DELETE', 'PATCH'):
            token = (request.form.get('csrf_token')
                     or request.get_json(silent=True, force=True) or {}).get('csrf_token', '') \
                    or request.headers.get('X-CSRF-Token', '')
            if not secrets.compare_digest(token, _csrf_token()):
                return jsonify({'ok': False, 'message': 'Invalid CSRF token'}), 403
        return f(*args, **kwargs)
    return decorated

app.jinja_env.globals['csrf_token'] = _csrf_token

# ── Auth ───────────────────────────────────────────────────────────────────────

def admin_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if session.get('admin'):
            return f(*args, **kwargs)
        # Allow passwordless local access only if no password is set
        if not ADMIN_PASSWORD and request.remote_addr in ('127.0.0.1', '::1'):
            return f(*args, **kwargs)
        if request.is_json or request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({'ok': False, 'message': 'Authentication required'}), 401
        return redirect(url_for('admin_login', next=request.url))
    return decorated

# ── Job system ─────────────────────────────────────────────────────────────────

_jobs: dict        = {}
_jobs_lock         = threading.Lock()
_JOB_NAMES         = {'scrape', 'scrape_broward', 'get_manga'}

def _run_subprocess(job_name: str, cmd: list) -> None:
    import subprocess, re
    with _jobs_lock:
        _jobs[job_name].update(running=True, progress=0, message='starting…')
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1
    )
    last_msg = ''
    progress  = 0
    for line in proc.stdout:
        line = line.rstrip()
        if not line:
            continue
        last_msg = line[:120]
        m = re.search(r'\[(\d+)/(\d+)\]', line)
        if m:
            n, total = int(m.group(1)), int(m.group(2))
            progress = int(n / total * 100) if total else 0
        with _jobs_lock:
            if not _jobs[job_name].get('stop_requested'):
                _jobs[job_name].update(progress=progress, message=last_msg)
            else:
                proc.terminate()
                break
    proc.wait()
    ok = proc.returncode == 0
    with _jobs_lock:
        _jobs[job_name].update(
            running=False,
            progress=100 if ok else progress,
            message=last_msg if ok else f'error: exited {proc.returncode}',
        )
    _append_job_history(job_name, 'done' if ok else 'error', last_msg)

def _start_job(job_name: str, cmd: list) -> tuple[bool, int, str]:
    with _jobs_lock:
        if _jobs.get(job_name, {}).get('running'):
            return False, 409, f'{job_name} is already running'
        _jobs[job_name] = {
            'running': False, 'progress': 0,
            'message': '', 'stop_requested': False,
        }
    t = threading.Thread(target=_run_subprocess, args=(job_name, cmd), daemon=True)
    with _jobs_lock:
        _jobs[job_name]['thread'] = t
    t.start()
    return True, 200, f'{job_name} started'

def _stop_job(job_name: str) -> bool:
    with _jobs_lock:
        job = _jobs.get(job_name)
        if not job or not job.get('running'):
            return False
        job['stop_requested'] = True
    return True

# ── Job history ────────────────────────────────────────────────────────────────

def _read_job_history() -> list:
    try:
        if JOB_HISTORY_PATH.exists():
            return json.loads(JOB_HISTORY_PATH.read_text(encoding='utf-8'))
    except Exception:
        pass
    return []

def _append_job_history(job: str, status: str, message: str) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    history = _read_job_history()
    history.append({
        'job': job, 'status': status, 'message': message,
        'at': datetime.now(timezone.utc).isoformat(),
    })
    try:
        JOB_HISTORY_PATH.write_text(
            json.dumps(history[-200:], indent=2, ensure_ascii=False),
            encoding='utf-8',
        )
    except Exception:
        pass

# ── Helpers ────────────────────────────────────────────────────────────────────

def parse_range_str(s: str, max_titles: int = 9999) -> tuple:
    import re
    s = s.strip().replace('\u2013', '-').replace('\u2014', '-')
    if not s:
        return 1, 1
    if s.isdigit():
        return 1, int(s)
    m = re.match(r'^(\d*)-(\d*)$', s)
    if m:
        lo = int(m.group(1)) if m.group(1) else 1
        hi = int(m.group(2)) if m.group(2) else max_titles
        return lo, hi
    return 1, 1

def _titles_count() -> int:
    try:
        res = execute_query("SELECT COUNT(*) AS n FROM manga", fetch_all=False)
        return res['n'] if res else 0
    except Exception:
        return 9999

def _missing_indices(library_pattern: str) -> list:
    try:
        manga_ids = [r['MangaID'] for r in
                     execute_query("SELECT MangaID FROM manga ORDER BY MangaID")]
        scraped = {r['MangaID'] for r in execute_query("""
            SELECT DISTINCT a.MangaID
            FROM availability a
            JOIN branch_availability_status bas ON a.AvailabilityID = bas.AvailabilityID
            JOIN branch b ON bas.BranchID = b.BranchID
            JOIN library l ON b.LibraryID = l.LibraryID
            WHERE l.LibraryName LIKE %s
        """, (library_pattern,))}
        return [i + 1 for i, mid in enumerate(manga_ids) if mid not in scraped]
    except Exception:
        return []

# ── Library ID cache ───────────────────────────────────────────────────────────

_library_id_cache: tuple | None = None

def _get_library_ids() -> tuple[int, int]:
    global _library_id_cache
    if _library_id_cache is not None:
        return _library_id_cache
    try:
        rows = execute_query("SELECT LibraryID, LibraryName FROM library")
        lcpl = broward = None
        for r in rows:
            name = r['LibraryName'] or ''
            if 'Leon' in name or 'LeRoy' in name or 'LCPL' in name:
                lcpl = r['LibraryID']
            elif 'Broward' in name:
                broward = r['LibraryID']
        if lcpl is not None and broward is not None:
            _library_id_cache = (lcpl, broward)
            return _library_id_cache
    except Exception:
        pass
    try:
        rows = execute_query("SELECT LibraryID FROM library ORDER BY LibraryID LIMIT 2")
        if len(rows) >= 2:
            return rows[0]['LibraryID'], rows[1]['LibraryID']
    except Exception:
        pass
    return 1, 2

# ── Branch display helpers ─────────────────────────────────────────────────────

def _branch_short(name: str, library_id: int, broward_library_id: int) -> str:
    if library_id != broward_library_id:
        for keyword, short in (
            ('Main',      'Main'),
            ('Leroy',     'Main'),
            ('Northeast', 'NE Branch'),
            ('Bruce',     'NE Branch'),
            ('Eastside',  'Eastside'),
            ('Perry',     'BL Perry'),
            ('Jackson',   'Lk Jackson'),
            ('Braden',    'Ft Braden'),
            ('Fort',      'Ft Braden'),
            ('Woodville', 'Woodville'),
        ):
            if keyword in name:
                return short
        return name.split(' ')[0]
    broward_shorts = (
        ('Northwest Regional',      'NW Regional'),
        ('North Regional',          'N Regional'),
        ('South Regional',          'S Regional'),
        ('Southwest Regional',      'SW Regional'),
        ('West Regional',           'W Regional'),
        ('Main Library',            'Main'),
        ('African American',        'AARLCC'),
        ('Hollywood Beach',         'Hollywood Bch'),
        ('Hollywood',               'Hollywood'),
        ('Lauderhill Central',      'Lauderhill CP'),
        ('Lauderhill Towne',        'Lauderhill TC'),
        ('Lauderdale Lakes',        'Laud. Lakes'),
        ('Pompano Beach',           'Pompano Bch'),
        ('Pembroke Pines',          'Pemb. Pines'),
        ('Miramar',                 'Miramar'),
        ('Weston',                  'Weston'),
        ('Tamarac',                 'Tamarac'),
        ('Sunrise',                 'Sunrise'),
        ('Margate',                 'Margate'),
        ('Deerfield Beach',         'Deerfield Bch'),
        ('Dania Beach',             'Dania Bch'),
        ('Hallandale',              'Hallandale'),
        ('Carver Ranches',          'Carver Ranch'),
        ('Century Plaza',           'Century Plz'),
        ('North Lauderdale',        'N. Laud.'),
        ('Imperial Point',          'Imperial Pt'),
        ('Riverland',               'Riverland'),
        ('Davie',                   'Davie/CC'),
        ('Beach Branch',            'Beach'),
        ('Jan Moran',               'Jan Moran'),
        ('Galt Ocean',              'Galt Ocean'),
        ('Fort Lauderdale Reading', 'FTL Reading'),
        ('Tyrone Bryant',           'Tyrone Bryant'),
        ('Northwest Branch',        'NW Branch'),
        ('Nova Southeastern',       'NSU'),
    )
    for keyword, short in broward_shorts:
        if keyword in name:
            return short
    return name.split(' ')[0]

STATUS_PRIORITY = {'Available': 2, 'On Hold': 1, 'Checked Out': 0}

# ── Catalog URL builder ────────────────────────────────────────────────────────

NOVEL_TYPES = {'light novel', 'light-novel', 'novel'}

def _is_novel(manga_type: str) -> bool:
    return (manga_type or '').lower().replace(' ', '-') in {'light-novel', 'novel'}

def _broward_search_url(title: str, author: str, manga_type: str = '',
                        volume: int | None = None) -> str:
    """
    Build Broward catalog search URL.
    - Novels/light novels exclude the 'Graphic novels.' subject to avoid manga hits.
    - Optional volume number is appended as an extra qu= term.
    - Results sorted by publication date ascending (st=PA) so vol 1 is first.
    """
    et = _quote_plus(title)
    ea = _quote_plus(author or '')
    base = (f"https://broward.ent.sirsi.net/client/en_US/default/search/results"
            f"?qu=TITLE%3D{et}&qu=AUTHOR%3D{ea}"
            f"&qf=FORMAT%09Special+Format%09BOOK%09Books")
    if _is_novel(manga_type):
        base += "&qf=-SUBJECT%09Subject%09Graphic+novels.%09Graphic+novels."
    if volume is not None and volume > 0:
        base += f"&qu={volume}"
    base += "&st=PA"   # publication date ascending → vol 1 first
    return base

def _lcpl_search_url(title: str, author: str, volume: int | None = None) -> str:
    """Build LCPL catalog search URL with optional volume number."""
    et = _quote_plus(title)
    ea = _quote_plus(author or '')
    url = (f"https://lcpl.ent.sirsi.net/client/en_US/lcpl/search/results"
           f"?qu=&qu=TITLE%3D{et}+&qu=AUTHOR%3D{ea}+&te=ILS&h=1")
    if volume is not None and volume > 0:
        url += f"&qu={volume}"
    return url

# ── DB schema & seed ───────────────────────────────────────────────────────────

SCHEMA = """
DROP TABLE IF EXISTS branch_availability_status;
DROP TABLE IF EXISTS availability;
DROP TABLE IF EXISTS branch;
DROP TABLE IF EXISTS manga;
DROP TABLE IF EXISTS library;
CREATE TABLE manga (
    MangaID INT PRIMARY KEY, Title VARCHAR(255) NOT NULL, `Type` VARCHAR(50),
    Volumes INT, Members INT, Score DECIMAL(4,2), Author VARCHAR(255),
    CoverMedium VARCHAR(512), CoverLarge VARCHAR(512)
);
CREATE TABLE library (
    LibraryID INT PRIMARY KEY AUTO_INCREMENT,
    LibraryName VARCHAR(255) NOT NULL, `URL` VARCHAR(255) NOT NULL
);
CREATE TABLE branch (
    BranchID INT PRIMARY KEY AUTO_INCREMENT, BranchName VARCHAR(255) NOT NULL,
    `Address` VARCHAR(255), LibraryID INT NOT NULL,
    FOREIGN KEY (LibraryID) REFERENCES library(LibraryID) ON DELETE CASCADE
);
CREATE TABLE availability (
    AvailabilityID INT AUTO_INCREMENT PRIMARY KEY,
    MangaID INT NOT NULL, Volume INT NOT NULL,
    FOREIGN KEY (MangaID) REFERENCES manga(MangaID) ON DELETE CASCADE
);
CREATE TABLE branch_availability_status (
    BranchStatusID INT AUTO_INCREMENT PRIMARY KEY,
    AvailabilityID INT NOT NULL, BranchID INT NOT NULL, `Status` VARCHAR(100) NOT NULL,
    FOREIGN KEY (AvailabilityID) REFERENCES availability(AvailabilityID) ON DELETE CASCADE,
    FOREIGN KEY (BranchID) REFERENCES branch(BranchID) ON DELETE CASCADE
)
"""

INSERT_OPS = [
    ("manga.csv",
     "INSERT INTO manga (MangaID, Title, Type, Volumes, Members, Score, Author, CoverMedium, CoverLarge) "
     "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"),
    ("libraries.csv",
     "INSERT INTO library (LibraryName, `URL`) VALUES (%s,%s)"),
    ("branches.csv",
     "INSERT INTO branch (BranchName, `Address`, LibraryID) VALUES (%s,%s,%s)"),
]

def insert_csv(filename, query):
    filepath = DATA_DIR / filename
    try:
        with open(filepath, encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)
            with get_db_connection() as conn:
                cursor = conn.cursor()
                for row in reader:
                    row = [None if v.strip().upper() in ('NULL', '') else v for v in row]
                    cursor.execute(query, tuple(row))
                conn.commit()
        return f'✓ {filename}'
    except Exception as e:
        return f'✗ {filename}: {e}'

# ── Auth routes ────────────────────────────────────────────────────────────────

@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    if not ADMIN_PASSWORD:
        session['admin'] = True
        return redirect(request.args.get('next') or url_for('admin'))

    error = None
    if request.method == 'POST':
        ip = _client_ip()
        if _rate_limited(f'login:{ip}', limit=10, window=300):
            error = 'Too many login attempts. Please wait a few minutes.'
        else:
            pw = request.form.get('password', '')
            if ADMIN_PASSWORD and secrets.compare_digest(pw, ADMIN_PASSWORD):
                session.clear()
                session['admin']      = True
                session['admin_time'] = datetime.now(timezone.utc).isoformat()
                session.permanent     = False
                return redirect(request.args.get('next') or url_for('admin'))
            error = 'Invalid password'

    return render_template('admin_login.html', error=error)

@app.route('/admin/logout')
def admin_logout():
    session.clear()
    return redirect('/')

# ── Admin GET/POST ─────────────────────────────────────────────────────────────

@app.route('/admin', methods=['GET', 'POST'])
@admin_required
def admin():
    if request.method == 'POST':
        return _handle_admin_post()

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
    return render_template('admin.html',
                           manga_per_library=manga_per_library,
                           job_history=list(reversed(_read_job_history()))[:30])

def _handle_admin_post():
    # Validate CSRF for all POST actions
    token = (request.form.get('csrf_token')
             or (request.get_json(silent=True) or {}).get('csrf_token', '')
             or request.headers.get('X-CSRF-Token', ''))
    if not secrets.compare_digest(token, _csrf_token()):
        return jsonify({'ok': False, 'message': 'Invalid CSRF token'}), 403

    action = request.form.get('action')

    if action == 'update':
        execute_update('UPDATE manga SET Volumes = %s WHERE Title = %s',
                       (request.form.get('volume'), request.form.get('title')))
        return jsonify({'ok': True, 'message': 'Volumes updated'})

    if action == 'delete':
        execute_update(
            'DELETE FROM availability WHERE MangaID = '
            '(SELECT MangaID FROM manga WHERE Title = %s) AND Volume = %s',
            (request.form.get('title'), request.form.get('volume')))
        return jsonify({'ok': True, 'message': 'Volume deleted'})

    if action in ('scrape', 'scrape_broward'):
        return _start_scrape_job(action)

    if action == 'get_manga':
        offset = request.form.get('offset', '0')
        try:
            offset = str(int(offset))   # validate it's a number
        except ValueError:
            return jsonify({'ok': False, 'message': 'Invalid offset'}), 400
        ok, status, msg = _start_job(
            'get_manga',
            [sys.executable, str(SCRIPTS_DIR / 'get_manga.py'), offset],
        )
        return jsonify({'ok': ok, 'message': msg}), status

    return jsonify({'ok': False, 'message': 'Unknown action'}), 400

def _start_scrape_job(action: str):
    is_broward  = action == 'scrape_broward'
    script      = 'broward_scrapper.py' if is_broward else 'scrapper.py'
    lib_pattern = '%Broward%' if is_broward else '%Leon%'
    range_str   = request.form.get('range', '').strip()
    only_new    = request.form.get('only_new') == '1'

    if only_new:
        missing = _missing_indices(lib_pattern)
        if not missing:
            return jsonify({
                'ok': False,
                'message': f'All titles already have '
                           f'{"Broward" if is_broward else "LCPL"} data',
            }), 400
        if range_str:
            lo, hi = parse_range_str(range_str, _titles_count())
            missing = [idx for idx in missing if lo <= idx <= hi]
            if not missing:
                return jsonify({
                    'ok': False,
                    'message': f'No new titles in range {range_str}',
                }), 400
        cmd = [sys.executable, str(SCRIPTS_DIR / script),
               '--indices', ','.join(map(str, missing))]
    else:
        cmd = [sys.executable, str(SCRIPTS_DIR / script)]
        if range_str:
            lo, hi = parse_range_str(range_str, _titles_count())
            cmd.extend(['--range', f'{lo}-{hi}'])

    ok, status, msg = _start_job(action, cmd)
    return jsonify({'ok': ok, 'message': msg}), status

# ── Admin reset ────────────────────────────────────────────────────────────────

@app.route('/admin/reset', methods=['POST'])
@admin_required
def admin_reset():
    global _library_id_cache
    # CSRF check
    token = (request.form.get('csrf_token')
             or (request.get_json(silent=True) or {}).get('csrf_token', '')
             or request.headers.get('X-CSRF-Token', ''))
    if not secrets.compare_digest(token, _csrf_token()):
        return jsonify({'ok': False, 'message': 'Invalid CSRF token'}), 403

    messages = []
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            for stmt in SCHEMA.strip().split(';'):
                stmt = stmt.strip()
                if stmt:
                    cursor.execute(stmt)
            conn.commit()
        messages.append('✓ Schema recreated')
    except Exception as e:
        return jsonify({'ok': False, 'messages': [f'Schema error: {e}']}), 500

    for filename, query in INSERT_OPS:
        messages.append(insert_csv(filename, query))

    _library_id_cache = None
    _append_job_history('reset', 'done', ' · '.join(messages))
    return jsonify({'ok': True, 'messages': messages})

# ── Public routes ──────────────────────────────────────────────────────────────

@app.route('/')
def home():
    lcpl_id, broward_id = _get_library_ids()
    return render_template('index.html',
                           LCPL_LIBRARY_ID=lcpl_id,
                           BROWARD_LIBRARY_ID=broward_id)

# ── API ────────────────────────────────────────────────────────────────────────

@app.route('/api/stats')
def api_stats():
    try:
        volumes      = execute_query('SELECT COUNT(*) AS n FROM availability', fetch_all=False)
        titles       = execute_query('SELECT COUNT(*) AS n FROM manga', fetch_all=False)
        last_scraped_msg = 'Never scraped'
        for log in reversed(_read_job_history()):
            if log['job'] in ('scrape', 'scrape_broward') and log['status'] == 'done':
                try:
                    dt = datetime.fromisoformat(log['at'])
                    last_scraped_msg = dt.strftime('Scraped %b %-d at %-I:%M %p')
                    break
                except ValueError:
                    pass
        return jsonify({
            'volumes':      volumes['n'] if volumes else 0,
            'titles':       titles['n']  if titles  else 0,
            'last_scraped': last_scraped_msg,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/suggestions')
def api_suggestions():
    q = request.args.get('q', '').strip()
    if len(q) < 2:
        return jsonify([])
    ip = _client_ip()
    if _rate_limited(f'suggest:{ip}', limit=120, window=60):
        return jsonify([]), 429
    try:
        rows = execute_query(
            'SELECT Title, Type, Score FROM manga WHERE Title LIKE %s '
            'ORDER BY Score DESC LIMIT 8',
            (f'%{q}%',),
        )
        all_manga  = execute_query("SELECT MangaID, Title FROM manga ORDER BY MangaID")
        index_map  = {r['Title']: i + 1 for i, r in enumerate(all_manga)}
        return jsonify([{
            'title': r['Title'],
            'type':  r['Type'],
            'score': str(r['Score'] or ''),
            'index': index_map.get(r['Title']),
        } for r in rows])
    except Exception:
        return jsonify([])

@app.route('/api/job/<name>')
@admin_required
def api_job_status(name):
    if name not in _JOB_NAMES:
        return jsonify({'ok': False, 'message': 'unknown job'}), 400
    with _jobs_lock:
        job = _jobs.get(name)
    if not job:
        return jsonify({'running': False, 'progress': 0, 'message': ''})
    return jsonify({
        'running':  job.get('running', False),
        'progress': job.get('progress', 0),
        'message':  job.get('message', ''),
    })

@app.route('/api/job/stop/<name>', methods=['POST'])
@admin_required
def api_job_stop(name):
    if name not in _JOB_NAMES:
        return jsonify({'ok': False, 'message': 'unknown job'}), 400
    stopped = _stop_job(name)
    return jsonify({
        'ok':      stopped,
        'message': 'stop signal sent' if stopped else 'job not running',
    })

@app.route('/api/job_history')
@admin_required
def api_job_history():
    return jsonify(list(reversed(_read_job_history()))[:50])

@app.route('/api/missing_titles')
@admin_required
def api_missing_titles():
    total = _titles_count()
    return jsonify({
        'count':         len(_missing_indices('%Leon%')),
        'broward_count': len(_missing_indices('%Broward%')),
        'total_titles':  total,
    })

@app.route('/api/title_index')
@admin_required
def api_title_index():
    title = request.args.get('title', '').strip()
    if not title:
        return jsonify({'ok': False, 'message': 'No title provided'}), 400
    try:
        rows = execute_query("SELECT MangaID, Title FROM manga ORDER BY MangaID")
        for i, r in enumerate(rows):
            if r['Title'] == title:
                return jsonify({'ok': True, 'index': i + 1, 'manga_id': r['MangaID']})
        return jsonify({'ok': False, 'index': None, 'message': 'Title not found'}), 404
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500

@app.route('/api/delete_title_results', methods=['POST'])
@admin_required
def api_delete_title_results():
    # CSRF check via header (JS sends it)
    token = request.headers.get('X-CSRF-Token', '')
    if not secrets.compare_digest(token, _csrf_token()):
        return jsonify({'ok': False, 'message': 'Invalid CSRF token'}), 403

    data   = request.get_json()
    title  = (data or {}).get('title', '').strip()
    lib_id = (data or {}).get('library')
    if not title:
        return jsonify({'ok': False, 'message': 'No title provided'}), 400
    try:
        rows = execute_query('SELECT MangaID FROM manga WHERE Title = %s', (title,))
        if not rows:
            return jsonify({'ok': False, 'message': 'Title not found'}), 404
        manga_id = rows[0]['MangaID']
        if lib_id:
            execute_update("""
                DELETE bas FROM branch_availability_status bas
                JOIN availability a ON bas.AvailabilityID = a.AvailabilityID
                JOIN branch b ON bas.BranchID = b.BranchID
                WHERE a.MangaID = %s AND b.LibraryID = %s
            """, (manga_id, lib_id))
            execute_update("""
                DELETE a FROM availability a
                LEFT JOIN branch_availability_status bas
                    ON a.AvailabilityID = bas.AvailabilityID
                WHERE a.MangaID = %s AND bas.AvailabilityID IS NULL
            """, (manga_id,))
        else:
            execute_update('DELETE FROM availability WHERE MangaID = %s', (manga_id,))
        return jsonify({'ok': True, 'message': f'Cleared availability for "{title}"'})
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500

@app.route('/api/title_volumes/<int:manga_id>')
@admin_required
def api_title_volumes(manga_id):
    try:
        rows = execute_query("""
            SELECT a.Volume, a.AvailabilityID,
                   GROUP_CONCAT(
                       CONCAT(b.BranchName, ': ', bas.Status)
                       ORDER BY b.BranchName SEPARATOR ' | '
                   ) AS branches
            FROM availability a
            LEFT JOIN branch_availability_status bas
                ON a.AvailabilityID = bas.AvailabilityID
            LEFT JOIN branch b ON bas.BranchID = b.BranchID
            WHERE a.MangaID = %s
            GROUP BY a.AvailabilityID, a.Volume
            ORDER BY a.Volume
        """, (manga_id,))
        return jsonify(rows)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ── MAL manga list proxy ───────────────────────────────────────────────────────
# The user's MAL access token is in the server .env — the browser never sees it.
# The frontend fetches /api/mal/mangalist which proxies the MAL API server-side.

@app.route('/api/mal/mangalist')
def api_mal_mangalist():
    """
    Proxy the authenticated user's MAL manga list.
    Returns {manga_id: status} for all entries so the frontend can filter.
    Supports pagination internally (MAL max 1000 per request).
    """
    ip = _client_ip()
    if _rate_limited(f'mal:{ip}', limit=10, window=60):
        return jsonify({'ok': False, 'message': 'Rate limited'}), 429

    import requests as req
    access_token = os.getenv('MAL_ACCESS_TOKEN', '')
    if not access_token:
        return jsonify({'ok': False, 'message': 'MAL access token not configured'}), 503

    all_statuses: dict[int, dict] = {}
    offset = 0
    limit  = 1000
    max_pages = 10   # safety cap — 10,000 entries

    for _ in range(max_pages):
        url = (f"https://api.myanimelist.net/v2/users/@me/mangalist"
               f"?fields=list_status&limit={limit}&offset={offset}")
        try:
            resp = req.get(url, headers={'Authorization': f'Bearer {access_token}'},
                           timeout=15)
        except Exception as e:
            return jsonify({'ok': False, 'message': f'MAL API error: {e}'}), 502

        if resp.status_code == 401:
            # Try token refresh
            from services.mal_client import refresh_tokens
            new_token = refresh_tokens()
            if not new_token:
                return jsonify({'ok': False, 'message': 'MAL token expired and refresh failed'}), 401
            access_token = new_token
            resp = req.get(url, headers={'Authorization': f'Bearer {access_token}'},
                           timeout=15)
            if resp.status_code != 200:
                return jsonify({'ok': False, 'message': f'MAL API error {resp.status_code}'}), 502

        if resp.status_code != 200:
            return jsonify({'ok': False, 'message': f'MAL API error {resp.status_code}'}), 502

        data = resp.json()
        for item in data.get('data', []):
            node       = item.get('node', {})
            mal_id     = node.get('id')
            lst_status = item.get('list_status', {})
            if mal_id:
                all_statuses[mal_id] = {
                    'status':            lst_status.get('status', ''),
                    'score':             lst_status.get('score', 0),
                    'num_volumes_read':  lst_status.get('num_volumes_read', 0),
                }

        # Check if there's a next page
        paging = data.get('paging', {})
        if not paging.get('next'):
            break
        offset += limit

    return jsonify({'ok': True, 'data': all_statuses})

# ── Search ─────────────────────────────────────────────────────────────────────

ON_SHELF = ('Graphic Novel', 'Youth Fiction', 'Adult Non-Fiction', 'Available',
            'General Collection', 'New Materials')

def _normalize_status(raw: str) -> str:
    if not raw:
        return 'Checked Out'
    if any(kw in raw for kw in ON_SHELF):
        return 'Available'
    if 'hold' in raw.lower():
        return 'On Hold'
    return 'Checked Out'


@app.route('/search')
def search():
    ip = _client_ip()
    if _rate_limited(f'search:{ip}', limit=60, window=60):
        return 'Too many requests', 429

    LCPL_LIBRARY_ID, BROWARD_LIBRARY_ID = _get_library_ids()

    title        = request.args.get('title',    '').strip()
    type_        = request.args.get('type',     '').strip()
    branch       = request.args.get('branch',   '').strip()
    volume       = request.args.get('volume',   '').strip()
    avail_filter = request.args.get('avail',    '').strip()
    lib_filter   = request.args.get('library',  '').strip()
    no_vol1      = request.args.get('no_vol1',  '').strip()  # exclude titles missing vol 1

    conditions = ['b.BranchName IS NOT NULL', 'b.BranchID IS NOT NULL']
    params = []
    if title:      conditions.append('m.Title LIKE %s');    params.append(f'%{title}%')
    if type_:      conditions.append('m.Type = %s');        params.append(type_)
    if volume:     conditions.append('a.Volume = %s');      params.append(volume)
    if branch:     conditions.append('b.BranchName = %s'); params.append(branch)
    if lib_filter: conditions.append('b.LibraryID = %s');  params.append(int(lib_filter))

    sql = f"""
        SELECT m.MangaID, m.Title, a.Volume, m.Volumes, m.Type,
               m.Members, m.Score, m.Author, m.CoverMedium,
               b.BranchName, b.BranchID, bas.Status, b.LibraryID, l.LibraryName
        FROM manga m
        JOIN availability a                  ON m.MangaID = a.MangaID
        JOIN branch_availability_status bas  ON a.AvailabilityID = bas.AvailabilityID
        JOIN branch b                        ON bas.BranchID = b.BranchID
        JOIN library l                       ON b.LibraryID = l.LibraryID
        WHERE {' AND '.join(conditions)}
        ORDER BY m.Score DESC, a.Volume ASC, b.BranchName ASC
    """
    rows = execute_query(sql, params)

    # ── Group: title → library → volume → {branch_id: status} ────────────────
    titles_map: dict = {}
    for r in rows:
        t   = r['Title']
        lid = r['LibraryID']
        bid = r['BranchID']
        if lid is None or bid is None:
            continue

        if t not in titles_map:
            titles_map[t] = {
                'MangaID':  r['MangaID'],  'Title': t,
                'Volumes':  r['Volumes'],  'Type':  r['Type'],
                'Members':  r['Members'],  'Score': r['Score'],
                'author':   r.get('Author') or '',
                'cover':    r.get('CoverMedium') or '',
                'lib_data': {},
                'has_lcpl': False, 'has_broward': False,
            }

        td = titles_map[t]
        td['lib_data'].setdefault(lid, {
            'library_id':   lid,
            'library_name': (
                'Broward County Library'
                if lid == BROWARD_LIBRARY_ID
                else 'Leon County Public Library'
            ),
            'volumes':      {},
            'branch_names': {},
        })
        ld = td['lib_data'][lid]

        vol         = r['Volume'] if r['Volume'] is not None else 0
        norm_status = _normalize_status(r.get('Status') or '')

        ld['volumes'].setdefault(vol, {})
        current = ld['volumes'][vol].get(bid)
        if current is None or STATUS_PRIORITY[norm_status] > STATUS_PRIORITY.get(current, -1):
            ld['volumes'][vol][bid] = norm_status

        ld['branch_names'][bid] = r['BranchName']

        if lid == BROWARD_LIBRARY_ID: td['has_broward'] = True
        elif lid == LCPL_LIBRARY_ID:  td['has_lcpl']    = True

    # ── Build final grouped list ───────────────────────────────────────────────
    grouped = []
    for td in titles_map.values():
        avail_count = out_count = hold_count = 0
        has_vol1 = False
        lib_list = []

        for linfo in sorted(td['lib_data'].values(), key=lambda x: x['library_id']):
            branch_best: dict[int, str] = {}
            vol_list = []

            for vol_num in sorted(linfo['volumes'].keys()):
                if vol_num == 1:
                    has_vol1 = True
                branch_statuses = linfo['volumes'][vol_num]
                vol_branches = []
                for bid in sorted(branch_statuses.keys(),
                                  key=lambda b: linfo['branch_names'].get(b, '')):
                    status = branch_statuses[bid]
                    bname  = linfo['branch_names'].get(bid, '')
                    vol_branches.append({
                        'name':   bname,
                        'short':  _branch_short(bname, linfo['library_id'], BROWARD_LIBRARY_ID),
                        'status': status,
                    })
                    if status == 'Available':  avail_count += 1
                    elif status == 'On Hold':  hold_count  += 1
                    else:                      out_count   += 1

                    cur_best = branch_best.get(bid)
                    if (cur_best is None or
                            STATUS_PRIORITY[status] > STATUS_PRIORITY.get(cur_best, -1)):
                        branch_best[bid] = status

                vol_list.append({'vol': vol_num, 'branches': vol_branches})

            branch_list = [{
                'name':   linfo['branch_names'].get(bid, ''),
                'short':  _branch_short(
                    linfo['branch_names'].get(bid, ''),
                    linfo['library_id'], BROWARD_LIBRARY_ID,
                ),
                'status': branch_best[bid],
            } for bid in sorted(branch_best.keys(),
                                 key=lambda b: linfo['branch_names'].get(b, ''))]

            lib_list.append({
                'library_id':   linfo['library_id'],
                'library_name': linfo['library_name'],
                'branch_list':  branch_list,
                'vol_list':     vol_list,
            })

        # Apply filters in Python after normalization
        if avail_filter == 'available' and avail_count == 0:
            continue
        if avail_filter == 'out' and out_count == 0:
            continue
        if no_vol1 == '1' and not has_vol1:
            continue

        manga_type = td.get('Type', '')
        author     = td.get('author', '')
        title_str  = td['Title']

        grouped.append({
            'MangaID':      td['MangaID'],
            'Title':        title_str,
            'Volumes':      td['Volumes'],
            'Type':         manga_type,
            'Members':      td['Members'],
            'Score':        td['Score'],
            'author':       author,
            'cover':        td['cover'],
            'has_lcpl':     td['has_lcpl'],
            'has_broward':  td['has_broward'],
            'lib_list':     lib_list,
            'vol_count':    len({v for linfo in td['lib_data'].values() for v in linfo['volumes']}),
            'avail_count':  avail_count,
            'out_count':    out_count,
            'hold_count':   hold_count,
            # Pre-build catalog URLs (used by template for the main link and per-volume links)
            'lcpl_url':     _lcpl_search_url(title_str, author),
            'broward_url':  _broward_search_url(title_str, author, manga_type),
            'is_novel':     _is_novel(manga_type),
        })

    filters = {
        'title': title, 'type': type_, 'branch': branch,
        'volume': volume, 'avail': avail_filter,
        'library': lib_filter, 'no_vol1': no_vol1,
    }
    return render_template(
        'results.html',
        results=grouped,
        count=len(grouped),
        filters=filters,
        has_filters=any(v for k, v in filters.items()),
        LCPL_LIBRARY_ID=LCPL_LIBRARY_ID,
        BROWARD_LIBRARY_ID=BROWARD_LIBRARY_ID,
    )

# ── Jinja2 filters ─────────────────────────────────────────────────────────────

_COVER_PALETTES = [
    ('hsl(260,40%,14%)', 'hsl(260,60%,55%)'),
    ('hsl(340,40%,13%)', 'hsl(340,60%,55%)'),
    ('hsl(200,45%,12%)', 'hsl(200,65%,50%)'),
    ('hsl(30, 50%,13%)', 'hsl(30, 70%,55%)'),
    ('hsl(150,40%,12%)', 'hsl(150,55%,46%)'),
    ('hsl(290,35%,14%)', 'hsl(290,55%,58%)'),
    ('hsl(10, 45%,13%)', 'hsl(10, 65%,55%)'),
    ('hsl(220,42%,14%)', 'hsl(220,62%,58%)'),
]

@app.template_filter('cover_gradient')
def cover_gradient_filter(manga_id):
    idx = int(manga_id or 0) % len(_COVER_PALETTES)
    base, accent = _COVER_PALETTES[idx]
    return (f'background: linear-gradient(160deg, {base} 0%, '
            f'color-mix(in srgb, {accent} 18%, {base}) 100%);')

app.jinja_env.filters['urlencode'] = lambda s: _quote_plus(str(s or ''))

if __name__ == '__main__':
    app.run(debug=False, host='127.0.0.1', port=5000)
