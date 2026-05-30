from flask import Flask, render_template, request, jsonify, redirect, url_for, session
import csv, json, sys, threading, functools, os
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent))
from config.settings import DB_CONFIG, DATA_DIR, SCRIPTS_DIR
from utils.database_utils import get_db_connection, execute_query, execute_update

app = Flask(__name__, static_folder='static')
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'manga-tracker-dev-secret-change-me')
ADMIN_PASSWORD   = os.getenv('ADMIN_PASSWORD', '')
JOB_HISTORY_PATH = DATA_DIR / "job_history.json"

# ── Admin auth ─────────────────────────────────────────────────────────────────
def admin_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if session.get('admin'):
            return f(*args, **kwargs)
        if not ADMIN_PASSWORD and request.remote_addr in ('127.0.0.1', '::1', 'localhost'):
            return f(*args, **kwargs)
        return redirect(url_for('admin_login', next=request.url))
    return decorated

# ── Job runner ─────────────────────────────────────────────────────────────────
# Lightweight in-process job system: each job runs in a daemon thread.
# State is kept in _jobs dict; persisted to job_history.json on completion.

_jobs: dict = {}          # name → {running, progress, message, thread}
_jobs_lock = threading.Lock()

_JOB_NAMES = {'scrape', 'scrape_broward', 'get_manga'}

def _run_subprocess(job_name: str, cmd: list) -> None:
    """Run cmd in a subprocess, stream stdout to job message, update progress."""
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
        # Parse "[N/total]" style lines for progress
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
    """Start a job thread. Returns (ok, http_status, message)."""
    with _jobs_lock:
        if _jobs.get(job_name, {}).get('running'):
            return False, 409, f'{job_name} is already running'
        _jobs[job_name] = {'running': False, 'progress': 0, 'message': '', 'stop_requested': False}

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

# ── Job history (JSON file) ────────────────────────────────────────────────────

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
    history.append({'job': job, 'status': status, 'message': message,
                    'at': datetime.utcnow().isoformat()})
    try:
        JOB_HISTORY_PATH.write_text(
            json.dumps(history[-200:], indent=2, ensure_ascii=False), encoding='utf-8'
        )
    except Exception:
        pass

# ── Helpers ────────────────────────────────────────────────────────────────────

def parse_range_str(s: str, max_titles: int = 9999) -> tuple:
    import re
    s = s.strip().replace('\u2013', '-').replace('\u2014', '-')  # normalize en/em-dash
    if not s: return 1, 1
    if s.isdigit(): return 1, int(s)
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

def _get_titles_without_lcpl_availability() -> list:
    try:
        manga_ids = [r['MangaID'] for r in execute_query(
            "SELECT MangaID FROM manga ORDER BY MangaID")]
        scraped = {r['MangaID'] for r in execute_query("""
            SELECT DISTINCT a.MangaID FROM availability a
            JOIN branch_availability_status bas ON a.AvailabilityID = bas.AvailabilityID
            JOIN branch b ON bas.BranchID = b.BranchID
            JOIN library l ON b.LibraryID = l.LibraryID
            WHERE l.LibraryName LIKE '%Leon%'
        """)}
        return [i + 1 for i, mid in enumerate(manga_ids) if mid not in scraped]
    except Exception:
        return []

def _get_titles_without_broward_availability() -> list:
    try:
        manga_ids = [r['MangaID'] for r in execute_query(
            "SELECT MangaID FROM manga ORDER BY MangaID")]
        scraped = {r['MangaID'] for r in execute_query("""
            SELECT DISTINCT a.MangaID FROM availability a
            JOIN branch_availability_status bas ON a.AvailabilityID = bas.AvailabilityID
            JOIN branch b ON bas.BranchID = b.BranchID
            JOIN library l ON b.LibraryID = l.LibraryID
            WHERE l.LibraryName LIKE '%Broward%'
        """)}
        return [i + 1 for i, mid in enumerate(manga_ids) if mid not in scraped]
    except Exception:
        return []

def _remove_from_unrecognized(title: str) -> None:
    path = DATA_DIR / 'unrecognized_authors.json'
    if not path.exists():
        return
    try:
        entries = json.loads(path.read_text(encoding='utf-8'))
        entries = [e for e in entries if e.get('title') != title]
        path.write_text(json.dumps(entries, indent=2, ensure_ascii=False), encoding='utf-8')
    except Exception:
        pass

# ── DB reset ───────────────────────────────────────────────────────────────────

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
    ("manga.csv",     "INSERT INTO manga (MangaID, Title, Type, Volumes, Members, Score, Author, CoverMedium, CoverLarge) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"),
    ("libraries.csv", "INSERT INTO library (LibraryName, `URL`) VALUES (%s,%s)"),
    ("branches.csv",  "INSERT INTO branch (BranchName, `Address`, LibraryID) VALUES (%s,%s,%s)"),
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

# ── Library ID cache (cleared on reset) ───────────────────────────────────────
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
            if 'LeRoy Collins' in name:
                lcpl = r['LibraryID']
            elif 'Broward' in name:
                broward = r['LibraryID']
        if lcpl is not None and broward is not None:
            _library_id_cache = (lcpl, broward)
            return _library_id_cache
    except Exception:
        pass
    return 1, 2

# ── Admin auth routes ──────────────────────────────────────────────────────────

@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    if not ADMIN_PASSWORD:
        session['admin'] = True
        return redirect(request.args.get('next') or url_for('admin'))
    error = None
    if request.method == 'POST':
        pwd = request.form.get('password', '')
        if pwd == ADMIN_PASSWORD:
            session['admin'] = True
            return redirect(request.args.get('next') or url_for('admin'))
        error = 'Invalid password'
    return render_template('admin_login.html', error=error)

@app.route('/admin/logout')
def admin_logout():
    session.pop('admin', None)
    return redirect('/')

# ── Admin routes ───────────────────────────────────────────────────────────────

@app.route('/admin', methods=['GET', 'POST'])
@admin_required
def admin():
    if request.method == 'POST':
        action = request.form.get('action')

        if action == 'update':
            execute_update('UPDATE manga SET Volumes = %s WHERE Title = %s',
                           (request.form.get('volume'), request.form.get('title')))
            return jsonify({'ok': True, 'message': 'Volumes updated'})

        elif action == 'delete':
            execute_update(
                'DELETE FROM availability WHERE MangaID = '
                '(SELECT MangaID FROM manga WHERE Title = %s) AND Volume = %s',
                (request.form.get('title'), request.form.get('volume')))
            return jsonify({'ok': True, 'message': 'Volume deleted'})

        elif action == 'scrape':
            range_str = request.form.get('range', '').strip()
            only_new  = request.form.get('only_new') == '1'
            if only_new:
                missing = _get_titles_without_lcpl_availability()
                if not missing:
                    return jsonify({'ok': False, 'message': 'All titles already have LCPL data'}), 400
                if range_str:
                    lo, hi = parse_range_str(range_str, _titles_count())
                    missing = [idx for idx in missing if lo <= idx <= hi]
                    if not missing:
                        return jsonify({'ok': False, 'message': f'No new titles in range {range_str}'}), 400
                cmd = [sys.executable, str(SCRIPTS_DIR / 'scrapper.py'),
                       '--indices', ','.join(map(str, missing))]
            else:
                if range_str:
                    start, end = parse_range_str(range_str, _titles_count())
                else:
                    start, end = 1, 1
                cmd = [sys.executable, str(SCRIPTS_DIR / 'scrapper.py'),
                       '--range', f'{start}-{end}']
            ok, status, msg = _start_job('scrape', cmd)
            return jsonify({'ok': ok, 'message': msg}), status

        elif action == 'scrape_broward':
            range_str = request.form.get('range', '').strip()
            only_new  = request.form.get('only_new') == '1'
            if only_new:
                missing = _get_titles_without_broward_availability()
                if not missing:
                    return jsonify({'ok': False, 'message': 'All titles already have Broward data'}), 400
                if range_str:
                    lo, hi = parse_range_str(range_str, _titles_count())
                    missing = [idx for idx in missing if lo <= idx <= hi]
                    if not missing:
                        return jsonify({'ok': False, 'message': f'No new titles in range {range_str}'}), 400
                cmd = [sys.executable, str(SCRIPTS_DIR / 'broward_scrapper.py'),
                       '--indices', ','.join(map(str, missing))]
            else:
                cmd = [sys.executable, str(SCRIPTS_DIR / 'broward_scrapper.py')]
                if range_str:
                    lo, hi = parse_range_str(range_str, _titles_count())
                    cmd.extend(['--range', f'{lo}-{hi}'])
            ok, status, msg = _start_job('scrape_broward', cmd)
            return jsonify({'ok': ok, 'message': msg}), status

        elif action == 'get_manga':
            offset = request.form.get('offset', '0')
            cmd = [sys.executable, str(SCRIPTS_DIR / 'get_manga.py'), offset]
            ok, status, msg = _start_job('get_manga', cmd)
            return jsonify({'ok': ok, 'message': msg}), status

        return jsonify({'ok': False, 'message': 'Unknown action'}), 400

    # GET
    manga_per_library = execute_query("""
        SELECT l.LibraryName, b.BranchName,
               COUNT(DISTINCT CONCAT(a.MangaID,'-',a.Volume)) AS VolumeCount
        FROM branch b
        JOIN library l ON b.LibraryID = l.LibraryID
        JOIN branch_availability_status bas ON b.BranchID = bas.BranchID
        JOIN availability a ON bas.AvailabilityID = a.AvailabilityID
        GROUP BY l.LibraryName, b.BranchName
        HAVING COUNT(DISTINCT CONCAT(a.MangaID,'-',a.Volume)) > 10
        ORDER BY l.LibraryName, VolumeCount DESC
    """)
    job_history = list(reversed(_read_job_history()))[:30]
    return render_template('admin.html',
                           manga_per_library=manga_per_library,
                           job_history=job_history)

@app.route('/admin/reset', methods=['POST'])
@admin_required
def admin_reset():
    global _library_id_cache
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

    # Clear library ID cache so next request re-reads from DB
    _library_id_cache = None

    _append_job_history('reset', 'done', ' · '.join(messages))
    return jsonify({'ok': True, 'messages': messages})

# ── Public routes ──────────────────────────────────────────────────────────────

@app.route('/')
def home():
    return render_template('index.html')

# ── API routes ─────────────────────────────────────────────────────────────────

@app.route('/api/stats')
def api_stats():
    try:
        volumes = execute_query('SELECT COUNT(*) AS n FROM availability', fetch_all=False)
        titles  = execute_query('SELECT COUNT(*) AS n FROM manga', fetch_all=False)
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
            'volumes': volumes['n'] if volumes else 0,
            'titles':  titles['n']  if titles  else 0,
            'last_scraped': last_scraped_msg,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/suggestions')
def api_suggestions():
    q = request.args.get('q', '').strip()
    if len(q) < 2:
        return jsonify([])
    try:
        rows = execute_query(
            'SELECT Title, Type, Score FROM manga WHERE Title LIKE %s ORDER BY Score DESC LIMIT 8',
            (f'%{q}%',))
        return jsonify([{'title': r['Title'], 'type': r['Type'],
                         'score': str(r['Score'] or '')} for r in rows])
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
    return jsonify({'ok': stopped, 'message': 'stop signal sent' if stopped else 'job not running'})

@app.route('/api/job_history')
@admin_required
def api_job_history():
    return jsonify(list(reversed(_read_job_history()))[:50])

@app.route('/api/missing_titles')
@admin_required
def api_missing_titles():
    missing_lcpl    = _get_titles_without_lcpl_availability()
    missing_broward = _get_titles_without_broward_availability()
    return jsonify({
        'count':           len(missing_lcpl),
        'indices':         missing_lcpl[:20],
        'broward_count':   len(missing_broward),
        'broward_indices': missing_broward[:20],
    })

@app.route('/api/delete_title_results', methods=['POST'])
@admin_required
def api_delete_title_results():
    data  = request.get_json()
    title = (data or {}).get('title', '').strip()
    if not title:
        return jsonify({'ok': False, 'message': 'No title provided'}), 400
    try:
        rows = execute_query('SELECT MangaID FROM manga WHERE Title = %s', (title,))
        if not rows:
            return jsonify({'ok': False, 'message': 'Title not found'}), 404
        execute_update('DELETE FROM availability WHERE MangaID = %s', (rows[0]['MangaID'],))
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
            LEFT JOIN branch_availability_status bas ON a.AvailabilityID = bas.AvailabilityID
            LEFT JOIN branch b ON bas.BranchID = b.BranchID
            WHERE a.MangaID = %s
            GROUP BY a.AvailabilityID, a.Volume
            ORDER BY a.Volume
        """, (manga_id,))
        return jsonify(rows)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/unrecognized_authors')
@admin_required
def api_unrecognized_authors():
    path = DATA_DIR / 'unrecognized_authors.json'
    if not path.exists():
        return jsonify([])
    try:
        return jsonify(json.loads(path.read_text(encoding='utf-8')))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/add_valid_author', methods=['POST'])
@admin_required
def api_add_valid_author():
    data    = request.get_json()
    surname = (data or {}).get('surname', '').strip()
    title   = (data or {}).get('title',   '').strip()
    if not surname:
        return jsonify({'ok': False, 'message': 'No surname provided'}), 400

    valid_path = DATA_DIR / 'valid_authors.txt'
    existing   = set()
    if valid_path.exists():
        existing = {l.strip().lower()
                    for l in valid_path.read_text(encoding='utf-8').splitlines() if l.strip()}
    if surname.lower() not in existing:
        with open(valid_path, 'a', encoding='utf-8') as f:
            f.write(surname + '\n')

    if title:
        execute_update('UPDATE manga SET Author = %s WHERE Title = %s', (surname, title))
        _remove_from_unrecognized(title)
        return jsonify({'ok': True, 'message': f'Set author for "{title}" → {surname}'})
    return jsonify({'ok': True, 'message': f'Added {surname} to valid_authors.txt'})

# ── Search ─────────────────────────────────────────────────────────────────────

ON_SHELF = ('Graphic Novel', 'Youth Fiction', 'Adult Non-Fiction', 'Available')

@app.route('/search')
def search():
    LCPL_LIBRARY_ID, BROWARD_LIBRARY_ID = _get_library_ids()

    title        = request.args.get('title',   '')
    type_        = request.args.get('type',    '')
    branch       = request.args.get('branch',  '')
    volume       = request.args.get('volume',  '')
    avail_filter = request.args.get('avail',   '')
    lib_filter   = request.args.get('library', '')

    sql = """
        SELECT m.MangaID, m.Title, a.Volume, m.Volumes, m.Type,
               m.Members, m.Score, m.Author, m.CoverMedium,
               b.BranchName, bas.Status, b.LibraryID, l.LibraryName
        FROM manga m
        LEFT JOIN availability a                 ON m.MangaID = a.MangaID
        LEFT JOIN branch_availability_status bas ON a.AvailabilityID = bas.AvailabilityID
        LEFT JOIN branch b                       ON bas.BranchID = b.BranchID
        LEFT JOIN library l                      ON b.LibraryID = l.LibraryID
        WHERE b.BranchName IS NOT NULL
    """
    params = []
    if title:  sql += ' AND m.Title LIKE %s';  params.append(f'%{title}%')
    if type_:  sql += ' AND m.Type = %s';      params.append(type_)
    if volume: sql += ' AND a.Volume = %s';    params.append(volume)
    if branch: sql += ' AND b.BranchName = %s'; params.append(branch)
    if lib_filter:
        sql += ' AND b.LibraryID = %s'; params.append(lib_filter)
    if avail_filter == 'out':
        sql += " AND bas.Status = 'Checked Out'"
    sql += ' ORDER BY m.Score DESC, a.Volume ASC'

    rows = execute_query(sql, params)

    # Filter available-only in Python (status strings vary)
    if avail_filter == 'available':
        rows = [r for r in rows if any(kw in (r.get('Status') or '') for kw in ON_SHELF)]

    # Group rows by title
    titles_map: dict = {}
    for r in rows:
        t = r['Title']
        if t not in titles_map:
            titles_map[t] = {
                'MangaID':     r['MangaID'],
                'Title':       t,
                'Volumes':     r['Volumes'],
                'Type':        r['Type'],
                'Members':     r['Members'],
                'Score':       r['Score'],
                'author':      r.get('Author') or '',
                'cover':       r.get('CoverMedium') or '',
                'volumes':     {},
                'has_lcpl':    False,
                'has_broward': False,
            }
        vol    = r['Volume']
        lib_id = r.get('LibraryID')
        titles_map[t]['volumes'].setdefault(vol, []).append({
            'name':   r['BranchName'],
            'status': r.get('Status') or '',
            'lib_id': lib_id,
        })
        if lib_id == BROWARD_LIBRARY_ID:
            titles_map[t]['has_broward'] = True
        elif lib_id == LCPL_LIBRARY_ID:
            titles_map[t]['has_lcpl'] = True

    grouped = []
    for td in titles_map.values():
        lib_data: dict = {}
        avail_count = out_count = hold_count = 0

        for vol, branches in td['volumes'].items():
            for b in branches:
                lid = b['lib_id']
                lib_data.setdefault(lid, {
                    'library_id':   lid,
                    'library_name': (
                        'Broward County Library' if lid == BROWARD_LIBRARY_ID
                        else 'Leon County Public Library'
                    ),
                    'vol_map': {},
                })
                lib_data[lid]['vol_map'].setdefault(vol, []).append(
                    {'name': b['name'], 'status': b['status']}
                )
                s = b['status']
                if 'Checked Out' in s:    out_count   += 1
                elif 'hold' in s.lower(): hold_count  += 1
                elif s:                   avail_count += 1

        lib_list = sorted([
            {
                'library_id':   linfo['library_id'],
                'library_name': linfo['library_name'],
                'vol_list': [
                    {'volume': v, 'branches': brs}
                    for v, brs in sorted(
                        linfo['vol_map'].items(),
                        key=lambda x: (x[0] is None, x[0])
                    )
                ],
            }
            for linfo in lib_data.values()
        ], key=lambda x: x['library_id'])

        grouped.append({
            **td,
            'lib_list':    lib_list,
            'vol_count':   len(td['volumes']),
            'avail_count': avail_count,
            'out_count':   out_count,
            'hold_count':  hold_count,
        })

    filters = {
        'title': title, 'type': type_, 'branch': branch,
        'volume': volume, 'avail': avail_filter, 'library': lib_filter,
    }
    return render_template(
        'results.html',
        results=grouped,
        count=len(grouped),
        filters=filters,
        has_filters=any(filters.values()),
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

from urllib.parse import quote_plus as _quote_plus
app.jinja_env.filters['urlencode'] = lambda s: _quote_plus(str(s or ''))

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5000)
