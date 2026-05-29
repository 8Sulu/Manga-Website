from flask import Flask, render_template, request, jsonify, redirect, url_for, session
import csv, json, subprocess, sys, time, threading, functools, os
from collections import defaultdict
from pathlib import Path
from datetime import datetime, timezone
import mysql.connector

sys.path.append(str(Path(__file__).parent.parent))
from config.settings import DB_CONFIG, DATA_DIR, SCRIPTS_DIR
from utils.database_utils import get_db_connection, execute_query, execute_update

app = Flask(__name__, static_folder='static')
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'manga-tracker-dev-secret-change-me')
ADMIN_PASSWORD  = os.getenv('ADMIN_PASSWORD', '')
SCRAPE_LOG_PATH  = DATA_DIR / "scrape_log.json"
JOB_HISTORY_PATH = DATA_DIR / "job_history.json"

# ── Admin auth ────────────────────────────────────────────────────────────────
def admin_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if session.get('admin'):
            return f(*args, **kwargs)
        if not ADMIN_PASSWORD and request.remote_addr in ('127.0.0.1', '::1', 'localhost'):
            return f(*args, **kwargs)
        return redirect(url_for('admin_login', next=request.url))
    return decorated

# ── Job state ─────────────────────────────────────────────────────────────────
_jobs        = {}
_jobs_lock   = threading.Lock()
_procs       = {}
_procs_lock  = threading.Lock()
_start_locks = {
    'scrape':    threading.Lock(),
    'get_manga': threading.Lock(),
    'scrape_broward': threading.Lock(),
}

def _job_start(name, message='starting…'):
    with _jobs_lock:
        _jobs[name] = {'running': True, 'progress': 0, 'message': message, 'started_at': time.time()}

def _job_update(name, progress, message):
    with _jobs_lock:
        if name in _jobs:
            _jobs[name].update({'progress': progress, 'message': message})

def _job_done(name, message='done'):
    with _jobs_lock:
        if name in _jobs:
            _jobs[name].update({'running': False, 'progress': 100, 'message': message})
    _append_job_history(name, 'done', message)

def _job_error(name, message):
    with _jobs_lock:
        _jobs[name] = {'running': False, 'progress': 0, 'message': message}
    _append_job_history(name, 'error', message)

def _run_script(job_name, cmd, post_hook=None):
    _job_start(job_name)
    try:
        import re
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        with _procs_lock:
            _procs[job_name] = proc
        for line in proc.stdout:
            line = line.strip()
            m = re.search(r'\[(\d+)[/\s]+(\d+)\]', line)
            if m:
                cur, tot = int(m.group(1)), int(m.group(2))
                _job_update(job_name, int(cur / tot * 100), line[-120:])
            elif line:
                _job_update(job_name, _jobs.get(job_name, {}).get('progress', 0), line[-120:])
        proc.wait()
        with _procs_lock:
            _procs.pop(job_name, None)
        if proc.returncode == -15:
            _job_done(job_name, 'stopped'); return
        if proc.returncode not in (0, -15):
            _job_error(job_name, f'script exited {proc.returncode}'); return
        if post_hook:
            _job_update(job_name, 99, 'updating database…')
            try:
                msg = post_hook()
                _job_done(job_name, msg or 'done')
            except Exception as e:
                _job_error(job_name, f'db error: {e}')
        else:
            _job_done(job_name)
    except Exception as e:
        _job_error(job_name, f'error: {e}')
        with _procs_lock:
            _procs.pop(job_name, None)

def _start_job_thread(name, cmd, post_hook=None):
    lock = _start_locks[name]
    if not lock.acquire(blocking=False):
        return False, 409, f'{name} is already starting'
    try:
        with _jobs_lock:
            if _jobs.get(name, {}).get('running'):
                return False, 409, f'{name} is already running'
        t = threading.Thread(target=_run_script, args=(name, cmd, post_hook), daemon=True)
        t.start()
        return True, 200, 'started'
    finally:
        lock.release()

def _stop_job(name):
    with _procs_lock:
        proc = _procs.get(name)
    if proc:
        proc.terminate()
        return True
    with _jobs_lock:
        if _jobs.get(name, {}).get('running'):
            _jobs[name].update({'running': False, 'progress': 0, 'message': 'stopped'})
            return True
    return False

# ── Job history ───────────────────────────────────────────────────────────────
_history_lock = threading.Lock()

def _read_job_history() -> list:
    try:
        if JOB_HISTORY_PATH.exists():
            return json.loads(JOB_HISTORY_PATH.read_text(encoding='utf-8'))
    except Exception:
        pass
    return []

def _append_job_history(job_name, status, message):
    with _history_lock:
        history = _read_job_history()
        history.append({
            'job': job_name,
            'status': status,
            'message': message,
            'at': datetime.now(timezone.utc).isoformat()
        })
        history = history[-100:]
        JOB_HISTORY_PATH.write_text(json.dumps(history, indent=2), encoding='utf-8')

# ── Scrape log ────────────────────────────────────────────────────────────────
_log_lock = threading.Lock()

def _read_scrape_log() -> list:
    try:
        if SCRAPE_LOG_PATH.exists():
            return json.loads(SCRAPE_LOG_PATH.read_text(encoding='utf-8'))
    except Exception:
        pass
    return []

def _append_scrape_log(start, end, message):
    with _log_lock:
        log = _read_scrape_log()
        log.append({'start': start, 'end': end,
                    'scraped_at': datetime.now(timezone.utc).isoformat(), 'message': message})
        SCRAPE_LOG_PATH.write_text(json.dumps(log, indent=2), encoding='utf-8')

def parse_range_str(s: str, max_titles: int = 9999) -> tuple:
    import re
    s = s.strip()
    if not s: return 1, 1
    if s.isdigit(): return 1, int(s)
    m = re.match(r'^(\d*)-(\d*)$', s)
    if m:
        lo = int(m.group(1)) if m.group(1) else 1
        hi = int(m.group(2)) if m.group(2) else max_titles
        return lo, hi
    return 1, 1

def _titles_count() -> int:
    p = DATA_DIR / 'titles.txt'
    if not p.exists(): return 9999
    with open(p, encoding='utf-8') as f:
        return sum(1 for l in f if l.strip())

def _get_titles_without_lcpl_availability() -> list:
    titles_path = DATA_DIR / 'titles.txt'
    if not titles_path.exists(): return []
    with open(titles_path, encoding='utf-8') as f:
        titles = [l.strip() for l in f if l.strip()]
    try:
        rows = execute_query("""
            SELECT DISTINCT a.MangaID
            FROM availability a
            JOIN branch_availability_status bas ON a.AvailabilityID = bas.AvailabilityID
            JOIN branch b ON bas.BranchID = b.BranchID
            JOIN library l ON b.LibraryID = l.LibraryID
            WHERE l.LibraryName LIKE '%Leon%'
        """)
        scraped_ids = {r['MangaID'] for r in rows}
        return [i + 1 for i in range(len(titles)) if (i + 1) not in scraped_ids]
    except Exception:
        return list(range(1, len(titles) + 1))

def _get_titles_without_broward_availability() -> list:
    titles_path = DATA_DIR / 'titles.txt'
    if not titles_path.exists(): return []
    with open(titles_path, encoding='utf-8') as f:
        titles = [l.strip() for l in f if l.strip()]
    try:
        rows = execute_query("""
            SELECT DISTINCT a.MangaID
            FROM availability a
            JOIN branch_availability_status bas ON a.AvailabilityID = bas.AvailabilityID
            JOIN branch b ON bas.BranchID = b.BranchID
            JOIN library l ON b.LibraryID = l.LibraryID
            WHERE l.LibraryName LIKE '%Broward%'
        """)
        scraped_ids = {r['MangaID'] for r in rows}
        return [i + 1 for i in range(len(titles)) if (i + 1) not in scraped_ids]
    except Exception:
        return list(range(1, len(titles) + 1))

# ── Authors helpers ───────────────────────────────────────────────────────────
def _load_lines(path: Path) -> list:
    if not path.exists(): return []
    return [l.rstrip('\n') for l in path.read_text(encoding='utf-8').splitlines()]

def _save_lines(path: Path, lines: list) -> None:
    with open(path, 'w', encoding='utf-8') as f:
        for line in lines: f.write(line + '\n')

def _remove_from_unrecognized(title: str) -> None:
    path = DATA_DIR / 'unrecognized_authors.json'
    if not path.exists(): return
    try:
        entries = json.loads(path.read_text(encoding='utf-8'))
        entries = [e for e in entries if e.get('title') != title]
        path.write_text(json.dumps(entries, indent=2, ensure_ascii=False), encoding='utf-8')
    except Exception:
        pass

# ── Schema ────────────────────────────────────────────────────────────────────
SCHEMA = """
DROP TABLE IF EXISTS branch_availability_status;
DROP TABLE IF EXISTS availability;
DROP TABLE IF EXISTS branch;
DROP TABLE IF EXISTS manga;
DROP TABLE IF EXISTS library;
CREATE TABLE manga (MangaID INT PRIMARY KEY, Title VARCHAR(255) NOT NULL, `Type` VARCHAR(50), Volumes INT, Members INT, Score DECIMAL(4,2), Author VARCHAR(255), CoverMedium VARCHAR(512), CoverLarge VARCHAR(512));
CREATE TABLE library (LibraryID INT PRIMARY KEY AUTO_INCREMENT, LibraryName VARCHAR(255) NOT NULL, `URL` VARCHAR(255) NOT NULL);
CREATE TABLE branch (BranchID INT PRIMARY KEY AUTO_INCREMENT, BranchName VARCHAR(255) NOT NULL, `Address` VARCHAR(255), LibraryID INT NOT NULL, FOREIGN KEY (LibraryID) REFERENCES library(LibraryID) ON DELETE CASCADE);
CREATE TABLE availability (AvailabilityID INT AUTO_INCREMENT PRIMARY KEY, MangaID INT NOT NULL, Volume INT NOT NULL, FOREIGN KEY (MangaID) REFERENCES manga(MangaID) ON DELETE CASCADE);
CREATE TABLE branch_availability_status (BranchStatusID INT AUTO_INCREMENT PRIMARY KEY, AvailabilityID INT NOT NULL, BranchID INT NOT NULL, `Status` VARCHAR(100) NOT NULL, FOREIGN KEY (AvailabilityID) REFERENCES availability(AvailabilityID) ON DELETE CASCADE, FOREIGN KEY (BranchID) REFERENCES branch(BranchID) ON DELETE CASCADE)
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
                'DELETE FROM availability WHERE MangaID = (SELECT MangaID FROM manga WHERE Title = %s) AND Volume = %s',
                (request.form.get('title'), request.form.get('volume')))
            return jsonify({'ok': True, 'message': 'Volume deleted'})

        elif action == 'scrape':
            range_str = request.form.get('range', '').strip()
            only_new  = request.form.get('only_new') == '1'
            if only_new:
                missing = _get_titles_without_lcpl_availability()
                if not missing:
                    return jsonify({'ok': False, 'message': 'All titles already have data'}), 400
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
                    start = int(request.form.get('start', 1))
                    end   = int(request.form.get('end', 1))
                cmd = [sys.executable, str(SCRIPTS_DIR / 'scrapper.py'), str(start), str(end)]

            ok, status, msg = _start_job_thread('scrape', cmd)
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

            ok, status, msg = _start_job_thread('scrape_broward', cmd)
            return jsonify({'ok': ok, 'message': msg}), status

        elif action == 'get_manga':
            offset = request.form.get('offset', '0')
            cmd = [sys.executable, str(SCRIPTS_DIR / 'get_manga.py'), offset]
            ok, status, msg = _start_job_thread('get_manga', cmd)
            return jsonify({'ok': ok, 'message': msg}), status

    # GET: render admin dashboard
    manga_per_library = execute_query("""
        SELECT l.LibraryName, b.BranchName, COUNT(DISTINCT CONCAT(a.MangaID,'-',a.Volume)) AS VolumeCount
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

# ── Public routes ──────────────────────────────────────────────────────────────

@app.route('/', methods=['GET'])
def home():
    return render_template('index.html')

# ── API routes ─────────────────────────────────────────────────────────────────

@app.route('/api/stats')
def api_stats():
    try:
        volumes = execute_query('SELECT COUNT(*) AS n FROM availability', fetch_all=False)
        titles  = execute_query('SELECT COUNT(*) AS n FROM manga', fetch_all=False)
        avail_csv = DATA_DIR / 'availability.csv'
        last_scraped = None
        if avail_csv.exists():
            ts = avail_csv.stat().st_mtime
            last_scraped = datetime.fromtimestamp(ts).strftime('Scraped %b %-d at %-I:%M %p')
        return jsonify({'volumes': volumes['n'] if volumes else 0,
                        'titles':  titles['n']  if titles  else 0,
                        'last_scraped': last_scraped or 'Never scraped'})
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
def api_job(name):
    with _jobs_lock:
        job = dict(_jobs.get(name, {}))
    return jsonify(job)

@app.route('/api/job/stop/<name>', methods=['POST'])
@admin_required
def api_job_stop(name):
    if name not in _start_locks:
        return jsonify({'ok': False, 'message': 'unknown job'}), 400
    stopped = _stop_job(name)
    return jsonify({'ok': stopped, 'message': 'stop signal sent' if stopped else 'job not running'})

@app.route('/api/job_history')
@admin_required
def api_job_history():
    history = list(reversed(_read_job_history()))[:50]
    return jsonify(history)

@app.route('/api/missing_titles')
@admin_required
def api_missing_titles():
    missing_lcpl = _get_titles_without_lcpl_availability()
    missing_broward = _get_titles_without_broward_availability()
    return jsonify({
        'count': len(missing_lcpl),
        'indices': missing_lcpl[:20],
        'broward_count': len(missing_broward),
        'broward_indices': missing_broward[:20]
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
            return jsonify({'ok': False, 'message': 'Title not found in manga table'}), 404
        manga_id = rows[0]['MangaID']
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
                   GROUP_CONCAT(CONCAT(b.BranchName, ': ', bas.Status) ORDER BY b.BranchName SEPARATOR ' | ') AS branches
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
    path = DATA_DIR / "unrecognized_authors.json"
    if not path.exists(): return jsonify([])
    try:
        return jsonify(json.loads(path.read_text(encoding="utf-8")))
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
    existing = set()
    if valid_path.exists():
        existing = {l.strip().lower() for l in valid_path.read_text(encoding='utf-8').splitlines() if l.strip()}
    if surname.lower() not in existing:
        with open(valid_path, 'a', encoding='utf-8') as f:
            f.write(surname + '\n')
    if title:
        titles_path  = DATA_DIR / 'titles.txt'
        authors_path = DATA_DIR / 'authors.txt'
        if titles_path.exists():
            titles  = _load_lines(titles_path)
            authors = _load_lines(authors_path)
            while len(authors) < len(titles):
                authors.append('')
            for i, t in enumerate(titles):
                if t.strip() == title:
                    authors[i] = surname
                    break
            _save_lines(authors_path, authors)
        _remove_from_unrecognized(title)
        return jsonify({'ok': True, 'message': f'Set author for "{title}" → {surname}'})
    return jsonify({'ok': True, 'message': f'Added {surname} to valid_authors.txt'})

# ── Search ─────────────────────────────────────────────────────────────────────

def _get_library_ids() -> tuple[int, int]:
    """
    Look up LCPL and Broward library IDs by name from the DB.
    Returns (lcpl_id, broward_id) — falls back to (1, 2) if not seeded yet.
    Cached after first successful load.
    """
    if hasattr(_get_library_ids, '_cache'):
        return _get_library_ids._cache
    try:
        rows = execute_query("SELECT LibraryID, LibraryName FROM library")
        lcpl = broward = None
        for r in rows:
            name = r['LibraryName'] or ''
            if 'Leon' in name or 'LCPL' in name:
                lcpl = r['LibraryID']
            elif 'Broward' in name:
                broward = r['LibraryID']
        if lcpl is not None and broward is not None:
            _get_library_ids._cache = (lcpl, broward)
            return lcpl, broward
    except Exception:
        pass
    return 1, 2  # fallback to seed order; not cached so we retry next request

@app.route('/search')
def search():
    LCPL_LIBRARY_ID, BROWARD_LIBRARY_ID = _get_library_ids()

    title        = request.args.get('title', '')
    type_        = request.args.get('type', '')
    branch       = request.args.get('branch', '')
    volume       = request.args.get('volume', '')
    avail_filter = request.args.get('avail', '')   # 'available' | 'out' | ''
    only_avail   = avail_filter == 'available'
    ON_SHELF     = ('Graphic Novel', 'Youth Fiction', 'Adult Non-Fiction', 'Available')

    query = """
        SELECT m.MangaID AS MangaID, m.Title, a.Volume, m.Volumes, m.Type,
               m.Members, m.Score, m.Author, m.CoverMedium, b.BranchName, bas.Status,
               b.LibraryID, l.LibraryName
        FROM manga m
        LEFT JOIN availability a                 ON m.MangaID = a.MangaID
        LEFT JOIN branch_availability_status bas ON a.AvailabilityID = bas.AvailabilityID
        LEFT JOIN branch b                       ON bas.BranchID = b.BranchID
        LEFT JOIN library l                      ON b.LibraryID = l.LibraryID
        WHERE 1=1
    """
    params = []
    if title:  query += ' AND m.Title LIKE %s';  params.append(f'%{title}%')
    if type_:  query += ' AND m.Type = %s';       params.append(type_)
    if volume: query += ' AND a.Volume = %s';     params.append(volume)
    if branch:
        if branch == 'N/A': query += ' AND b.BranchName IS NULL'
        else: query += ' AND b.BranchName = %s'; params.append(branch)
    if avail_filter == 'out':
        query += " AND bas.Status = 'Checked Out'"
    query += ' ORDER BY m.Score DESC, a.Volume ASC'

    results = execute_query(query, params)
    results = [r for r in results if r.get('BranchName')]
    if only_avail:
        results = [r for r in results if any(kw in (r.get('Status') or '') for kw in ON_SHELF)]

    titles_map = {}
    for r in results:
        t = r['Title']
        if t not in titles_map:
            titles_map[t] = {
                'MangaID':     r['MangaID'],
                'Title':       t,
                'Volumes':     r['Volumes'],
                'Type':        r['Type'],
                'Members':     r['Members'],
                'Score':       r['Score'],
                'author':      r.get('Author', ''),
                'cover':       r.get('CoverMedium') or '',
                'volumes':     {},
                'has_lcpl':    False,
                'has_broward': False,
            }
        vol    = r['Volume']
        lib_id = r.get('LibraryID')

        if vol not in titles_map[t]['volumes']:
            titles_map[t]['volumes'][vol] = []

        titles_map[t]['volumes'][vol].append({
            'name':    r['BranchName'],
            'status':  r.get('Status') or '',
            'lib_id':  lib_id,
        })

        # Use the actual seeded library IDs (1 = LCPL, 2 = Broward)
        if lib_id == BROWARD_LIBRARY_ID:
            titles_map[t]['has_broward'] = True
        elif lib_id == LCPL_LIBRARY_ID:
            titles_map[t]['has_lcpl'] = True

    grouped = []
    for td in titles_map.values():
        lib_data = {}
        avail_count = out_count = hold_count = 0

        for vol, branches in td['volumes'].items():
            for b in branches:
                lid = b['lib_id']
                if lid not in lib_data:
                    lib_data[lid] = {
                        'library_id':   lid,
                        'library_name': (
                            'Broward County Library'       if lid == BROWARD_LIBRARY_ID
                            else 'Leon County Public Library'
                        ),
                        'vol_map': {}
                    }
                if vol not in lib_data[lid]['vol_map']:
                    lib_data[lid]['vol_map'][vol] = []
                lib_data[lid]['vol_map'][vol].append({'name': b['name'], 'status': b['status']})

                s = b['status']
                if 'Checked Out' in s:  out_count   += 1
                elif 'hold' in s.lower(): hold_count += 1
                elif s:                 avail_count  += 1

        lib_list = []
        for lid, linfo in lib_data.items():
            vlist = [
                {'volume': v, 'branches': brs}
                for v, brs in sorted(linfo['vol_map'].items(), key=lambda x: (x[0] is None, x[0]))
            ]
            lib_list.append({
                'library_id':   linfo['library_id'],
                'library_name': linfo['library_name'],
                'vol_list':     vlist,
            })

        # LCPL (id=1) first, Broward (id=2) second
        lib_list.sort(key=lambda x: x['library_id'])

        grouped.append({
            **td,
            'lib_list':   lib_list,
            'vol_count':  len(td['volumes']),
            'avail_count': avail_count,
            'out_count':   out_count,
            'hold_count':  hold_count,
        })

    library_filter = request.args.get('library', '')
    if library_filter == str(BROWARD_LIBRARY_ID):
        grouped = [g for g in grouped if g['has_broward']]
    elif library_filter == str(LCPL_LIBRARY_ID):
        grouped = [g for g in grouped if g['has_lcpl']]

    filters = {
        'title': title, 'type': type_, 'branch': branch,
        'volume': volume, 'avail': avail_filter, 'library': library_filter
    }
    has_filters = any(v for v in filters.values())
    return render_template('results.html', results=grouped, count=len(grouped),
                           filters=filters, has_filters=has_filters,
                           LCPL_LIBRARY_ID=LCPL_LIBRARY_ID,
                           BROWARD_LIBRARY_ID=BROWARD_LIBRARY_ID)

# ── Jinja2 template filters ────────────────────────────────────────────────────

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
    return (
        f'background: linear-gradient(160deg, {base} 0%, '
        f'color-mix(in srgb, {accent} 18%, {base}) 100%);'
    )

from urllib.parse import quote_plus as _quote_plus
app.jinja_env.filters['urlencode'] = lambda s: _quote_plus(str(s or ''))


if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5000)
