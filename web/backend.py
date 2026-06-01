from flask import Flask, render_template, request, jsonify, redirect, url_for, session
import csv, json, sys, threading, functools, os
from pathlib import Path
from datetime import datetime
from urllib.parse import quote_plus as _quote_plus

sys.path.append(str(Path(__file__).parent.parent))
from config.settings import DB_CONFIG, DATA_DIR, SCRIPTS_DIR
from utils.database_utils import get_db_connection, execute_query, execute_update

app = Flask(__name__, static_folder='static')
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'manga-tracker-dev-secret-change-me')
ADMIN_PASSWORD   = os.getenv('ADMIN_PASSWORD', '')
JOB_HISTORY_PATH = DATA_DIR / "job_history.json"

# ── Auth ───────────────────────────────────────────────────────────────────────

def admin_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if session.get('admin'):
            return f(*args, **kwargs)
        if not ADMIN_PASSWORD and request.remote_addr in ('127.0.0.1', '::1', 'localhost'):
            return f(*args, **kwargs)
        return redirect(url_for('admin_login', next=request.url))
    return decorated

# ── Job system ─────────────────────────────────────────────────────────────────

_jobs: dict = {}
_jobs_lock  = threading.Lock()
_JOB_NAMES  = {'scrape', 'scrape_broward', 'get_manga'}

def _run_subprocess(job_name: str, cmd: list) -> None:
    import subprocess, re
    with _jobs_lock:
        _jobs[job_name].update(running=True, progress=0, message='starting…')
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
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
    history.append({'job': job, 'status': status, 'message': message,
                    'at': datetime.utcnow().isoformat()})
    try:
        JOB_HISTORY_PATH.write_text(
                json.dumps(history[-200:], indent=2, ensure_ascii=False), encoding='utf-8')
    except Exception:
        pass

# ── Helpers ────────────────────────────────────────────────────────────────────

def parse_range_str(s: str, max_titles: int = 9999) -> tuple:
    import re
    s = s.strip().replace('\u2013', '-').replace('\u2014', '-')
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

def _missing_indices(library_pattern: str) -> list:
    try:
        manga_ids = [r['MangaID'] for r in execute_query("SELECT MangaID FROM manga ORDER BY MangaID")]
        scraped = {r['MangaID'] for r in execute_query(f"""
                                                       SELECT DISTINCT a.MangaID FROM availability a
                                                       JOIN branch_availability_status bas ON a.AvailabilityID = bas.AvailabilityID
                                                       JOIN branch b ON bas.BranchID = b.BranchID
                                                       JOIN library l ON b.LibraryID = l.LibraryID
                                                       WHERE l.LibraryName LIKE '{library_pattern}'
                                                       """)}
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
            if 'LeRoy Collins' in name or ('Leon' in name and 'Public' in name):
                lcpl = r['LibraryID']
            elif 'Broward' in name:
                broward = r['LibraryID']
        if lcpl is not None and broward is not None:
            _library_id_cache = (lcpl, broward)
            return _library_id_cache
    except Exception:
        pass
    return 1, 15

# ── Branch display helpers ─────────────────────────────────────────────────────

def _branch_short(name: str, library_id: int, broward_library_id: int) -> str:
    """Compact display name for any branch."""
    if library_id != broward_library_id:
        if 'Main'      in name or 'Leroy'  in name: return 'Main'
        if 'Northeast' in name or 'Bruce'  in name: return 'NE Branch'
        if 'Eastside'  in name:                      return 'Eastside'
        if 'Perry'     in name or 'BL'     in name: return 'BL Perry'
        if 'Jackson'   in name:                      return 'Lk Jackson'
        if 'Braden'    in name or 'Fort'   in name: return 'Ft Braden'
        if 'Woodville' in name:                      return 'Woodville'
        return name.split(' ')[0]
    # Broward
    if name == 'Main Library':                       return 'Main'
    if 'Northwest Regional' in name:                 return 'NW Regional'
    if 'North Regional'     in name:                 return 'N Regional'
    if 'South Regional'     in name:                 return 'S Regional'
    if 'Southwest Regional' in name:                 return 'SW Regional'
    if 'West Regional'      in name:                 return 'W Regional'
    if 'African American'   in name:                 return 'AARLCC'
    if 'Hollywood Beach'    in name:                 return 'Hollywood Bch'
    if 'Hollywood'          in name:                 return 'Hollywood'
    if 'Lauderhill Central' in name:                 return 'Lauderhill CP'
    if 'Lauderhill Towne'   in name:                 return 'Lauderhill TC'
    if 'Lauderdale Lakes'   in name:                 return 'Laud. Lakes'
    if 'Pompano Beach'      in name:                 return 'Pompano Bch'
    if 'Pembroke Pines'     in name:                 return 'Pemb. Pines'
    if 'Miramar'            in name:                 return 'Miramar'
    if 'Weston'             in name:                 return 'Weston'
    if 'Tamarac'            in name:                 return 'Tamarac'
    if 'Sunrise'            in name:                 return 'Sunrise'
    if 'Margate'            in name:                 return 'Margate'
    if 'Deerfield Beach'    in name:                 return 'Deerfield Bch'
    if 'Dania Beach'        in name:                 return 'Dania Bch'
    if 'Hallandale'         in name:                 return 'Hallandale'
    if 'Carver Ranches'     in name:                 return 'Carver Ranch'
    if 'Century Plaza'      in name:                 return 'Century Plz'
    if 'North Lauderdale'   in name:                 return 'N. Laud.'
    if 'Imperial Point'     in name:                 return 'Imperial Pt'
    if 'Riverland'          in name:                 return 'Riverland'
    if 'Davie'              in name:                 return 'Davie/CC'
    if 'Beach Branch'       in name:                 return 'Beach'
    if 'Jan Moran'          in name:                 return 'Jan Moran'
    if 'Galt Ocean'         in name:                 return 'Galt Ocean'
    if 'Fort Lauderdale Reading' in name:            return 'FTL Reading'
    if 'Tyrone Bryant'      in name:                 return 'Tyrone Bryant'
    if 'Northwest Branch'   in name:                 return 'NW Branch'
    if 'Nova Southeastern'  in name:                 return 'NSU'
    return name.split(' ')[0]

def _status_priority(status: str) -> int:
    """Higher = prefer this status when a branch has multiple rows."""
    if 'Available' in status:                      return 2
    if any(kw in status for kw in ('Collection', 'Youth', 'Graphic', 'Materials', 'General')): return 2
    if 'Hold' in status or 'hold' in status.lower(): return 1
    return 0

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

# ── Auth routes ────────────────────────────────────────────────────────────────

@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    if not ADMIN_PASSWORD:
        session['admin'] = True
        return redirect(request.args.get('next') or url_for('admin'))
    error = None
    if request.method == 'POST':
        if request.form.get('password', '') == ADMIN_PASSWORD:
            session['admin'] = True
            return redirect(request.args.get('next') or url_for('admin'))
        error = 'Invalid password'
    return render_template('admin_login.html', error=error)

@app.route('/admin/logout')
def admin_logout():
    session.pop('admin', None)
    return redirect('/')

# ── Admin GET/POST ─────────────────────────────────────────────────────────────

@app.route('/admin', methods=['GET', 'POST'])
@admin_required
def admin():
    if request.method == 'POST':
        return _handle_admin_post()

    manga_per_library = execute_query("""
                                      SELECT l.LibraryName, b.BranchName,
                                      COUNT(DISTINCT CONCAT(a.MangaID,'-',a.Volume)) AS VolumeCount
                                      FROM branch b
                                      JOIN library l ON b.LibraryID = l.LibraryID
                                      JOIN branch_availability_status bas ON b.BranchID = bas.BranchID
                                      JOIN availability a ON bas.AvailabilityID = a.AvailabilityID
                                      GROUP BY l.LibraryName, b.BranchName
                                      HAVING COUNT(DISTINCT CONCAT(a.MangaID,'-',a.Volume)) > 0
                                      ORDER BY l.LibraryName, VolumeCount DESC
                                      """)
    return render_template('admin.html',
                           manga_per_library=manga_per_library,
                           job_history=list(reversed(_read_job_history()))[:30])

def _handle_admin_post():
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
        ok, status, msg = _start_job('get_manga', [sys.executable, str(SCRIPTS_DIR / 'get_manga.py'), offset])
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
            return jsonify({'ok': False, 'message': f'All titles already have {"Broward" if is_broward else "LCPL"} data'}), 400
        if range_str:
            lo, hi = parse_range_str(range_str, _titles_count())
            missing = [idx for idx in missing if lo <= idx <= hi]
            if not missing:
                return jsonify({'ok': False, 'message': f'No new titles in range {range_str}'}), 400
        cmd = [sys.executable, str(SCRIPTS_DIR / script), '--indices', ','.join(map(str, missing))]
    else:
        cmd = [sys.executable, str(SCRIPTS_DIR / script)]
        if range_str and not is_broward:
            start, end = parse_range_str(range_str, _titles_count())
            cmd = [sys.executable, str(SCRIPTS_DIR / script), '--range', f'{start}-{end}']
        elif range_str:
            lo, hi = parse_range_str(range_str, _titles_count())
            cmd.extend(['--range', f'{lo}-{hi}'])

    ok, status, msg = _start_job(action, cmd)
    return jsonify({'ok': ok, 'message': msg}), status

# ── Admin reset ────────────────────────────────────────────────────────────────

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

    _library_id_cache = None
    _append_job_history('reset', 'done', ' · '.join(messages))
    return jsonify({'ok': True, 'messages': messages})

# ── Public routes ──────────────────────────────────────────────────────────────

@app.route('/')
def home():
    return render_template('index.html')

# ── API ────────────────────────────────────────────────────────────────────────

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
        return jsonify({'volumes': volumes['n'] if volumes else 0,
                        'titles':  titles['n']  if titles  else 0,
                        'last_scraped': last_scraped_msg})
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
    return jsonify({'running': job.get('running', False),
                    'progress': job.get('progress', 0),
                    'message':  job.get('message', '')})

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

# ── Search ─────────────────────────────────────────────────────────────────────

# ── Search ─────────────────────────────────────────────────────────────────────
#
# PATCH: Replace the entire search() route in backend.py with this version.
# Key change: lib_data now tracks volumes → {branch_id: status} instead of
# collapsing everything to branch → status, so Broward gets the same vol grid as LCPL.

ON_SHELF = ('Graphic Novel', 'Youth Fiction', 'Adult Non-Fiction', 'Available',
            'General Collection', 'New Materials')

STATUS_PRIORITY = {'Available': 2, 'On Hold': 1, 'Checked Out': 0}

def _normalize_status(raw: str) -> str:
    """Collapse any raw catalog status string into one of three display values."""
    if not raw:
        return 'Checked Out'
    if any(kw in raw for kw in ON_SHELF):
        return 'Available'
    if 'hold' in raw.lower():
        return 'On Hold'
    return 'Checked Out'


@app.route('/search')
def search():
    LCPL_LIBRARY_ID, BROWARD_LIBRARY_ID = _get_library_ids()
    title        = request.args.get('title',   '')
    type_        = request.args.get('type',    '')
    branch       = request.args.get('branch',  '')
    volume       = request.args.get('volume',  '')
    avail_filter = request.args.get('avail',   '')
    lib_filter   = request.args.get('library', '')

    conditions = ['b.BranchName IS NOT NULL', 'b.BranchID IS NOT NULL']
    params = []
    if title:      conditions.append('m.Title LIKE %s');     params.append(f'%{title}%')
    if type_:      conditions.append('m.Type = %s');         params.append(type_)
    if volume:     conditions.append('a.Volume = %s');       params.append(volume)
    if branch:     conditions.append('b.BranchName = %s');   params.append(branch)
    if lib_filter: conditions.append('b.LibraryID = %s');    params.append(lib_filter)
    if avail_filter == 'out': conditions.append("bas.Status = 'Checked Out'")

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

    if avail_filter == 'available':
        rows = [r for r in rows if _normalize_status(r.get('Status') or '') == 'Available']

    # ── Group: title → library → volume → {branch_id: status} ────────────────
    titles_map: dict = {}
    for r in rows:
        t   = r['Title']
        lid = r['LibraryID']
        bid = r['BranchID']
        if lid is None or bid is None:
            continue  # skip malformed rows

        if t not in titles_map:
            titles_map[t] = {
                    'MangaID': r['MangaID'], 'Title': t, 'Volumes': r['Volumes'],
                    'Type': r['Type'], 'Members': r['Members'], 'Score': r['Score'],
                    'author': r.get('Author') or '', 'cover': r.get('CoverMedium') or '',
                    'lib_data': {},
                    'has_lcpl': False, 'has_broward': False,
                    }

        td = titles_map[t]
        td['lib_data'].setdefault(lid, {
            'library_id':   lid,
            'library_name': ('Broward County Library' if lid == BROWARD_LIBRARY_ID
                             else 'Leon County Public Library'),
            'volumes':      {},   # vol_num -> {branch_id -> normalized_status}
            'branch_names': {},   # branch_id -> BranchName string
            })
        ld = td['lib_data'][lid]

        vol         = r['Volume'] if r['Volume'] is not None else 0
        norm_status = _normalize_status(r.get('Status') or '')

        ld['volumes'].setdefault(vol, {})
        current = ld['volumes'][vol].get(bid)
        new_pri = STATUS_PRIORITY[norm_status]
        cur_pri = STATUS_PRIORITY.get(current, -1)
        if current is None or new_pri > cur_pri:
            ld['volumes'][vol][bid] = norm_status

        ld['branch_names'][bid] = r['BranchName']

        if lid == BROWARD_LIBRARY_ID: td['has_broward'] = True
        elif lid == LCPL_LIBRARY_ID:  td['has_lcpl']    = True

    # ── Build final grouped list ───────────────────────────────────────────────
    grouped = []
    for td in titles_map.values():
        avail_count = out_count = hold_count = 0
        lib_list = []

        for linfo in sorted(td['lib_data'].values(), key=lambda x: x['library_id']):
            branch_best: dict[int, str] = {}

            vol_list = []
            for vol_num in sorted(linfo['volumes'].keys()):
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
                    if cur_best is None or STATUS_PRIORITY[status] > STATUS_PRIORITY.get(cur_best, -1):
                        branch_best[bid] = status

                vol_list.append({'vol': vol_num, 'branches': vol_branches})

            branch_list = []
            for bid in sorted(branch_best.keys(),
                              key=lambda b: linfo['branch_names'].get(b, '')):
                bname = linfo['branch_names'].get(bid, '')
                branch_list.append({
                    'name':   bname,
                    'short':  _branch_short(bname, linfo['library_id'], BROWARD_LIBRARY_ID),
                    'status': branch_best[bid],
                    })

            lib_list.append({
                'library_id':   linfo['library_id'],
                'library_name': linfo['library_name'],
                'branch_list':  branch_list,
                'vol_list':     vol_list,
                })

        grouped.append({
            'MangaID':     td['MangaID'],
            'Title':       td['Title'],
            'Volumes':     td['Volumes'],
            'Type':        td['Type'],
            'Members':     td['Members'],
            'Score':       td['Score'],
            'author':      td['author'],
            'cover':       td['cover'],
            'has_lcpl':    td['has_lcpl'],
            'has_broward': td['has_broward'],
            'lib_list':    lib_list,
            'vol_count':   sum(len(linfo['volumes']) for linfo in td['lib_data'].values()),
            'avail_count': avail_count,
            'out_count':   out_count,
            'hold_count':  hold_count,
            })

    filters = {'title': title, 'type': type_, 'branch': branch,
               'volume': volume, 'avail': avail_filter, 'library': lib_filter}
    return render_template('results.html', results=grouped, count=len(grouped),
                           filters=filters, has_filters=any(filters.values()),
                           LCPL_LIBRARY_ID=LCPL_LIBRARY_ID,
                           BROWARD_LIBRARY_ID=BROWARD_LIBRARY_ID)

# ── Jinja2 filters ─────────────────────────────────────────────────────────────

_COVER_PALETTES = [
        ('hsl(260,40%,14%)', 'hsl(260,60%,55%)'), ('hsl(340,40%,13%)', 'hsl(340,60%,55%)'),
        ('hsl(200,45%,12%)', 'hsl(200,65%,50%)'), ('hsl(30, 50%,13%)', 'hsl(30, 70%,55%)'),
        ('hsl(150,40%,12%)', 'hsl(150,55%,46%)'), ('hsl(290,35%,14%)', 'hsl(290,55%,58%)'),
        ('hsl(10, 45%,13%)', 'hsl(10, 65%,55%)'), ('hsl(220,42%,14%)', 'hsl(220,62%,58%)'),
        ]

@app.template_filter('cover_gradient')
def cover_gradient_filter(manga_id):
    idx = int(manga_id or 0) % len(_COVER_PALETTES)
    base, accent = _COVER_PALETTES[idx]
    return (f'background: linear-gradient(160deg, {base} 0%, '
                                          f'color-mix(in srgb, {accent} 18%, {base}) 100%);')

app.jinja_env.filters['urlencode'] = lambda s: _quote_plus(str(s or ''))

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5000)
