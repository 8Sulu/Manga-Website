from flask import Flask, render_template, request, jsonify
import csv, subprocess, sys, time, threading
from pathlib import Path
from datetime import datetime
import mysql.connector

sys.path.append(str(Path(__file__).parent.parent))
from config.settings import DB_CONFIG, DATA_DIR, SCRIPTS_DIR
from utils.database_utils import get_db_connection, execute_query, execute_update

app = Flask(__name__)

# ── Job state ────────────────────────────────────────────────────────────────
# Tracks running background jobs so the frontend can poll progress.
# Structure: { job_name: { running, progress, message, started_at } }

_jobs = {}
_jobs_lock = threading.Lock()

def _job_start(name, message='starting…'):
    with _jobs_lock:
        _jobs[name] = {'running': True, 'progress': 0, 'message': message, 'started_at': time.time()}

def _job_update(name, progress, message):
    with _jobs_lock:
        if name in _jobs:
            _jobs[name].update({'progress': progress, 'message': message})

def _job_done(name):
    with _jobs_lock:
        if name in _jobs:
            _jobs[name].update({'running': False, 'progress': 100, 'message': 'done'})

def _run_script(job_name, cmd, total):
    """Run a subprocess, tail its log output to update progress."""
    _job_start(job_name)
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1
        )
        current = 0
        for line in proc.stdout:
            line = line.strip()
            # Parse lines like "[3/10]" to extract progress
            import re
            m = re.search(r'\[(\d+)[/\s]+(\d+)\]', line)
            if m:
                current = int(m.group(1))
                total_found = int(m.group(2))
                pct = int(current / total_found * 100)
                _job_update(job_name, pct, f'{current}/{total_found}')
            elif line:
                # Show last meaningful log line as message
                short = line[-60:] if len(line) > 60 else line
                _job_update(job_name, _jobs.get(job_name, {}).get('progress', 0), short)
        proc.wait()
        _job_done(job_name)
    except Exception as e:
        with _jobs_lock:
            _jobs[job_name] = {'running': False, 'progress': 0, 'message': f'error: {e}'}


# ── Schema & seeding ─────────────────────────────────────────────────────────

SCHEMA = """
DROP TABLE IF EXISTS branch_availability_status;
DROP TABLE IF EXISTS availability;
DROP TABLE IF EXISTS branch;
DROP TABLE IF EXISTS manga;
DROP TABLE IF EXISTS library;

CREATE TABLE manga (
    MangaID   INT PRIMARY KEY AUTO_INCREMENT,
    Title     VARCHAR(255) NOT NULL,
    `Type`    VARCHAR(50),
    Volumes   INT,
    Members   INT,
    Score     DECIMAL(4,2)
);

CREATE TABLE library (
    LibraryID   INT PRIMARY KEY AUTO_INCREMENT,
    LibraryName VARCHAR(255) NOT NULL,
    `URL`       VARCHAR(255) NOT NULL
);

CREATE TABLE branch (
    BranchID   INT PRIMARY KEY AUTO_INCREMENT,
    BranchName VARCHAR(255) NOT NULL,
    `Address`  VARCHAR(255),
    LibraryID  INT NOT NULL,
    FOREIGN KEY (LibraryID) REFERENCES library(LibraryID) ON DELETE CASCADE
);

CREATE TABLE availability (
    AvailabilityID INT AUTO_INCREMENT PRIMARY KEY,
    MangaID        INT NOT NULL,
    Volume         INT NOT NULL,
    FOREIGN KEY (MangaID) REFERENCES manga(MangaID) ON DELETE CASCADE
);

CREATE TABLE branch_availability_status (
    BranchStatusID INT AUTO_INCREMENT PRIMARY KEY,
    AvailabilityID INT NOT NULL,
    BranchID       INT NOT NULL,
    `Status`       VARCHAR(100) NOT NULL,
    FOREIGN KEY (AvailabilityID) REFERENCES availability(AvailabilityID) ON DELETE CASCADE,
    FOREIGN KEY (BranchID)       REFERENCES branch(BranchID)             ON DELETE CASCADE
)
"""

INSERT_OPS = [
    ("manga.csv",                      "INSERT INTO manga (Title, Type, Volumes, Members, Score) VALUES (%s,%s,%s,%s,%s)"),
    ("libraries.csv",                  "INSERT INTO library (LibraryName, `URL`) VALUES (%s,%s)"),
    ("branches.csv",                   "INSERT INTO branch (BranchName, `Address`, LibraryID) VALUES (%s,%s,%s)"),
    ("availability.csv",               "INSERT INTO availability (MangaID, Volume) VALUES (%s,%s)"),
    ("branch_availability_status.csv", "INSERT INTO branch_availability_status (AvailabilityID, BranchID, `Status`) VALUES (%s,%s,%s)"),
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


# ── Routes ───────────────────────────────────────────────────────────────────

@app.route('/reset', methods=['POST'])
def reset_database():
    messages = []
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SET FOREIGN_KEY_CHECKS=0')
            for cmd in SCHEMA.strip().split(';'):
                cmd = cmd.strip()
                if cmd:
                    cursor.execute(cmd)
            cursor.execute('SET FOREIGN_KEY_CHECKS=1')
            conn.commit()
        messages.append('✓ Schema reset')
    except mysql.connector.Error as e:
        return f'✗ Schema reset failed: {e}'

    for filename, query in INSERT_OPS:
        messages.append(insert_csv(filename, query))

    return '<br>'.join(messages)


@app.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'POST':
        action = request.form.get('action')

        if action == 'update':
            execute_update(
                'UPDATE manga SET Volumes = %s WHERE Title = %s',
                (request.form.get('volume'), request.form.get('title'))
            )

        elif action == 'delete':
            execute_update(
                'DELETE FROM availability WHERE MangaID = (SELECT MangaID FROM manga WHERE Title = %s) AND Volume = %s',
                (request.form.get('title'), request.form.get('volume'))
            )

        elif action == 'scrape':
            books   = request.form.get('books', '1')
            visible = '--visible' if request.form.get('visible') else ''
            cmd = [sys.executable, str(SCRIPTS_DIR / 'scrapper.py'), books]
            if visible:
                cmd.append('--visible')
            t = threading.Thread(target=_run_script, args=('scrape', cmd, int(books)), daemon=True)
            t.start()

        elif action == 'get_authors':
            n   = request.form.get('authors', '1')
            cmd = [sys.executable, str(SCRIPTS_DIR / 'get_authors.py'), n]
            t   = threading.Thread(target=_run_script, args=('get_authors', cmd, int(n)), daemon=True)
            t.start()

        elif action == 'get_titles':
            n   = request.form.get('titles', '1')
            cmd = [sys.executable, str(SCRIPTS_DIR / 'get_titles.py'), n]
            t   = threading.Thread(target=_run_script, args=('get_titles', cmd, int(n)), daemon=True)
            t.start()

    manga_per_library = execute_query("""
        SELECT
            b.BranchName,
            COUNT(DISTINCT CONCAT(a.MangaID, '-', a.Volume)) AS VolumeCount
        FROM branch b
        JOIN branch_availability_status bas ON b.BranchID = bas.BranchID
        JOIN availability a ON bas.AvailabilityID = a.AvailabilityID
        GROUP BY b.BranchName
        HAVING COUNT(DISTINCT CONCAT(a.MangaID, '-', a.Volume)) > 10
        ORDER BY VolumeCount DESC
    """)

    return render_template('index.html', manga_per_library=manga_per_library)


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
        return jsonify({
            'volumes':      volumes['n'] if volumes else 0,
            'titles':       titles['n']  if titles  else 0,
            'last_scraped': last_scraped or 'Never scraped',
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/job/<name>')
def api_job(name):
    with _jobs_lock:
        job = _jobs.get(name, {})
    return jsonify(job)


@app.route('/search')
def search():
    title      = request.args.get('title', '')
    type_      = request.args.get('type', '')
    branch     = request.args.get('branch', '')
    volume     = request.args.get('volume', '')
    status     = request.args.get('status', '')
    only_avail = request.args.get('only_available') == '1'

    ON_SHELF_KEYWORDS = ('Graphic Novel', 'Youth Fiction', 'Adult Non-Fiction')

    query = """
        SELECT m.Title, a.Volume, m.Volumes, m.Type, m.Members, m.Score,
               b.BranchName, bas.Status
        FROM manga m
        LEFT JOIN availability a                 ON m.MangaID = a.MangaID
        LEFT JOIN branch_availability_status bas ON a.AvailabilityID = bas.AvailabilityID
        LEFT JOIN branch b                       ON bas.BranchID = b.BranchID
        WHERE 1=1
    """
    params = []

    if title:  query += ' AND m.Title LIKE %s';    params.append(f'%{title}%')
    if type_:  query += ' AND m.Type = %s';         params.append(type_)
    if volume: query += ' AND a.Volume = %s';       params.append(volume)
    if branch:
        if branch == 'N/A': query += ' AND b.BranchName IS NULL'
        else:               query += ' AND b.BranchName = %s'; params.append(branch)
    if status:
        if status == 'N/A': query += ' AND bas.Status IS NULL'
        else:               query += ' AND bas.Status = %s';   params.append(status)

    query += ' ORDER BY m.Score DESC'
    results = execute_query(query, params)

    results = [r for r in results if r.get('BranchName')]
    if only_avail:
        results = [r for r in results if any(kw in (r.get('Status') or '') for kw in ON_SHELF_KEYWORDS)]

    # Group rows by (Title, Volume) so each unique volume is one row
    # with all its branches/statuses collected into a list
    grouped = {}
    for r in results:
        key = (r['Title'], r['Volume'])
        if key not in grouped:
            grouped[key] = {
                'Title':    r['Title'],
                'Volume':   r['Volume'],
                'Volumes':  r['Volumes'],
                'Type':     r['Type'],
                'Members':  r['Members'],
                'Score':    r['Score'],
                'branches': [],
            }
        if r.get('BranchName'):
            grouped[key]['branches'].append({
                'name':   r['BranchName'],
                'status': r.get('Status') or '',
            })

    results = list(grouped.values())

    status_summary = {}
    for row in results:
        for b in row['branches']:
            s = b['status'] or 'N/A'
            status_summary[s] = status_summary.get(s, 0) + 1

    return render_template('results.html', results=results, count=len(results), status_summary=status_summary)


if __name__ == '__main__':
    app.run(debug=True)
