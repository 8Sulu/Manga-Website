import os
from pathlib import Path

ENV_FILE = Path(__file__).parent.parent / '.env'

try:
    from dotenv import load_dotenv
    load_dotenv(ENV_FILE)          # explicit path — works regardless of cwd
except ImportError:
    if ENV_FILE.exists():
        with open(ENV_FILE) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, v = line.split('=', 1)
                    os.environ[k.strip()] = v.strip()

BASE_DIR    = Path(__file__).parent.parent
DATA_DIR    = BASE_DIR / 'data'
SCRIPTS_DIR = BASE_DIR / 'scripts'

DB_CONFIG = {
    'host':     os.getenv('DB_HOST', 'localhost'),
    'user':     os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'manga'),
}

# ── Library registry ──────────────────────────────────────────────────────────
# LibraryID values must match what is seeded into the `library` table (libraries.csv)
LIBRARY_MAPPING = {
    1: {
        "name":     "Leon County Public Library",
        "short":    "LCPL",
        "base_url": "https://lcpl.ent.sirsi.net/client/en_US/lcpl",
    },
    2: {
        "name":     "Broward County Library",
        "short":    "BCL",
        "base_url": "https://broward.ent.sirsi.net/client/en_US/default",
    },
}

LIBRARY_BASE_URL = LIBRARY_MAPPING[1]["base_url"]   # backward compat

# ── LCPL branch mapping ───────────────────────────────────────────────────────
# BranchID values must match what is seeded in branches.csv
BRANCH_MAPPING = {
    "NORTHEAST": 1,
    "BLPERRY":   2,
    "EASTSIDE":  3,
    "FTBRADEN":  4,
    "LAKEJAX":   5,
    "MAIN":      6,
    "WOODVILLE": 7,
}

# Broward uses a single virtual branch (no per-branch breakdown)
BROWARD_LIBRARY_ID = 2
BROWARD_BRANCH_ID  = 8   # "Broward County Library (All Branches)"

SCRAPE_DELAY     = 2
MAX_RETRIES      = 3
REQUEST_TIMEOUT  = 30

GOOGLE_BOOKS_API_KEY = os.getenv('GOOGLE_BOOKS_API_KEY', '')
