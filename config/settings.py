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

# ── Broward branch mapping ──────────────────────────────────────────────────
# Maps the names returned by the library catalog to your database entry names
BROWARD_BRANCH_MAPPING = {
    "African-American Research Library": "African American Research Library & Cultural Center",
    "Beach Branch Library": "Beach Branch",
    "Carver Ranches Branch Library": "Carver Ranches Branch",
    "Century Plaza Branch Library": "Century Plaza/Leon Slatin Branch",
    "Dania Beach Paul DeMaio Branch Library": "Dania Beach Paul DeMaio Branch",
    "Davie Cooper City Branch Library": "Davie/Cooper City Branch",
    "Deerfield Beach P. White Branch Library": "Deerfield Beach Percy White Branch",
    "Fort Lauderdale Reading Center": "Fort Lauderdale Reading Center",
    "Galt Ocean Mile Branch Library": "Galt Ocean Mile Reading Center",
    "Hallandale Beach Branch Library": "Hallandale Beach Branch",
    "Hollywood Bernice P Oster Reading Center": "Hollywood Beach Bernice P. Oster Branch",
    "Hollywood Branch Library": "Hollywood Branch",
    "Imperial Point Branch Library": "Imperial Point Branch",
    "Jan Moran Collier City Learning Library": "Jan Moran Collier City Learning Library",
    "Lauderdale Lakes Branch": "Lauderdale Lakes Library/Educational & Cultural Center",
    "Lauderhill Central Park Library": "Lauderhill Central Park Library",
    "Lauderhill Towne Centre Library": "Lauderhill Towne Centre Branch",
    "Main Library": "Main Library",
    "Margate Catharine Young Library": "Margate Catharine Young Branch",
    "Marta-Beth Friedman Stirling Road Branch": "Marta,Beth Friedman Stirling Road Branch",
    "Miramar Branch Library and Ed Center": "Miramar Branch Library & Education Center",
    "North Lauderdale Saraniero Branch": "North Lauderdale Saraniero Branch",
    "North Regional/BC Library": "North Regional/Broward College Library",
    "Northwest Branch": "Northwest Branch",
    "Northwest Regional Library": "Northwest Regional Library",
    "Pembroke Pines/Walter C. Young Branch": "Pembroke Pines/Walter C. Young Resource Center",
    "Pompano Beach Branch": "Pompano Beach Library & Cultural Center",
    "Riverland Branch": "Riverland Branch",
    "South Regional/Broward College Library": "South Regional/Broward College Library",
    "Southwest Regional Library": "Southwest Regional Library",
    "Sunrise Dan Pearl Branch": "Sunrise Dan Pearl Branch",
    "Tamarac Branch": "Tamarac Branch",
    "Tyrone Bryant Branch Library": "Tyrone Bryant Branch",
    "West Regional Library": "West Regional Library",
    "Weston Branch": "Weston Branch",
}

# Broward uses a single virtual branch (no per-branch breakdown)
BROWARD_LIBRARY_ID = 2
BROWARD_BRANCH_ID  = 8   # "Broward County Library (All Branches)"

SCRAPE_DELAY     = 2
MAX_RETRIES      = 3
REQUEST_TIMEOUT  = 30
