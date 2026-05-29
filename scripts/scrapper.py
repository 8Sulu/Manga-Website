"""
Library manga scraper — no Selenium, no browser.

Usage (named flags — preferred, matches Broward scraper):
    python scrapper.py --line 3                scrape line 3
    python scrapper.py --range 1-50            scrape lines 1 to 50
    python scrapper.py --indices 1,4,7         scrape specific 1-based indices
    python scrapper.py --range 1-50 --debug    also write raw JSON to debug/

Usage (legacy positional — still supported):
    python scrapper.py <end>                   scrape titles 1 → end
    python scrapper.py <start> <end>           scrape titles start → end
"""
from __future__ import annotations

import re
import json
import time
import csv
import sys
import logging
from pathlib import Path

import requests
from bs4 import BeautifulSoup

sys.path.append(str(Path(__file__).parent.parent))
from config.settings import DATA_DIR, BRANCH_MAPPING

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ── Endpoints ────────────────────────────────────────────────────────────────

CATALOG_BASE     = "https://lcpl.ent.sirsi.net/client/en_US/lcpl"
SEARCH_BASE      = f"{CATALOG_BASE}/search/results"
ILSWS_BASE       = "https://lcpl.sirsi.net/lcpl_ilsws/rest/standard/lookupTitleInfo"
RESULTS_PER_PAGE = 12
REQUEST_DELAY    = 0.5
MAX_RETRIES      = 3

def _load_title_author_map() -> list[tuple[int, str, str]]:
    """Return list of (index, title, author) from the manga DB table, ordered by MangaID."""
    from config.settings import DB_CONFIG
    import mysql.connector
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT MangaID, Title, Author FROM manga ORDER BY MangaID")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [(i + 1, r['Title'], r['Author'] or '') for i, r in enumerate(rows)]

# ── manga.csv → MAL ID map ────────────────────────────────────────────────────

def _load_manga_id_map() -> dict[str, int]:
    """
    Return {title: mal_id} from manga.csv so the scraper can write the correct
    MangaID. Forces lowercase keys and strips whitespace for strict matching.
    """
    path = DATA_DIR / "manga.csv"
    if not path.exists():
        log.error("manga.csv not found — MangaIDs will be missing")
        return {}
    result: dict[str, int] = {}
    with open(path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            # FIX: Ensure everything is stripped cleanly
            title  = (row.get("Title") or "").strip()
            mal_id = (row.get("MangaID") or row.get("Id") or "").strip()
            if title and mal_id:
                try:
                    val = int(mal_id)
                    # Force all keys to lowercase for foolproof lookups
                    result[title.lower()] = val  
                except ValueError:
                    pass
    return result

# ── HTTP session ──────────────────────────────────────────────────────────────

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    })
    # Prime session cookies — SirsiDynix requires a session established on the
    # homepage before search results pages return data correctly.
    try:
        s.get(CATALOG_BASE, timeout=20)
        s.headers.update({"Referer": CATALOG_BASE})
        log.info("Session primed")
    except Exception as e:
        log.warning(f"Session prime failed (continuing): {e}")
    return s


# ── Step 1: search page → catalog keys ───────────────────────────────────────

def _extract_keys_from_href(href: str, keys: list, seen: set) -> None:
    for pattern in (
        r"SD_ILS[:\u003a](\d+)",
        r"SD_ILS%3[Aa](\d+)",
        r"SD_ILS\$003[Aa](\d+)",
        r"SD_ILS:(\d+)",
    ):
        for m in re.finditer(pattern, href, re.IGNORECASE):
            k = int(m.group(1))
            if k not in seen:
                seen.add(k)
                keys.append(k)


def fetch_catalog_keys(session: requests.Session, title: str, author: str,
                       page: int = 0) -> list:
    params: dict = {
        "qu": ["", f"TITLE={title}", f"AUTHOR={author}"],
        "te": "ILS",
        "h":  "1",
        "lm": "BOOKS",
    }
    if page > 0:
        params["rw"] = page * RESULTS_PER_PAGE

    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(SEARCH_BASE, params=params, timeout=15)
            r.raise_for_status()
            break
        except requests.RequestException as e:
            log.warning(f"  Search attempt {attempt+1} failed: {e}")
            if attempt == MAX_RETRIES - 1:
                return []
            time.sleep(2 ** attempt)
    else:
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    result_text = soup.find(id="searchResultText")
    if result_text and "No results" in result_text.get_text():
        return []

    keys: list = []
    seen: set  = set()

    # Primary: links with id starting "detailLink"
    for a in soup.find_all("a", id=re.compile(r"^detailLink")):
        _extract_keys_from_href(a.get("href", ""), keys, seen)

    # Fallback: any link whose href contains SD_ILS
    if not keys:
        for a in soup.find_all("a", href=True):
            _extract_keys_from_href(a["href"], keys, seen)

    return keys


# ── Step 2: ILSWS API ─────────────────────────────────────────────────────────

def _strip_jsonp(text: str) -> str:
    text = text.strip()
    m = re.match(r'^[^(]*\((.*)\)\s*;?\s*$', text, re.DOTALL)
    return m.group(1) if m else text


def fetch_title_info(session: requests.Session, catalog_key: int,
                     debug_dir=None) -> dict:
    params = {
        "clientID":        "DS_CLIENT",
        "titleID":         catalog_key,
        "includeItemInfo": "true",
        "includeOPACInfo": "false",
        "json":            "true",
        "callback":        "lcpl_cb",
        "_":               int(time.time() * 1000),
    }
    headers = {
        "Referer":          SEARCH_BASE,
        "Accept":           "*/*",
        "X-Requested-With": "XMLHttpRequest",
    }
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(ILSWS_BASE, params=params, headers=headers, timeout=15)
            r.raise_for_status()
            raw = _strip_jsonp(r.text)
            if debug_dir:
                Path(debug_dir).mkdir(parents=True, exist_ok=True)
                (Path(debug_dir) / f"ilsws_{catalog_key}.json").write_text(raw, encoding="utf-8")
            return json.loads(raw)
        except (requests.RequestException, json.JSONDecodeError) as e:
            log.warning(f"  ILSWS attempt {attempt+1} for key {catalog_key}: {e}")
            if attempt == MAX_RETRIES - 1:
                return {}
            time.sleep(2 ** attempt)
    return {}


# ── Step 3: parse response ────────────────────────────────────────────────────

def extract_volume_from_callnumber(call_number: str) -> int:
    m = re.search(r'\bV(?:OL)?[.\s]+(\d+)', call_number, re.IGNORECASE)
    if m:
        return int(m.group(1))
    nums = re.findall(r'\d+', call_number)
    return int(nums[-1]) if nums else 0


def item_status(current_loc: str, due_date) -> str:
    loc = (current_loc or "").upper()
    if loc in ("CHECKEDOUT", "CHECKED-OUT", "OUT") or due_date:
        return "Checked Out"
    if "HOLD" in loc:
        return "On Hold"
    if "TRANSIT" in loc:
        return "In Transit"
    return "Graphic Novel - Young Adult Fiction"


def parse_title_info(data: dict, manga_id: int, availability_id: int) -> tuple:
    books = []
    valid_branch_keys = {k.upper() for k in BRANCH_MAPPING}

    for title_entry in data.get("TitleInfo", []):
        for call in title_entry.get("CallInfo", []):
            library_id = (call.get("libraryID") or "").strip().upper()
            if library_id not in valid_branch_keys:
                log.debug(f"  Unknown libraryID {library_id!r} — skipped")
                continue

            call_number = call.get("callNumber") or ""
            volume      = extract_volume_from_callnumber(call_number)

            statuses = [
                item_status(item.get("currentLocationID"), item.get("dueDate"))
                for item in call.get("ItemInfo", [])
            ] or ["Graphic Novel - Young Adult Fiction"]

            # If any copy is on shelf, report the branch as available
            final_status = next(
                (s for s in statuses if "Checked Out" not in s),
                "Checked Out",
            )

            books.append({
                "manga_id":        manga_id,
                "volume":          volume,
                "branch_status":   [(library_id, final_status)],
                "availability_id": availability_id,
            })
            availability_id += 1

    return books, availability_id


# ── Orchestration ─────────────────────────────────────────────────────────────

def _read_positional(path: Path) -> list[str]:
    """
    Read a positional flat file preserving blank lines.
    Returns list where index i = line i (0-based), '' for blank/gap slots.
    """
    if not path.exists():
        return []
    return [line.rstrip('\n') for line in path.open(encoding='utf-8')]


def scrape(start: int = 1, end: int = 1,
           indices: list[int] | None = None,
           debug: bool = False) -> list:
    """
    Scrape library availability.

    Titles and authors are read positionally — blank lines are gap slots from
    out-of-order get_manga runs and are silently skipped.  Indices are always
    1-based line numbers in the positional files.
    """
    titles_path  = DATA_DIR / "titles.txt"
    authors_path = DATA_DIR / "authors.txt"
    debug_dir    = DATA_DIR.parent / "debug" if debug else None

    # Positional read — preserves blank gap slots
    titles  = _read_positional(titles_path)
    authors = _read_positional(authors_path)

    manga_id_map = _load_manga_id_map()

    def _resolve(pos: int, label: str):
        """Return (title, author, mal_id) for 0-based pos, or None to skip."""
        if pos < 0 or pos >= len(titles):
            log.warning(f"  {label} out of range (file has {len(titles)} lines) — skipping")
            return None
        t = titles[pos].strip()
        if not t:
            log.debug(f"  {label} is a gap slot (blank line) — skipping")
            return None
        a = authors[pos].strip() if pos < len(authors) else ""
        if not a:
            log.warning(f"  {label} '{t}' has no author — skipping")
            return None
        mal_id = manga_id_map.get(t.lower())
        if mal_id is None:
            log.warning(f"  {label} '{t}' not found in manga.csv — skipping")
            return None
        return t, a, mal_id

    if indices is not None:
        pairs = []
        for i in indices:
            result = _resolve(i - 1, f"Index {i}")
            if result:
                pairs.append(result)
    else:
        start = max(1, start)
        end   = min(end, len(titles))
        pairs = []
        for i in range(start - 1, end):
            result = _resolve(i, f"Line {i+1}")
            if result:
                pairs.append(result)

    log.info(f"Scraping {len(pairs)} titles via ILSWS REST API")

    session         = make_session()
    all_books: list = []
    availability_id = 1

    for progress, (title, author, manga_id) in enumerate(pairs, start=1):
        log.info(f"[{progress}/{len(pairs)}] (ID {manga_id}) {title!r} by {author!r}")
        print(f"[{progress}/{len(pairs)}] {title}", flush=True)

        catalog_keys: list = []
        seen_keys:    set  = set()
        page = 0
        while True:
            keys = fetch_catalog_keys(session, title, author, page)
            log.info(f"  Search page {page}: {len(keys)} catalog keys")
            for k in keys:
                if k not in seen_keys:
                    seen_keys.add(k)
                    catalog_keys.append(k)
            if len(keys) < RESULTS_PER_PAGE:
                break
            page += 1
            time.sleep(REQUEST_DELAY)

        if not catalog_keys:
            log.warning("  No catalog keys found — skipping")
            continue

        log.info(f"  {len(catalog_keys)} unique catalog key(s)")

        for key in catalog_keys:
            time.sleep(REQUEST_DELAY)
            data = fetch_title_info(session, key, debug_dir)
            if not data:
                log.warning(f"  No data for key {key}")
                continue

            books, availability_id = parse_title_info(data, manga_id, availability_id)
            all_books.extend(books)

            if books:
                vols     = [b["volume"] for b in books]
                branches = [b["branch_status"][0][0] for b in books]
                log.info(f"  Key {key}: vol(s) {vols} @ {branches}")

    log.info(f"Scraping complete — {len(all_books)} volume entries")
    return all_books


def write_to_db(books: list) -> str:
    from config.settings import DB_CONFIG
    import mysql.connector
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    manga_ids = list({b["manga_id"] for b in books})
    # Delete old LCPL rows for these titles only
    cursor.execute("""
        DELETE bas FROM branch_availability_status bas
        JOIN availability a ON bas.AvailabilityID = a.AvailabilityID
        JOIN branch b ON bas.BranchID = b.BranchID
        JOIN library l ON b.LibraryID = l.LibraryID
        WHERE a.MangaID IN ({})
          AND l.LibraryName LIKE '%Leon%'
    """.format(','.join(['%s'] * len(manga_ids))), tuple(manga_ids))
    cursor.execute("""
        DELETE a FROM availability a
        LEFT JOIN branch_availability_status bas ON a.AvailabilityID = bas.AvailabilityID
        JOIN branch b ON bas.BranchID = b.BranchID  
        JOIN library l ON b.LibraryID = l.LibraryID
        WHERE a.MangaID IN ({})
          AND l.LibraryName LIKE '%Leon%'
          AND bas.AvailabilityID IS NULL
    """.format(','.join(['%s'] * len(manga_ids))), tuple(manga_ids))

    branch_id_map = _load_branch_id_map(cursor)  # SELECT BranchID, BranchName FROM branch
    inserted = 0
    for b in books:
        cursor.execute("INSERT INTO availability (MangaID, Volume) VALUES (%s, %s)",
                       (b["manga_id"], b["volume"]))
        avail_id = cursor.lastrowid
        for branch_key, status in b["branch_status"]:
            branch_id = branch_id_map.get(branch_key.upper())
            if branch_id:
                cursor.execute(
                    "INSERT INTO branch_availability_status (AvailabilityID, BranchID, Status) "
                    "VALUES (%s, %s, %s)", (avail_id, branch_id, status))
                inserted += 1
    conn.commit()
    cursor.close()
    conn.close()
    return f"inserted {inserted} LCPL availability rows"


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="LCPL manga scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("positional", nargs="*",
                        help="Legacy: <end>  OR  <start> <end>")
    parser.add_argument("--debug",   action="store_true",
                        help="Write raw ILSWS JSON to debug/ folder")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--line",    type=int,
                       help="Scrape a single 1-based line number (e.g. --line 3)")
    group.add_argument("--range",   type=str,
                       help="Scrape a range of lines (e.g. --range 1-50)")
    group.add_argument("--indices", type=lambda s: [int(x) for x in s.split(",")],
                       metavar="N,N,…",
                       help="Scrape specific comma-separated 1-based indices (e.g. --indices 1,4,7)")

    args = parser.parse_args()

    if args.indices:
        books = scrape(indices=args.indices, debug=args.debug)

    elif args.line:
        books = scrape(indices=[args.line], debug=args.debug)

    elif args.range:
        try:
            parts = args.range.split("-")
            start = max(1, int(parts[0]))
            end   = int(parts[1])
        except (ValueError, IndexError):
            print("Invalid --range format. Use START-END (e.g. --range 1-50)")
            sys.exit(1)
        books = scrape(start=start, end=end, debug=args.debug)

    elif args.positional:
        # Legacy positional interface — still used by backend.py subprocess calls
        pos = args.positional
        try:
            if len(pos) == 1:
                books = scrape(start=1, end=int(pos[0]), debug=args.debug)
            elif len(pos) == 2:
                books = scrape(start=int(pos[0]), end=int(pos[1]), debug=args.debug)
            else:
                print(__doc__)
                sys.exit(1)
        except ValueError:
            print(__doc__)
            sys.exit(1)

    else:
        print(__doc__)
        sys.exit(1)

    write_to_db(books)
