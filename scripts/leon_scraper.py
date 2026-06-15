"""
scripts/leon_scraper.py  —  Leon County Public Library manga scraper

Fetches per-branch availability from the SirsiDynix ILSWS REST API.

Usage:
    python leon_scraper.py                      scrape all titles
    python leon_scraper.py --range 1-50         scrape lines 1 to 50
    python leon_scraper.py --manga-ids 21,42    scrape specific MangaIDs
    python leon_scraper.py --range 1-50 --debug write raw ILSWS JSON to debug/
"""
from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from config.settings import DATA_DIR, BRANCH_MAPPING
from utils.database_utils import get_connection
from utils.scraper_utils import (
    STATUS_PRIORITY,
    extract_volume,
    is_novel,
    load_title_author_map,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Endpoints ─────────────────────────────────────────────────────────────────

CATALOG_BASE     = "https://lcpl.ent.sirsi.net/client/en_US/lcpl"
SEARCH_BASE      = f"{CATALOG_BASE}/search/results"
ILSWS_BASE       = "https://lcpl.sirsi.net/lcpl_ilsws/rest/standard/lookupTitleInfo"
RESULTS_PER_PAGE = 12
REQUEST_DELAY    = 0.5
MAX_RETRIES      = 3
DB_BATCH_SIZE    = 20


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
    try:
        s.get(CATALOG_BASE, timeout=20)
        s.headers.update({"Referer": CATALOG_BASE})
        log.info("Session primed")
    except Exception as e:
        log.warning(f"Session prime failed (continuing): {e}")
    return s


# ── Step 1: search page → catalog keys ───────────────────────────────────────

def _extract_keys_and_volume(a_tag, keys_map: dict) -> None:
    href       = a_tag.get("href", "")
    title_text = a_tag.get("title", "")
    html_vol   = extract_volume(title_text) or None

    for pattern in (
        r"SD_ILS[:\u003a](\d+)",
        r"SD_ILS%3[Aa](\d+)",
        r"SD_ILS\$003[Aa](\d+)",
        r"SD_ILS:(\d+)",
    ):
        for m in re.finditer(pattern, href, re.IGNORECASE):
            k = int(m.group(1))
            if k not in keys_map or (keys_map[k] is None and html_vol is not None):
                keys_map[k] = html_vol


def fetch_catalog_keys(
    session: requests.Session,
    title: str,
    author: str,
    manga_type: str = "",
    page: int = 0,
) -> dict:
    params: dict = {
        "qu": ["", f"TITLE={title}", f"AUTHOR={author}"],
        "te": "ILS",
        "h":  "1",
        "lm": "BOOKS",
    }

    if is_novel(manga_type):
        params["qu"] = ["", f"TITLE={title}", f"AUTHOR={author}", "-SUBJECT=Comic"]

    if page > 0:
        params["rw"] = page * RESULTS_PER_PAGE

    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(SEARCH_BASE, params=params, timeout=15)
            r.raise_for_status()
            break
        except requests.RequestException as e:
            log.warning(f"  Search attempt {attempt + 1} failed: {e}")
            if attempt == MAX_RETRIES - 1:
                return {}
            time.sleep(2 ** attempt)
    else:
        return {}

    soup = BeautifulSoup(r.text, "html.parser")
    result_text = soup.find(id="searchResultText")
    if result_text and "No results" in result_text.get_text():
        return {}

    keys_map: dict = {}
    for a in soup.find_all("a", id=re.compile(r"^detailLink")):
        _extract_keys_and_volume(a, keys_map)

    if not keys_map:
        for a in soup.find_all("a", href=True):
            _extract_keys_and_volume(a, keys_map)

    return keys_map


# ── Step 2: ILSWS REST API ────────────────────────────────────────────────────

def _strip_jsonp(text: str) -> str:
    text = text.strip()
    m    = re.match(r"^[^(]*\((.*)\)\s*;?\s*$", text, re.DOTALL)
    return m.group(1) if m else text


def fetch_title_info(
    session: requests.Session,
    catalog_key: int,
    debug_dir=None,
) -> dict:
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
            log.warning(f"  ILSWS attempt {attempt + 1} for key {catalog_key}: {e}")
            if attempt == MAX_RETRIES - 1:
                return {}
            time.sleep(2 ** attempt)

    return {}


# ── Step 3: parse ILSWS response ──────────────────────────────────────────────

def item_status(current_loc: str, due_date) -> str:
    loc = (current_loc or "").upper()
    if loc in ("CHECKEDOUT", "CHECKED-OUT", "OUT") or due_date:
        return "Checked Out"
    if "HOLD" in loc:
        return "On Hold"
    if "TRANSIT" in loc:
        return "In Transit"
    return "Graphic Novel - Young Adult Fiction"


def parse_title_info(
    data: dict,
    manga_id: int,
    availability_id: int,
    manga_type: str,
    html_volume: int | None = None,
) -> tuple:
    books            = []
    valid_branch_keys = {k.upper() for k in BRANCH_MAPPING}

    for title_entry in data.get("TitleInfo", []):
        for call in title_entry.get("CallInfo", []):
            library_id  = (call.get("libraryID") or "").strip().upper()
            if library_id not in valid_branch_keys:
                continue

            call_number = call.get("callNumber") or ""
            if is_novel(manga_type) and "GRAPHIC" in call_number.upper():
                continue

            volume = html_volume if html_volume is not None else extract_volume(call_number)

            statuses = [
                item_status(item.get("currentLocationID"), item.get("dueDate"))
                for item in call.get("ItemInfo", [])
            ] or ["Graphic Novel - Young Adult Fiction"]

            final_status = next(
                (s for s in statuses if "Checked Out" not in s), "Checked Out"
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

def scrape(
    start: int = 1,
    end: int = 999_999,
    manga_ids: list[int] | None = None,
    debug: bool = False,
) -> list:
    debug_dir = DATA_DIR.parent / "debug" if debug else None
    all_pairs = load_title_author_map()

    if manga_ids:
        target_ids      = set(manga_ids)
        pairs_to_scrape = [p for p in all_pairs if p[3] in target_ids]
    else:
        pairs_to_scrape = all_pairs[max(0, start - 1):end]

    log.info(f"Scraping {len(pairs_to_scrape)} titles via ILSWS REST API")

    session         = make_session()
    all_books: list = []
    availability_id = 1

    for progress, (idx, title, author, manga_id, manga_type) in enumerate(pairs_to_scrape, 1):
        if not author:
            log.warning(f"  Index {idx} '{title}' has no author — skipping")
            continue

        log.info(f"[{progress}/{len(pairs_to_scrape)}] (ID {manga_id}) {title!r} by {author!r}")
        print(f"[{progress}/{len(pairs_to_scrape)}] {title}", flush=True)

        catalog_keys: dict = {}
        page = 0
        while True:
            page_keys = fetch_catalog_keys(session, title, author, manga_type, page)
            log.info(f"  Search page {page}: {len(page_keys)} catalog keys")
            catalog_keys.update({k: v for k, v in page_keys.items() if k not in catalog_keys})

            if len(page_keys) < RESULTS_PER_PAGE:
                break
            page += 1
            time.sleep(REQUEST_DELAY)

        if not catalog_keys:
            log.warning("  No catalog keys found — skipping")
            continue

        log.info(f"  {len(catalog_keys)} unique catalog key(s)")

        for key, html_vol in catalog_keys.items():
            time.sleep(REQUEST_DELAY)
            data = fetch_title_info(session, key, debug_dir)
            if not data:
                log.warning(f"  No data for key {key}")
                continue

            books, availability_id = parse_title_info(
                data, manga_id, availability_id, manga_type, html_vol
            )
            all_books.extend(books)

            if books:
                vols     = [b["volume"] for b in books]
                branches = [b["branch_status"][0][0] for b in books]
                log.info(f"  Key {key}: vol(s) {vols} @ {branches}")

    log.info(f"Scraping complete — {len(all_books)} volume entries")
    return all_books


# ── Database write with batch commits ─────────────────────────────────────────

def write_to_db(books: list) -> str:
    if not books:
        return "no books to write"

    conn   = get_connection()
    cursor = conn.cursor()

    branch_id_map = {k.upper(): v for k, v in BRANCH_MAPPING.items()}

    cursor.execute("SELECT LibraryID FROM library WHERE LibraryName LIKE '%Leon%' LIMIT 1")
    row = cursor.fetchone()
    if not row:
        conn.close()
        return "error: LCPL library not found in DB — run a DB reset first"
    lcpl_library_id = row[0]

    manga_ids    = list({b["manga_id"] for b in books})
    placeholders = ",".join(["%s"] * len(manga_ids))

    cursor.execute(f"""
        DELETE bas FROM branch_availability_status bas
        JOIN availability a ON bas.AvailabilityID = a.AvailabilityID
        JOIN branch b ON bas.BranchID = b.BranchID
        WHERE a.MangaID IN ({placeholders}) AND b.LibraryID = %s
    """, (*manga_ids, lcpl_library_id))

    cursor.execute(f"""
        DELETE a FROM availability a
        LEFT JOIN branch_availability_status bas ON a.AvailabilityID = bas.AvailabilityID
        WHERE a.MangaID IN ({placeholders}) AND bas.AvailabilityID IS NULL
    """, tuple(manga_ids))

    conn.commit()

    scraped_at = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    inserted    = skipped     = 0
    batch_rows  = 0
    manga_batch = 0
    current_manga_id: int | None = None

    for b in books:
        if b["manga_id"] != current_manga_id:
            current_manga_id = b["manga_id"]
            manga_batch += 1

        cursor.execute(
            "INSERT INTO availability (MangaID, Volume, ScrapedAt) VALUES (%s, %s, %s)",
            (b["manga_id"], b["volume"], scraped_at),
        )
        avail_id = cursor.lastrowid
        batch_rows += 1

        for branch_key, status in b["branch_status"]:
            branch_id = branch_id_map.get(branch_key.upper())
            if branch_id:
                cursor.execute(
                    "INSERT INTO branch_availability_status "
                    "(AvailabilityID, BranchID, Status) VALUES (%s, %s, %s)",
                    (avail_id, branch_id, status),
                )
                inserted   += 1
                batch_rows += 1
            else:
                skipped += 1
                log.warning(f"  Unknown branch key '{branch_key}' — not in BRANCH_MAPPING")

        if manga_batch >= DB_BATCH_SIZE:
            conn.commit()
            print(f"  [DB] committed batch ({manga_batch} titles, {batch_rows} rows)", flush=True)
            manga_batch = batch_rows = 0

    if batch_rows > 0:
        conn.commit()
        print(f"  [DB] final commit ({manga_batch} titles, {batch_rows} rows)", flush=True)

    cursor.close()
    conn.close()

    msg = f"inserted {inserted} LCPL availability rows"
    if skipped:
        msg += f" ({skipped} branch keys unrecognized — check BRANCH_MAPPING in settings.py)"
    return msg


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Leon County Public Library manga scraper")
    parser.add_argument("--debug",     action="store_true", help="Write raw ILSWS JSON to debug/")
    parser.add_argument("--range",     type=str, help="Scrape a range of titles (e.g. 1-50)")
    parser.add_argument("--manga-ids", type=lambda s: [int(x) for x in s.split(",")],
                        metavar="ID,ID", help="Comma-separated MangaIDs")
    args = parser.parse_args()

    if args.manga_ids:
        books = scrape(manga_ids=args.manga_ids, debug=args.debug)
    elif args.range:
        try:
            lo, hi = args.range.split("-")
            books  = scrape(start=max(1, int(lo)), end=int(hi), debug=args.debug)
        except (ValueError, IndexError):
            print("Invalid --range format. Use START-END (e.g. --range 1-50)")
            raise SystemExit(1)
    else:
        books = scrape(debug=args.debug)

    print(write_to_db(books))
