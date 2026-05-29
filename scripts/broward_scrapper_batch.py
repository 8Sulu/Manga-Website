"""
Broward County Library batch scraper.

Searches each title from titles.txt against the Broward catalog and writes
combined availability data to:
    data/broward_availability.csv   — one row per (MangaID, volume=0)
    data/broward_bas.csv            — one row per title with a single "BROWARD" branch entry

Usage:
    python broward_scrapper_batch.py                  scrape all titles
    python broward_scrapper_batch.py <start> <end>    scrape 1-based index range
    python broward_scrapper_batch.py --indices 1,4,7  scrape specific indices
"""
from __future__ import annotations

import csv
import json
import re
import sys
import time
import logging
from pathlib import Path

import requests
from bs4 import BeautifulSoup

sys.path.append(str(Path(__file__).parent.parent))
from config.settings import DATA_DIR

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ── Broward endpoints ─────────────────────────────────────────────────────────
BASE_URL         = "https://broward.ent.sirsi.net"
SEARCH_URL       = f"{BASE_URL}/client/en_US/default/search/results"
REQUEST_DELAY    = 0.8
MAX_RETRIES      = 3

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


# ── Load manga ID map from manga.csv ──────────────────────────────────────────

def _load_manga_id_map() -> dict[str, int]:
    path = DATA_DIR / "manga.csv"
    if not path.exists():
        log.error("manga.csv not found")
        return {}
    result: dict[str, int] = {}
    with open(path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            title  = (row.get("Title") or "").strip()
            mal_id = (row.get("MangaID") or "").strip()
            if title and mal_id:
                try:
                    result[title.lower()] = int(mal_id)
                except ValueError:
                    pass
    return result


# ── Broward catalog search ────────────────────────────────────────────────────

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    try:
        s.get(f"{BASE_URL}/client/en_US/default", timeout=20)
        log.info("Broward session primed")
    except Exception as e:
        log.warning(f"Session prime failed: {e}")
    return s


def search_broward(session: requests.Session, title: str) -> dict:
    """
    Search Broward catalog for a title. Returns a summary dict:
      {found: bool, total_copies: int, available_copies: int, hold_count: int}
    """
    params = {
        "qu": ["", f"TITLE={title}"],
        "te": "ILS",
        "lm": "BOOKS",
        "h":  "1",
    }

    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(SEARCH_URL, params=params, timeout=20)
            r.raise_for_status()
            break
        except requests.RequestException as e:
            log.warning(f"  Search attempt {attempt+1} failed: {e}")
            if attempt == MAX_RETRIES - 1:
                return {"found": False}
            time.sleep(2 ** attempt)
    else:
        return {"found": False}

    soup = BeautifulSoup(r.text, "html.parser")

    # Check "No results"
    result_text = soup.find(id="searchResultText") or soup.find(class_="noResultsScreen")
    if result_text and "No results" in result_text.get_text():
        return {"found": False}

    # Try to extract item count from result summary text
    total_copies    = 0
    available_copies = 0
    hold_count      = 0

    # Look for availability summary divs (SirsiDynix pattern)
    avail_spans = soup.find_all(class_=re.compile(r"(?i)(avail|copies|holds)"))
    for span in avail_spans:
        text = span.get_text(" ", strip=True)
        m = re.search(r'(\d+)\s+of\s+(\d+)\s+cop', text, re.IGNORECASE)
        if m:
            available_copies = int(m.group(1))
            total_copies     = int(m.group(2))
        m2 = re.search(r'(\d+)\s+hold', text, re.IGNORECASE)
        if m2:
            hold_count = int(m2.group(1))

    # Fallback: count result rows to detect if the title exists at all
    result_rows = soup.select(".results_cell, .result_cell, .detailItemArrayCell")
    found = bool(result_rows) or total_copies > 0

    # Try alternate pattern: "N copies, N available"
    if not total_copies:
        for tag in soup.find_all(string=re.compile(r'\d+ cop', re.IGNORECASE)):
            m = re.search(r'(\d+)\s+cop', tag, re.IGNORECASE)
            if m:
                total_copies = int(m.group(1))
                break
        for tag in soup.find_all(string=re.compile(r'\d+ avail', re.IGNORECASE)):
            m = re.search(r'(\d+)\s+avail', tag, re.IGNORECASE)
            if m:
                available_copies = int(m.group(1))
                break

    # If we got search results but couldn't parse counts, at least mark it found
    if not found:
        # Check for any search result links
        detail_links = soup.find_all("a", id=re.compile(r"detailLink"))
        found = bool(detail_links)

    if not found:
        return {"found": False}

    # Determine status string
    if available_copies > 0:
        status = "Available"
    elif total_copies > 0:
        status = "Checked Out"
    else:
        status = "Available"   # found but no count — assume available

    return {
        "found":             True,
        "total_copies":      total_copies,
        "available_copies":  available_copies,
        "hold_count":        hold_count,
        "status":            status,
    }


# ── Orchestration ─────────────────────────────────────────────────────────────

def scrape_broward(start: int = 1, end: int | None = None,
                   indices: list[int] | None = None) -> list:
    """
    Scrape Broward availability for titles in titles.txt.
    Returns list of dicts: {manga_id, status, total_copies, available_copies, hold_count}
    """
    titles_path = DATA_DIR / "titles.txt"
    if not titles_path.exists():
        log.error("titles.txt not found")
        return []

    with open(titles_path, encoding="utf-8") as f:
        all_titles = [l.strip() for l in f if l.strip()]

    manga_id_map = _load_manga_id_map()

    # Build work list
    if indices is not None:
        pairs = []
        for i in indices:
            pos = i - 1
            if pos < 0 or pos >= len(all_titles):
                log.warning(f"Index {i} out of range")
                continue
            t = all_titles[pos]
            mal_id = manga_id_map.get(t.lower())
            if mal_id is None:
                log.warning(f"'{t}' not in manga.csv — skipping")
                continue
            pairs.append((t, mal_id))
    else:
        end = end or len(all_titles)
        start = max(1, start)
        end   = min(end, len(all_titles))
        pairs = []
        for i in range(start - 1, end):
            t = all_titles[i]
            mal_id = manga_id_map.get(t.lower())
            if mal_id is None:
                log.warning(f"'{t}' not in manga.csv — skipping")
                continue
            pairs.append((t, mal_id))

    log.info(f"Scraping {len(pairs)} titles from Broward catalog")
    session = make_session()
    results = []

    for progress, (title, manga_id) in enumerate(pairs, 1):
        print(f"[{progress}/{len(pairs)}] {title}", flush=True)
        log.info(f"[{progress}/{len(pairs)}] (ID {manga_id}) {title!r}")

        time.sleep(REQUEST_DELAY)
        data = search_broward(session, title)

        if not data["found"]:
            log.info(f"  Not found in Broward catalog")
            continue

        results.append({
            "manga_id":        manga_id,
            "status":          data.get("status", "Available"),
            "total_copies":    data.get("total_copies", 0),
            "available_copies": data.get("available_copies", 0),
            "hold_count":      data.get("hold_count", 0),
        })
        log.info(f"  Found — status: {data.get('status')} "
                 f"({data.get('available_copies')}/{data.get('total_copies')} copies)")

    log.info(f"Broward scrape complete — {len(results)} titles found")
    return results


# ── CSV output ────────────────────────────────────────────────────────────────
# LibraryID 2 = Broward County Library (seeded in libraries.csv)
BROWARD_LIBRARY_ID = 2
BROWARD_BRANCH_ID  = 8   # single virtual branch for all of Broward

def write_broward_csvs(results: list) -> None:
    """
    Write broward_availability.csv and broward_bas.csv.

    These are separate files from LCPL's csvs so they can be loaded
    independently and merged by the backend reset / upsert logic.

    Schema mirrors availability.csv / branch_availability_status.csv but adds
    LibraryID so the backend knows which library each row belongs to.
    """
    avail_path = DATA_DIR / "broward_availability.csv"
    bas_path   = DATA_DIR / "broward_bas.csv"

    avail_id = 1

    with open(avail_path, "w", newline="", encoding="utf-8") as fa, \
         open(bas_path,   "w", newline="", encoding="utf-8") as fb:

        avail_writer = csv.DictWriter(fa, fieldnames=["AvailabilityID", "MangaID", "Volume", "LibraryID"])
        avail_writer.writeheader()

        bas_writer = csv.DictWriter(fb, fieldnames=["AvailabilityID", "BranchID", "Status"])
        bas_writer.writeheader()

        for row in results:
            avail_writer.writerow({
                "AvailabilityID": avail_id,
                "MangaID":        row["manga_id"],
                "Volume":         0,           # Broward: no per-volume data
                "LibraryID":      BROWARD_LIBRARY_ID,
            })
            bas_writer.writerow({
                "AvailabilityID": avail_id,
                "BranchID":       BROWARD_BRANCH_ID,
                "Status":         row["status"],
            })
            avail_id += 1

    log.info(f"Wrote {len(results)} Broward entries → {avail_path.name}, {bas_path.name}")
    print(f"[+] Wrote {len(results)} Broward availability rows", flush=True)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if "--indices" in sys.argv:
        idx_idx = sys.argv.index("--indices")
        if idx_idx + 1 >= len(sys.argv):
            print("--indices requires a comma-separated list, e.g. --indices 1,4,7")
            sys.exit(1)
        idx_list = [int(x) for x in sys.argv[idx_idx + 1].split(",")]
        results = scrape_broward(indices=idx_list)
    else:
        argv = [a for a in sys.argv[1:] if not a.startswith("--")]
        if not argv:
            results = scrape_broward()
        elif len(argv) == 1:
            results = scrape_broward(start=1, end=int(argv[0]))
        elif len(argv) == 2:
            results = scrape_broward(start=int(argv[0]), end=int(argv[1]))
        else:
            print(__doc__)
            sys.exit(1)

    write_broward_csvs(results)
