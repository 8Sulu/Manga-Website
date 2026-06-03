"""
Broward County Library manga scraper — volume-aware, per-branch edition.

Mirrors the LCPL data model exactly:
  - One availability row per (MangaID, Volume)
  - One branch_availability_status row per (availability, branch)

Usage:
    python broward_scrapper.py                     scrape all titles
    python broward_scrapper.py --line 3            scrape line 3
    python broward_scrapper.py --range 1-50        scrape lines 1 to 50
    python broward_scrapper.py --indices 1,4,7     scrape specific 1-based indices
    python broward_scrapper.py --output file.csv   also write a CSV (debug/audit)
    python broward_scrapper.py --debug             verbose URL/field logging
"""
from __future__ import annotations

import csv
import time
import argparse
import logging
import re
import sys
import urllib.parse
from collections import defaultdict
from pathlib import Path

import requests
from bs4 import BeautifulSoup

sys.path.append(str(Path(__file__).parent.parent))
from config.settings import DATA_DIR, DB_CONFIG, BROWARD_BRANCH_MAPPING

log = logging.getLogger(__name__)

BASE_URL = "https://broward.ent.sirsi.net"
CLIENT   = "/client/en_US/default"

HEADERS = {
    "User-Agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36",
    "Accept":           "text/javascript, text/html, application/json, */*",
    "Accept-Language":  "en-US,en;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
    "Origin":           BASE_URL,
}

# SD_ITEM_STATUS values meaning the item is physically on the shelf
ON_SHELF_STATUSES = {
    "general collection",
    "new materials",
    "reference",
    "graphic novels",
    "young adult",
    "children",
}

# ── Volume extraction ─────────────────────────────────────────────────────────

_OMNIBUS_RANGE = re.compile(r'(\d+)\s*[-–]\s*\d+')   # "7-8-9" or "7-9" → 7

_VOL_PATTERNS = [
    re.compile(r'\bvol(?:ume)?\.?\s*(\d+)', re.I),
    re.compile(r'\bv\.?\s*(\d+)\b', re.I),
    re.compile(r'#\s*(\d+)\b'),
    re.compile(r'\bno\.?\s*(\d+)\b', re.I),
    re.compile(r'\bpart\s*(\d+)\b', re.I),
    re.compile(r'\bbook\s*(\d+)\b', re.I),
    re.compile(r'\bep(?:isode)?\.?\s*(\d+)\b', re.I),
]


def extract_volume(text: str) -> int:
    if not text:
        return 0

    m = _OMNIBUS_RANGE.search(text)
    if m:
        return int(m.group(1))

    for pat in _VOL_PATTERNS:
        m = pat.search(text)
        if m:
            return int(m.group(1))

    m = re.search(r'[.,]\s*(\d+)\s*$', text)
    if m:
        return int(m.group(1))

    m = re.search(r'\b(\d+)\s*$', text)
    if m:
        return int(m.group(1))

    return 0


def _is_novel(manga_type: str) -> bool:
    """Return True for Light Novel and Novel types (handles both 'light novel' and 'light-novel')."""
    return manga_type.lower().replace(' ', '-') in ('light-novel', 'novel')


# ── DB helpers ────────────────────────────────────────────────────────────────

def _get_db():
    import mysql.connector
    return mysql.connector.connect(**DB_CONFIG)


def _load_title_author_map() -> list[tuple[int, str, str, int, str]]:
    conn = _get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute("SELECT MangaID, Title, Author, Type FROM manga ORDER BY MangaID")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [(i + 1, r["Title"], r["Author"] or "", r["MangaID"], r["Type"] or "") for i, r in enumerate(rows)]


def _load_broward_branch_map() -> dict[str, int]:
    conn = _get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT b.BranchID, b.BranchName FROM branch b "
        "JOIN library l ON b.LibraryID = l.LibraryID "
        "WHERE l.LibraryName LIKE %s",
        ("%Broward%",),
    )
    rows = cur.fetchall()
    cur.close(); conn.close()
    if not rows:
        raise RuntimeError(
            "No Broward branches found in the database. "
            "Run a DB reset to seed libraries.csv / branches.csv first."
        )
    mapping = {r["BranchName"]: r["BranchID"] for r in rows}
    log.info(f"Loaded {len(mapping)} Broward branch(es) from DB")
    return mapping


def _upsert_broward_results(cursor, manga_id: int, broward_library_id: int,
                             volume_branch_map: dict[int, dict[int, str]]) -> str:
    if not volume_branch_map:
        return "no volume data to store"

    cursor.execute(
        """
        DELETE bas FROM branch_availability_status bas
        JOIN availability a  ON bas.AvailabilityID = a.AvailabilityID
        JOIN branch b        ON bas.BranchID = b.BranchID
        WHERE a.MangaID = %s AND b.LibraryID = %s
        """,
        (manga_id, broward_library_id),
    )
    cursor.execute(
        """
        DELETE a FROM availability a
        LEFT JOIN branch_availability_status bas ON a.AvailabilityID = bas.AvailabilityID
        WHERE a.MangaID = %s AND bas.AvailabilityID IS NULL
        """,
        (manga_id,),
    )

    avail_inserted = branch_inserted = 0
    for volume_num in sorted(volume_branch_map.keys()):
        branch_statuses = volume_branch_map[volume_num]
        if not branch_statuses:
            continue
        cursor.execute(
            "INSERT INTO availability (MangaID, Volume) VALUES (%s, %s)",
            (manga_id, volume_num),
        )
        avail_id = cursor.lastrowid
        avail_inserted += 1
        for branch_id, status in branch_statuses.items():
            cursor.execute(
                "INSERT INTO branch_availability_status "
                "(AvailabilityID, BranchID, `Status`) VALUES (%s, %s, %s)",
                (avail_id, branch_id, status),
            )
            branch_inserted += 1

    return f"inserted {avail_inserted} avail + {branch_inserted} branch rows"


# ── Detail page parser ────────────────────────────────────────────────────────

def parse_detail_page(soup: BeautifulSoup, manga_type: str = '') -> dict | None:
    """
    Parse a single SirsiDynix detail page (redirect when there is exactly one result).
    Returns an item dict compatible with the multi-result path, or None on failure.
    """
    try:
        id_element = soup.find(string=re.compile(r'SD_ILS:\d+'))
        if not id_element:
            return None

        m_id = re.search(r'SD_ILS:(\d+)', id_element).group(1)

        title_h1 = soup.find('h1') or soup.find('h2')
        title_text = title_h1.get_text(strip=True) if title_h1 else ""

        # For novel-type titles skip entries that are graphic-novel format
        availability = []
        table = soup.find('table', {'class': 'availability'})
        if table:
            for row in table.find_all('tr')[1:]:   # skip header
                cols = row.find_all('td')
                if len(cols) >= 4:
                    call_number = cols[1].get_text(strip=True)
                    if _is_novel(manga_type) and 'GRAPHIC' in call_number.upper():
                        continue
                    availability.append({
                        "library":     cols[0].get_text(strip=True),
                        "call_number": call_number,
                        "status":      cols[3].get_text(strip=True),
                    })

        return {
            "item_id":      m_id,
            "volume":       extract_volume(title_text),
            "availability": availability,
            "index":        "0",
        }
    except Exception as e:
        log.error(f"Error parsing detail page: {e}")
        return None


# ── Search results: collect item IDs, titles, and CSRF ────────────────────────

def get_search_results(session: requests.Session, title: str, author: str,
                       manga_type: str, debug: bool) -> tuple[list[dict], str]:
    """
    Return parsed items and the global sdcsrf token for the session.
    Items are dicts with: item_id, volume, index.
    """
    all_items = []
    seen      = set()
    offset    = 0
    page      = 1
    sdcsrf    = ""

    log.info(f"  Searching: '{title}' by {author}")

    while True:
        params: list[tuple[str, str]] = [
            ("qu",  f"TITLE={title}"),
            ("qu",  f"AUTHOR={author}"),
            ("qf",  "FORMAT\tSpecial Format\tBOOK\tBooks"),
            ("h",   "1"),
        ]

        if _is_novel(manga_type):
            params.append(("qu", "-SUBJECT=Comic"))

        if offset > 0:
            params.append(("rw", str(offset)))
            params.append(("isd", "true"))

        url = f"{BASE_URL}{CLIENT}/search/results"
        if debug:
            print(f"[DEBUG] search page {page}: {url} params={params}")

        try:
            r = session.get(url, params=params, headers=HEADERS, timeout=15, allow_redirects=True)
            if r.status_code != 200:
                break

            # ── Single-result redirect ────────────────────────────────────────
            if "/one" in r.url:
                log.info("Redirected to detail page. Parsing directly.")
                soup = BeautifulSoup(r.text, "html.parser")
                token_input = soup.find("input", {"name": "sdcsrf"})
                if token_input:
                    sdcsrf = token_input.get("value", "")
                else:
                    m = re.search(r'var __sdcsrf = "([^"]+)"', r.text)
                    if m:
                        sdcsrf = m.group(1)
                single_item = parse_detail_page(soup, manga_type)
                if single_item:
                    all_items.append(single_item)
                break

            # ── Multiple-results page ─────────────────────────────────────────
            # Extract the CSRF token needed for subsequent availability lookups
            if not sdcsrf:
                m = re.search(r'var __sdcsrf = "([^"]+)"', r.text)
                if not m:
                    m = re.search(r'name="sdcsrf"\s*value="([^"]+)"', r.text)
                if m:
                    sdcsrf = m.group(1)

            soup  = BeautifulSoup(r.text, "html.parser")
            cells = soup.select("div.results_cell")
            if not cells:
                break

            new_found = False
            for cell in cells:
                inp = cell.select_one("input[value*='ent://SD_ILS/0/SD_ILS:']")
                if not inp:
                    continue

                val  = inp.get("value", "")
                m_id = re.search(r'SD_ILS:(\d+)', val)
                if not m_id:
                    continue
                item_id = m_id.group(1)

                if item_id in seen:
                    continue

                seen.add(item_id)
                new_found = True

                title_link = cell.select_one("div.displayDetailLink a")
                title_text_cell = title_link.get("title", "") if title_link else ""

                call_div  = cell.select_one("div.PREFERRED_CALLNUMBER div.displayElementText")
                call_text = call_div.get_text(" ", strip=True) if call_div else ""

                # For novel-type titles, skip entries shelved as graphic novels
                has_graphic = "GRAPHIC" in call_text.upper()
                if _is_novel(manga_type) and has_graphic:
                    if debug:
                        print(f"[DEBUG] Skipping manga-format entry for novel title {title_text_cell!r}")
                    continue

                vol_from_title = extract_volume(title_text_cell)
                vol_from_call  = extract_volume(call_text)
                volume = vol_from_title if vol_from_title else vol_from_call

                cell_id = cell.get("id", "")
                m_idx   = re.search(r'\d+', cell_id)
                idx     = m_idx.group(0) if m_idx else "0"

                all_items.append({
                    "item_id": item_id,
                    "volume":  volume,
                    "index":   idx,
                })

            if not new_found or len(cells) < 12:
                break

            offset += 12
            page   += 1
            time.sleep(0.8)

        except Exception as e:
            log.warning(f"  Search error: {e}")
            break

    log.info(f"  Found {len(all_items)} catalog item(s)")
    return all_items, sdcsrf


# ── Per-item JSON fetch (availability mapping) ────────────────────────────────

def fetch_item_availability(session: requests.Session, item_id: str, idx: str,
                            title: str, author: str, sdcsrf: str, debug: bool) -> list[dict]:
    """POST lookuptitleinfo → JSON with per-copy branch/status."""
    ent_encoded = f"ent:$002f$002fSD_ILS$002f0$002fSD_ILS:{item_id}"

    info_url = (
        f"{BASE_URL}{CLIENT}/search/results"
        f".displaypanel.displaycell_0.detail.detailavailabilityaccordions"
        f":lookuptitleinfo"
        f"/{ent_encoded}/ILS/{idx}/true/true"
    )

    params = [
        ("qu",  f"TITLE={title}"),
        ("qu",  f"AUTHOR={author}"),
        ("qf",  "FORMAT\tSpecial Format\tBOOK\tBooks"),
        ("rw",  idx),
        ("d",   f"ent://SD_ILS/0/SD_ILS:{item_id}~ILS~{idx}"),
        ("st",  "RE"),
        ("isd", "true"),
        ("h",   "8"),
    ]

    if debug:
        print(f"[DEBUG] lookuptitleinfo: {info_url}")

    lookup_headers = HEADERS.copy()
    lookup_headers["sdcsrf"]        = sdcsrf
    lookup_headers["Content-Length"] = "0"

    try:
        r = session.post(info_url, params=params, headers=lookup_headers, timeout=15)
        if r.status_code != 200:
            log.warning(f"  lookuptitleinfo {item_id} → {r.status_code}")
            return []

        try:
            data = r.json()
        except ValueError:
            log.warning(f"  lookuptitleinfo {item_id}: invalid JSON response")
            return []

        copies = []
        for rec in data.get("childRecords", []):
            library    = (rec.get("LIBRARY") or "").strip()
            raw_status = (rec.get("SD_ITEM_STATUS") or "").strip()
            on_shelf   = raw_status.lower() in ON_SHELF_STATUSES
            copies.append({"library": library, "status": raw_status, "on_shelf": on_shelf})

        if debug:
            print(f"[DEBUG] item {item_id}: {len(copies)} copies")
            for c in copies:
                print(f"  [{'✓' if c['on_shelf'] else '✗'}] {c['library']} — {c['status']}")

        return copies

    except Exception as e:
        log.warning(f"  lookuptitleinfo error {item_id}: {e}")
        return []


# ── Aggregate per-item data ───────────────────────────────────────────────────

def build_volume_branch_map(
    item_copies: list[tuple[int, list[dict]]],
    branch_map: dict[str, int],
    debug: bool,
) -> dict[int, dict[int, str]]:
    STATUS_PRIORITY = {"Available": 2, "On Hold": 1, "Checked Out": 0}
    result: dict[int, dict[int, str]] = defaultdict(dict)
    unmatched: set[str] = set()

    for volume, copies in item_copies:
        for copy in copies:
            raw_name = copy["library"]
            db_name  = BROWARD_BRANCH_MAPPING.get(raw_name, raw_name)
            branch_id = branch_map.get(db_name)

            if branch_id is None:
                unmatched.add(raw_name)
                continue

            status = (
                "Available"   if copy["on_shelf"]
                else "On Hold" if "hold" in copy["status"].lower()
                else "Checked Out"
            )

            current = result[volume].get(branch_id)
            if current is None or STATUS_PRIORITY[status] > STATUS_PRIORITY[current]:
                result[volume][branch_id] = status

    if unmatched:
        log.warning(f"  Unmatched branch names (not in DB): {sorted(unmatched)}")
        if debug:
            print(f"[DEBUG] unmatched branches: {sorted(unmatched)}")

    return dict(result)


# ── Main processing ───────────────────────────────────────────────────────────

def process_batch(args: argparse.Namespace) -> None:
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    all_pairs = _load_title_author_map()
    if not all_pairs:
        print("[-] No titles found in DB.")
        return

    try:
        branch_map = _load_broward_branch_map()
    except RuntimeError as e:
        print(f"[-] {e}")
        return

    conn = _get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute("SELECT LibraryID FROM library WHERE LibraryName LIKE %s LIMIT 1", ("%Broward%",))
    lib_row = cur.fetchone()
    cur.close(); conn.close()

    if not lib_row:
        print("[-] Broward library not found in DB.")
        return
    broward_library_id = lib_row["LibraryID"]

    # ── Select titles to scrape ───────────────────────────────────────────────
    if args.manga_ids:
        target_ids = set(args.manga_ids)
        pairs = [p for p in all_pairs if p[3] in target_ids]
    elif args.range:
        try:
            s, e = args.range.split("-")
            pairs = all_pairs[max(0, int(s) - 1):int(e)]
        except (ValueError, IndexError):
            print("[-] Invalid --range. Use START-END (e.g. --range 1-50)")
            return
    else:
        pairs = all_pairs

    if not pairs:
        print("[-] No valid titles matched the criteria.")
        return

    print(f"[*] Broward library ID  : {broward_library_id}")
    print(f"[*] Broward branch count: {len(branch_map)}")
    print(f"[*] Scraping {len(pairs)} title(s)…\n")

    csv_file = csv_writer = None
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        csv_file   = open(out_path, "w", newline="", encoding="utf-8")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["Title", "Author", "ItemID", "Volume", "Library", "Status", "OnShelf"])
        print(f"[*] CSV → {out_path}\n")

    db_conn = _get_db()
    cursor  = db_conn.cursor()

    session = requests.Session()
    session.get(f"{BASE_URL}{CLIENT}", timeout=20)
    session.headers.update({
        "User-Agent":      HEADERS["User-Agent"],
        "Accept-Language": HEADERS["Accept-Language"],
        "Referer":         f"{BASE_URL}{CLIENT}",
    })

    for progress, (idx, title, author, manga_id, manga_type) in enumerate(pairs, start=1):
        if not author:
            log.warning(f"Index {idx} '{title}' — no author, skipping")
            continue

        print(f"[{progress}/{len(pairs)}] {title}", flush=True)

        items, sdcsrf = get_search_results(session, title, author, manga_type, args.debug)
        if not items:
            print("  [-] No catalog entries found")
            continue

        if not sdcsrf:
            print("  [-] Warning: no sdcsrf token found — availability lookups may fail.")

        item_copies: list[tuple[int, list[dict]]] = []
        for item in items:
            volume = item["volume"]
            copies = fetch_item_availability(
                session, item["item_id"], item["index"],
                title, author, sdcsrf, args.debug,
            )
            item_copies.append((volume, copies))
            if csv_writer:
                for c in copies:
                    csv_writer.writerow([
                        title, author, item["item_id"], volume,
                        c["library"], c["status"],
                        "Yes" if c["on_shelf"] else "No",
                    ])
            time.sleep(1.2)

        volume_branch_map = build_volume_branch_map(item_copies, branch_map, args.debug)

        total_vols   = len(volume_branch_map)
        avail_vols   = sum(
            1 for statuses in volume_branch_map.values()
            if any(s == "Available" for s in statuses.values())
        )
        total_copies = sum(len(c) for _, c in item_copies)
        vol_list     = sorted(volume_branch_map.keys())
        vol_display  = str(vol_list) if len(vol_list) <= 10 else f"{vol_list[:5]}…"
        print(
            f"  [{'✓' if avail_vols else '✗'}] "
            f"{avail_vols}/{total_vols} vols available "
            f"({total_copies} copies) — vols {vol_display}",
            flush=True,
        )

        if args.debug:
            for vol_num, branch_statuses in sorted(volume_branch_map.items()):
                for bid, status in branch_statuses.items():
                    bname = next((k for k, v in branch_map.items() if v == bid), str(bid))
                    mark  = "✓" if status == "Available" else ("~" if status == "On Hold" else "✗")
                    print(f"  [{mark}] vol {vol_num} @ {bname} — {status}")

        if volume_branch_map:
            try:
                msg = _upsert_broward_results(
                    cursor, manga_id, broward_library_id, volume_branch_map,
                )
                db_conn.commit()
                print(f"  [DB] {msg}", flush=True)
            except Exception as e:
                db_conn.rollback()
                print(f"  [DB] Error: {e}", flush=True)
        else:
            print("  [--] No matched branches to store", flush=True)

    cursor.close()
    db_conn.close()
    if csv_file:
        csv_file.close()
    print("\n[*] Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Broward County Library manga scraper")
    parser.add_argument("--debug",  action="store_true", help="Print visited URLs and per-copy details")
    parser.add_argument("--output", type=str,            help="Also save results to CSV")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--range",     type=str,
                       help="Range (e.g. 1-50)")
    group.add_argument("--manga-ids", type=lambda s: [int(x) for x in s.split(",")],
                       help="Comma-separated MangaIDs")

    process_batch(parser.parse_args())
