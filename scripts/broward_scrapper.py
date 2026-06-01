"""
Broward County Library manga scraper — volume-aware, per-branch edition.

Mirrors the LCPL data model exactly:
  - One availability row per (MangaID, Volume)
  - One branch_availability_status row per (availability, branch)

Volume numbers are extracted from the detail-panel HTML returned by the
detailclick POST (the proven two-step flow). The extractor handles a wide
variety of title formats including omnibus ranges.

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
from config.settings import DATA_DIR, DB_CONFIG

log = logging.getLogger(__name__)

BASE_URL = "https://broward.ent.sirsi.net"
CLIENT   = "/client/en_US/default"

HEADERS = {
    "User-Agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36",
    "Accept":           "text/javascript, text/html, application/json, */*",
    "Accept-Language":  "en-US,en;q=0.9",
    "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
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
#
# Handles all formats seen in Broward catalog titles:
#   "Berserk. 32"                    → 32
#   "Berserk, Vol. 7"                → 7
#   "One Piece, v.1"                 → 1
#   "Berserk (3-in-1 ed.). 7-8-9"   → 7   (omnibus: take lowest)
#   "Berserk Deluxe Edition. 1"      → 1
#   "Nana"                           → 0   (no volume)

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
    """
    Extract volume number from a catalog title or call-number string.
    Returns 0 if no volume can be found.
    """
    if not text:
        return 0

    # Omnibus range first — "7-8-9" or "1-3" → take lowest
    m = _OMNIBUS_RANGE.search(text)
    if m:
        return int(m.group(1))

    # Keyword patterns: "Vol. 7", "v.1", "#22", etc.
    for pat in _VOL_PATTERNS:
        m = pat.search(text)
        if m:
            return int(m.group(1))

    # Bare integer after a period/comma near end of string: "Berserk. 32"
    m = re.search(r'[.,]\s*(\d+)\s*$', text)
    if m:
        return int(m.group(1))

    # Final fallback: any trailing integer
    m = re.search(r'\b(\d+)\s*$', text)
    if m:
        return int(m.group(1))

    return 0


# ── DB helpers ────────────────────────────────────────────────────────────────

def _get_db():
    import mysql.connector
    return mysql.connector.connect(**DB_CONFIG)


def _load_title_author_map() -> list[tuple[int, str, str, int]]:
    conn = _get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute("SELECT MangaID, Title, Author FROM manga ORDER BY MangaID")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [(i + 1, r["Title"], r["Author"] or "", r["MangaID"]) for i, r in enumerate(rows)]


def _load_broward_branch_map() -> dict[str, int]:
    """Return {branch_name: BranchID} for all Broward branches."""
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
    """
    volume_branch_map: {volume_num: {branch_id: status}}

    Deletes all existing Broward availability rows for this manga_id, then inserts:
      - One availability row per volume
      - One branch_availability_status row per branch for that volume
    """
    if not volume_branch_map:
        return "no volume data to store"

    # Delete old Broward rows for this title
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


# ── Search results: collect item IDs ─────────────────────────────────────────

def get_search_results(session: requests.Session, title: str, author: str,
                       debug: bool) -> list[str]:
    """Return de-duplicated SD_ILS item IDs for this title/author."""
    et = urllib.parse.quote_plus(title)
    ea = urllib.parse.quote_plus(author)
    base_url = (
        f"{BASE_URL}{CLIENT}/search/results"
        f"?qu=&qu=TITLE%3D{et}+&qu=AUTHOR%3D{ea}+"
        f"&qf=FORMAT%09Special+Format%09BOOK%09Books"
    )
    all_ids: list[str] = []
    seen: set[str]     = set()
    offset = 0
    page   = 1

    log.info(f"  Searching: '{title}' by {author}")

    while True:
        url = base_url + ("&h=1" if offset == 0 else f"&rw={offset}&isd=true&h=1")
        if debug:
            print(f"[DEBUG] search page {page}: {url}")
        try:
            r = session.get(url, headers=HEADERS, timeout=15)
            if r.status_code != 200:
                break
            matches = re.findall(r"SD_ILS:(\d+)", r.text)
            new_ids = [m for m in dict.fromkeys(matches) if m not in seen]
            if not new_ids:
                break
            all_ids.extend(new_ids)
            seen.update(new_ids)
            if len(matches) < 12:
                break
            offset += 12
            page   += 1
            time.sleep(0.8)
        except Exception as e:
            log.warning(f"  Search error: {e}")
            break

    log.info(f"  Found {len(all_ids)} catalog item(s)")
    return all_ids


# ── Per-item detail fetch (proven two-step flow) ──────────────────────────────

def fetch_item_detail(session: requests.Session, item_id: str,
                      title: str, author: str, debug: bool) -> tuple[int, list[dict]]:
    """
    Two-step fetch:
      1. POST detailclick  → HTML panel; extract volume number from the title fields
      2. POST lookuptitleinfo → JSON with per-copy branch/status

    Returns (volume_number, [{"library": str, "status": str, "on_shelf": bool}])
    """
    et = urllib.parse.quote_plus(title)
    ea = urllib.parse.quote_plus(author)
    ei = urllib.parse.quote_plus(f"ent://SD_ILS/0/SD_ILS:{item_id}~ILS~0")
    qs = (f"qu=TITLE%3D{et}&qu=AUTHOR%3D{ea}"
          f"&qf=FORMAT%09Special+Format%09BOOK%09Books&d={ei}&h=3")

    payload = {
        "qu":  [f"TITLE%3D{et}", f"AUTHOR%3D{ea}"],
        "qf":  "FORMAT\tSpecial Format\tBOOK\tBooks",
        "d":   f"ent://SD_ILS/0/SD_ILS:{item_id}~ILS~0",
        "h":   "3",
    }
    session.headers.update({
        "Referer": f"{BASE_URL}{CLIENT}/search/results?qu=TITLE%3D{et}"
    })

    # ── Step 1: detailclick — HTML panel ─────────────────────────────────────
    detail_url = (
        f"{BASE_URL}{CLIENT}/search/results"
        f".displaypanel.displaycell_0:detailclick"
        f"/ent:$002f$002fSD_ILS$002f0$002fSD_ILS:{item_id}/0/0"
        f"/tabDISCOVERY_ALLlistItem?{qs}"
    )
    if debug:
        print(f"[DEBUG] detailclick: {detail_url}")

    volume = 0
    sdcsrf = None
    try:
        r1 = session.post(detail_url, data=payload, timeout=15)
        if r1.status_code != 200:
            log.warning(f"  detailclick {item_id} → {r1.status_code}")
            return 0, []

        # Extract sdcsrf token
        sdcsrf = session.cookies.get("sdcsrf")
        m = re.search(r"sdcsrf=([a-f0-9\-]+)", r1.text)
        if m:
            sdcsrf = m.group(1)
        if not sdcsrf:
            log.warning(f"  No sdcsrf for item {item_id}")
            return 0, []
        session.headers.update({"sdcsrf": sdcsrf})

        # Extract volume from the detail panel
        volume = _extract_volume_from_panel(r1.text, item_id, debug)

    except Exception as e:
        log.warning(f"  detailclick error {item_id}: {e}")
        return 0, []

    # ── Step 2: lookuptitleinfo — per-copy branch/status ─────────────────────
    info_url = (
        f"{BASE_URL}{CLIENT}/search/results"
        f".displaypanel.displaycell_0.detail.detailavailabilityaccordions"
        f":lookuptitleinfo"
        f"/ent:$002f$002fSD_ILS$002f0$002fSD_ILS:{item_id}/ILS/0/true/true?{qs}"
    )
    if debug:
        print(f"[DEBUG] lookuptitleinfo: {info_url}")

    try:
        r2 = session.post(info_url, data={**payload, "sdcsrf": sdcsrf}, timeout=15)
        if r2.status_code != 200:
            log.warning(f"  lookuptitleinfo {item_id} → {r2.status_code}")
            return volume, []

        records = r2.json().get("childRecords", [])
        copies  = []
        for rec in records:
            library    = (rec.get("LIBRARY")        or "").strip()
            raw_status = (rec.get("SD_ITEM_STATUS") or "").strip()
            on_shelf   = raw_status.lower() in ON_SHELF_STATUSES
            copies.append({"library": library, "status": raw_status, "on_shelf": on_shelf})

        if debug:
            print(f"[DEBUG] item {item_id} vol={volume}: {len(copies)} copies")
            for c in copies:
                print(f"  [{'✓' if c['on_shelf'] else '✗'}] {c['library']} — {c['status']}")

        return volume, copies

    except Exception as e:
        log.warning(f"  lookuptitleinfo error {item_id}: {e}")
        return volume, []


def _extract_volume_from_panel(html: str, item_id: str, debug: bool) -> int:
    """
    Parse the detail panel HTML to find the volume number.

    Tries multiple strategies in order so that the improved extractor gets
    applied to every candidate string we can find in the panel:
      1. Specific CSS selectors for bibliographic title fields
      2. The full text of the entire panel (catches call numbers like "v.22")
    """
    soup = BeautifulSoup(html, "html.parser")

    # Strategy 1: known selector patterns for title / call-number fields
    for selector in (
        "[id*='detailValue'][id*='TITLE']",
        ".detailValue",
        ".displayDetailTitle",
        "td.detailValue",
        "span.displayDetailValue",
        "[id*='callnumber']",
        ".callNumber",
    ):
        for el in soup.select(selector):
            text = el.get_text(" ", strip=True)
            vol  = extract_volume(text)
            if vol:
                if debug:
                    print(f"[DEBUG]   vol {vol} from selector '{selector}': {text[:80]}")
                return vol

    # Strategy 2: scan all visible text in the panel
    full_text = soup.get_text(" ")
    vol = extract_volume(full_text)
    if vol:
        if debug:
            print(f"[DEBUG]   vol {vol} from full panel text")
        return vol

    if debug:
        print(f"[DEBUG]   no volume found in panel for item {item_id}")
    return 0


# ── Aggregate per-item data ───────────────────────────────────────────────────

def build_volume_branch_map(
    item_copies: list[tuple[int, list[dict]]],
    branch_map: dict[str, int],
    debug: bool,
) -> dict[int, dict[int, str]]:
    """
    Aggregate per-item copy data into {volume_num: {branch_id: best_status}}.
    For each (volume, branch) pair, prefer Available > On Hold > Checked Out.
    """
    STATUS_PRIORITY = {"Available": 2, "On Hold": 1, "Checked Out": 0}
    result: dict[int, dict[int, str]] = defaultdict(dict)
    unmatched: set[str] = set()

    for volume, copies in item_copies:
        for copy in copies:
            branch_name = copy["library"]
            branch_id   = branch_map.get(branch_name)
            if branch_id is None:
                unmatched.add(branch_name)
                continue

            if copy["on_shelf"]:
                status = "Available"
            elif "hold" in copy["status"].lower():
                status = "On Hold"
            else:
                status = "Checked Out"

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

    # Resolve LibraryID for Broward
    conn = _get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute("SELECT LibraryID FROM library WHERE LibraryName LIKE %s LIMIT 1", ("%Broward%",))
    lib_row = cur.fetchone()
    cur.close(); conn.close()
    if not lib_row:
        print("[-] Broward library not found in DB.")
        return
    broward_library_id = lib_row["LibraryID"]

    # Resolve index list
    if args.indices:
        indices = args.indices
    elif args.line:
        indices = [args.line]
    elif args.range:
        try:
            s, e = args.range.split("-")
            indices = list(range(max(1, int(s)), int(e) + 1))
        except (ValueError, IndexError):
            print("[-] Invalid --range. Use START-END (e.g. --range 1-50)")
            return
    else:
        indices = list(range(1, len(all_pairs) + 1))

    pairs = [all_pairs[i - 1] for i in indices if 1 <= i <= len(all_pairs)]
    if not pairs:
        print("[-] No valid titles.")
        return

    print(f"[*] Broward library ID  : {broward_library_id}")
    print(f"[*] Broward branch count: {len(branch_map)}")
    print(f"[*] Scraping {len(pairs)} title(s)…\n")

    # Optional CSV output
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
    session.get(f"{BASE_URL}{CLIENT}", timeout=20)   # prime cookies
    session.headers.update({**HEADERS, "Referer": f"{BASE_URL}{CLIENT}"})

    for progress, (idx, title, author, manga_id) in enumerate(pairs, start=1):
        if not author:
            log.warning(f"Index {idx} '{title}' — no author, skipping")
            continue

        print(f"[{progress}/{len(pairs)}] {title}", flush=True)

        item_ids = get_search_results(session, title, author, args.debug)
        if not item_ids:
            print("  [-] No catalog entries found")
            continue

        # Fetch detail + copies for each item
        item_copies: list[tuple[int, list[dict]]] = []
        for item_id in item_ids:
            volume, copies = fetch_item_detail(
                session, item_id, title, author, args.debug,
            )
            item_copies.append((volume, copies))
            if csv_writer:
                for c in copies:
                    csv_writer.writerow([
                        title, author, item_id, volume,
                        c["library"], c["status"],
                        "Yes" if c["on_shelf"] else "No",
                    ])
            time.sleep(1.2)

        # Aggregate into {volume → {branch_id → best_status}}
        volume_branch_map = build_volume_branch_map(item_copies, branch_map, args.debug)

        # Summary line
        total_vols  = len(volume_branch_map)
        avail_vols  = sum(
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
    parser = argparse.ArgumentParser(
        description="Broward County Library manga scraper — volume-aware",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--debug",  action="store_true",
                        help="Print visited URLs and per-copy details")
    parser.add_argument("--output", type=str,
                        help="Also save results to CSV")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--line",    type=int,
                       help="Single 1-based index (e.g. --line 3)")
    group.add_argument("--range",   type=str,
                       help="Range (e.g. --range 1-50)")
    group.add_argument("--indices", type=lambda s: [int(x) for x in s.split(",")],
                       metavar="N,N,…",
                       help="Comma-separated 1-based indices")
    process_batch(parser.parse_args())
