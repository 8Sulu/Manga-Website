"""
Broward County Library manga scraper — per-branch, per-volume output.

Matches the LCPL scraper's data model:
    availability row  →  MangaID + Volume
    branch_availability_status row  →  AvailabilityID + BranchID + Status

Strategy (no ILSWS — Broward 404s that endpoint):
  For each catalog item (one per volume of a series):
    1. POST detailclick  →  parse HTML for:
         - Volume number  (from title string "One piece. Vol. 22 Hope!!")
         - Branch name per copy  (asyncFieldLIBRARY hidden divs — server-rendered)
         - Copy ID per copy  (R0XXXXXXXXXX from async field element IDs)
    2. POST lookupavailability  →  aggregate counts:
         - availableCount, copyCount, holdCount
       Status is assigned per branch using a best-effort distribution:
         - The first `availableCount` copies get "Graphic Novel - Young Adult Fiction"
           (on-shelf), the rest get "Checked Out" or "On Hold" if holdCount > 0.

This gives branch-level granularity (which branches hold the title) and an
approximate per-copy status — the same data fidelity used by the LCPL scraper's
`item_status()` function, which also infers status from location/dueDate fields.

Usage (same flags as scrapper.py):
    python broward_scrapper.py                     scrape all titles
    python broward_scrapper.py --line 3            scrape line 3
    python broward_scrapper.py --range 1-50        scrape lines 1 to 50
    python broward_scrapper.py --indices 1,4,7     scrape specific 1-based indices
    python broward_scrapper.py --debug             verbose logging
    python broward_scrapper.py --output file.csv   also write a CSV (debug/audit)
"""
from __future__ import annotations

import re
import csv
import json
import time
import argparse
import logging
import sys
from pathlib import Path

import requests
import urllib.parse
from bs4 import BeautifulSoup

sys.path.append(str(Path(__file__).parent.parent))
from config.settings import DATA_DIR, DB_CONFIG

log = logging.getLogger(__name__)

BASE_URL          = "https://broward.ent.sirsi.net"
RESULTS_PER_PAGE  = 12
REQUEST_DELAY     = 1.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":           "text/javascript, text/html, application/json, */*",
    "Accept-Language":  "en-US,en;q=0.9",
    "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin":           BASE_URL,
}

# ── DB helpers ─────────────────────────────────────────────────────────────────

def _get_db():
    import mysql.connector
    return mysql.connector.connect(**DB_CONFIG)

def _load_title_author_map() -> list[tuple[int, str, str, int]]:
    """Return list of (1-based-index, title, author, manga_id) from the manga DB table."""
    conn = _get_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT MangaID, Title, Author FROM manga ORDER BY MangaID")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [(i + 1, r["Title"], r["Author"] or "", r["MangaID"]) for i, r in enumerate(rows)]

def _get_broward_branch_id_map() -> dict[str, int]:
    """Return {normalised_branch_name: BranchID} for all Broward branches."""
    conn = _get_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT b.BranchID, b.BranchName
        FROM branch b
        JOIN library l ON b.LibraryID = l.LibraryID
        WHERE l.LibraryName LIKE %s
        """,
        ("%Broward%",),
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    if not rows:
        raise RuntimeError(
            "No Broward branches found in the database. "
            "Make sure libraries.csv / branches.csv include Broward entries and run DB reset."
        )
    return {r["BranchName"].strip().lower(): r["BranchID"] for r in rows}

def _write_to_db(conn, cursor, manga_id: int, books: list[dict],
                 branch_id_map: dict[str, int]) -> int:
    """
    Upsert availability rows for one manga title.
    books: list of {volume, branches: [{name, status}]}
    Returns number of branch_availability_status rows inserted.
    """
    if not books:
        return 0

    # Delete existing Broward data for this title
    broward_ids = list(branch_id_map.values())
    placeholders = ",".join(["%s"] * len(broward_ids))
    cursor.execute(
        f"""
        DELETE bas FROM branch_availability_status bas
        JOIN availability a ON bas.AvailabilityID = a.AvailabilityID
        WHERE a.MangaID = %s AND bas.BranchID IN ({placeholders})
        """,
        (manga_id, *broward_ids),
    )
    cursor.execute(
        """
        DELETE a FROM availability a
        LEFT JOIN branch_availability_status bas ON a.AvailabilityID = bas.AvailabilityID
        WHERE a.MangaID = %s AND bas.AvailabilityID IS NULL
        """,
        (manga_id,),
    )

    inserted = 0
    for book in books:
        cursor.execute(
            "INSERT INTO availability (MangaID, Volume) VALUES (%s, %s)",
            (manga_id, book["volume"]),
        )
        avail_id = cursor.lastrowid
        for branch_info in book["branches"]:
            branch_id = branch_id_map.get(branch_info["name"].strip().lower())
            if branch_id is None:
                # Fuzzy match: check if any key contains the branch name fragment
                branch_name_lower = branch_info["name"].strip().lower()
                for db_name, bid in branch_id_map.items():
                    # Match on first significant word
                    first_word = branch_name_lower.split()[0] if branch_name_lower else ""
                    if first_word and first_word in db_name:
                        branch_id = bid
                        break
            if branch_id:
                cursor.execute(
                    "INSERT INTO branch_availability_status "
                    "(AvailabilityID, BranchID, Status) VALUES (%s, %s, %s)",
                    (avail_id, branch_id, branch_info["status"]),
                )
                inserted += 1
            else:
                log.warning(f"  No DB match for branch: {branch_info['name']!r}")

    conn.commit()
    return inserted

# ── HTTP session ───────────────────────────────────────────────────────────────

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    try:
        s.get(BASE_URL + "/client/en_US/default", timeout=20)
        s.headers.update({"Referer": BASE_URL + "/client/en_US/default"})
        log.info("Session primed")
    except Exception as e:
        log.warning(f"Session prime failed (continuing): {e}")
    return s

# ── Step 1: search page → catalog item IDs ────────────────────────────────────

def fetch_catalog_ids(session: requests.Session, title: str, author: str) -> list[str]:
    """Return list of SD_ILS catalog IDs for this title+author search."""
    encoded_title  = urllib.parse.quote_plus(title)
    encoded_author = urllib.parse.quote_plus(author)
    base_url = (
        f"{BASE_URL}/client/en_US/default/search/results"
        f"?qu=&qu=TITLE%3D{encoded_title}+&qu=AUTHOR%3D{encoded_author}+"
        f"&qf=FORMAT%09Special+Format%09BOOK%09Books"
    )

    all_ids: list[str] = []
    offset   = 0
    page     = 1

    while True:
        url = (
            f"{base_url}&h=1"
            if offset == 0
            else f"{base_url}&rw={offset}&isd=true&h=1"
        )
        try:
            r = session.get(url, timeout=15)
            r.raise_for_status()
        except requests.RequestException as e:
            log.warning(f"  Search page {page} failed: {e}")
            break

        ids = list(dict.fromkeys(re.findall(r"SD_ILS:(\d+)", r.text)))
        new = [i for i in ids if i not in all_ids]
        if not new:
            break
        all_ids.extend(new)
        log.info(f"  Search page {page}: {len(new)} new IDs (total {len(all_ids)})")

        if len(ids) < RESULTS_PER_PAGE:
            break
        offset += RESULTS_PER_PAGE
        page   += 1
        time.sleep(REQUEST_DELAY)

    return all_ids

# ── Step 2: detail panel → volume + per-copy branch data ──────────────────────

def _extract_volume_from_title(title_str: str) -> int:
    """Extract volume number from a Sirsi title string like 'One piece. Vol. 22 Hope!!'"""
    # Try "Vol. N" or "Volume N" or "v. N"
    m = re.search(r'\b[Vv]ol(?:ume)?\.?\s*(\d+)', title_str)
    if m:
        return int(m.group(1))
    # Try ", N" at end (e.g. "Fullmetal alchemist, 5")
    m = re.search(r',\s*(\d+)\s*$', title_str.strip())
    if m:
        return int(m.group(1))
    return 0

def fetch_detail(session: requests.Session, item_id: str, search_title: str,
                 is_debug: bool) -> dict | None:
    """
    POST to the detailclick endpoint for one catalog item.
    Returns {title, volume, copies: [{copy_id, branch_name}]} or None on failure.
    The HTML has branch names server-rendered in hidden divs and copy IDs in element IDs.
    """
    url = (
        f"{BASE_URL}/client/en_US/default/search/results"
        f".displaypanel.displaycell_0:detailclick"
        f"/ent:$002f$002fSD_ILS$002f0$002fSD_ILS:{item_id}/1/1"
        f"/tabDISCOVERY_ALLlistItem"
    )
    payload = {
        "qu":  search_title,
        "qf":  "ITYPE\tMaterial Type\t1:BOOK\tBook",
        "d":   f"ent://SD_ILS/0/SD_ILS:{item_id}~ILS~1",
        "h":   "8",
    }
    session.headers.update({
        "Referer": (
            f"{BASE_URL}/client/en_US/default/search/results"
            f"?qu={urllib.parse.quote_plus(search_title)}"
        )
    })

    try:
        r = session.post(url, data=payload, timeout=15)
        r.raise_for_status()
    except requests.RequestException as e:
        log.warning(f"  Detail fetch failed for {item_id}: {e}")
        return None

    # Unescape the response (Sirsi escapes / as \/ in HTML)
    html = r.text.replace("\\/", "/")
    soup = BeautifulSoup(html, "html.parser")

    # Extract title string and volume number
    title_str = ""
    for el in soup.find_all(class_="INITIAL_TITLE_SRCH"):
        text = el.get_text(separator=" ", strip=True)
        if text and "Title" not in text and len(text) < 300:
            title_str = text
            break
    volume = _extract_volume_from_title(title_str)

    if is_debug:
        log.debug(f"    item_id={item_id}  title={title_str!r}  volume={volume}")

    # Extract per-copy branch names and copy IDs
    # Branch name: asyncFieldLIBRARY hidden div (server-rendered, always present)
    # Copy ID: from the element ID of the asyncInProgress sibling
    copies: list[dict] = []
    for row in soup.find_all(class_="detailItemsTableRow"):
        # Branch name — from the hidden div that contains the real value
        lib_el = row.find(class_="asyncFieldLIBRARY hidden")
        branch_name = lib_el.get_text(strip=True) if lib_el else ""

        # Copy ID — from the asyncInProgress element's ID attribute
        # e.g. asyncFielddetailItemsDiv1SD_ITEM_STATUSR0115215530 → R0115215530
        async_el = row.find(class_="asyncFieldSD_ITEM_STATUS asyncInProgressSD_ITEM_STATUS")
        copy_id = ""
        if async_el:
            m = re.search(r"(R\d+)$", async_el.get("id", ""))
            if m:
                copy_id = m.group(1)

        if branch_name:
            copies.append({"copy_id": copy_id, "branch_name": branch_name})

    return {"title": title_str, "volume": volume, "copies": copies}

# ── Step 3: availability counts ───────────────────────────────────────────────

def fetch_availability_counts(session: requests.Session, item_id: str,
                               search_title: str, sdcsrf: str) -> dict:
    """
    POST to lookupavailability to get aggregate counts.
    Returns {"available": int, "total": int, "holds": int}
    """
    url = (
        f"{BASE_URL}/client/en_US/default/search/results"
        f".displaypanel.displaycell_0.detail.detailavailabilityaccordions"
        f".boundwithzone:lookupavailability"
        f"/ent:$002f$002fSD_ILS$002f0$002fSD_ILS:{item_id}/ILS/1/false"
        f"/LIBRARY$002cCALLNUMBER"
    )
    session.headers.update({"sdcsrf": sdcsrf})
    payload = {
        "qu":     search_title,
        "qf":     "ITYPE\tMaterial Type\t1:BOOK\tBook",
        "d":      f"ent://SD_ILS/0/SD_ILS:{item_id}~ILS~1",
        "h":      "8",
        "sdcsrf": sdcsrf,
    }

    defaults = {"available": 0, "total": 0, "holds": 0}
    try:
        r = session.post(url, data=payload, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning(f"  Avail counts failed for {item_id}: {e}")
        return defaults

    try:
        eval_script = data["inits"][0]["evalScript"][0]
        m = re.search(r"updateWebServiceFields\((.*?)\);", eval_script)
        if m:
            meta = json.loads(m.group(1))
            return {
                "available": int(meta.get("availableCount", 0)),
                "total":     int(meta.get("copyCount",      0)),
                "holds":     int(meta.get("holdCount",      0)),
            }
    except (KeyError, IndexError, json.JSONDecodeError, ValueError):
        pass
    return defaults

def _get_sdcsrf(session: requests.Session, html_text: str) -> str:
    """Extract the sdcsrf token from the detail HTML or session cookies."""
    m = re.search(r"sdcsrf=([a-f0-9\-]+)", html_text)
    if m:
        return m.group(1)
    return session.cookies.get("sdcsrf", "")

# ── Status assignment ──────────────────────────────────────────────────────────

def assign_statuses(copies: list[dict], available: int, holds: int) -> list[dict]:
    """
    Assign a status to each copy using aggregate counts.

    Available copies are assigned to the first `available` entries in the list
    (Sirsi typically returns on-shelf copies first). Remaining copies are
    assigned "Checked Out" unless holds > 0, in which case the last `holds`
    copies are "On Hold".

    This matches the fidelity of the LCPL scraper which derives status from
    currentLocationID + dueDate — both are coarse signals, not exact.
    """
    n = len(copies)
    result = []
    for i, copy in enumerate(copies):
        if i < available:
            status = "Graphic Novel - Young Adult Fiction"  # on shelf, matches LCPL
        elif holds > 0 and i >= n - holds:
            status = "On Hold"
        else:
            status = "Checked Out"
        result.append({**copy, "status": status})
    return result

# ── Main scrape loop ───────────────────────────────────────────────────────────

def scrape_title(session: requests.Session, title: str, author: str,
                 manga_id: int, is_debug: bool) -> list[dict]:
    """
    Scrape one manga title. Returns list of book dicts:
    {manga_id, volume, branches: [{name, status}]}
    """
    log.info(f"  Searching: {title!r} by {author!r}")
    item_ids = fetch_catalog_ids(session, title, author)
    if not item_ids:
        log.warning("  No catalog IDs found")
        return []

    log.info(f"  {len(item_ids)} catalog item(s)")
    books: list[dict] = []
    seen_volumes: set = set()

    for item_id in item_ids:
        time.sleep(REQUEST_DELAY)

        detail = fetch_detail(session, item_id, title, is_debug)
        if not detail or not detail["copies"]:
            log.warning(f"  Item {item_id}: no detail / no copies")
            continue

        volume  = detail["volume"]
        copies  = detail["copies"]

        # Avoid duplicate volumes (different catalog records for same vol)
        if volume in seen_volumes and volume != 0:
            log.info(f"  Item {item_id}: vol {volume} already seen, skipping")
            continue

        # Get sdcsrf from the last detail response for the avail call
        # Re-POST to get it (we need the raw HTML again — fetch_detail already discards it)
        # Use it from session cookies (set during detail POST)
        sdcsrf = session.cookies.get("sdcsrf", "")
        if not sdcsrf:
            log.warning(f"  Item {item_id}: no sdcsrf — skipping avail counts")
            counts = {"available": 0, "total": len(copies), "holds": 0}
        else:
            time.sleep(REQUEST_DELAY)
            counts = fetch_availability_counts(session, item_id, title, sdcsrf)

        copies_with_status = assign_statuses(copies, counts["available"], counts["holds"])

        avail_n = counts["available"]
        total_n = counts["total"]
        holds_n = counts["holds"]
        if is_debug:
            log.debug(
                f"  Item {item_id}: vol={volume}  "
                f"avail={avail_n}/{total_n}  holds={holds_n}  "
                f"branches={[c['branch_name'] for c in copies]}"
            )

        indicator = "[✓]" if avail_n > 0 else ("[H]" if holds_n > 0 else "[✗]")
        title_str = detail["title"] or title
        print(
            f"  {indicator} {title_str}  "
            f"avail={avail_n}/{total_n} holds={holds_n}  "
            f"branches: {', '.join(c['branch_name'] for c in copies_with_status)}"
        )

        seen_volumes.add(volume)
        books.append({
            "manga_id": manga_id,
            "volume":   volume,
            "branches": [
                {"name": c["branch_name"], "status": c["status"]}
                for c in copies_with_status
            ],
        })

    return books

# ── Process batch ──────────────────────────────────────────────────────────────

def process_batch(args: argparse.Namespace) -> None:
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    all_pairs = _load_title_author_map()
    if not all_pairs:
        print("[-] No titles found in DB.")
        return

    # Resolve which 1-based indices to process
    if args.indices:
        indices = args.indices
    elif args.line:
        indices = [args.line]
    elif args.range:
        try:
            parts = args.range.split("-")
            start = max(1, int(parts[0]))
            end   = int(parts[1])
            indices = list(range(start, end + 1))
        except (ValueError, IndexError):
            print("[-] Invalid --range format. Use START-END (e.g. --range 1-50)")
            return
    else:
        indices = list(range(1, len(all_pairs) + 1))

    pairs_to_scrape = []
    for idx in indices:
        if 1 <= idx <= len(all_pairs):
            pairs_to_scrape.append(all_pairs[idx - 1])
        else:
            log.warning(f"Index {idx} out of range — skipping")

    if not pairs_to_scrape:
        print("[-] No valid titles to scrape.")
        return

    # Load branch ID map once
    try:
        branch_id_map = _get_broward_branch_id_map()
    except RuntimeError as e:
        print(f"[-] {e}")
        return

    print(f"[*] {len(branch_id_map)} Broward branches in DB")
    print(f"[*] Scraping {len(pairs_to_scrape)} title(s)…\n")

    # Optional CSV output
    csv_file = csv_writer = None
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        csv_file   = open(out_path, "w", newline="", encoding="utf-8")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow([
            "Search Title", "Author", "Volume", "Branch", "Status",
        ])
        print(f"[*] Saving CSV to: {out_path}\n")

    conn   = _get_db()
    cursor = conn.cursor()
    session = make_session()

    total_inserted = 0

    for progress, (idx, title, author, manga_id) in enumerate(pairs_to_scrape, start=1):
        if not author:
            log.warning(f"Index {idx} '{title}' has no author — skipping")
            continue

        print(f"[{progress}/{len(pairs_to_scrape)}] {title}", flush=True)

        books = scrape_title(session, title, author, manga_id, args.debug)

        if books:
            try:
                n = _write_to_db(conn, cursor, manga_id, books, branch_id_map)
                total_inserted += n
                print(f"  [DB] inserted {n} branch_availability_status rows")
            except Exception as e:
                conn.rollback()
                print(f"  [DB] Error saving '{title}': {e}")

            if csv_writer:
                for book in books:
                    for branch in book["branches"]:
                        csv_writer.writerow([
                            title, author, book["volume"],
                            branch["name"], branch["status"],
                        ])
        else:
            print(f"  [--] No results found")

    cursor.close()
    conn.close()
    if csv_file:
        csv_file.close()

    print(f"\n[*] Done. Total branch_availability_status rows inserted: {total_inserted}")

# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Broward County Library manga scraper (per-branch, per-volume)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--debug",  action="store_true",
                        help="Verbose URL and availability logging")
    parser.add_argument("--output", type=str,
                        help="Also save results to a CSV file")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--line",    type=int,
                       help="Scrape a single 1-based line number (e.g. --line 3)")
    group.add_argument("--range",   type=str,
                       help="Scrape a range of lines (e.g. --range 1-50)")
    group.add_argument("--indices", type=lambda s: [int(x) for x in s.split(",")],
                       metavar="N,N,…",
                       help="Scrape specific comma-separated 1-based indices")

    args = parser.parse_args()
    process_batch(args)
