"""
Broward County Library manga scraper.

Usage:
    python new_broward_scrapper.py                     scrape all titles
    python new_broward_scrapper.py --line 3            scrape line 3
    python new_broward_scrapper.py --range 1-50        scrape lines 1 to 50
    python new_broward_scrapper.py --indices 1,4,7     scrape specific 1-based indices
    python new_broward_scrapper.py --output file.csv   also write a CSV (debug/audit)
    python new_broward_scrapper.py --debug             verbose URL/field logging
"""
from __future__ import annotations

import os
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

sys.path.append(str(Path(__file__).parent.parent))
from config.settings import DATA_DIR, DB_CONFIG

log = logging.getLogger(__name__)

BASE_URL = "https://broward.ent.sirsi.net"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/javascript, text/html, application/json, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": BASE_URL,
}

# ── DB helpers ────────────────────────────────────────────────────────────────

def _get_db():
    import mysql.connector
    return mysql.connector.connect(**DB_CONFIG)


def _get_broward_branch_id() -> int:
    """
    Return the BranchID for the single Broward County system-wide branch.
    The branch row must already exist in the DB (seeded via branches.csv /
    reset).  Raises RuntimeError if not found.
    """
    conn = _get_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        "SELECT b.BranchID FROM branch b "
        "JOIN library l ON b.LibraryID = l.LibraryID "
        "WHERE l.LibraryName LIKE %s LIMIT 1",
        ("%Broward%",),
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if not row:
        raise RuntimeError(
            "No Broward branch found in the database. "
            "Make sure libraries.csv / branches.csv include a Broward entry and run DB reset."
        )
    return int(row["BranchID"])


def _load_manga_id_map() -> dict[str, int]:
    """Return {title_lower: mal_id} from manga.csv."""
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


def _upsert_broward_results(manga_id: int, broward_branch_id: int,
                             results: list[dict]) -> str:
    """
    Upsert availability rows for one manga title into the DB.

    Each entry in `results` represents one catalog item found at Broward
    (a volume or edition).  We store a single row per item:
      - availability(MangaID, Volume=0)  — Broward doesn't give per-volume nums
      - branch_availability_status(BranchID=broward_branch_id, Status=…)

    Existing Broward rows for this MangaID are deleted first so re-scraping
    is idempotent.
    """
    if not results:
        return "no results to store"

    conn   = _get_db()
    cursor = conn.cursor()

    # Delete old Broward rows for this manga
    cursor.execute(
        """
        DELETE bas FROM branch_availability_status bas
        JOIN availability a ON bas.AvailabilityID = a.AvailabilityID
        WHERE a.MangaID = %s AND bas.BranchID = %s
        """,
        (manga_id, broward_branch_id),
    )

    # Clean up orphan availability rows that now have no branch_status children
    cursor.execute(
        """
        DELETE a FROM availability a
        LEFT JOIN branch_availability_status bas ON a.AvailabilityID = bas.AvailabilityID
        WHERE a.MangaID = %s AND bas.AvailabilityID IS NULL
        """,
        (manga_id,),
    )

    inserted = 0
    for item in results:
        available     = int(item.get("available", 0))
        total_copies  = int(item.get("total_copies", 0))
        holds         = int(item.get("holds", 0))

        if available > 0:
            status = "Available"
        elif holds > 0:
            status = "On Hold"
        elif total_copies > 0:
            status = "Checked Out"
        else:
            status = "Checked Out"

        cursor.execute(
            "INSERT INTO availability (MangaID, Volume) VALUES (%s, %s)",
            (manga_id, 0),
        )
        avail_id = cursor.lastrowid

        cursor.execute(
            "INSERT INTO branch_availability_status (AvailabilityID, BranchID, `Status`) "
            "VALUES (%s, %s, %s)",
            (avail_id, broward_branch_id, status),
        )
        inserted += 1

    conn.commit()
    cursor.close()
    conn.close()
    return f"inserted {inserted} Broward availability row(s)"


# ── Scraping logic (unchanged from original) ──────────────────────────────────

def get_search_results(session: requests.Session, title: str, author: str,
                       is_debug: bool) -> list[str]:
    encoded_title  = urllib.parse.quote_plus(title)
    encoded_author = urllib.parse.quote_plus(author)
    base_search_url = (
        f"{BASE_URL}/client/en_US/default/search/results"
        f"?qu=&qu=TITLE%3D{encoded_title}+&qu=AUTHOR%3D{encoded_author}+"
        f"&qf=FORMAT%09Special+Format%09BOOK%09Books"
    )

    all_item_ids: list[str] = []
    offset   = 0
    page_num = 1

    if is_debug:
        print(f"\n[*] Searching for: '{title}' by {author}...")
    else:
        print(f"\n[*] '{title}' (by {author})")
        print("-" * 50)

    while True:
        search_url = (
            f"{base_search_url}&h=1"
            if offset == 0
            else f"{base_search_url}&rw={offset}&isd=true&h=1"
        )
        if is_debug:
            print(f"[DEBUG] Visited Page {page_num} URL: {search_url}")
        try:
            response = session.get(search_url, headers=HEADERS, timeout=15)
            if response.status_code != 200:
                if is_debug:
                    print(f"[-] Search failed on page {page_num}: Status {response.status_code}")
                break

            matches          = re.findall(r"SD_ILS:(\d+)", response.text)
            current_page_ids = list(dict.fromkeys(matches))
            new_ids          = [uid for uid in current_page_ids if uid not in all_item_ids]

            if not new_ids:
                if is_debug:
                    print(f"[*] No new results on page {page_num}. Ending pagination.")
                break

            all_item_ids.extend(new_ids)

            if len(current_page_ids) < 12:
                break

            offset   += 12
            page_num += 1
            time.sleep(1)

        except Exception as e:
            print(f"[!] Search connection error on page {page_num}: {e}")
            break

    if is_debug:
        print(f"[+] Total unique Item IDs found: {len(all_item_ids)}")
    elif not all_item_ids:
        print(" [-] No results found in the catalog.")

    return all_item_ids


def fetch_availability(session: requests.Session, item_id: str, index: int,
                       total: int, base_title: str,
                       is_debug: bool) -> dict | None:
    INIT_URL = (
        f"{BASE_URL}/client/en_US/default/search/results"
        f".displaypanel.displaycell_0:detailclick"
        f"/ent:$002f$002fSD_ILS$002f0$002fSD_ILS:{item_id}/1/1"
        f"/tabDISCOVERY_ALLlistItem"
    )
    AVAILABILITY_URL = (
        f"{BASE_URL}/client/en_US/default/search/results"
        f".displaypanel.displaycell_0.detail.detailavailabilityaccordions"
        f".boundwithzone:lookupavailability"
        f"/ent:$002f$002fSD_ILS$002f0$002fSD_ILS:{item_id}/ILS/1/false"
        f"/LIBRARY$002cCALLNUMBER"
    )

    session.headers.update({
        "Referer": f"{BASE_URL}/client/en_US/default/search/results"
                   f"?qu={urllib.parse.quote_plus(base_title)}"
    })

    if is_debug:
        print(f"\n[*] Processing Item {index}/{total} (ID: {item_id})")
        print(f"[DEBUG] Visited INIT URL: {INIT_URL}")

    init_params = {
        "qu":  base_title,
        "qf":  "ITYPE\tMaterial Type\t1:BOOK\tBook",
        "d":   f"ent://SD_ILS/0/SD_ILS:{item_id}~ILS~1",
        "h":   "8",
    }

    try:
        response_1 = session.post(INIT_URL, data=init_params, timeout=15)
        if response_1.status_code != 200:
            if is_debug:
                print(f"[-] Step 1 failed: Status {response_1.status_code}")
            return None

        specific_title = base_title
        title_match = re.search(
            r'class="[^"]*TITLE[^"]*">([^<]+)<', response_1.text, re.IGNORECASE
        )
        if title_match:
            specific_title = title_match.group(1).replace("&#x20;", " ").strip()

        sdcsrf_match = re.search(r"sdcsrf=([a-f0-9\-]+)", response_1.text)
        if not sdcsrf_match:
            sdcsrf_token = session.cookies.get("sdcsrf")
            if not sdcsrf_token:
                if is_debug:
                    print("[-] Critical: Could not locate 'sdcsrf' token.")
                return None
        else:
            sdcsrf_token = sdcsrf_match.group(1)

        session.headers.update({"sdcsrf": sdcsrf_token})

        if is_debug:
            print(f"[DEBUG] Visited AVAIL URL: {AVAILABILITY_URL}")

        avail_payload = {
            "qu":      base_title,
            "qf":      "ITYPE\tMaterial Type\t1:BOOK\tBook",
            "d":       f"ent://SD_ILS/0/SD_ILS:{item_id}~ILS~1",
            "h":       "8",
            "sdcsrf":  sdcsrf_token,
        }

        response_2 = session.post(AVAILABILITY_URL, data=avail_payload, timeout=15)
        if response_2.status_code != 200:
            if is_debug:
                print(f"[-] Step 2 failed: Status {response_2.status_code}")
            return None

        data        = response_2.json()
        eval_script = data["inits"][0]["evalScript"][0]
        json_match  = re.search(r"updateWebServiceFields\((.*?)\);", eval_script)

        if json_match:
            meta         = json.loads(json_match.group(1))
            available    = meta.get("availableCount", "0")
            total_copies = meta.get("copyCount", "0")
            holds        = meta.get("holdCount", "0")

            if is_debug:
                print(f"[+] '{specific_title}' — avail={available}/{total_copies} holds={holds}")
                fields = meta.get("fields", [])
                if fields:
                    print(f"\n{'LIBRARY BRANCH':<35} | {'CALL NUMBER':<20} | STATUS")
                    print("-" * 75)
                    for field in fields:
                        print(field)
            else:
                indicator = "[✓]" if int(available) > 0 else "[X]"
                print(f"  {indicator} {specific_title} — Available: {available}/{total_copies} | Holds: {holds}")

            return {
                "volume_title":  specific_title,
                "available":     available,
                "total_copies":  total_copies,
                "holds":         holds,
            }

    except Exception as e:
        if is_debug:
            print(f"[-] Parsing failed: {e}")
    return None


# ── Positional file helpers (mirrors scrapper.py) ─────────────────────────────

def _read_positional(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [line.rstrip("\n") for line in path.open(encoding="utf-8")]


# ── Main processing ───────────────────────────────────────────────────────────

def process_batch(args: argparse.Namespace) -> None:
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(levelname)s %(message)s")

    titles_path  = DATA_DIR / "titles.txt"
    authors_path = DATA_DIR / "authors.txt"

    # Positional read — blank lines are gap slots, silently skipped
    titles  = _read_positional(titles_path)
    authors = _read_positional(authors_path)

    if not titles:
        print("[-] titles.txt not found or empty")
        return

    manga_id_map = _load_manga_id_map()

    # ── Resolve which 1-based indices to process ──────────────────────────────
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
        indices = list(range(1, len(titles) + 1))

    # Build (title, author, mal_id) triples, skipping blanks/missing
    pairs: list[tuple[str, str, int]] = []
    for idx in indices:
        pos = idx - 1
        if pos < 0 or pos >= len(titles):
            log.warning(f"Index {idx} out of range — skipping")
            continue
        title = titles[pos].strip()
        if not title:
            log.debug(f"Index {idx} is a gap slot — skipping")
            continue
        author = authors[pos].strip() if pos < len(authors) else ""
        if not author:
            log.warning(f"Index {idx} '{title}' has no author — skipping")
            continue
        mal_id = manga_id_map.get(title.lower())
        if mal_id is None:
            log.warning(f"Index {idx} '{title}' not found in manga.csv — skipping")
            continue
        pairs.append((title, author, mal_id))

    if not pairs:
        print("[-] No valid titles to scrape.")
        return

    # ── Resolve Broward branch ID once ───────────────────────────────────────
    try:
        broward_branch_id = _get_broward_branch_id()
    except RuntimeError as e:
        print(f"[-] {e}")
        return

    print(f"[*] Broward branch ID: {broward_branch_id}")
    print(f"[*] Scraping {len(pairs)} title(s)…\n")

    # ── Set up optional CSV output ────────────────────────────────────────────
    csv_file = None
    csv_writer = None
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        csv_file   = open(out_path, "w", newline="", encoding="utf-8")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow([
            "Search Title", "Author", "Volume Title",
            "Available", "Total Copies", "Holds", "In Stock",
        ])
        print(f"[*] Saving CSV to: {out_path}\n")

    # ── Scrape ────────────────────────────────────────────────────────────────
    session = requests.Session()
    session.headers.update(HEADERS)

    for progress, (title, author, manga_id) in enumerate(pairs, start=1):
        print(f"[{progress}/{len(pairs)}] {title}", flush=True)

        item_ids = get_search_results(session, title, author, args.debug)

        title_results: list[dict] = []
        total_items = len(item_ids)

        for idx, item_id in enumerate(item_ids, start=1):
            result = fetch_availability(session, item_id, idx, total_items,
                                        title, args.debug)
            if result:
                title_results.append(result)
                if csv_writer:
                    in_stock = "Yes" if int(result["available"]) > 0 else "No"
                    csv_writer.writerow([
                        title, author, result["volume_title"],
                        result["available"], result["total_copies"],
                        result["holds"], in_stock,
                    ])
            time.sleep(1.5)

        # ── Write to DB ───────────────────────────────────────────────────────
        if title_results:
            try:
                msg = _upsert_broward_results(manga_id, broward_branch_id, title_results)
                print(f"  [DB] {msg}")
            except Exception as e:
                print(f"  [DB] Error saving '{title}': {e}")
        else:
            print(f"  [--] No results to store for '{title}'")

    if csv_file:
        csv_file.close()

    print("\n[*] Done.")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Broward County Library manga scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--debug",   action="store_true",
                        help="Print visited URLs and detailed availability tables")
    parser.add_argument("--output",  type=str,
                        help="Also save results to a CSV file (e.g. --output data/broward.csv)")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--line",    type=int,
                       help="Scrape a single 1-based line number (e.g. --line 3)")
    group.add_argument("--range",   type=str,
                       help="Scrape a range of lines (e.g. --range 1-50)")
    group.add_argument("--indices", type=lambda s: [int(x) for x in s.split(",")],
                       metavar="N,N,…",
                       help="Scrape specific comma-separated 1-based indices (e.g. --indices 1,4,7)")

    args = parser.parse_args()
    process_batch(args)
