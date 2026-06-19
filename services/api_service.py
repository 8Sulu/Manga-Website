import csv
import logging
import os
import time
from pathlib import Path

import requests

# ── .env — always resolve from this file's location ──────────────────────────
_BASE_DIR = Path(__file__).parent.parent.resolve()
try:
    from dotenv import load_dotenv
    load_dotenv(_BASE_DIR / '.env', override=False)
except ImportError:
    _env_file = _BASE_DIR / '.env'
    if _env_file.exists():
        for _line in _env_file.read_text().splitlines():
            _line = _line.strip()
            if _line and not _line.startswith('#') and '=' in _line:
                _k, _v = _line.split('=', 1)
                if _k.strip() not in os.environ:
                    os.environ[_k.strip()] = _v.strip().strip('"\'')

from config.settings import DATA_DIR, REQUEST_TIMEOUT, MAX_RETRIES  # noqa: E402
from services.mal_client import authenticated_request  # noqa: E402

logger = logging.getLogger(__name__)

# ── Stop flag ─────────────────────────────────────────────────────────────────
_stop_requested = False


def request_stop():
    global _stop_requested
    _stop_requested = True


def clear_stop():
    global _stop_requested
    _stop_requested = False


def is_stop_requested():
    return _stop_requested

# ── manga.csv field names ─────────────────────────────────────────────────────
MANGA_CSV_FIELDS = [
    "MangaID",
    "Title",
    "Type",
    "Volumes",
    "Members",
    "Score",
    "Author",
    "CoverMedium",
    "CoverLarge",
]


# ── Detail fetcher ────────────────────────────────────────────────────────────


def _fetch_manga_details(mal_id: str) -> dict:
    """
    Fetch type, volumes, members, score, author, and English title for one MAL ID.
    Returns a dict containing English Title (if available) along with metadata fields.
    """
    # FIX: Correctly query alternative_titles as a core field
    url = (
        f"https://api.myanimelist.net/v2/manga/{mal_id}"
        f"?fields=media_type,num_volumes,mean,num_list_users,authors{{first_name,last_name}},alternative_titles,main_picture"
    )

    for attempt in range(MAX_RETRIES):
        try:
            resp = authenticated_request(url, timeout=REQUEST_TIMEOUT)

            if resp.status_code == 200:
                data = resp.json()
                media_type = data.get("media_type", "Unknown").replace("_", " ").title()
                num_volumes = data.get("num_volumes") or None  # 0 → None → "NULL"

                # FIX: Handle object extraction and fix 'englisth_title' typo
                english_title = None
                alt_titles_dict = data.get("alternative_titles") or {}
                if isinstance(alt_titles_dict, dict):
                    english_title = (alt_titles_dict.get("en") or "").strip()

                # Extract primary author surname
                author = "NULL"
                authors_list = data.get("authors", [])
                if authors_list:
                    node = authors_list[0].get("node", {})
                    last_name = (node.get("last_name") or "").strip()
                    first_name = (node.get("first_name") or "").strip()
                    if last_name:
                        author = last_name
                    elif first_name:
                        author = first_name.split()[-1]

                main_pic = data.get("main_picture") or {}
                cover_medium = main_pic.get("medium") or "NULL"
                cover_large = main_pic.get("large") or "NULL"

                return {
                    "EnglishTitle": english_title
                    or None,  # temporary container for title logic handling
                    "Type": media_type,
                    "Volumes": num_volumes or "NULL",
                    "Members": data.get("num_list_users") or "NULL",
                    "Score": data.get("mean") or "NULL",
                    "Author": author,
                    "CoverMedium": cover_medium,
                    "CoverLarge": cover_large,
                }

            elif resp.status_code in (429, 503):
                wait = min(60, 5 * (2**attempt))
                logger.warning(f"  HTTP {resp.status_code} for ID {mal_id} — retrying in {wait}s")
                time.sleep(wait)
            else:
                logger.error(f"  HTTP {resp.status_code} for ID {mal_id}: {resp.text[:200]}")
                break

        except requests.exceptions.RequestException as e:
            wait = min(60, 5 * (2**attempt))
            logger.warning(
                f"  Network error ({type(e).__name__}) for ID {mal_id} — retrying in {wait}s"
            )
            print(
                f"  [!] Network timeout/error fetching details for ID {mal_id}, retrying...",
                flush=True,
            )
            time.sleep(wait)

    return {
        "EnglishTitle": None,
        "Type": "NULL",
        "Volumes": "NULL",
        "Members": "NULL",
        "Score": "NULL",
        "Author": "NULL",
        "CoverMedium": "NULL",
        "CoverLarge": "NULL",
    }


# ── Batch processor ───────────────────────────────────────────────────────────


def _upsert_manga_to_db(rows: list[dict]) -> str:
    """
    Insert new manga rows into the DB, skipping any MangaID already present.
    Returns a human-readable summary string.
    """
    if not rows:
        return "no new rows to insert"
    try:
        # Import here to avoid circular deps when api_service is used standalone
        import mysql.connector
        from config.settings import DB_CONFIG

        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        inserted = skipped = 0
        for row in rows:
            try:
                cursor.execute(
                    "INSERT INTO manga (MangaID, Title, Type, Volumes, Members, Score, Author, CoverMedium, CoverLarge) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        int(row["MangaID"]),
                        row["Title"],
                        None if row["Type"] in ("NULL", "", None) else row["Type"],
                        None if row["Volumes"] in ("NULL", "", None) else row["Volumes"],
                        None if row["Members"] in ("NULL", "", None) else row["Members"],
                        None if row["Score"] in ("NULL", "", None) else row["Score"],
                        None if row["Author"] in ("NULL", "", None) else row["Author"],
                        None
                        if row.get("CoverMedium") in ("NULL", "", None)
                        else row.get("CoverMedium"),
                        None
                        if row.get("CoverLarge") in ("NULL", "", None)
                        else row.get("CoverLarge"),
                    ),
                )
                inserted += 1
            except mysql.connector.IntegrityError:
                skipped += 1  # duplicate PK — already exists
        conn.commit()
        cursor.close()
        conn.close()
        return f"DB: inserted {inserted} new manga rows, skipped {skipped} duplicates"
    except Exception as e:
        logger.warning(f"DB upsert failed (CSV still updated): {e}")
        return f"DB upsert failed: {e}"


def process_manga_batch(offset: int) -> bool:
    """
    Fetch up to 500 ranked titles from MAL starting at <offset>.
    New rows are appended to manga.csv and upserted into the manga DB table.
    """
    clear_stop()

    ranking_url = (
        f"https://api.myanimelist.net/v2/manga/ranking?ranking_type=all&limit=500&offset={offset}"
    )
    logger.info(f"Fetching rankings at offset {offset}…")
    print(f"Fetching rankings at offset {offset}…", flush=True)

    resp = authenticated_request(ranking_url, timeout=REQUEST_TIMEOUT)
    if resp.status_code != 200:
        logger.error(f"Failed to fetch rankings: HTTP {resp.status_code}")
        print(f"[-] Rankings request failed: HTTP {resp.status_code}", flush=True)
        return False

    items = resp.json().get("data", [])
    if not items:
        logger.info("No items returned — offset may be beyond total ranked manga")
        print("[*] No items in response — batch is empty", flush=True)
        return True

    # ── Load existing IDs from CSV (source of truth for dedup) ────────────────
    manga_csv = DATA_DIR / "manga.csv"
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    existing_ids: set[str] = set()
    if manga_csv.exists():
        with open(manga_csv, encoding="utf-8-sig") as f:
            existing_ids = {row["MangaID"] for row in csv.DictReader(f) if row.get("MangaID")}
    else:
        with open(manga_csv, "w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(MANGA_CSV_FIELDS)

    # ── Process entries ────────────────────────────────────────────────────────
    new_rows: list[dict] = []
    total = len(items)

    for i, item in enumerate(items):
        if is_stop_requested():
            logger.info("Stop requested during get_manga")
            print(f"[{i}/{total}] Stop requested", flush=True)
            break

        node = item.get("node", {})
        mal_id = str(node.get("id", ""))
        canonical_title = node.get("title", "")

        if not mal_id or mal_id in existing_ids:
            print(f"[{i + 1}/{total}] skip {canonical_title}", flush=True)
            continue

        print(f"[{i + 1}/{total}] Fetching details for {mal_id}: {canonical_title}", flush=True)

        details = _fetch_manga_details(mal_id)
        chosen_title = details.pop("EnglishTitle") or canonical_title

        row = {"MangaID": mal_id, "Title": chosen_title, **details}
        new_rows.append(row)
        existing_ids.add(mal_id)
        time.sleep(0.5)

    # ── Persist to CSV & DB ───────────────────────────────────────────────────
    if new_rows:
        with open(manga_csv, "a", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=MANGA_CSV_FIELDS)
            w.writerows(new_rows)
        logger.info(f"Appended {len(new_rows)} entries to manga.csv")

        db_msg = _upsert_manga_to_db(new_rows)
        print(f"[+] {db_msg}", flush=True)
        print(f"[+] Added {len(new_rows)} new manga (offset {offset})", flush=True)
    else:
        print(f"[*] No new manga found at offset {offset} (all already in manga.csv)", flush=True)

    return True


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if len(sys.argv) != 2:
        print("Usage: python api_service.py <offset>")
        sys.exit(1)

    if process_manga_batch(int(sys.argv[1])):
        print("Done")
    else:
        print("Failed")
        sys.exit(1)
