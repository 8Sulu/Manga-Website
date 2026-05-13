"""
Library manga scraper - scrapes Leon County Library for manga availability.

Usage:
    python scrapper.py <number_of_titles> [--visible] [--debug]

    --visible   Show the browser window while scraping (default: headless)
    --debug     Save page HTML to debug/ folder for troubleshooting
"""
import re
import urllib.parse
import sys
import csv
import time
import logging
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

sys.path.append(str(Path(__file__).parent.parent))
from config.settings import DATA_DIR, BRANCH_MAPPING

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

SEARCH_BASE  = "https://lcpl.ent.sirsi.net/client/en_US/lcpl/search/results"
WAIT_TIMEOUT = 20
PAGE_SETTLE  = 3   # seconds to let JS render after navigation


# ---------------------------------------------------------------------------
# Driver setup — Linux Chromium only, no Windows fallback
# ---------------------------------------------------------------------------

def make_driver(visible: bool = False) -> webdriver.Chrome:
    opts = Options()

    if not visible:
        opts.add_argument('--headless=new')

    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('--disable-gpu')
    opts.add_argument('--window-size=1920,1080')
    opts.add_argument('--disable-extensions')
    opts.add_argument('--blink-settings=imagesEnabled=false')

    linux_bins = [
        '/usr/bin/chromium-browser',
        '/usr/bin/chromium',
        '/usr/bin/google-chrome',
        '/usr/bin/google-chrome-stable',
    ]

    found = False
    for path in linux_bins:
        if Path(path).exists():
            opts.binary_location = path
            log.info(f"Using browser: {path}")
            found = True
            break

    if not found:
        log.error("No Linux Chromium/Chrome binary found. Install with:")
        log.error("  sudo apt install chromium-browser")
        sys.exit(1)

    from selenium.webdriver.chrome.service import Service

    # Prefer the Linux chromedriver that matches the system Chromium.
    # Avoids accidentally picking up a Windows chromedriver from PATH in WSL.
    linux_drivers = [
        '/usr/bin/chromedriver',
        '/usr/lib/chromium-browser/chromedriver',
        '/usr/lib/chromium/chromedriver',
        '/snap/bin/chromium.chromedriver',
    ]

    driver_path = None
    for p in linux_drivers:
        if Path(p).exists():
            driver_path = p
            log.info(f"Using chromedriver: {p}")
            break

    if not driver_path:
        log.error("No Linux chromedriver found. Install with:")
        log.error("  sudo apt install chromium-chromedriver")
        sys.exit(1)

    return webdriver.Chrome(service=Service(driver_path), options=opts)


# ---------------------------------------------------------------------------
# Scraping logic
# ---------------------------------------------------------------------------

def extract_volume(raw_title: str):
    """
    Return (clean_title, volume_int) from a raw library title string.
    Uses the LAST number found — avoids misreading series numbers as volumes.
      'Berserk, Vol. 3'                        → ('Berserk', 3)
      'JoJo Part 7: Steel Ball Run, Vol. 12'   → ('JoJo Part 7: Steel Ball Run', 12)
    """
    nums = re.findall(r'\d+', raw_title)
    if nums:
        vol = int(nums[-1])
        idx = raw_title.rfind(nums[-1])
        clean = raw_title[:idx].strip(' ,.-')
        return clean, vol
    return raw_title.strip(), 0


def wait_for_results(driver, wait):
    """
    Wait until either a result or a no-results message appears.
    Both are valid signals that the page has finished loading.
    Returns True if results found, False if no-results or timeout.
    """
    time.sleep(PAGE_SETTLE)

    try:
        # Accept either detailLink (results) or searchResultText (no results)
        wait.until(EC.any_of(
            EC.presence_of_element_located((By.CSS_SELECTOR, "[id^='detailLink']")),
            EC.presence_of_element_located((By.ID, "searchResultText")),
        ))
    except TimeoutException:
        log.warning("  Timeout — page never loaded results or no-results message")
        return False

    # Now check which one appeared
    try:
        msg = driver.find_element(By.ID, 'searchResultText').text
        if 'No results' in msg:
            log.info("  No results")
            return False
    except NoSuchElementException:
        pass

    return True


def scrape_page(driver, title_number: int, availability_id: int):
    """Extract all book entries visible on the current page. Returns list of dicts."""
    links = driver.find_elements(By.CSS_SELECTOR, "[id^='detailLink']")
    books = []

    for elem in links:
        elem_id = elem.get_attribute('id') or ''
        if not elem_id.startswith('detailLink'):
            continue
        try:
            book_num = int(elem_id.replace('detailLink', ''))
        except ValueError:
            continue

        raw_title = elem.text.strip()
        if not raw_title:
            continue

        clean_title, vol = extract_volume(raw_title)

        branch_status = []
        try:
            table = driver.find_element(By.ID, f'detailItemTableCust{book_num}')
            for row in table.find_elements(By.TAG_NAME, 'tr'):
                cells = row.find_elements(By.TAG_NAME, 'td')
                if cells:
                    branch_status.append((cells[0].text.strip(), cells[-1].text.strip()))
        except NoSuchElementException:
            pass

        books.append({
            'title':           clean_title,
            'manga_id':        title_number,
            'volume':          vol,
            'branch_status':   branch_status,
            'availability_id': availability_id,
        })
        availability_id += 1

    return books, availability_id


def scrape(n: int, visible: bool = False, debug: bool = False):
    titles_file  = DATA_DIR / 'titles.txt'
    authors_file = DATA_DIR / 'authors.txt'

    with open(titles_file, encoding='utf-8') as f:
        titles = [l.strip() for l in f if l.strip()]
    with open(authors_file, encoding='utf-8') as f:
        authors = [l.strip() for l in f if l.strip()]

    # Only scrape pairs where we actually have an author
    pairs = [(t, a) for t, a in zip(titles, authors) if a.strip()][:n]
    log.info(f"Scraping {len(pairs)} titles (visible={visible})")

    driver = make_driver(visible=visible)
    wait   = WebDriverWait(driver, WAIT_TIMEOUT)
    all_books      = []
    availability_id = 1

    try:
        for title_number, (title, author) in enumerate(pairs, start=1):
            title_enc  = urllib.parse.quote(title)
            author_enc = urllib.parse.quote(author)
            url = (
                f"{SEARCH_BASE}"
                f"?qu=&qu=TITLE%3D{title_enc}+&qu=AUTHOR%3D{author_enc}+"
                f"&te=ILS&h=1"
            )
            log.info(f"[{title_number}/{len(pairs)}] {title!r} by {author!r}")
            driver.get(url)

            # SirsiDynix uses &rw=N to offset results (12 per page).
            # We navigate directly to each page URL instead of clicking the
            # AJAX next button, which triggers Cloudflare security checks.
            RESULTS_PER_PAGE = 12
            page = 0
            while True:
                rw = page * RESULTS_PER_PAGE
                page_url = url + (f"&rw={rw}" if rw > 0 else "")
                if page > 0:
                    log.info(f"  Navigating to page {page} (rw={rw})")
                    driver.get(page_url)

                if not wait_for_results(driver, wait):
                    break

                if debug:
                    safe      = re.sub(r'[^\w]', '_', title)
                    debug_dir = DATA_DIR.parent / 'debug'
                    debug_dir.mkdir(exist_ok=True)
                    (debug_dir / f"{safe}_p{page}.html").write_text(
                        driver.page_source, encoding='utf-8'
                    )

                books, availability_id = scrape_page(driver, title_number, availability_id)
                all_books.extend(books)
                log.info(f"  Page {page} (rw={rw}): {len(books)} volumes")

                if not books:
                    break

                # If we got fewer than a full page, there are no more pages
                if len(books) < RESULTS_PER_PAGE:
                    break

                page += 1

    finally:
        driver.quit()

    return all_books


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------

def write_csvs(books: list):
    branch_id_map = {k.upper(): v for k, v in BRANCH_MAPPING.items()}

    avail_path = DATA_DIR / 'availability.csv'
    bas_path   = DATA_DIR / 'branch_availability_status.csv'

    with open(avail_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['MangaID', 'Volume'])
        w.writeheader()
        for b in books:
            w.writerow({'MangaID': b['manga_id'], 'Volume': b['volume']})

    with open(bas_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['AvailabilityID', 'BranchID', 'Status'])
        w.writeheader()
        for b in books:
            for branch, status in b['branch_status']:
                branch_id = branch_id_map.get(branch.upper(), -1)
                if branch_id == -1:
                    log.warning(f"  Unknown branch (skipped): {branch!r}")
                    continue
                w.writerow({
                    'AvailabilityID': b['availability_id'],
                    'BranchID':       branch_id,
                    'Status':         status,
                })

    log.info(f"Wrote {len(books)} volumes → {avail_path.name}, {bas_path.name}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    n       = int(sys.argv[1])
    visible = '--visible' in sys.argv
    debug   = '--debug' in sys.argv

    books = scrape(n, visible=visible, debug=debug)
    log.info(f"Scraping complete — {len(books)} volumes found")
    write_csvs(books)
