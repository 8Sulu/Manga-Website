"""
broward_debug.py — diagnose why detailclick returns no copies.
Run this and paste the output back.
"""
import re, json, time, sys, urllib.parse, requests
from bs4 import BeautifulSoup
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

BASE_URL = "https://broward.ent.sirsi.net"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/javascript, text/html, application/json, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": BASE_URL,
}

title  = "Berserk"
author = "Miura"

s = requests.Session()
s.headers.update(HEADERS)
s.get(BASE_URL + "/client/en_US/default", timeout=20)
s.headers.update({"Referer": BASE_URL + "/client/en_US/default"})
print(f"Cookies after prime: {dict(s.cookies)}\n")

# ── Step 1: search ──────────────────────────────────────────────
search_url = (
    f"{BASE_URL}/client/en_US/default/search/results"
    f"?qu=&qu=TITLE%3D{urllib.parse.quote_plus(title)}+"
    f"&qu=AUTHOR%3D{urllib.parse.quote_plus(author)}+"
    f"&qf=FORMAT%09Special+Format%09BOOK%09Books&h=1"
)
r = s.get(search_url, timeout=15)
ids = list(dict.fromkeys(re.findall(r"SD_ILS:(\d+)", r.text)))
print(f"Search status: {r.status_code}  IDs: {ids[:6]}\n")

if not ids:
    print("No IDs found — check search URL")
    sys.exit(1)

item_id = ids[0]
print(f"=== Testing item_id={item_id} ===\n")

# ── Step 2a: Try the detailclick URL from the SEARCH PAGE (not hardcoded) ──
# The search page embeds the exact detailclick URL for each result
# Pattern from search_page.html:
# results.displaypanel.displaycell_0:detailclick/ent:.../SD_ILS:782916/0/0/tabDISCOVERY_ALLlistItem
# Note: the position indices (0/0 vs 1/1 vs 2/2) vary per result!

# Extract actual detailclick URLs from search page
detail_urls = re.findall(
    r'results\.displaypanel\.displaycell_0:detailclick'
    r'(/ent:[^\s"\'<>&?]+)',
    r.text
)
print(f"Detail URLs in search page (first 3):")
for u in detail_urls[:3]:
    print(f"  {urllib.parse.unquote(u.replace('$002f','/').replace('$0022','\"'))[:120]}")
print()

# ── Step 2b: Try three URL variants to find which works ────────────────────
s.headers.update({
    "Referer": f"{BASE_URL}/client/en_US/default/search/results?qu={urllib.parse.quote_plus(title)}"
})

variants = [
    # Original (hardcoded 1/1)
    (
        "original (1/1)",
        f"{BASE_URL}/client/en_US/default/search/results"
        f".displaypanel.displaycell_0:detailclick"
        f"/ent:$002f$002fSD_ILS$002f0$002fSD_ILS:{item_id}/1/1"
        f"/tabDISCOVERY_ALLlistItem",
    ),
    # Position 0/0 (first result)
    (
        "position 0/0",
        f"{BASE_URL}/client/en_US/default/search/results"
        f".displaypanel.displaycell_0:detailclick"
        f"/ent:$002f$002fSD_ILS$002f0$002fSD_ILS:{item_id}/0/0"
        f"/tabDISCOVERY_ALLlistItem",
    ),
    # No position numbers
    (
        "no position",
        f"{BASE_URL}/client/en_US/default/search/results"
        f".displaypanel.displaycell_0:detailclick"
        f"/ent:$002f$002fSD_ILS$002f0$002fSD_ILS:{item_id}"
        f"/tabDISCOVERY_ALLlistItem",
    ),
]

payload = {
    "qu":  title,
    "qf":  "ITYPE\tMaterial Type\t1:BOOK\tBook",
    "d":   f"ent://SD_ILS/0/SD_ILS:{item_id}~ILS~1",
    "h":   "8",
}

for label, url in variants:
    print(f"--- Variant: {label} ---")
    print(f"URL: {url}")
    try:
        r2 = s.post(url, data=payload, timeout=15)
        html = r2.text.replace("\\/", "/")
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.find_all(class_="detailItemsTableRow")
        title_els = [el for el in soup.find_all(class_="INITIAL_TITLE_SRCH")
                     if "Title" not in el.get_text() and len(el.get_text(strip=True)) < 300]
        system_err = "System Error" in html or "exceptionTitle" in html
        print(f"  Status: {r2.status_code}  len={len(html)}")
        print(f"  System error: {system_err}")
        print(f"  detailItemsTableRow count: {len(rows)}")
        print(f"  Title found: {title_els[0].get_text(strip=True)[:80] if title_els else 'none'}")
        print(f"  Cookies: {dict(s.cookies)}")
        if rows:
            print(f"  First row cells:")
            for row in rows[:2]:
                lib_el = row.find(class_="asyncFieldLIBRARY hidden")
                print(f"    branch={lib_el.get_text(strip=True) if lib_el else '?'}")
            break  # Found working variant
        # Show first 500 chars of body on failure
        if system_err or len(rows) == 0:
            print(f"  Body preview: {html[:400]}")
    except Exception as e:
        print(f"  Exception: {e}")
    print()
    time.sleep(1)

# ── Step 2c: Try using the EXACT URL from the search page ──────────────────
if detail_urls:
    print("--- Variant: exact URL from search page ---")
    # The URLs in the search page use a different path format
    # Reconstruct proper URL
    raw_path = detail_urls[0]  # e.g. /ent:$002f$002fSD_ILS...
    full_url = (
        f"{BASE_URL}/client/en_US/default/search/results"
        f".displaypanel.displaycell_0:detailclick{raw_path}"
    )
    # Add query params
    full_url += f"?qu={urllib.parse.quote_plus(title)}&qf=ITYPE%09Material+Type%091%3ABOOK%09Book&h=8"
    print(f"URL: {full_url[:200]}")
    try:
        r3 = s.get(full_url, timeout=15)
        html3 = r3.text.replace("\\/", "/")
        soup3 = BeautifulSoup(html3, "html.parser")
        rows3 = soup3.find_all(class_="detailItemsTableRow")
        print(f"  Status: {r3.status_code}  rows: {len(rows3)}")
        print(f"  Body preview: {html3[:300]}")
    except Exception as e:
        print(f"  Exception: {e}")
