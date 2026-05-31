"""
broward_explore.py — Broward County Library catalog exploration tool.

Probes the Broward Sirsi catalog for a single manga title and dumps
everything needed to design a per-branch, per-volume scraper that
matches the LCPL scraper's output format.

Usage:
    python broward_explore.py "Berserk"
    python broward_explore.py "One Piece" --author "Eiichiro Oda"
    python broward_explore.py "Fullmetal Alchemist" --save-html

Output files written to ./broward_explore_output/<title>/
    search_page.html          — raw HTML of the search results page
    item_<id>_init.html       — raw HTML of the detail panel (step 1) per item
    item_<id>_avail_raw.json  — raw JSON response from the availability endpoint (step 2)
    item_<id>_avail_eval.txt  — the evalScript string extracted from step 2
    summary.txt               — human-readable summary of everything found
"""
from __future__ import annotations

import re
import sys
import json
import time
import argparse
import textwrap
import urllib.parse
from pathlib import Path

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://broward.ent.sirsi.net"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/javascript, text/html, application/json, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type":    "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": BASE_URL,
}

# Known Broward branch names (from branches.csv) for matching
BROWARD_BRANCHES = [
    "Northwest Regional Library",
    "Nova Southeastern University Alvin Sherman Library",
    "South Regional/Broward College Library",
    "Marta",
    "West Regional Library",
    "African American Research Library & Cultural Center",
    "Carver Ranches Branch",
    "Century Plaza/Leon Slatin Branch",
    "Dania Beach Paul DeMaio Branch",
    "Deerfield Beach Percy White Branch",
    "Hallandale Beach Branch",
    "Hollywood Branch",
    "Imperial Point Branch",
    "Lauderhill Central Park Library",
    "Lauderhill Towne Centre Branch",
    "Main Library",
    "Margate Catharine Young Branch",
    "Miramar Branch Library & Education Center",
    "North Lauderdale Saraniero Branch",
    "North Regional/Broward College Library",
    "Pembroke Pines/Walter C. Young Resource Center",
    "Pompano Beach Library & Cultural Center",
    "Riverland Branch",
    "Southwest Regional Library",
    "Sunrise Dan Pearl Branch",
    "Tamarac Branch",
    "Weston Branch",
    "Beach Branch",
    "Broward County Branch formerly Young at Art",
    "Davie/Cooper City Branch",
    "Fort Lauderdale Reading Center",
    "Galt Ocean Mile Reading Center",
    "Hollywood Beach Bernice P. Oster Branch",
    "Jan Moran Collier City Learning Library",
    "Lauderdale Lakes Library/Educational & Cultural Center",
    "Northwest Branch",
    "Tyrone Bryant Branch",
]

# ── Helpers ────────────────────────────────────────────────────────────────────

def sep(title="", char="─", width=70):
    if title:
        pad = max(0, width - len(title) - 2)
        print(f"\n{char * 2} {title} {char * pad}")
    else:
        print(char * width)

def dump(label, data):
    print(f"  {label}: {json.dumps(data, indent=4, ensure_ascii=False)}")

# ── Session ────────────────────────────────────────────────────────────────────

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    print("[*] Priming session (GET base URL)…")
    try:
        r = s.get(BASE_URL + "/client/en_US/default", timeout=20)
        print(f"    Status: {r.status_code}  Cookies: {dict(s.cookies)}")
    except Exception as e:
        print(f"    Warning: {e}")
    return s

# ── Step 1: search ─────────────────────────────────────────────────────────────

def search(session, title, author, out_dir: Path, save_html: bool) -> list[str]:
    sep("STEP 1 — SEARCH")

    encoded_title  = urllib.parse.quote_plus(title)
    encoded_author = urllib.parse.quote_plus(author) if author else ""
    url = (
        f"{BASE_URL}/client/en_US/default/search/results"
        f"?qu=&qu=TITLE%3D{encoded_title}+"
        + (f"&qu=AUTHOR%3D{encoded_author}+" if author else "")
        + "&qf=FORMAT%09Special+Format%09BOOK%09Books&h=1"
    )
    print(f"  URL: {url}")

    r = session.get(url, timeout=20)
    print(f"  Status: {r.status_code}   Content-Length: {len(r.text)}")

    if save_html:
        path = out_dir / "search_page.html"
        path.write_text(r.text, encoding="utf-8")
        print(f"  Saved: {path}")

    # Extract SD_ILS item IDs
    ids = list(dict.fromkeys(re.findall(r"SD_ILS:(\d+)", r.text)))
    print(f"\n  Item IDs found: {ids}")

    # Try to extract result count from page
    soup = BeautifulSoup(r.text, "html.parser")
    count_el = soup.find(id="searchResultText") or soup.find(class_="results-count")
    if count_el:
        print(f"  Result text: {count_el.get_text(strip=True)[:120]}")

    # Check for pagination
    next_page = soup.find("a", class_=re.compile(r"next|nextPage", re.I))
    print(f"  Pagination 'next' link found: {bool(next_page)}")

    # Show any title/call-number data visible in the search results
    detail_links = soup.find_all("a", id=re.compile(r"^detailLink"))
    print(f"\n  Detail link hrefs (first 5):")
    for a in detail_links[:5]:
        print(f"    {a['href'][:120]}")

    return ids

# ── Step 2: detail panel (init) ────────────────────────────────────────────────

def fetch_detail(session, item_id: str, title: str, out_dir: Path, save_html: bool) -> str | None:
    """POST to the detail panel endpoint, extract sdcsrf token. Returns token or None."""
    sep(f"STEP 2 — DETAIL PANEL  item_id={item_id}")

    init_url = (
        f"{BASE_URL}/client/en_US/default/search/results"
        f".displaypanel.displaycell_0:detailclick"
        f"/ent:$002f$002fSD_ILS$002f0$002fSD_ILS:{item_id}/1/1"
        f"/tabDISCOVERY_ALLlistItem"
    )
    print(f"  URL: {init_url}")

    params = {
        "qu": title,
        "qf": "ITYPE\tMaterial Type\t1:BOOK\tBook",
        "d":  f"ent://SD_ILS/0/SD_ILS:{item_id}~ILS~1",
        "h":  "8",
    }
    session.headers.update({"Referer": f"{BASE_URL}/client/en_US/default/search/results?qu={urllib.parse.quote_plus(title)}"})

    r = session.post(init_url, data=params, timeout=20)
    print(f"  Status: {r.status_code}   Content-Length: {len(r.text)}")

    if save_html:
        path = out_dir / f"item_{item_id}_init.html"
        path.write_text(r.text, encoding="utf-8")
        print(f"  Saved: {path}")

    soup = BeautifulSoup(r.text, "html.parser")

    # Extract title from detail panel
    title_el = soup.find(class_=re.compile(r"TITLE", re.I))
    if title_el:
        print(f"  Title in panel: {title_el.get_text(strip=True)[:100]}")

    # Extract call number / volume info
    call_els = soup.find_all(class_=re.compile(r"callnum|call.?number|CALL", re.I))
    print(f"\n  Call number elements ({len(call_els)} found):")
    for el in call_els[:5]:
        print(f"    [{el.get('class')}]: {el.get_text(strip=True)[:100]}")

    # Look for volume-related text anywhere
    vol_matches = re.findall(r"[Vv]ol(?:ume)?\.?\s*\d+|[Vv]\.\s*\d+|\bv\d+\b", r.text)
    print(f"\n  Volume patterns in HTML: {list(dict.fromkeys(vol_matches))[:20]}")

    # sdcsrf token
    sdcsrf = session.cookies.get("sdcsrf")
    m = re.search(r"sdcsrf=([a-f0-9\-]+)", r.text)
    if m:
        sdcsrf = m.group(1)
    print(f"\n  sdcsrf token: {sdcsrf}")

    # Look for any other tokens/forms
    forms = soup.find_all("form")
    print(f"  Forms: {len(forms)}")
    for form in forms[:3]:
        print(f"    action={form.get('action','')}  inputs={[i.get('name') for i in form.find_all('input')]}")

    # Check cookies
    print(f"\n  Session cookies after init: {dict(session.cookies)}")

    return sdcsrf

# ── Step 3: availability endpoint ─────────────────────────────────────────────

def fetch_availability(session, item_id: str, title: str, sdcsrf: str, out_dir: Path) -> dict | None:
    sep(f"STEP 3 — AVAILABILITY  item_id={item_id}")

    avail_url = (
        f"{BASE_URL}/client/en_US/default/search/results"
        f".displaypanel.displaycell_0.detail.detailavailabilityaccordions"
        f".boundwithzone:lookupavailability"
        f"/ent:$002f$002fSD_ILS$002f0$002fSD_ILS:{item_id}/ILS/1/false"
        f"/LIBRARY$002cCALLNUMBER"
    )
    print(f"  URL: {avail_url}")

    session.headers.update({"sdcsrf": sdcsrf})
    payload = {
        "qu":     title,
        "qf":     "ITYPE\tMaterial Type\t1:BOOK\tBook",
        "d":      f"ent://SD_ILS/0/SD_ILS:{item_id}~ILS~1",
        "h":      "8",
        "sdcsrf": sdcsrf,
    }

    r = session.post(avail_url, data=payload, timeout=20)
    print(f"  Status: {r.status_code}   Content-Length: {len(r.text)}")
    print(f"  Content-Type: {r.headers.get('Content-Type','')}")

    # Save raw JSON
    raw_path = out_dir / f"item_{item_id}_avail_raw.json"
    raw_path.write_text(r.text, encoding="utf-8")
    print(f"  Saved raw: {raw_path}")

    try:
        data = r.json()
    except Exception as e:
        print(f"  ✗ Failed to parse JSON: {e}")
        print(f"  Raw text (first 500 chars): {r.text[:500]}")
        return None

    print(f"\n  Top-level keys: {list(data.keys())}")

    # Drill into inits[0].evalScript
    inits = data.get("inits", [])
    print(f"  inits count: {len(inits)}")
    if inits:
        first = inits[0]
        print(f"  inits[0] keys: {list(first.keys())}")
        eval_scripts = first.get("evalScript", [])
        print(f"  evalScript entries: {len(eval_scripts)}")
        for i, es in enumerate(eval_scripts[:3]):
            path = out_dir / f"item_{item_id}_avail_eval_{i}.txt"
            path.write_text(es, encoding="utf-8")
            print(f"\n  evalScript[{i}] (saved to {path.name}):")
            print(textwrap.indent(es[:2000], "    "))
            if len(es) > 2000:
                print(f"    … ({len(es)} chars total)")

    # Look for branch/location data in the full JSON
    json_str = json.dumps(data)

    # Search for known branch name fragments
    print(f"\n  Branch name hits in response:")
    found_branches = []
    for branch in BROWARD_BRANCHES:
        keyword = branch.split("/")[0].split("&")[0].strip()[:20]
        if keyword.lower() in json_str.lower():
            found_branches.append(branch)
            print(f"    ✓ {branch}")
    if not found_branches:
        print("    (none of the known branch names matched)")

    # Look for updateWebServiceFields call
    m = re.search(r"updateWebServiceFields\((.*?)\);", json_str, re.DOTALL)
    if m:
        print(f"\n  updateWebServiceFields payload:")
        try:
            meta = json.loads(m.group(1))
            dump("  meta", meta)
        except Exception:
            print(f"    raw: {m.group(1)[:500]}")

    # Look for any JSON blobs inside evalScript that mention locations/branches
    all_json_blobs = re.findall(r'\{[^{}]{20,}\}', json_str)
    location_blobs = [b for b in all_json_blobs if re.search(r'[Ll]ibrary|[Bb]ranch|[Ll]ocation|callNum', b)]
    print(f"\n  JSON blobs mentioning library/branch/location/callNum: {len(location_blobs)}")
    for blob in location_blobs[:5]:
        try:
            parsed = json.loads(blob)
            print(f"    {json.dumps(parsed, indent=2)[:400]}")
        except Exception:
            print(f"    (unparseable) {blob[:200]}")

    # Also look for HTML tables inside the eval script that list copies
    html_in_eval = re.findall(r'<table[^>]*>.*?</table>', json_str, re.DOTALL | re.IGNORECASE)
    print(f"\n  HTML <table> blocks inside JSON: {len(html_in_eval)}")
    for i, tbl in enumerate(html_in_eval[:2]):
        tbl_clean = re.sub(r'\\n|\\t|\\r|\\/', '', tbl)
        soup = BeautifulSoup(tbl_clean, "html.parser")
        rows = soup.find_all("tr")
        print(f"\n  Table {i} — {len(rows)} rows:")
        for row in rows[:15]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if any(cells):
                print(f"    {cells}")

    # Look for setContent / innerHTML calls that might contain per-copy HTML
    set_content = re.findall(r'setContent[^"]*"([^"]{50,})"', json_str)
    print(f"\n  setContent calls: {len(set_content)}")
    for sc in set_content[:2]:
        unescaped = sc.replace('\\n', '\n').replace('\\t', '\t').replace('\\"', '"')
        print(f"    (first 600 chars): {unescaped[:600]}")

    return data

# ── Alternative: ILSWS REST API (same as LCPL) ────────────────────────────────

def probe_ilsws(session, catalog_key: str, out_dir: Path) -> None:
    sep(f"PROBE — ILSWS REST API  key={catalog_key}")

    ilsws_url = "https://broward.sirsi.net/broward_ilsws/rest/standard/lookupTitleInfo"
    params = {
        "clientID":        "DS_CLIENT",
        "titleID":         catalog_key,
        "includeItemInfo": "true",
        "includeOPACInfo": "false",
        "json":            "true",
        "callback":        "broward_cb",
        "_":               int(time.time() * 1000),
    }
    print(f"  URL: {ilsws_url}?{urllib.parse.urlencode(params)}")

    try:
        r = session.get(ilsws_url, params=params, timeout=15)
        print(f"  Status: {r.status_code}   Content-Length: {len(r.text)}")
        out = out_dir / f"ilsws_{catalog_key}.json"
        out.write_text(r.text, encoding="utf-8")
        print(f"  Saved: {out}")

        # Strip JSONP wrapper if present
        text = r.text.strip()
        m = re.match(r'^[^(]*\((.*)\)\s*;?\s*$', text, re.DOTALL)
        if m:
            data = json.loads(m.group(1))
            print(f"  JSONP response — top keys: {list(data.keys())}")
            for title_entry in data.get("TitleInfo", [])[:2]:
                print(f"\n  TitleInfo entry keys: {list(title_entry.keys())}")
                for call in title_entry.get("CallInfo", [])[:5]:
                    lib_id = call.get("libraryID", "")
                    call_num = call.get("callNumber", "")
                    items = call.get("ItemInfo", [])
                    print(f"    libraryID={lib_id!r:30s}  callNumber={call_num!r:40s}  items={len(items)}")
                    for item in items[:3]:
                        print(f"      currentLocationID={item.get('currentLocationID')!r}  "
                              f"dueDate={item.get('dueDate')!r}  "
                              f"itemID={item.get('itemID')!r}")
        else:
            print(f"  Raw (first 500): {text[:500]}")
    except Exception as e:
        print(f"  ✗ ILSWS probe failed: {e}")

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("title",          help="Manga title to search")
    parser.add_argument("--author", "-a", default="",  help="Author name (optional but recommended)")
    parser.add_argument("--save-html",    action="store_true", help="Save raw HTML pages to disk")
    parser.add_argument("--max-items",    type=int, default=3, help="Max item IDs to probe in detail (default 3)")
    parser.add_argument("--ilsws",        action="store_true", help="Also probe the ILSWS REST API")
    args = parser.parse_args()

    out_dir = Path("broward_explore_output") / re.sub(r'[^\w]', '_', args.title)
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"[*] Output directory: {out_dir.resolve()}")

    summary_lines = []
    def log(s):
        print(s)
        summary_lines.append(s)

    session = make_session()

    # Step 1: search
    item_ids = search(session, args.title, args.author, out_dir, args.save_html)
    log(f"\n[SUMMARY] Search returned {len(item_ids)} item IDs: {item_ids[:10]}")

    if not item_ids:
        log("[!] No items found — try a different title or author.")
        return

    # Step 2+3: detail + availability for first N items
    items_to_probe = item_ids[:args.max_items]
    log(f"[*] Probing first {len(items_to_probe)} items in detail…")

    for item_id in items_to_probe:
        time.sleep(1)
        sdcsrf = fetch_detail(session, item_id, args.title, out_dir, args.save_html)
        if not sdcsrf:
            log(f"  [!] No sdcsrf for item {item_id} — skipping availability step")
            continue
        time.sleep(1)
        avail_data = fetch_availability(session, item_id, args.title, sdcsrf, out_dir)
        log(f"  [item {item_id}] availability probe {'ok' if avail_data else 'FAILED'}")

        # Also probe ILSWS with this catalog key
        if args.ilsws:
            time.sleep(1)
            probe_ilsws(session, item_id, out_dir)

    # Always try ILSWS for the first item regardless of flag
    sep("PROBE — ILSWS REST API (always runs for first item)")
    probe_ilsws(session, item_ids[0], out_dir)

    # Write summary
    summary_path = out_dir / "summary.txt"
    summary_path.write_text("\n".join(summary_lines), encoding="utf-8")
    sep("DONE")
    print(f"  All output written to: {out_dir.resolve()}")
    print(f"  Key files to send back:")
    for f in sorted(out_dir.iterdir()):
        print(f"    {f.name}  ({f.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
