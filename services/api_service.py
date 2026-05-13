import requests
import urllib.parse
import time
import logging
from typing import Dict, Optional
from pathlib import Path

from config.settings import DATA_DIR, REQUEST_TIMEOUT, MAX_RETRIES

logger = logging.getLogger(__name__)

KNOWN_AUTHORS = {
    "berserk":                                                    "Miura",
    "jojo no kimyou na bouken part 7: steel ball run":            "Araki",
    "vagabond":                                                   "Inoue",
    "one piece":                                                  "Oda",
    "monster":                                                    "Urasawa",
    "slam dunk":                                                  "Inoue",
    "vinland saga":                                               "Yukimura",
    "fullmetal alchemist":                                        "Arakawa",
    "grand blue":                                                 "Inoue",
    "oyasumi punpun":                                             "Asano",
    "kingdom":                                                    "Hara",
    "houseki no kuni":                                            "Ichikawa",
    "20th century boys":                                          "Urasawa",
    "ashita no joe":                                              "Chiba",
    "real":                                                       "Inoue",
    "kaguya-sama wa kokurasetai: tensai-tachi no renai zunousen": "Akasaka",
    "gto":                                                        "Fujisawa",
    "3-gatsu no lion":                                            "Umino",
    "yotsuba to!":                                                "Azuma",
    "koe no katachi":                                             "Oima",
    "haikyuu!!":                                                  "Furudate",
    "akatsuki no yona":                                           "Kusanagi",
    "kaze no tani no nausicaa":                                   "Miyazaki",
    "mushishi":                                                   "Urushibara",
    "nana":                                                       "Yazawa",
    "made in abyss":                                              "Tsukushi",
    "chainsaw man":                                               "Fujimoto",
    "one punch-man":                                              "Murata",
    "hunter x hunter":                                           "Togashi",
    "hajime no ippo":                                             "Morikawa",
    "kokou no hito":                                              "Sakamoto",
    "sayonara eri":                                               "Fujimoto",
    "death note":                                                 "Obata",
    "attack on titan":                                            "Isayama",
    "naruto":                                                     "Kishimoto",
    "dragon ball":                                                "Toriyama",
    "bleach":                                                     "Kubo",
    "tokyo ghoul":                                                "Ishida",
    "demon slayer":                                               "Gotouge",
    "my hero academia":                                           "Horikoshi",
    "spy x family":                                               "Endo",
    "jujutsu kaisen":                                             "Akutami",
    "blue period":                                                "Yamaguchi",
    "dungeon meshi":                                              "Kui",
    "solanin":                                                    "Asano",
    "punpun":                                                     "Asano",
    "i am a hero":                                                "Hanazawa",
}

class GoogleBooksAPI:
    BASE_URL = "https://www.googleapis.com/books/v1/volumes"

    def __init__(self):
        from config.settings import GOOGLE_BOOKS_API_KEY
        self.api_key = GOOGLE_BOOKS_API_KEY
        if not self.api_key:
            logger.warning("No GOOGLE_BOOKS_API_KEY — requests will be heavily rate-limited")
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Manga-Website/1.0'})

    def browse_link(self, title: str) -> str:
        """Inspectable URL — includes key so you see the same results the code sees."""
        q = urllib.parse.quote(f"{title} manga")
        url = f"{self.BASE_URL}?q={q}&printType=books&maxResults=5"
        if self.api_key:
            url += f"&key={self.api_key}"
        return url

    def get_books_data(self, title: str) -> Dict:
        params = {
            "q":         f"{title} manga",
            "printType": "books",
            "maxResults": 10,
        }
        if self.api_key:
            params["key"] = self.api_key

        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(self.BASE_URL, params=params, timeout=REQUEST_TIMEOUT)

                if response.status_code == 200:
                    data = response.json()
                    logger.info(f"  API: {len(data.get('items', []))} items")
                    return data

                if response.status_code in (429, 503):
                    wait = 10 * (attempt + 1)
                    logger.warning(f"  HTTP {response.status_code} — retrying in {wait}s")
                    time.sleep(wait)
                    continue

                logger.error(f"  HTTP {response.status_code}: {response.text[:300]}")
                return {}

            except Exception as e:
                logger.error(f"  Request error: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(3)

        return {}

    def find_valid_author(self, data: Dict, valid_names: set) -> Optional[str]:
        for item in data.get("items", []):
            for author in item.get("volumeInfo", {}).get("authors", []):
                words = [w.strip('.,').lower() for w in author.split()]
                if any(w in valid_names for w in words):
                    return author
        return None

    def list_authors_found(self, data: Dict) -> list:
        seen = []
        for item in data.get("items", []):
            for author in item.get("volumeInfo", {}).get("authors", []):
                if author not in seen:
                    seen.append(author)
        return seen


def _load_lines(path: Path) -> list:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [line.rstrip("\n") for line in f]


def _save_lines(path: Path, lines: list) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")


def get_authors_from_api(num_authors: int) -> bool:
    """
    Resolve author surnames for the first num_authors titles in titles.txt.

    Resolution order per title:
      1. Already in authors.txt → skip (no API call)
      2. Title found in KNOWN_AUTHORS table → use directly (no API call)
      3. Google Books API → query as '<title> manga' with key auth

    authors.txt is always kept the same length as titles.txt.
    titles.txt is never modified.
    Flushes to disk after every title.

    Every title logs a [N/M] line so the backend progress tracker can parse it.
    """
    titles_path  = DATA_DIR / "titles.txt"
    authors_path = DATA_DIR / "authors.txt"
    valid_path   = DATA_DIR / "valid_authors.txt"

    if not titles_path.exists():
        logger.error("titles.txt not found"); return False
    if not valid_path.exists():
        logger.error("valid_authors.txt not found"); return False

    with open(valid_path, "r", encoding="utf-8") as f:
        valid_names = {line.strip().lower() for line in f if line.strip()}

    all_titles = _load_lines(titles_path)
    if not all_titles:
        logger.error("titles.txt is empty"); return False

    # Keep authors list exactly as long as titles list
    authors = _load_lines(authors_path)
    if len(authors) != len(all_titles):
        authors = (authors + [""] * len(all_titles))[:len(all_titles)]
        logger.info(f"Resynced authors.txt to {len(authors)} entries")
        _save_lines(authors_path, authors)

    books_api   = GoogleBooksAPI()
    found_local = 0
    found_api   = 0
    failed      = 0

    for i, title in enumerate(all_titles[:num_authors]):
        # ── Always emit [N/M] so _run_script can compute a percentage ──
        prefix = f"[{i + 1}/{num_authors}]"

        if not title.strip():
            logger.info(f"{prefix} (blank title — skipped)")
            continue

        # 1. Already resolved
        if authors[i].strip():
            logger.info(f"{prefix} '{title}' → '{authors[i]}' (cached)")
            continue

        # 2. Known authors table
        known = KNOWN_AUTHORS.get(title.strip().lower())
        if known:
            authors[i] = known
            logger.info(f"{prefix} '{title}' → '{known}' (local table)")
            _save_lines(authors_path, authors)
            found_local += 1
            continue

        # 3. Google Books API
        logger.info(f"{prefix} '{title}' — querying API")
        data = books_api.get_books_data(title)

        if not data or not data.get("items"):
            logger.warning(f"  No results")
            logger.warning(f"  Inspect: {books_api.browse_link(title)}")
            authors[i] = ""
            failed += 1
        else:
            author = books_api.find_valid_author(data, valid_names)
            if author:
                surname = author.strip().split()[-1]
                authors[i] = surname
                logger.info(f"  Matched: '{author}' → '{surname}'")
                found_api += 1
            else:
                all_found = books_api.list_authors_found(data)
                logger.warning(f"  No match in valid_authors.txt")
                logger.warning(f"  Authors returned: {all_found}")
                logger.warning(f"  Inspect: {books_api.browse_link(title)}")
                authors[i] = ""
                failed += 1

        _save_lines(authors_path, authors)
        time.sleep(1.0)

    logger.info(f"Done — local: {found_local}, API: {found_api}, failed: {failed}")
    logger.info(f"authors.txt: {len(authors)} lines | titles.txt: {len(all_titles)} lines")
    return True


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')

    if len(sys.argv) != 2:
        print("Usage: python api_service.py <number_of_authors>")
        sys.exit(1)

    if get_authors_from_api(int(sys.argv[1])):
        print("Done")
    else:
        print("Failed")
        sys.exit(1)
