import sys
import csv
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from config.settings import DATA_DIR


def get_titles(num_titles: int) -> bool:
    manga_path  = DATA_DIR / "manga.csv"
    titles_path = DATA_DIR / "titles.txt"

    if not manga_path.exists():
        print(f"Error: {manga_path} not found")
        return False

    with open(manga_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        titles = [row["Title"].strip() for row in reader if row["Title"].strip()]

    titles = titles[:num_titles]

    with open(titles_path, "w", encoding="utf-8") as f:
        for title in titles:
            f.write(title + "\n")

    print(f"Wrote {len(titles)} titles to {titles_path}")
    return True


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python get_titles.py <number_of_titles>")
        sys.exit(1)

    if not get_titles(int(sys.argv[1])):
        sys.exit(1)
