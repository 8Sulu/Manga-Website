import csv
from pathlib import Path

BASE = Path(__file__).parent.parent / 'data'

with open(BASE / 'manga-backup.csv', ...) as infile:
    reader = csv.DictReader(infile)
    original_rows = list(reader)

    columns_to_remove = ['id', 'Published', 'page_url', 'image_url', 'Rank']

    fieldnames = [field for field in reader.fieldnames if field not in columns_to_remove]

    cleaned_rows = []
    for row in original_rows:
        for col in columns_to_remove:
            row.pop(col, None)
        cleaned_rows.append(row)

    cleaned_rows.sort(key=lambda x: float(x['Score']) if x['Score'] != '?' else 0, reverse=True)

    with open(BASE / 'manga.csv', ...) as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in cleaned_rows:
            writer.writerow(row)
