import csv

with open('../manga-backup.csv', mode='r', newline='', encoding='utf-8') as infile:
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

    with open('../manga.csv', mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in cleaned_rows:
            writer.writerow(row)