import pymysql
import csv
import sys
import requests
import urllib.parse
import time
import os

end_index = int(sys.argv[1])

API_URL = "https://www.googleapis.com/books/v1/volumes"
VALID_AUTHORS_PATH = "/home/ethan/Documents/School/DataStructures/Final/data/valid_authors.txt"
OUTPUT_CSV_PATH = "/home/ethan/Documents/School/DataStructures/Final/data/titles.txt"
AUTHORS_PATH = "/home/ethan/Documents/School/DataStructures/Final/data/authors.txt"

def get_books_data(title):
    try:
        query = urllib.parse.quote(title)
        params = {"q": title}
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching data for {title}: {e}")
        return {}

def find_valid_author(data, valid_names):
    for item in data.get("items", []):
        authors = item["volumeInfo"].get("authors", [])
        for author in authors:
            parts = author.strip().split()
            if parts and parts[-1].lower() in valid_names:
                return author
    return None

with open(VALID_AUTHORS_PATH, "r", encoding="utf-8") as f:
    valid_names = {line.strip().lower() for line in f}

conn = pymysql.connect(host='localhost', user='root', password='4756', db='manga')
cursor = conn.cursor()
cursor.execute("SELECT title FROM manga")
rows = cursor.fetchall()
conn.close()

titles = [row[0] for row in rows][:end_index]

authors_output = []
with open(OUTPUT_CSV_PATH, "w", newline="", encoding="utf-8") as title_file:
    writer = csv.writer(title_file)
    for title in titles:
        data = get_books_data(title)
        writer.writerow([title])

        author = find_valid_author(data, valid_names)
        authors_output.append(author.strip().split()[-1] if author else "")

        time.sleep(0.1)

# Write authors
with open(AUTHORS_PATH, "w", encoding="utf-8") as f:
    for last_name in authors_output:
        f.write(f"{last_name}\n")