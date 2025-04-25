import re
import sys
import csv
import time
from selenium import webdriver
from selenium.webdriver.common.by import By

driver = webdriver.Chrome()
driver.maximize_window()
books_list = []

def get_book(book_number, title_number, availability_id):
    title = driver.find_element(By.ID, f"detailLink{book_number}").text
    author = driver.find_element(By.XPATH, '//div[contains(@class, "INITIAL_AUTHOR_SRCH")]/div[@class="displayElementText text-p highlightMe INITIAL_AUTHOR_SRCH"]/span/a').text

    if re.findall(r"\d+", title):
        volume = re.findall(r"\d+", title)
        number_index = title.find(volume[0])
        title = title[:number_index]
    else:
        volume = [0]

    table = driver.find_element(By.ID, f"detailItemTableCust{book_number}")
    rows = table.find_elements(By.TAG_NAME, "tr")

    data = []
    for row in rows:
        cells = row.find_elements(By.TAG_NAME, "td")
        if cells:
            first_col = cells[0].text
            last_col = cells[-1].text
            data.append((first_col, last_col))

    branch = [entry[0] for entry in data]
    status = [entry[1] for entry in data]

    book_data = {
        "title": title,
        "author": author,
        "manga_id": title_number,
        "volume": int(volume[0]) if volume else None,
        "status": status,
        "branch": branch,
        "availability_id": availability_id,
    }
    books_list.append(book_data)

n = int(sys.argv[1])

with open("/home/ethan/Documents/School/DataStructures/Final/data/titles.txt", "r", encoding="utf-8") as titles_file, \
     open("/home/ethan/Documents/School/DataStructures/Final/data/authors.txt", "r", encoding="utf-8") as authors_file:
    
    title_number = 1
    availability_id = 1

    for index,(title,author) in enumerate(zip(titles_file,authors_file)):
        if index == n:
            break
        if author == "\n":
            title_number += 1
            continue    
        
        driver.get(f"https://lcpl.ent.sirsi.net/client/en_US/lcpl/search/results?qu=&qu=TITLE%3D{title}+&qu=AUTHOR%3D{author}+&te=ILS&lm=ON_ORDER+%7C%7C+BOOKS&h=1")
        time.sleep(6) 

        page_number = 0

        while True:
            book_number = page_number * 12
            
            while True:
                try:
                    get_book(book_number, title_number, availability_id)
                    book_number += 1
                    availability_id += 1
                except:
                    break

            try:
                next_page = driver.find_element(By.ID, "NextPagetop")
                next_page.click()
                time.sleep(6)
            except:
                break

            page_number += 1

        title_number += 1

with open("/home/ethan/Documents/School/DataStructures/Final/data/availability.csv", mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=["MangaID", "Volume"])
    writer.writeheader()

    for book in books_list:
        writer.writerow({
            "MangaID": book["manga_id"],
            "Volume": book["volume"],
        })

branch_name_to_id = {
    "NORTHEAST": 1,
    "BLPERRY": 2,
    "EASTSIDE": 3,
    "FTBRADEN": 4,
    "LAKEJAX": 5,
    "MAIN": 6,
    "WOODVILLE": 7
}

with open("/home/ethan/Documents/School/DataStructures/Final/data/branch_availability_status.csv", mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=["AvailabilityID", "BranchID", "Status"])
    writer.writeheader()

    for book in books_list:
        for i in range(len(book["branch"])):
            branch_name = book["branch"][i].strip().upper()
            branch_id = branch_name_to_id.get(branch_name, -1)

            if branch_id == -1:
                print(f"Unknown branch name: {branch_name}")

            writer.writerow({
                "AvailabilityID": book["availability_id"],
                "BranchID": branch_id,
                "Status": book["status"][i],
            })

driver.quit()
