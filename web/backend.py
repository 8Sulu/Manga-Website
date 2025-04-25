from flask import Flask, render_template, request
import mysql.connector
import csv
import subprocess

app = Flask(__name__)

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '4756',
    'database': 'manga'
}

@app.route('/reset', methods=['POST'])
def reset_database():
    results = []

    schema_setup = """
        DROP TABLE IF EXISTS branch_availability_status;
        DROP TABLE IF EXISTS availability;
        DROP TABLE IF EXISTS branch;
        DROP TABLE IF EXISTS manga;
        DROP TABLE IF EXISTS library;

        CREATE TABLE manga (
            MangaID INT PRIMARY KEY AUTO_INCREMENT,
            Title VARCHAR(255) NOT NULL,
            `Rank` INT,
            `Type` VARCHAR(50),
            Volumes INT,
            Members INT,
            Score DECIMAL(3,2)
        );

        CREATE TABLE library (
            LibraryID INT PRIMARY KEY AUTO_INCREMENT,
            LibraryName VARCHAR(255) NOT NULL,
            `URL` VARCHAR(255) NOT NULL
        );

        CREATE TABLE branch (
            BranchID INT PRIMARY KEY AUTO_INCREMENT,
            BranchName VARCHAR(255) NOT NULL,
            `Address` VARCHAR(255),
            LibraryID INT NOT NULL,
            FOREIGN KEY (LibraryID) REFERENCES library(LibraryID) ON DELETE CASCADE
        );

        CREATE TABLE availability (
            AvailabilityID INT AUTO_INCREMENT PRIMARY KEY,
            MangaID INT NOT NULL,
            Volume INT NOT NULL,
            FOREIGN KEY (MangaID) REFERENCES manga(MangaID) ON DELETE CASCADE
        );

        CREATE TABLE branch_availability_status (
            BranchStatusID INT AUTO_INCREMENT PRIMARY KEY,
            AvailabilityID INT NOT NULL,
            BranchID INT NOT NULL,
            `Status` VARCHAR(100) NOT NULL,
            FOREIGN KEY (AvailabilityID) REFERENCES availability(AvailabilityID) ON DELETE CASCADE,
            FOREIGN KEY (BranchID) REFERENCES branch(BranchID) ON DELETE CASCADE
        );
    """

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        for command in schema_setup.strip().split(';'):
            if command.strip():
                cursor.execute(command + ';')
        conn.commit()
        results.append("Database Reset Succesfully")
    except mysql.connector.Error as err:
        return f"Error resetting schema: {err}"
    finally:
        cursor.close()
        conn.close()

    insert_operations = [
        ("../data/manga.csv", """
            INSERT INTO manga (Title, Type, Volumes, Members, Score)
            VALUES (%s, %s, %s, %s, %s)
        """),
        ("../data/libraries.csv", """
            INSERT INTO library (LibraryName, `URL`)
            VALUES (%s, %s)
        """),
        ("../data/branches.csv", """
            INSERT INTO branch (BranchName, `Address`, LibraryID)
            VALUES (%s, %s, %s)
        """),
        ("../data/availability.csv", """
            INSERT INTO availability (MangaID, Volume)
            VALUES (%s, %s)
        """),
        ("../data/branch_availability_status.csv", """
            INSERT INTO branch_availability_status (AvailabilityID, BranchID, `Status`)
            VALUES (%s, %s, %s)
        """)
    ]

    for file, query in insert_operations:
        result = insert_data(file, query)
        results.append(result)

    return "<br>".join(results)

@app.route('/', methods=['GET', 'POST'])
def home():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)

    query = """
        SELECT 
            b.BranchName,
            COUNT(DISTINCT CONCAT(a.MangaID, '-', a.Volume)) AS VolumeCount
        FROM branch b
        JOIN branch_availability_status bas ON b.BranchID = bas.BranchID
        JOIN availability a ON bas.AvailabilityID = a.AvailabilityID
        GROUP BY b.BranchName
        HAVING COUNT(DISTINCT CONCAT(a.MangaID, '-', a.Volume)) > 10
        ORDER BY VolumeCount DESC
    """

    cursor.execute(query)
    manga_per_library = cursor.fetchall()

    if request.method == 'POST':
        action = request.form.get('action')
        title = request.form.get('title')
        volume = request.form.get('volume')

        if action == 'update':
            update_query = """
                UPDATE manga
                SET Volumes = %s
                WHERE Title = %s
            """
            cursor = conn.cursor()
            cursor.execute(update_query, (volume, title))
            conn.commit()

        elif action == 'delete':
            delete_query = """
                DELETE FROM availability
                WHERE MangaID = (SELECT MangaID FROM manga WHERE Title = %s)
                AND Volume = %s
            """
            cursor = conn.cursor()
            cursor.execute(delete_query, (title, volume))
            conn.commit()

        elif action == 'scrape':
            print("Running scraper...")
            books = request.form.get('books')
            subprocess.run(["python3","/home/ethan/Documents/School/DataStructures/Final/data/get_data/scrapper.py",books])

        elif action == 'get_authors':
            print("Running author fetcher...")
            authors = request.form.get('authors')
            subprocess.run(["python3","/home/ethan/Documents/School/DataStructures/Final/data/get_data/get_authors.py",authors])

    cursor.close()
    conn.close()

    return render_template('index.html', manga_per_library=manga_per_library)

@app.route('/search')
def search():
    title = request.args.get('title', '')
    type_ = request.args.get('type', '')
    branch = request.args.get('branch', '')
    volume = request.args.get('volume', '')
    status = request.args.get('status', '')
    only_available = request.args.get('only_available') == '1'

    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)

    query = """
        SELECT 
            m.Title, a.Volume, m.Volumes, m.Type, m.Members, m.Score,
            b.BranchName, bas.Status
        FROM manga m
        LEFT JOIN availability a ON m.MangaID = a.MangaID
        LEFT JOIN branch_availability_status bas ON a.AvailabilityID = bas.AvailabilityID
        LEFT JOIN branch b ON bas.BranchID = b.BranchID
        WHERE 1=1
    """
    params = []

    if title:
        query += " AND m.Title LIKE %s"
        params.append(f"%{title}%")

    if type_:
        query += " AND m.Type = %s"
        params.append(type_)

    if branch:
        if branch == "N/A":
            query += " AND b.BranchName IS NULL"
        else:
            query += " AND b.BranchName = %s"
            params.append(branch)

    if volume:
        query += " AND a.Volume = %s"
        params.append(volume)
    if status:
        if status == "N/A":
            query += " AND bas.Status IS NULL"
        else:
            query += " AND bas.Status = %s"
            params.append(status)

    query += " ORDER BY m.Score DESC"

    cursor.execute(query, params)
    results = cursor.fetchall()

    if only_available:
        results = [row for row in results if row.get('Status') not in ['Checked Out']]

    results = [row for row in results if row.get('BranchName') not in [None, '', 'N/A']]
    count = len(results)
    
    status_summary = {}
    for row in results:
        status = row.get('Status') or 'N/A'
        if status in status_summary:
            status_summary[status] += 1
        else:
            status_summary[status] = 1

    cursor.close()
    conn.close()

    return render_template('results.html', results=results, count=count, status_summary=status_summary)

def insert_data(filepath, query):
    try:
        with open(filepath, "r", encoding="utf-8") as file:
            csv_reader = csv.reader(file)
            headers = next(csv_reader)

            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor()

            for row in csv_reader:
                row = [None if val.strip().upper() == "NULL" or val.strip() == "" else val for val in row]
                cursor.execute(query, tuple(row))

            conn.commit()
            return f"{filepath} Data inserted successfully!"
    except mysql.connector.Error as err:
        return f"{filepath} — Error: {err}"
    except Exception as e:
        return f"{filepath} — Error reading file: {e}"
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

if __name__ == '__main__':
    app.run(debug=True)
