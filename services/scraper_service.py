import re
import sys
import time
import csv
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from typing import List, Dict, Optional
import logging

from config.settings import LIBRARY_BASE_URL, DATA_DIR, BRANCH_MAPPING, SCRAPE_DELAY, MAX_RETRIES, REQUEST_TIMEOUT

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LibraryScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.books_list = []

    def search_library(self, title: str, author: str = "") -> Optional[BeautifulSoup]:
        """Search the library catalog for a title and author"""
        search_url = f"{LIBRARY_BASE_URL}/search/results"
        params = {
            'qu': title,  # Simplified query - just title for now
            'te': 'ILS',
            'lm': 'BOOKS',  # Focus on books only
            'h': '1'
        }
        
        full_url = f"{search_url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}"
        logger.info(f"Searching library for: '{title}' by '{author}'")
        logger.debug(f"Full URL: {full_url}")
        
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Attempt {attempt + 1}/{MAX_RETRIES} for {title}")
                response = self.session.get(search_url, params=params, timeout=REQUEST_TIMEOUT)
                logger.info(f"Response status: {response.status_code}")
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Look for the new structure - results are loaded via JavaScript
                    # Check for search results container
                    results_container = soup.find(id='searchResultsColumn')
                    if results_container:
                        logger.info("Found search results container")
                        
                        # Look for any result items
                        result_cells = soup.find_all(class_='cell_wrapper')
                        logger.info(f"Found {len(result_cells)} result cells")
                        
                        if len(result_cells) == 0:
                            # Check for "no results" message
                            no_results = soup.find(text=lambda t: t and ('no results' in t.lower() or 'found' in t.lower()))
                            if no_results:
                                logger.warning(f"No results found: {no_results.strip()}")
                            else:
                                logger.warning("No result cells found - may need JavaScript to load")
                        
                        return soup
                    else:
                        # Fallback to old structure check
                        detail_links = soup.find_all(id=lambda x: x and x.startswith('detailLink'))
                        logger.info(f"Found {len(detail_links)} detail links (old structure)")
                        
                        if len(detail_links) == 0:
                            logger.warning("No results found - structure may have changed")
                        
                        return soup
                else:
                    logger.error(f"HTTP {response.status_code}: {response.text[:200]}")
                    
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {title}: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(SCRAPE_DELAY * (attempt + 1))
                else:
                    logger.error(f"Failed to search for {title} after {MAX_RETRIES} attempts")
                    return None

    def extract_book_data(self, soup: BeautifulSoup, book_number: int, title_number: int, availability_id: int) -> Optional[Dict]:
        """Extract book data from search results page"""
        try:
            logger.debug(f"Extracting book data for book_number: {book_number}")
            
            # Try new structure first - look for result cells
            result_cells = soup.find_all(class_='cell_wrapper')
            if book_number < len(result_cells):
                cell = result_cells[book_number]
                logger.debug(f"Processing result cell {book_number}")
                
                # Find title in new structure
                title_element = cell.find(class_='discoveryTitle') or cell.find('a', class_='titleLink')
                if title_element:
                    title = title_element.get_text(strip=True)
                    logger.debug(f"Found title: '{title}'")
                else:
                    logger.debug(f"No title found in result cell {book_number}")
                    return None
                
                # Extract volume number from title
                volume_match = re.findall(r"\d+", title)
                if volume_match:
                    volume = int(volume_match[0])
                    number_index = title.find(volume_match[0])
                    clean_title = title[:number_index].strip()
                    logger.debug(f"Extracted volume: {volume}, clean title: '{clean_title}'")
                else:
                    volume = 0
                    clean_title = title
                    logger.debug("No volume found in title")
                
                # Find author in new structure
                author = ""
                author_selectors = [
                    '.INITIAL_AUTHOR_SRCH .displayElementText',
                    '.author',
                    '[class*="author"]'
                ]
                
                for selector in author_selectors:
                    author_element = cell.select_one(selector)
                    if author_element:
                        author = author_element.get_text(strip=True)
                        logger.debug(f"Found author with selector '{selector}': '{author}'")
                        break
                
                # For now, create a basic result without availability data
                # The new structure loads availability via JavaScript
                result = {
                    "title": clean_title,
                    "author": author,
                    "manga_id": title_number,
                    "volume": volume,
                    "status": ["Available"],  # Default status
                    "branch": ["Main Library"],  # Default branch
                    "availability_id": availability_id,
                }
                
                logger.info(f"Successfully extracted basic book data: {result['title']} by {result['author']}")
                return result
            
            # Fallback to old structure
            title_element = soup.find(id=f"detailLink{book_number}")
            if not title_element:
                logger.debug(f"No title element found with id 'detailLink{book_number}'")
                return None
            
            title = title_element.get_text(strip=True)
            logger.debug(f"Found title: '{title}'")
            
            # Extract volume number from title
            volume_match = re.findall(r"\d+", title)
            if volume_match:
                volume = int(volume_match[0])
                number_index = title.find(volume_match[0])
                clean_title = title[:number_index].strip()
                logger.debug(f"Extracted volume: {volume}, clean title: '{clean_title}'")
            else:
                volume = 0
                clean_title = title
                logger.debug("No volume found in title")
            
            # Find author (try multiple selectors)
            author = ""
            author_selectors = [
                '.INITIAL_AUTHOR_SRCH .displayElementText span a',
                '.author span a',
                '[class*="author"] span a'
            ]
            
            for selector in author_selectors:
                author_element = soup.select_one(selector)
                if author_element:
                    author = author_element.get_text(strip=True)
                    logger.debug(f"Found author with selector '{selector}': '{author}'")
                    break
            
            # Find availability table
            table = soup.find(id=f"detailItemTableCust{book_number}")
            if not table:
                logger.debug(f"No availability table found with id 'detailItemTableCust{book_number}'")
                # Still return basic info without availability
                result = {
                    "title": clean_title,
                    "author": author,
                    "manga_id": title_number,
                    "volume": volume,
                    "status": ["Unknown"],
                    "branch": ["Unknown"],
                    "availability_id": availability_id,
                }
                logger.info(f"Extracted basic book data (no availability): {result['title']} by {result['author']}")
                return result
            
            logger.debug(f"Found availability table with {len(table.find_all('tr'))} rows")
            
            # Extract branch and status data
            branches = []
            statuses = []
            rows = table.find_all('tr')
            
            for i, row in enumerate(rows):
                cells = row.find_all('td')
                if len(cells) >= 2:
                    branch = cells[0].get_text(strip=True)
                    status = cells[-1].get_text(strip=True)
                    branches.append(branch)
                    statuses.append(status)
                    logger.debug(f"Row {i}: Branch='{branch}', Status='{status}'")
            
            result = {
                "title": clean_title,
                "author": author,
                "manga_id": title_number,
                "volume": volume,
                "status": statuses,
                "branch": branches,
                "availability_id": availability_id,
            }
            
            logger.info(f"Successfully extracted book data: {result['title']} by {result['author']}")
            return result
            
        except Exception as e:
            logger.error(f"Error extracting book data for book {book_number}: {e}")
            return None

    def scrape_titles(self, num_titles: int) -> bool:
        """Scrape library for specified number of titles"""
        try:
            titles_file = DATA_DIR / "titles.txt"
            authors_file = DATA_DIR / "authors.txt"
            
            if not titles_file.exists() or not authors_file.exists():
                logger.error("titles.txt or authors.txt not found")
                return False
            
            with open(titles_file, "r", encoding="utf-8") as tf, \
                 open(authors_file, "r", encoding="utf-8") as af:
                
                titles = [line.strip() for line in tf.readlines()]
                authors = [line.strip() for line in af.readlines()]
                
                title_number = 1
                availability_id = 1
                
                for i, (title, author) in enumerate(zip(titles, authors)):
                    if i >= num_titles:
                        break
                    
                    if not author.strip():
                        title_number += 1
                        continue
                    
                    logger.info(f"Processing {i+1}/{num_titles}: {title} by {author}")
                    
                    # Search library
                    soup = self.search_library(title, author)
                    if not soup:
                        logger.warning(f"No search results for '{title}' by '{author}', skipping")
                        title_number += 1
                        continue
                    
                    # Process all books found for this title
                    page_number = 0
                    books_found_for_title = 0
                    while True:
                        book_number = page_number * 12
                        books_on_page = 0
                        
                        # Extract books on current page
                        while True:
                            book_data = self.extract_book_data(soup, book_number, title_number, availability_id)
                            if book_data:
                                self.books_list.append(book_data)
                                book_number += 1
                                availability_id += 1
                                books_on_page += 1
                                books_found_for_title += 1
                            else:
                                break
                        
                        logger.info(f"Page {page_number}: Found {books_on_page} books for '{title}'")
                        
                        if books_on_page == 0:
                            break
                        
                        # Try to go to next page
                        next_link = soup.find(id="NextPagetop")
                        if next_link and next_link.get('href'):
                            next_url = LIBRARY_BASE_URL + next_link['href']
                            try:
                                response = self.session.get(next_url, timeout=REQUEST_TIMEOUT)
                                response.raise_for_status()
                                soup = BeautifulSoup(response.content, 'html.parser')
                                page_number += 1
                                time.sleep(SCRAPE_DELAY)
                            except requests.RequestException as e:
                                logger.warning(f"Failed to load next page: {e}")
                                break
                        else:
                            break
                    
                    logger.info(f"Total books found for '{title}': {books_found_for_title}")
                    title_number += 1
                    time.sleep(SCRAPE_DELAY)
            
            return True
            
        except Exception as e:
            logger.error(f"Error during scraping: {e}")
            return False

    def save_to_csv(self) -> None:
        """Save scraped data to CSV files"""
        # Save availability data
        with open(DATA_DIR / "availability.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["MangaID", "Volume"])
            writer.writeheader()
            for book in self.books_list:
                writer.writerow({
                    "MangaID": book["manga_id"],
                    "Volume": book["volume"]
                })
        
        # Save branch availability status
        with open(DATA_DIR / "branch_availability_status.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["AvailabilityID", "BranchID", "Status"])
            writer.writeheader()
            
            for book in self.books_list:
                for i, branch_name in enumerate(book["branch"]):
                    branch_clean = branch_name.strip().upper()
                    branch_id = BRANCH_MAPPING.get(branch_clean, -1)
                    
                    if branch_id == -1:
                        logger.warning(f"Unknown branch name: {branch_clean}")
                        continue
                    
                    if i < len(book["status"]):
                        writer.writerow({
                            "AvailabilityID": book["availability_id"],
                            "BranchID": branch_id,
                            "Status": book["status"][i]
                        })

def main():
    if len(sys.argv) != 2:
        print("Usage: python scraper_service.py <number_of_titles>")
        sys.exit(1)
    
    num_titles = int(sys.argv[1])
    scraper = LibraryScraper()
    
    if scraper.scrape_titles(num_titles):
        scraper.save_to_csv()
        logger.info(f"Successfully scraped {len(scraper.books_list)} books")
    else:
        logger.error("Scraping failed")
        sys.exit(1)

if __name__ == "__main__":
    import sys
    main()
