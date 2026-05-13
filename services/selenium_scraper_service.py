#!/usr/bin/env python3
import time
import re
import logging
from typing import Dict, Optional, List
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup

from config.settings import DATA_DIR, SCRAPE_DELAY, MAX_RETRIES, REQUEST_TIMEOUT, LIBRARY_BASE_URL

logger = logging.getLogger(__name__)

class SeleniumScraperService:
    """Service for scraping library catalog using Selenium for JavaScript-heavy sites"""
    
    def __init__(self):
        self.books_list = []
        self.driver = None
        self._setup_driver()
    
    def _setup_driver(self):
        """Setup Selenium WebDriver with appropriate options"""
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless')  # Run in background
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(REQUEST_TIMEOUT)
            logger.info("Selenium WebDriver initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Selenium WebDriver: {e}")
            raise

    def search_library(self, title: str, author: str = "") -> Optional[BeautifulSoup]:
        """Search the library catalog for a title and author using Selenium"""
        search_url = f"{LIBRARY_BASE_URL}/search/results"
        params = {
            'qu': title,  # Simple title search
            'te': 'ILS',
            'lm': 'BOOKS',
            'h': '1'
        }
        
        full_url = f"{search_url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}"
        logger.info(f"Searching library for: '{title}' by '{author}'")
        logger.debug(f"Full URL: {full_url}")
        
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Attempt {attempt + 1}/{MAX_RETRIES} for {title}")
                
                # Navigate to the search page
                self.driver.get(full_url)
                
                # Wait for page to load and results to appear
                wait = WebDriverWait(self.driver, REQUEST_TIMEOUT)
                
                # Wait for either results container or "no results" message
                try:
                    wait.until(EC.presence_of_element_located((By.ID, 'searchResultsColumn')))
                    logger.info("Found search results container")
                except TimeoutException:
                    # Check for no results message
                    try:
                        no_results = self.driver.find_element(By.XPATH, "//*[contains(text(), 'no results' ) or contains(text(), 'found')]")
                        logger.warning(f"No results found: {no_results.text}")
                        return None
                    except NoSuchElementException:
                        logger.warning("No results container found - structure may have changed")
                        return None
                
                # Get page source and parse with BeautifulSoup
                page_source = self.driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                
                # Check for result cells
                result_cells = soup.find_all(class_='cell_wrapper')
                logger.info(f"Found {len(result_cells)} result cells")
                
                if len(result_cells) == 0:
                    # Fallback to old structure
                    detail_links = soup.find_all(id=lambda x: x and x.startswith('detailLink'))
                    logger.info(f"Found {len(detail_links)} detail links (old structure)")
                    
                    if len(detail_links) == 0:
                        logger.warning("No results found on page")
                        return None
                
                return soup
                
            except Exception as e:
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
            
            # Try new structure first
            result_cells = soup.find_all(class_='cell_wrapper')
            if book_number < len(result_cells):
                cell = result_cells[book_number]
                logger.debug(f"Processing result cell {book_number}")
                
                # Find title in new structure
                title_element = cell.find(class_='discoveryTitle') or cell.find('a', class_='titleLink')
                if not title_element:
                    # Try other selectors
                    title_element = cell.find('a') or cell.find('h3') or cell.find('h2')
                
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
                
                # Find author
                author = ""
                author_selectors = [
                    '.INITIAL_AUTHOR_SRCH .displayElementText',
                    '.author',
                    '[class*="author"]',
                    'span:contains("by")',
                    'div:contains("by")'
                ]
                
                for selector in author_selectors:
                    try:
                        author_element = cell.select_one(selector)
                        if author_element:
                            author = author_element.get_text(strip=True)
                            # Clean up author text
                            if 'by ' in author.lower():
                                author = author.replace('by ', '').replace('By ', '').strip()
                            logger.debug(f"Found author with selector '{selector}': '{author}'")
                            break
                    except:
                        continue
                
                # Try to find availability data
                branches = []
                statuses = []
                
                # Look for summary items container (loaded by JavaScript)
                summary_items = soup.find_all(class_='summaryitems')
                if summary_items and book_number < len(summary_items):
                    summary = summary_items[book_number]
                    tables = summary.find_all('table')
                    if tables:
                        table = tables[0]
                        rows = table.find_all('tr')[1:]  # Skip header row
                        
                        for row in rows:
                            cells = row.find_all('td')
                            if len(cells) >= 2:
                                branch = cells[0].get_text(strip=True)
                                status = cells[-1].get_text(strip=True)
                                branches.append(branch)
                                statuses.append(status)
                                logger.debug(f"Row: Branch='{branch}', Status='{status}'")
                
                # If no availability found, use defaults
                if not branches:
                    branches = ["Main Library"]
                    statuses = ["Available"]
                    logger.info("Using default availability (JavaScript data not loaded)")
                
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
            
            # Find author
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
        """Scrape library for specified number of titles using Selenium"""
        try:
            titles_file = DATA_DIR / "titles.txt"
            authors_file = DATA_DIR / "authors.txt"
            
            # Read titles and authors
            with open(titles_file, "r", encoding="utf-8") as f:
                titles = [line.strip() for line in f.readlines() if line.strip()]
            
            with open(authors_file, "r", encoding="utf-8") as f:
                authors = [line.strip() for line in f.readlines() if line.strip()]
            
            if len(titles) < num_titles:
                num_titles = len(titles)
            
            logger.info(f"Starting to scrape {num_titles} titles")
            
            title_number = 0
            availability_id = 0
            
            for i in range(num_titles):
                if i >= len(titles):
                    break
                
                title = titles[i]
                author = authors[i] if i < len(authors) else ""
                
                if not title.strip():
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
                books_found_for_title = 0
                
                # Extract books (try up to 12 per title)
                for book_number in range(12):
                    book_data = self.extract_book_data(soup, book_number, title_number, availability_id)
                    if book_data:
                        self.books_list.append(book_data)
                        availability_id += 1
                        books_found_for_title += 1
                    else:
                        break
                
                logger.info(f"Total books found for '{title}': {books_found_for_title}")
                title_number += 1
                time.sleep(SCRAPE_DELAY)
            
            return True
            
        except Exception as e:
            logger.error(f"Error during scraping: {e}")
            return False
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("Selenium WebDriver closed")

    def save_to_csv(self) -> None:
        """Save scraped data to CSV file"""
        if not self.books_list:
            logger.warning("No books to save")
            return
        
        csv_path = DATA_DIR / "scraped_data.csv"
        
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            f.write("Title,Author,Volume,Branch,Status\n")
            
            for book in self.books_list:
                for branch, status in zip(book["branch"], book["status"]):
                    f.write(f'"{book["title"]}","{book["author"]}",{book["volume"]},"{branch}","{status}"\n')
        
        logger.info(f"Saved {len(self.books_list)} books to {csv_path}")

    def close(self):
        """Close the Selenium WebDriver"""
        if self.driver:
            self.driver.quit()
            logger.info("Selenium WebDriver closed")
