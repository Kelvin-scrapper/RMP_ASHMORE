"""
Ashmore RMP Data Scraper - Playwright Version
Automated web scraping script for extracting fund data from Ashmore
with comprehensive logging and error handling.
"""

import os
import logging
import time
import json
import re
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import pandas as pd
import requests

# ============================================================================
# CONFIGURATION
# ============================================================================
CONFIG = {
    'headless': True,          # Set to True for headless mode
    'browser': 'chrome',      # Options: 'chromium', 'firefox', 'webkit'
    'timeout': 30000,           # Page load timeout in ms
    'viewport': {'width': 1920, 'height': 1080}
}

# ============================================================================
# DIRECTORY SETUP
# ============================================================================
def setup_directories():
    """Create necessary directories for downloads and logs"""
    directories = ['downloads', 'logs']
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"[OK] Directory ensured: {directory}/")
    return True

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
def setup_logging():
    """Configure comprehensive logging"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"logs/scraper_{timestamp}.log"

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

    logging.info("=" * 80)
    logging.info("ASHMORE RMP DATA SCRAPER - PLAYWRIGHT VERSION - SESSION START")
    logging.info("=" * 80)
    logging.info(f"Log file created: {log_filename}")
    logging.info(f"Session timestamp: {timestamp}")

    return timestamp

# ============================================================================
# TABLE EXTRACTION FUNCTIONS
# ============================================================================
def extract_table_data(page, table_element, table_name):
    """Extract data from a table element including tfoot rows"""
    start_time = time.time()
    logging.info(f"Extracting data from table element: {table_name}")

    try:
        headers = []
        rows_data = []

        # Get headers
        header_elements = table_element.query_selector_all("thead th, tr:first-child th")
        if not header_elements:
            header_elements = table_element.query_selector_all("tr:first-child td")

        for header in header_elements:
            headers.append(header.inner_text().strip())

        # Get body rows
        row_elements = table_element.query_selector_all("tbody tr")
        if not row_elements:
            all_rows = table_element.query_selector_all("tr")
            row_elements = all_rows[1:] if len(all_rows) > 1 else all_rows

        for row in row_elements:
            cells = row.query_selector_all("td")
            if not cells:
                cells = row.query_selector_all("th")

            if cells:
                row_data = [cell.inner_text().strip() for cell in cells]
                if any(row_data):
                    rows_data.append(row_data)

        # Get tfoot rows (for Total)
        tfoot_rows = table_element.query_selector_all("tfoot tr")
        if tfoot_rows:
            logging.debug(f"Found {len(tfoot_rows)} rows in tfoot")
            for row in tfoot_rows:
                cells = row.query_selector_all("td")
                if not cells:
                    cells = row.query_selector_all("th")

                if cells:
                    row_data = [cell.inner_text().strip() for cell in cells]
                    if any(row_data):
                        rows_data.append(row_data)

        elapsed = time.time() - start_time
        logging.info(f"[OK] Extracted {len(rows_data)} rows from '{table_name}' in {elapsed:.2f}s")

        # Create DataFrame
        if headers:
            df = pd.DataFrame(rows_data, columns=headers)
        else:
            df = pd.DataFrame(rows_data)

        return df

    except Exception as e:
        logging.error(f"[FAIL] Failed to extract '{table_name}': {str(e)}")
        return None

def find_table_by_keywords(page, keywords, table_name):
    """Find a table by searching for header keywords"""
    logging.info(f"Searching for table: {table_name}")

    try:
        tables = page.query_selector_all("table.ash-xml-factsheet-data__grid")
        logging.info(f"Found {len(tables)} total factsheet tables on page")

        for idx, table in enumerate(tables, 1):
            try:
                headers = table.query_selector_all("thead th, tr:first-child th")
                header_text = " ".join([h.inner_text().strip().lower() for h in headers])

                for keyword in keywords:
                    if keyword.lower() in header_text:
                        logging.info(f"[OK] Found matching table for '{table_name}' (Table #{idx})")
                        return table
            except:
                continue

        logging.warning(f"[WARN] Could not find table matching: {table_name}")
        return None

    except Exception as e:
        logging.error(f"Error finding table: {str(e)}")
        return None

# ============================================================================
# DATE EXTRACTION
# ============================================================================
def extract_fund_date(page):
    """Extract fund update date from the page"""
    try:
        month_map = {
            'jan': '01', 'january': '01',
            'feb': '02', 'february': '02',
            'mar': '03', 'march': '03',
            'apr': '04', 'april': '04',
            'may': '05',
            'jun': '06', 'june': '06',
            'jul': '07', 'july': '07',
            'aug': '08', 'august': '08',
            'sep': '09', 'september': '09',
            'oct': '10', 'october': '10',
            'nov': '11', 'november': '11',
            'dec': '12', 'december': '12'
        }

        # Strategy 1: Find FUND UPDATE element
        try:
            fund_update_elements = page.query_selector_all("//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'fund update')]")
            logging.info(f"Found {len(fund_update_elements)} elements with 'FUND UPDATE' text")

            for elem in fund_update_elements:
                try:
                    parent = elem.query_selector("xpath=./ancestor::*[1]")
                    if parent:
                        parent_text = parent.inner_text().lower()
                        logging.info(f"Checking parent text: {parent_text[:200]}")

                        for month_name, month_num in month_map.items():
                            pattern = rf'{month_name}\s+(\d{{4}})'
                            match = re.search(pattern, parent_text)
                            if match:
                                year = match.group(1)
                                if 2020 <= int(year) <= 2030:
                                    date_str = f"{year}-{month_num}"
                                    logging.info(f"[OK] Extracted fund date: {date_str} ({month_name.title()} {year})")
                                    return date_str
                except:
                    continue
        except:
            pass

        # Strategy 2: Search page content
        page_content = page.content().lower()

        fund_update_positions = [m.start() for m in re.finditer(r'fund update', page_content)]
        for pos in fund_update_positions:
            context = page_content[pos:pos+200]
            for month_name, month_num in month_map.items():
                pattern = rf'{month_name}\s+(\d{{4}})'
                match = re.search(pattern, context)
                if match:
                    year = match.group(1)
                    if 2020 <= int(year) <= 2030:
                        date_str = f"{year}-{month_num}"
                        logging.info(f"[OK] Extracted fund date near FUND UPDATE: {date_str}")
                        return date_str

        # Fallback
        fallback_date = datetime.now().strftime('%Y-%m')
        logging.warning(f"Could not extract fund date, using fallback: {fallback_date}")
        return fallback_date

    except Exception as e:
        logging.error(f"Error extracting fund date: {str(e)}")
        return datetime.now().strftime('%Y-%m')

# ============================================================================
# MAIN SCRAPING FUNCTION
# ============================================================================
def scrape_ashmore_data():
    """Main scraping function using Playwright"""

    print("=" * 80)
    print("ASHMORE RMP DATA SCRAPER - PLAYWRIGHT VERSION")
    print("=" * 80)
    print()

    # Setup
    setup_directories()
    timestamp = setup_logging()

    logging.info("=" * 80)
    logging.info("STARTING SCRAPING WORKFLOW")
    logging.info("=" * 80)

    start_time = time.time()
    scraped_data = {}
    errors_encountered = []
    fund_date = None

    with sync_playwright() as p:
        try:
            # Step 1: Launch browser
            browser_type = CONFIG.get('browser', 'chromium')
            headless = CONFIG.get('headless', False)
            logging.info(f"\n[STEP 1/10] Launching Playwright browser ({browser_type}, headless={headless})")

            # Select browser engine
            if browser_type == 'firefox':
                browser_engine = p.firefox
            elif browser_type == 'webkit':
                browser_engine = p.webkit
            else:
                browser_engine = p.chromium

            browser = browser_engine.launch(
                headless=headless,
                args=['--start-maximized'] if browser_type == 'chromium' else []
            )
            context = browser.new_context(
                viewport=CONFIG.get('viewport', {'width': 1920, 'height': 1080}),
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            page = context.new_page()
            logging.info(f"[OK] {browser_type.title()} browser launched successfully")

            # Step 2: Navigate to fund page
            logging.info("\n[STEP 2/10] Navigating to Ashmore fund page")
            fund_url = "https://www.ashmoregroup.com/en-gb/our-funds/sicav-lcbf-ins-inc-usd"
            page.goto(fund_url, wait_until='networkidle', timeout=CONFIG.get('timeout', 30000))
            logging.info(f"[OK] Loaded {fund_url}")
            page.screenshot(path=f"logs/01_initial_load_{timestamp}.png")

            # Step 3: Handle cookie consent
            logging.info("\n[STEP 3/10] Handling cookie consent")
            try:
                cookie_btn = page.wait_for_selector("button:has-text('Accept'), button:has-text('accept')", timeout=5000)
                if cookie_btn:
                    cookie_btn.click()
                    logging.info("[OK] Cookie consent accepted")
                    page.wait_for_timeout(1000)
            except:
                logging.info("No cookie consent found or already accepted")

            # Step 4: Handle Audience Confirmation modal
            logging.info("\n[STEP 4/10] Handling Audience Confirmation modal")
            try:
                # Wait for modal to appear
                page.wait_for_timeout(2000)

                # Step 1/4: Select United Kingdom
                logging.info("Modal Step 1/4: Selecting United Kingdom")
                uk_link = page.wait_for_selector("text=United Kingdom", timeout=10000)
                if uk_link:
                    uk_link.click()
                    logging.info("[OK] Selected United Kingdom")
                    page.wait_for_timeout(2000)

                # Step 2/4: Select Intermediaries
                logging.info("Modal Step 2/4: Selecting Intermediaries")
                try:
                    intermediaries = page.wait_for_selector("text=Intermediaries", timeout=5000)
                    if intermediaries:
                        intermediaries.click()
                        logging.info("[OK] Selected Intermediaries")
                        page.wait_for_timeout(2000)
                except:
                    logging.info("No Intermediaries step found")

                # Step 3/4: Accept terms
                logging.info("Modal Step 3/4: Accepting terms")
                try:
                    accept_btn = page.wait_for_selector("button:has-text('I Agree'), button:has-text('Accept'), button:has-text('Confirm')", timeout=5000)
                    if accept_btn:
                        accept_btn.click()
                        logging.info("[OK] Accepted terms")
                        page.wait_for_timeout(2000)
                except:
                    logging.info("No accept button found")

                logging.info("[OK] Modal handling completed")
            except Exception as e:
                logging.warning(f"Modal handling issue: {str(e)}")

            page.screenshot(path=f"logs/02_after_modal_{timestamp}.png")

            # Step 5: Click Factsheet tab
            logging.info("\n[STEP 5/10] Clicking Factsheet tab")
            try:
                factsheet_tab = page.wait_for_selector("text=Factsheet", timeout=10000)
                if factsheet_tab:
                    factsheet_tab.click()
                    logging.info("[OK] Clicked Factsheet tab")
                    page.wait_for_timeout(3000)
            except:
                logging.info("Factsheet tab not found or already selected")

            page.screenshot(path=f"logs/03_factsheet_tab_{timestamp}.png")

            # Extract fund date
            fund_date = extract_fund_date(page)
            logging.info(f"[INFO] Fund date extracted: {fund_date}")

            # Scroll down to load all content
            logging.info("Scrolling to load all table content...")
            for i in range(5):
                page.evaluate("window.scrollBy(0, 500)")
                page.wait_for_timeout(500)

            # Scroll back to top
            page.evaluate("window.scrollTo(0, 0)")
            page.wait_for_timeout(1000)

            # Step 6: Extract Currency Exposure
            logging.info("\n[STEP 6/10] Extracting Top 10 EM Currency Exposure")
            currency_keywords = ["Top 10 EM currency exposure %", "Top 10 EM currency exposure", "currency exposure"]
            currency_table = find_table_by_keywords(page, currency_keywords, "Top 10 EM Currency Exposure")

            if currency_table:
                # Scroll table into view
                currency_table.scroll_into_view_if_needed()
                page.wait_for_timeout(500)

                currency_df = extract_table_data(page, currency_table, "Top 10 EM Currency Exposure")
                if currency_df is not None and len(currency_df) > 0:
                    scraped_data['em_currency_exposure'] = currency_df
                    logging.info(f"Data preview:\n{currency_df.head()}")
                else:
                    errors_encountered.append("Failed to extract EM Currency Exposure")
            else:
                errors_encountered.append("Failed to find EM Currency Exposure table")

            # Step 7: Extract Country Exposure
            logging.info("\n[STEP 7/10] Extracting Top 10 Country Exposure")
            country_keywords = ["Top 10 country exposure %", "Top 10 country exposure", "country exposure"]
            country_table = find_table_by_keywords(page, country_keywords, "Top 10 Country Exposure")

            if country_table:
                # Scroll table into view
                country_table.scroll_into_view_if_needed()
                page.wait_for_timeout(500)

                country_df = extract_table_data(page, country_table, "Top 10 Country Exposure")
                if country_df is not None and len(country_df) > 0:
                    scraped_data['country_exposure'] = country_df
                    logging.info(f"Data preview:\n{country_df.head()}")
                else:
                    errors_encountered.append("Failed to extract Country Exposure")
            else:
                errors_encountered.append("Failed to find Country Exposure table")

            # Step 8: Extract Fund Statistics
            logging.info("\n[STEP 8/10] Extracting Fund Statistics")
            stats_keywords = ["Fund statistics", "statistics", "Yield to maturity"]
            stats_table = find_table_by_keywords(page, stats_keywords, "Fund Statistics")

            if stats_table:
                # Scroll table into view
                stats_table.scroll_into_view_if_needed()
                page.wait_for_timeout(500)

                stats_df = extract_table_data(page, stats_table, "Fund Statistics")
                if stats_df is not None and len(stats_df) > 0:
                    scraped_data['fund_statistics'] = stats_df
                    logging.info(f"Data preview:\n{stats_df.head()}")
                else:
                    errors_encountered.append("Failed to extract Fund Statistics")
            else:
                errors_encountered.append("Failed to find Fund Statistics table")

            # Step 9: Extract Performance Attribution
            logging.info("\n[STEP 9/10] Extracting Performance Attribution")
            perf_keywords = ["Performance attribution", "attribution", "Bottom 3", "Top 3"]

            tables = page.query_selector_all("table.ash-xml-factsheet-data__grid")
            for table in tables:
                try:
                    headers = table.query_selector_all("thead th, tr:first-child th")
                    header_text = " ".join([h.inner_text().strip().lower() for h in headers])

                    if "performance" in header_text and "attribution" in header_text:
                        # Scroll table into view
                        table.scroll_into_view_if_needed()
                        page.wait_for_timeout(500)

                        perf_df = extract_table_data(page, table, "Performance Attribution")
                        if perf_df is not None and len(perf_df) > 0:
                            scraped_data['performance_top3'] = perf_df
                            logging.info(f"Performance preview:\n{perf_df.head()}")
                        break
                except:
                    continue

            # Step 10: Download factsheet
            logging.info("\n[STEP 10/10] Downloading factsheet from Document Library")
            try:
                # Click Document library tab
                doc_lib_tab = page.get_by_role("tab", name="Document library")
                doc_lib_tab.click()
                page.wait_for_timeout(2000)
                page.screenshot(path=f"logs/04_document_library_{timestamp}.png")

                # Find "Fund Share Class Documents" section
                fund_docs_heading = page.get_by_role("heading", name="Fund Share Class Documents")
                if fund_docs_heading:
                    fund_docs_heading.scroll_into_view_if_needed()
                    page.wait_for_timeout(1000)
                    logging.info("[OK] Found Fund Share Class Documents section")

                # Find Institutional Factsheet link and get href directly
                pdf_downloaded = False

                # Look for link containing "Institutional Factsheet"
                factsheet_link = page.locator("a:has-text('Institutional Factsheet')").first

                if factsheet_link.count():
                    # Get href attribute directly - don't click and wait for tab
                    href = factsheet_link.get_attribute("href")
                    logging.info(f"Found Institutional Factsheet link, href: {href}")

                    if href:
                        pdf_url = href if href.startswith("http") else "https://www.ashmoregroup.com" + href
                        logging.info(f"[OK] PDF URL: {pdf_url}")

                        # Download PDF using requests
                        pdf_filename = f"downloads/ASHMORE_Factsheet_{timestamp}.pdf"
                        response = requests.get(pdf_url, timeout=60)
                        if response.status_code == 200:
                            with open(pdf_filename, 'wb') as f:
                                f.write(response.content)
                            logging.info(f"[OK] Factsheet saved: {pdf_filename} ({len(response.content)} bytes)")
                            pdf_downloaded = True
                        else:
                            logging.warning(f"Failed to download PDF: HTTP {response.status_code}")
                else:
                    # Fallback: look for any link with href containing factsheet
                    all_links = page.locator("a").all()
                    for link in all_links:
                        href = link.get_attribute("href") or ""
                        text = link.inner_text().strip().lower()
                        if ("institutional" in text or "institutional" in href.lower()) and "factsheet" in href.lower():
                            pdf_url = href if href.startswith("http") else "https://www.ashmoregroup.com" + href
                            logging.info(f"[OK] Fallback PDF URL: {pdf_url}")

                            pdf_filename = f"downloads/ASHMORE_Factsheet_{timestamp}.pdf"
                            response = requests.get(pdf_url, timeout=60)
                            if response.status_code == 200:
                                with open(pdf_filename, 'wb') as f:
                                    f.write(response.content)
                                logging.info(f"[OK] Factsheet saved: {pdf_filename}")
                                pdf_downloaded = True
                            break

                if not pdf_downloaded:
                    logging.warning("Could not find or download Institutional Factsheet PDF")

                    # Save extracted data only if PDF download failed
                    logging.info("\n" + "=" * 80)
                    logging.info("SAVING EXTRACTED DATA (PDF download failed)")
                    logging.info("=" * 80)

                    if scraped_data:
                        # Save individual CSVs
                        for key, df in scraped_data.items():
                            filename = f"downloads/{key}_{timestamp}.csv"
                            df.to_csv(filename, index=False)
                            logging.info(f"[OK] Saved: {filename} ({len(df)} rows)")

                        # Save combined JSON
                        json_data = {key: df.to_dict('records') for key, df in scraped_data.items()}
                        json_filename = f"downloads/ashmore_data_{timestamp}.json"
                        with open(json_filename, 'w') as f:
                            json.dump(json_data, f, indent=2)
                        logging.info(f"[OK] Saved combined JSON: {json_filename}")

                        logging.info("\n[INFO] Use map.py to convert extracted data to RMP format")
                    else:
                        logging.warning("No data was extracted!")

            except Exception as e:
                logging.warning(f"Could not download factsheet: {str(e)}")

                # Save extracted data on exception
                if scraped_data:
                    logging.info("\n[INFO] Saving extracted data after PDF download error...")
                    for key, df in scraped_data.items():
                        filename = f"downloads/{key}_{timestamp}.csv"
                        df.to_csv(filename, index=False)
                        logging.info(f"[OK] Saved: {filename} ({len(df)} rows)")

            browser.close()

        except Exception as e:
            logging.error(f"Fatal error: {str(e)}")
            errors_encountered.append(f"Fatal error: {str(e)}")

    # Session summary
    elapsed = time.time() - start_time
    logging.info("\n" + "=" * 80)
    logging.info("SESSION SUMMARY")
    logging.info("=" * 80)
    logging.info(f"Total execution time: {elapsed:.2f}s ({elapsed/60:.2f} minutes)")
    logging.info(f"Data items extracted: {len(scraped_data)}")
    logging.info(f"Errors encountered: {len(errors_encountered)}")

    if errors_encountered:
        logging.warning("\nErrors during session:")
        for i, err in enumerate(errors_encountered, 1):
            logging.warning(f"  {i}. {err}")
    else:
        logging.info("[OK] Session completed with no errors")

    logging.info("=" * 80)
    logging.info("SESSION END")
    logging.info("=" * 80)

    return scraped_data

# ============================================================================
# ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    scrape_ashmore_data()
