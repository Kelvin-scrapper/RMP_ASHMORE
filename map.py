"""
RMP Data Mapper
Converts scraped Ashmore fund data CSVs and PDFs into RMP column format
"""

import pandas as pd
import pdfplumber
import re
import os
import logging
from datetime import datetime
from config import (
    RMP_COLUMN_ORDER,
    RMP_HEADER_ROW2,
    CURRENCY_CODE_MAP,
    COUNTRY_CODE_MAP,
    FUND_STATS_MAP
)


def setup_logging():
    """Configure logging"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f"logs/mapper_{timestamp}.log"

    os.makedirs("logs", exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return timestamp


def find_latest_pdf(downloads_dir="downloads"):
    """Find the most recent PDF factsheet"""
    pdf_files = [f for f in os.listdir(downloads_dir) if f.startswith('ASHMORE_Factsheet_') and f.endswith('.pdf')]
    if pdf_files:
        pdf_files.sort(reverse=True)
        return os.path.join(downloads_dir, pdf_files[0])
    return None


def extract_date_from_pdf(pdf_path):
    """Extract fund date from PDF header (e.g., 'FUND UPDATE Oct 2025' -> '2025-10')"""
    import re

    month_map = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
    }

    try:
        with pdfplumber.open(pdf_path) as pdf:
            page = pdf.pages[0]
            text = page.extract_text() or ""

            # Look for "FUND UPDATE Month Year" pattern
            match = re.search(r'FUND UPDATE\s+(\w+)\s+(\d{4})', text, re.IGNORECASE)
            if match:
                month_str = match.group(1).lower()[:3]
                year = match.group(2)
                month_num = month_map.get(month_str)
                if month_num:
                    date_str = f"{year}-{month_num}"
                    logging.info(f"Extracted fund date from PDF: {date_str}")
                    return date_str

            # Alternative: look for "Information at DD.MM.YYYY"
            match = re.search(r'Information at\s+(\d{1,2})\.(\d{1,2})\.(\d{4})', text)
            if match:
                year = match.group(3)
                month = match.group(2).zfill(2)
                date_str = f"{year}-{month}"
                logging.info(f"Extracted fund date from PDF (alt): {date_str}")
                return date_str

    except Exception as e:
        logging.warning(f"Could not extract date from PDF: {e}")

    return None


def extract_data_from_pdf(pdf_path):
    """Extract table data from Ashmore factsheet PDF using pdfplumber with context awareness"""
    logging.info(f"Extracting data from PDF: {pdf_path}")

    data = {
        'currency': [],
        'country': [],
        'statistics': [],
        'performance': []
    }

    # Track which tables we've already extracted
    # Note: We allow continuation tables across pages by checking content, not just first occurrence
    # Performance tables are special - we extract both Top 3 and Bottom 3
    extracted_tables = {
        'currency': False,
        'country': False,
        'statistics': False,
        'performance': False  # For performance, this just tracks if we found any
    }

    # Define table title patterns to search for in the PDF
    table_title_patterns = {
        'currency': ['top 10 em currency exposure', 'currency exposure %', 'em currency exposure'],
        'country': ['top 10 country exposure', 'country exposure %'],
        'statistics': ['fund statistics'],
        'performance': ['performance attribution']
    }

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages):
            # Get full page text for context - this contains table titles
            page_text = page.extract_text() or ""
            page_text_lower = page_text.lower()
            tables = page.extract_tables()

            logging.info(f"Page {page_num + 1}: Found {len(tables)} tables")

            # Scan page text to find all table titles and their positions
            found_titles = {}
            for table_type, patterns in table_title_patterns.items():
                for pattern in patterns:
                    pos = page_text_lower.find(pattern)
                    if pos != -1:
                        if table_type not in found_titles or pos < found_titles[table_type]['pos']:
                            found_titles[table_type] = {'pattern': pattern, 'pos': pos}
                        logging.info(f"  Found title '{pattern}' at position {pos} -> {table_type}")

            for table_idx, table in enumerate(tables):
                if not table or len(table) < 2:
                    continue

                # Get first cell content for title-based matching
                first_cell = str(table[0][0]).lower() if table[0] and table[0][0] else ''

                # Debug: log first row of each table
                if table[0]:
                    first_row_preview = [str(cell)[:25] if cell else '' for cell in table[0][:4]]
                    logging.info(f"  Table {table_idx + 1}: {first_row_preview}")

                # Check first column values for content-based detection
                first_col_values = [str(row[0]).strip() if row and row[0] else '' for row in table[1:]]
                first_col_text = ' '.join(first_col_values).lower()

                # Determine table type using multiple methods:
                # 1. First check table header/title cell
                # 2. Then check content indicators
                table_type = None

                # Method 1: Title-based detection (most reliable)
                if 'performance attribut' in first_cell:
                    table_type = 'performance'
                elif 'fund statistics' in first_cell:
                    table_type = 'statistics'
                elif 'currency exposure' in first_cell or 'em currency' in first_cell:
                    table_type = 'currency'
                elif 'country exposure' in first_cell:
                    table_type = 'country'
                elif 'top 10' in first_cell:
                    # Check what kind of Top 10
                    if 'currency' in first_cell:
                        table_type = 'currency'
                    elif 'country' in first_cell:
                        table_type = 'country'

                # Method 2: Content-based detection (fallback)
                if table_type is None:
                    # Fund Statistics - look for yield, duration, coupon
                    stats_indicators = ['yield to maturity', 'average coupon', 'current yield', 'modified duration', 'average life']
                    if any(stat in first_col_text for stat in stats_indicators):
                        table_type = 'statistics'

                    # Currency Exposure - look for currency names (Rupee, Peso, Baht, Lira, etc.)
                    currency_indicators = ['indian rupee', 'polish zloty', 'malaysian ringgit', 'indonesian rupiah',
                                          'mexican peso', 'thai baht', 'brazilian real', 'south african rand',
                                          'czech koruna', 'hungarian forint', 'chinese yuan', 'turkish lira']
                    if table_type is None and any(curr in first_col_text for curr in currency_indicators):
                        table_type = 'currency'

                    # Country Exposure - look for country names (but NOT currency names)
                    country_indicators = ['india', 'poland', 'malaysia', 'indonesia', 'mexico', 'thailand',
                                         'brazil', 'south africa', 'czech republic', 'hungary', 'china', 'turkey', 'turkiye']
                    if table_type is None:
                        has_country = any(country in first_col_text for country in country_indicators)
                        has_currency = any(curr in first_col_text for curr in currency_indicators)
                        # It's country exposure if we have country names but NO currency-specific words
                        if has_country and not has_currency:
                            table_type = 'country'

                # Skip if we already extracted this table type (except performance - we need both Top 3 and Bottom 3)
                if table_type and table_type != 'performance' and extracted_tables.get(table_type, False):
                    logging.info(f"  -> Skipping duplicate {table_type} table")
                    continue

                # Extract data based on identified type
                if table_type == 'currency':
                    logging.info(f"  -> Detected as: CURRENCY EXPOSURE (table {table_idx + 1})")
                    for row in table[1:]:
                        if row and len(row) >= 3 and row[0]:
                            name = str(row[0]).strip()
                            if name and not name.lower().startswith('top') and '%' not in name:
                                # Handle multi-column PDF structure (cols may be: 0=Name, 1=empty, 2=Fund, 3=empty, 4=Benchmark)
                                fund_val = ''
                                bench_val = ''
                                # Find Fund value (first non-empty numeric after name)
                                for i in range(1, len(row)):
                                    val = str(row[i]).strip() if row[i] else ''
                                    if val and val.replace('.', '').replace('-', '').isdigit():
                                        if not fund_val:
                                            fund_val = val
                                        else:
                                            bench_val = val
                                            break
                                data['currency'].append({
                                    'name': name,
                                    'fund': fund_val,
                                    'benchmark': bench_val
                                })
                    extracted_tables['currency'] = True

                elif table_type == 'country':
                    logging.info(f"  -> Detected as: COUNTRY EXPOSURE (table {table_idx + 1})")
                    for row in table[1:]:
                        if row and len(row) >= 3 and row[0]:
                            name = str(row[0]).strip()
                            if name and not name.lower().startswith('top') and '%' not in name:
                                # Handle multi-column PDF structure
                                fund_val = ''
                                bench_val = ''
                                for i in range(1, len(row)):
                                    val = str(row[i]).strip() if row[i] else ''
                                    if val and val.replace('.', '').replace('-', '').isdigit():
                                        if not fund_val:
                                            fund_val = val
                                        else:
                                            bench_val = val
                                            break
                                data['country'].append({
                                    'name': name,
                                    'fund': fund_val,
                                    'benchmark': bench_val
                                })
                    extracted_tables['country'] = True

                elif table_type == 'statistics':
                    logging.info(f"  -> Detected as: FUND STATISTICS (table {table_idx + 1})")
                    for row in table[1:]:
                        if row and len(row) >= 3 and row[0]:
                            name = str(row[0]).strip()
                            if name:
                                # Handle multi-column PDF structure
                                fund_val = ''
                                bench_val = ''
                                for i in range(1, len(row)):
                                    val = str(row[i]).strip() if row[i] else ''
                                    # Stats can have % or be numeric
                                    clean_val = val.replace('%', '').replace('.', '').replace('-', '')
                                    if val and clean_val.isdigit():
                                        if not fund_val:
                                            fund_val = val
                                        else:
                                            bench_val = val
                                            break
                                data['statistics'].append({
                                    'name': name,
                                    'fund': fund_val,
                                    'benchmark': bench_val
                                })
                    extracted_tables['statistics'] = True

                elif table_type == 'performance':
                    logging.info(f"  -> Detected as: PERFORMANCE ATTRIBUTION (table {table_idx + 1})")
                    for row in table[1:]:
                        if row and len(row) >= 5 and row[0]:
                            name = str(row[0]).strip()
                            # Skip header rows
                            if '(Top' in name or '(Bottom' in name or name.endswith('%') or not name:
                                continue

                            # Extract numeric values from multi-column structure
                            # PDF structure: Name, empty, AssetAlloc, empty, SecSelect, empty, CurrEffect(split), Total
                            numeric_vals = []
                            i = 1
                            while i < len(row) and len(numeric_vals) < 4:
                                val = str(row[i]).strip() if row[i] else ''
                                # Check if this is a split decimal (e.g., '0.' followed by '08')
                                if val.endswith('.') and i + 1 < len(row):
                                    next_val = str(row[i + 1]).strip() if row[i + 1] else ''
                                    if next_val and next_val.replace('-', '').isdigit():
                                        val = val + next_val
                                        i += 1
                                # Check if it's a negative split (e.g., '-0.' followed by '01')
                                elif val == '-' and i + 1 < len(row):
                                    next_val = str(row[i + 1]).strip() if row[i + 1] else ''
                                    if next_val:
                                        val = val + next_val
                                        i += 1

                                # Check if it's a valid number
                                if val:
                                    clean = val.replace('.', '').replace('-', '')
                                    if clean.isdigit() or val in ['0', '0.0', '0.00']:
                                        numeric_vals.append(val)
                                i += 1

                            # Ensure we have 4 values
                            while len(numeric_vals) < 4:
                                numeric_vals.append('')

                            data['performance'].append({
                                'name': name,
                                'asset_allocation': numeric_vals[0],
                                'security_selection': numeric_vals[1],
                                'currency_effect': numeric_vals[2],
                                'total': numeric_vals[3]
                            })
                    extracted_tables['performance'] = True

    # Log extraction results
    for key, items in data.items():
        logging.info(f"  Extracted {len(items)} {key} items from PDF")

    return data


def find_latest_files(downloads_dir="downloads"):
    """Find the most recent scraped data files"""
    files = {}

    # Find latest of each type
    file_patterns = {
        'currency': 'em_currency_exposure_',
        'country': 'country_exposure_',
        'statistics': 'fund_statistics_',
        'performance': 'performance_top3_'
    }

    for key, pattern in file_patterns.items():
        matching = [f for f in os.listdir(downloads_dir) if f.startswith(pattern) and f.endswith('.csv')]
        if matching:
            matching.sort(reverse=True)  # Most recent first
            files[key] = os.path.join(downloads_dir, matching[0])
            logging.info(f"Found {key}: {matching[0]}")

    return files


def load_data_files(file_paths):
    """Load CSV files into DataFrames"""
    data = {}

    for key, path in file_paths.items():
        try:
            df = pd.read_csv(path)
            data[key] = df
            logging.info(f"Loaded {key}: {len(df)} rows")
        except Exception as e:
            logging.error(f"Error loading {path}: {e}")

    return data


def map_currency_exposure(df, rmp_data):
    """Map currency exposure data to RMP format"""
    logging.info("Mapping currency exposure...")
    mapped_count = 0

    for _, row in df.iterrows():
        currency_name = str(row.iloc[0]).strip()

        # Handle Total row (Total number of currencies)
        if "Total" in currency_name or "total" in currency_name.lower():
            try:
                fund_val = str(row.iloc[1]).strip()
                bench_val = str(row.iloc[2]).strip() if len(row) > 2 else ""
                rmp_data["RMP.ASHMORE.CURRENCYEXPOSURE.TOTAL.FUND.M"] = fund_val
                rmp_data["RMP.ASHMORE.CURRENCYEXPOSURE.TOTAL.BENCHMARK.M"] = bench_val
                mapped_count += 2
                logging.debug(f"  Total currencies: {fund_val}/{bench_val}")
            except:
                pass
            continue

        # Map currency
        currency_code = CURRENCY_CODE_MAP.get(currency_name)
        if currency_code:
            try:
                fund_val = str(row.iloc[1]).strip()
                bench_val = str(row.iloc[2]).strip() if len(row) > 2 else ""
                rmp_data[f"RMP.ASHMORE.CURRENCYEXPOSURE.{currency_code}.FUND.M"] = fund_val
                rmp_data[f"RMP.ASHMORE.CURRENCYEXPOSURE.{currency_code}.BENCHMARK.M"] = bench_val
                mapped_count += 2
                logging.debug(f"  {currency_name} -> {currency_code}: {fund_val}/{bench_val}")
            except Exception as e:
                logging.warning(f"Error mapping {currency_name}: {e}")
        else:
            logging.warning(f"Unknown currency: {currency_name}")

    logging.info(f"  Mapped {mapped_count} currency values")
    return mapped_count


def map_country_exposure(df, rmp_data):
    """Map country exposure data to RMP format"""
    logging.info("Mapping country exposure...")
    mapped_count = 0

    for _, row in df.iterrows():
        country_name = str(row.iloc[0]).strip()

        # Handle Total row
        if "Total" in country_name:
            try:
                fund_val = str(row.iloc[1]).strip()
                bench_val = str(row.iloc[2]).strip() if len(row) > 2 else ""
                rmp_data["RMP.ASHMORE.COUNTRYEXPOSURE.TOTAL.FUND.M"] = fund_val
                rmp_data["RMP.ASHMORE.COUNTRYEXPOSURE.TOTAL.BENCHMARK.M"] = bench_val
                mapped_count += 2
            except:
                pass
            continue

        # Map country
        country_code = COUNTRY_CODE_MAP.get(country_name)
        if country_code:
            try:
                fund_val = str(row.iloc[1]).strip()
                bench_val = str(row.iloc[2]).strip() if len(row) > 2 else ""
                rmp_data[f"RMP.ASHMORE.COUNTRYEXPOSURE.{country_code}.FUND.M"] = fund_val
                rmp_data[f"RMP.ASHMORE.COUNTRYEXPOSURE.{country_code}.BENCHMARK.M"] = bench_val
                mapped_count += 2
                logging.debug(f"  {country_name} -> {country_code}: {fund_val}/{bench_val}")
            except Exception as e:
                logging.warning(f"Error mapping {country_name}: {e}")
        else:
            logging.warning(f"Unknown country: {country_name}")

    logging.info(f"  Mapped {mapped_count} country values")
    return mapped_count


def map_fund_statistics(df, rmp_data):
    """Map fund statistics data to RMP format"""
    logging.info("Mapping fund statistics...")
    mapped_count = 0

    for _, row in df.iterrows():
        stat_name = str(row.iloc[0]).strip()
        stat_key = FUND_STATS_MAP.get(stat_name)

        if stat_key:
            try:
                fund_val = str(row.iloc[1]).strip().replace('%', '')
                bench_val = str(row.iloc[2]).strip().replace('%', '') if len(row) > 2 else ""
                rmp_data[f"RMP.ASHMORE.FUNDSTATISTICS.{stat_key}.FUND.M"] = fund_val
                rmp_data[f"RMP.ASHMORE.FUNDSTATISTICS.{stat_key}.BENCHMARK.M"] = bench_val
                mapped_count += 2
                logging.debug(f"  {stat_name} -> {stat_key}: {fund_val}/{bench_val}")
            except Exception as e:
                logging.warning(f"Error mapping {stat_name}: {e}")
        else:
            logging.warning(f"Unknown statistic: {stat_name}")

    logging.info(f"  Mapped {mapped_count} statistics values")
    return mapped_count


def map_performance_attribution(df, rmp_data):
    """Map performance attribution data to RMP format"""
    logging.info("Mapping performance attribution...")
    mapped_count = 0

    for _, row in df.iterrows():
        currency_name = str(row.iloc[0]).strip()
        currency_code = CURRENCY_CODE_MAP.get(currency_name)

        if currency_code:
            try:
                asset_alloc = str(row.iloc[1]).strip()
                sec_select = str(row.iloc[2]).strip()
                curr_effect = str(row.iloc[3]).strip()
                total = str(row.iloc[4]).strip()

                rmp_data[f"RMP.ASHMORE.PERFORMANCEATTRIBUTION.{currency_code}.ASSETALLOCATION.M"] = asset_alloc
                rmp_data[f"RMP.ASHMORE.PERFORMANCEATTRIBUTION.{currency_code}.SECURITYSELECTION.M"] = sec_select
                rmp_data[f"RMP.ASHMORE.PERFORMANCEATTRIBUTION.{currency_code}.CURRENCYEFFECT.M"] = curr_effect
                rmp_data[f"RMP.ASHMORE.PERFORMANCEATTRIBUTION.{currency_code}.TOTAL.M"] = total
                mapped_count += 4
                logging.debug(f"  {currency_name} -> {currency_code}")
            except Exception as e:
                logging.warning(f"Error mapping {currency_name}: {e}")
        else:
            logging.warning(f"Unknown currency for performance: {currency_name}")

    logging.info(f"  Mapped {mapped_count} performance values")
    return mapped_count


def create_rmp_csv(rmp_data, output_path):
    """Create RMP format CSV file"""
    logging.info(f"Creating RMP CSV: {output_path}")

    # Build data row matching column order
    rmp_row = []
    for col in RMP_COLUMN_ORDER:
        rmp_row.append(rmp_data.get(col, ""))

    # Write CSV with 2 header rows + data row
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(','.join(RMP_COLUMN_ORDER) + '\n')
        f.write(','.join(RMP_HEADER_ROW2) + '\n')
        f.write(','.join(rmp_row) + '\n')

    # Count populated columns
    populated = sum(1 for v in rmp_row if v)
    logging.info(f"[OK] Saved RMP CSV: {output_path}")
    logging.info(f"Populated {populated} out of {len(RMP_COLUMN_ORDER)} columns")

    return populated


def map_data_to_rmp(fund_date=None, downloads_dir="downloads", use_pdf=True):
    """Main function to map scraped data to RMP format"""

    print("=" * 60)
    print("ASHMORE RMP DATA MAPPER")
    print("=" * 60)
    print()

    timestamp = setup_logging()

    data = {}

    # Try PDF first if enabled
    if use_pdf:
        pdf_path = find_latest_pdf(downloads_dir)
        if pdf_path:
            logging.info(f"Found PDF: {pdf_path}")

            # Extract date from PDF if not provided
            if fund_date is None:
                fund_date = extract_date_from_pdf(pdf_path)

            pdf_data = extract_data_from_pdf(pdf_path)

            # Convert PDF data to DataFrames
            if pdf_data['currency']:
                data['currency'] = pd.DataFrame(pdf_data['currency'])
                data['currency'].columns = ['name', 'Fund', 'Benchmark']
            if pdf_data['country']:
                data['country'] = pd.DataFrame(pdf_data['country'])
                data['country'].columns = ['name', 'Fund', 'Benchmark']
            if pdf_data['statistics']:
                data['statistics'] = pd.DataFrame(pdf_data['statistics'])
                data['statistics'].columns = ['name', 'Fund', 'Benchmark']
            if pdf_data['performance']:
                data['performance'] = pd.DataFrame(pdf_data['performance'])
                data['performance'].columns = ['name', 'Asset allocation', 'Security selection', 'Currency effect', 'Total']
        else:
            logging.info("No PDF found, falling back to CSV files")

    # Fall back to CSV files if no PDF data
    if not data:
        logging.info("Finding latest scraped CSV files...")
        file_paths = find_latest_files(downloads_dir)

        if not file_paths:
            logging.error("No data files found in downloads directory!")
            return None

        # Load data from CSVs
        logging.info("\nLoading CSV files...")
        data = load_data_files(file_paths)

    # Initialize RMP data with date
    if fund_date is None:
        # Try to extract from JSON if available
        fund_date = datetime.now().strftime('%Y-%m')

    rmp_data = {"": fund_date}
    total_mapped = 0

    # Map each data type
    logging.info("\nMapping data to RMP format...")

    if 'currency' in data:
        total_mapped += map_currency_exposure(data['currency'], rmp_data)

    if 'country' in data:
        total_mapped += map_country_exposure(data['country'], rmp_data)

    if 'statistics' in data:
        total_mapped += map_fund_statistics(data['statistics'], rmp_data)

    if 'performance' in data:
        total_mapped += map_performance_attribution(data['performance'], rmp_data)

    logging.info(f"\nTotal values mapped: {total_mapped}")

    # Create output file
    os.makedirs("output", exist_ok=True)
    output_path = f"output/RMP_ASHMORE_DATA_{timestamp}.csv"
    populated = create_rmp_csv(rmp_data, output_path)

    print("\n" + "=" * 60)
    print("MAPPING COMPLETE")
    print("=" * 60)
    print(f"Output: {output_path}")
    print(f"Columns populated: {populated}/{len(RMP_COLUMN_ORDER)}")
    print()

    return output_path


if __name__ == "__main__":
    # Run mapper - date will be extracted from PDF automatically
    # You can also pass a specific date like: map_data_to_rmp("2025-10")
    map_data_to_rmp()
