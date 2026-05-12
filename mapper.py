"""
RMP_ASHMORE mapper — Ashmore SICAV LCBF Ins Inc USD

Responsibility: EXTRACT + TRANSFORM.
  Opens the downloaded PDF factsheet (using pdfplumber), extracts:
    - Top 10 EM Currency Exposure
    - Top 10 Country Exposure
    - Fund Statistics
    - Performance Attribution
  Transforms the result into the standard 2-header-row output DataFrame.

Raw file layout (PDF, ~2 pages):
  Page 1: Fund Statistics, Currency Exposure, Country Exposure tables
  Page 1/2: Performance Attribution table
  158 series total → loaded from headers.json

Output: 2 header rows (codes, descriptions) + data rows, one row per period.
"""

import json
import logging
import os
import re
import sys
from datetime import datetime

import pandas as pd
import pdfplumber

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# 158 series → load from headers.json
with open(os.path.join(os.path.dirname(__file__), "headers.json"), encoding="utf-8") as _f:
    COLUMN_HEADERS = json.load(_f)

# ── Lookup maps ───────────────────────────────────────────────────────────────

CURRENCY_CODE_MAP = {
    "Mexican Peso":              "MXN",
    "Malaysian Ringgit":         "MYR",
    "Indian Rupee":              "INR",
    "Indonesian Rupiah":         "IDR",
    "Polish Zloty":              "PLN",
    "Thai Baht":                 "THB",
    "Brazilian Real":            "BRL",
    "South African Rand":        "ZAR",
    "Czech Koruna":              "CZK",
    "Hungarian Forint":          "HUF",
    "Chinese Yuan (offshore)":   "CNH",
    "Chinese Yuan (onshore)":    "CNY",
    "Nigerian Naira":            "NGN",
    "Argentine Peso":            "ARS",
    "South Korean Won":          "KRW",
    "Turkish Lira":              "TRY",
    "Colombian Peso":            "COP",
    "Chilean Peso":              "CLP",
    "Peruvian Sol":              "PEN",
    "Egyptian Pound":            "EGP",
    "Philippine Peso":           "PHP",
    "Singapore Dollar":          "SGD",
    "Romanian Leu":              "RON",
    "Kazakhstani Tenge":         "KZT",
}

COUNTRY_CODE_MAP = {
    "Mexico":         "MEX",
    "Malaysia":       "MYS",
    "India":          "IND",
    "Indonesia":      "IDN",
    "Poland":         "POL",
    "Thailand":       "THA",
    "Brazil":         "BRA",
    "South Africa":   "ZAF",
    "Czech Republic": "CZE",
    "Hungary":        "HUN",
    "China":          "CHN",
    "Turkey":         "TUR",
    "Turkiye":        "TUR",
    "Romania":        "ROU",
}

FUND_STATS_MAP = {
    "Yield to maturity":  "YIELDTOMATURITY",
    "Average coupon":     "AVERAGECOUPON",
    "Current yield":      "CURRENTYIELD",
    "Modified duration":  "MODIFIEDDURATION",
    "Average life":       "AVERAGELIFE",
    "Distribution yield": "DISTRIBUTIONYIELD",
}

_MONTH_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}

# ── PDF extraction ────────────────────────────────────────────────────────────

def _extract_date(pdf_path: str):
    try:
        with pdfplumber.open(pdf_path) as pdf:
            text = pdf.pages[0].extract_text() or ""
            m = re.search(r"FUND UPDATE\s+(\w+)\s+(\d{4})", text, re.IGNORECASE)
            if m:
                month = _MONTH_MAP.get(m.group(1).lower()[:3])
                if month:
                    return f"{m.group(2)}-{month}"
            m = re.search(r"Information at\s+(\d{1,2})\.(\d{1,2})\.(\d{4})", text)
            if m:
                return f"{m.group(3)}-{m.group(2).zfill(2)}"
    except Exception as e:
        logging.warning(f"[mapper] Could not extract date from PDF: {e}")
    return None


def _numeric_vals_from_row(row, max_vals=2):
    vals = []
    i = 1
    while i < len(row) and len(vals) < max_vals:
        val = str(row[i]).strip() if row[i] else ""
        if val.endswith(".") and i + 1 < len(row):
            nxt = str(row[i + 1]).strip() if row[i + 1] else ""
            if nxt.replace("-", "").isdigit():
                val = val + nxt
                i += 1
        elif val == "-" and i + 1 < len(row):
            nxt = str(row[i + 1]).strip() if row[i + 1] else ""
            if nxt:
                val = val + nxt
                i += 1
        if val:
            clean = val.replace(".", "").replace("-", "").replace("%", "")
            if clean.isdigit() or val in ("0", "0.0", "0.00"):
                vals.append(val)
        i += 1
    while len(vals) < max_vals:
        vals.append("")
    return vals


def _extract_pdf(pdf_path: str) -> dict:
    """Parse PDF and return raw dicts for currency, country, statistics, performance."""
    data = {"currency": [], "country": [], "statistics": [], "performance": []}
    done = {k: False for k in data}

    table_title_patterns = {
        "currency":    ["top 10 em currency exposure", "currency exposure %", "em currency exposure"],
        "country":     ["top 10 country exposure", "country exposure %"],
        "statistics":  ["fund statistics"],
        "performance": ["performance attribution"],
    }

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages):
            page_text_lower = (page.extract_text() or "").lower()
            tables = page.extract_tables()
            logging.info(f"[mapper] Page {page_num + 1}: {len(tables)} tables")

            for table_idx, table in enumerate(tables):
                if not table or len(table) < 2:
                    continue

                first_cell = str(table[0][0]).lower() if table[0] and table[0][0] else ""
                first_col = " ".join(str(r[0]).strip() if r and r[0] else "" for r in table[1:]).lower()

                # Detect table type
                ttype = None
                if "performance attribut" in first_cell:
                    ttype = "performance"
                elif "fund statistics" in first_cell:
                    ttype = "statistics"
                elif "currency exposure" in first_cell or "em currency" in first_cell:
                    ttype = "currency"
                elif "country exposure" in first_cell:
                    ttype = "country"
                elif "top 10" in first_cell:
                    ttype = "currency" if "currency" in first_cell else "country" if "country" in first_cell else None

                if ttype is None:
                    stats_kw = ["yield to maturity", "average coupon", "current yield", "modified duration", "average life"]
                    if any(kw in first_col for kw in stats_kw):
                        ttype = "statistics"
                    currency_kw = ["indian rupee", "polish zloty", "malaysian ringgit", "indonesian rupiah",
                                   "mexican peso", "thai baht", "brazilian real", "south african rand",
                                   "czech koruna", "hungarian forint", "chinese yuan", "turkish lira"]
                    if ttype is None and any(kw in first_col for kw in currency_kw):
                        ttype = "currency"
                    country_kw = ["india", "poland", "malaysia", "indonesia", "mexico", "thailand",
                                  "brazil", "south africa", "czech republic", "hungary", "china", "turkey", "turkiye", "romania"]
                    if ttype is None:
                        has_country = any(kw in first_col for kw in country_kw)
                        has_currency = any(kw in first_col for kw in currency_kw)
                        if has_country and not has_currency:
                            ttype = "country"

                if ttype and ttype != "performance" and done.get(ttype):
                    continue

                if ttype in ("currency", "country"):
                    for row in table[1:]:
                        if not row or not row[0]:
                            continue
                        name = str(row[0]).strip()
                        if not name or name.lower().startswith("top") or "%" in name:
                            continue
                        vals = _numeric_vals_from_row(row, 2)
                        data[ttype].append({"name": name, "fund": vals[0], "benchmark": vals[1]})
                    done[ttype] = True

                elif ttype == "statistics":
                    for row in table[1:]:
                        if not row or not row[0]:
                            continue
                        name = str(row[0]).strip()
                        if not name:
                            continue
                        vals = _numeric_vals_from_row(row, 2)
                        fund_v = vals[0].replace("%", "")
                        bench_v = vals[1].replace("%", "")
                        data["statistics"].append({"name": name, "fund": fund_v, "benchmark": bench_v})
                    done["statistics"] = True

                elif ttype == "performance":
                    for row in table[1:]:
                        if not row or not row[0]:
                            continue
                        name = str(row[0]).strip()
                        if "(Top" in name or "(Bottom" in name or name.endswith("%") or not name:
                            continue
                        vals = _numeric_vals_from_row(row, 4)
                        data["performance"].append({
                            "name": name,
                            "asset_allocation":    vals[0],
                            "security_selection":  vals[1],
                            "currency_effect":     vals[2],
                            "total":               vals[3],
                        })
                    done["performance"] = True

    for k, items in data.items():
        logging.info(f"[mapper] Extracted {len(items)} {k} items from PDF")
    return data


# ── mapping to RMP codes ──────────────────────────────────────────────────────

def _map_to_codes(pdf_data: dict) -> dict:
    rmp = {}

    for item in pdf_data["currency"]:
        name = item["name"]
        if name.lower() in ("total", "total number of currencies"):
            rmp["RMP.ASHMORE.CURRENCYEXPOSURE.TOTAL.FUND.M"]      = item["fund"]
            rmp["RMP.ASHMORE.CURRENCYEXPOSURE.TOTAL.BENCHMARK.M"] = item["benchmark"]
        else:
            code = CURRENCY_CODE_MAP.get(name)
            if code:
                rmp[f"RMP.ASHMORE.CURRENCYEXPOSURE.{code}.FUND.M"]      = item["fund"]
                rmp[f"RMP.ASHMORE.CURRENCYEXPOSURE.{code}.BENCHMARK.M"] = item["benchmark"]

    for item in pdf_data["country"]:
        name = item["name"]
        if name.lower() in ("total", "total number of countries"):
            rmp["RMP.ASHMORE.COUNTRYEXPOSURE.TOTAL.FUND.M"]      = item["fund"]
            rmp["RMP.ASHMORE.COUNTRYEXPOSURE.TOTAL.BENCHMARK.M"] = item["benchmark"]
        else:
            code = COUNTRY_CODE_MAP.get(name)
            if code:
                rmp[f"RMP.ASHMORE.COUNTRYEXPOSURE.{code}.FUND.M"]      = item["fund"]
                rmp[f"RMP.ASHMORE.COUNTRYEXPOSURE.{code}.BENCHMARK.M"] = item["benchmark"]

    for item in pdf_data["statistics"]:
        key = FUND_STATS_MAP.get(item["name"])
        if key:
            rmp[f"RMP.ASHMORE.FUNDSTATISTICS.{key}.FUND.M"]      = item["fund"]
            rmp[f"RMP.ASHMORE.FUNDSTATISTICS.{key}.BENCHMARK.M"] = item["benchmark"]

    for item in pdf_data["performance"]:
        code = CURRENCY_CODE_MAP.get(item["name"])
        if code:
            rmp[f"RMP.ASHMORE.PERFORMANCEATTRIBUTION.{code}.ASSETALLOCATION.M"]   = item["asset_allocation"]
            rmp[f"RMP.ASHMORE.PERFORMANCEATTRIBUTION.{code}.SECURITYSELECTION.M"] = item["security_selection"]
            rmp[f"RMP.ASHMORE.PERFORMANCEATTRIBUTION.{code}.CURRENCYEFFECT.M"]    = item["currency_effect"]
            rmp[f"RMP.ASHMORE.PERFORMANCEATTRIBUTION.{code}.TOTAL.M"]             = item["total"]

    return rmp


# ── public interface ──────────────────────────────────────────────────────────

def _record_to_row(period: str, data: dict) -> list:
    codes = COLUMN_HEADERS["codes"]
    row = [period] + [None] * (len(codes) - 1)
    for i, code in enumerate(codes[1:], start=1):
        val = data.get(code)
        if val is not None and val != "":
            try:
                row[i] = float(str(val).replace(",", "").replace("%", "").strip())
            except (ValueError, TypeError):
                row[i] = val
    return row


def map_to_output(pdf_path: str, existing_path: str = None) -> pd.DataFrame:
    """
    Extract data from the PDF and merge into existing history.

    pdf_path:      path to the downloaded Ashmore PDF factsheet
    existing_path: path to existing DATA xlsx (rows 0/1 = headers, row 2+ = data)

    Returns DataFrame with 2 header rows + date-sorted data rows.
    """
    existing_rows: dict = {}

    if existing_path and os.path.exists(existing_path):
        ex = pd.read_excel(existing_path, header=None)
        for _, r in ex.iloc[2:].iterrows():
            date_val = str(r.iloc[0]) if pd.notna(r.iloc[0]) else None
            if date_val:
                existing_rows[date_val] = list(r)
        print(f"[mapper] Loaded {len(existing_rows)} existing rows from {existing_path}")

    period = _extract_date(pdf_path)
    if not period:
        period = datetime.utcnow().strftime("%Y-%m")
        print(f"[mapper] WARNING: Could not extract date, using {period}")
    print(f"[mapper] Period: {period}")

    pdf_data = _extract_pdf(pdf_path)
    rmp_codes = _map_to_codes(pdf_data)
    print(f"[mapper] Total series mapped: {len(rmp_codes)}")

    row = _record_to_row(period, rmp_codes)
    existing_rows[period] = row

    sorted_rows = [existing_rows[d] for d in sorted(existing_rows)]
    all_rows = [COLUMN_HEADERS["codes"], COLUMN_HEADERS["descriptions"]] + sorted_rows
    return pd.DataFrame(all_rows)


def build_metadata_rows() -> list:
    """Return list of metadata dicts for each series — used by main.py for META xlsx."""
    codes = COLUMN_HEADERS["codes"][1:]
    descs = COLUMN_HEADERS["descriptions"][1:]
    rows = []
    for code, desc in zip(codes, descs):
        rows.append({
            "CODE":              code,
            "DESCRIPTION":       desc,
            "FREQUENCY":         "Monthly",
            "UNIT":              "%",
            "SOURCE_NAME":       "Ashmore",
            "SOURCE_URL":        "https://www.ashmoregroup.com/en-gb/our-funds/sicav-lcbf-ins-inc-usd",
            "DATASET":           "RMP_ASHMORE",
            "NEXT_RELEASE_DATE": "",
        })
    return rows
