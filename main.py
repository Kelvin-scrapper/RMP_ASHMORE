"""
RMP_ASHMORE main — Ashmore SICAV LCBF Ins Inc USD

Outputs:
  output/RMP_ASHMORE_DATA_YYYYMMDD.xlsx
  output/RMP_ASHMORE_META_YYYYMMDD.xlsx
  output/RMP_ASHMORE_YYYYMMDD.ZIP

Usage:
  cd RMP_ASHMORE
  python main.py                        # download + extract latest PDF
  python main.py --seed path/to.pdf     # extract from a local PDF (no download)
"""

import argparse
import glob
import os
import sys
import zipfile
from datetime import datetime

import openpyxl
import pandas as pd

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

OUTPUT_DIR    = "output"
DOWNLOADS_DIR = "downloads"
OUTPUT_PREFIX = "RMP_ASHMORE"


def _datestamp() -> str:
    return datetime.utcnow().strftime("%Y%m%d")


def _apply_number_format(filepath: str) -> None:
    wb = openpyxl.load_workbook(filepath)
    ws = wb.active
    for row in ws.iter_rows(min_row=3, min_col=2):
        for cell in row:
            if isinstance(cell.value, (int, float)):
                cell.number_format = "#,##0.##"
    wb.save(filepath)


def _save_data(df: pd.DataFrame, datestamp: str) -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f"{OUTPUT_PREFIX}_DATA_{datestamp}.xlsx")
    df.to_excel(path, index=False, header=False)
    _apply_number_format(path)
    print(f"[main] DATA saved: {path}")
    return path


def _save_metadata(datestamp: str) -> str:
    import mapper
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f"{OUTPUT_PREFIX}_META_{datestamp}.xlsx")
    pd.DataFrame(mapper.build_metadata_rows()).to_excel(path, index=False)
    print(f"[main] META saved: {path}")
    return path


def _create_zip(data_path: str, meta_path: str, datestamp: str) -> str:
    zip_path = os.path.join(OUTPUT_DIR, f"{OUTPUT_PREFIX}_{datestamp}.ZIP")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(data_path, os.path.basename(data_path))
        zf.write(meta_path, os.path.basename(meta_path))
    print(f"[main] ZIP saved: {zip_path}")
    return zip_path


def run(seed_path: str = None) -> None:
    """Download (or use seed) → extract → save DATA + META + ZIP."""
    datestamp = _datestamp()

    if seed_path:
        pdf_path = seed_path
        print(f"[main] Using seed PDF: {pdf_path}")
    else:
        import scraper
        pdf_path = scraper.fetch_data(DOWNLOADS_DIR)

    import mapper

    existing_files = sorted(glob.glob(os.path.join(OUTPUT_DIR, f"{OUTPUT_PREFIX}_DATA_*.xlsx")))
    existing_path = existing_files[-1] if existing_files else None
    if existing_path:
        print(f"[main] Merging into existing: {existing_path}")

    df = mapper.map_to_output(pdf_path, existing_path)

    data_path = _save_data(df, datestamp)
    meta_path = _save_metadata(datestamp)
    _create_zip(data_path, meta_path, datestamp)

    print("[main] Done.")


def main():
    parser = argparse.ArgumentParser(description="RMP_ASHMORE pipeline")
    parser.add_argument("--seed", metavar="PDF_PATH",
                        help="Local PDF to process instead of downloading")
    args = parser.parse_args()
    run(seed_path=args.seed)


if __name__ == "__main__":
    main()
