"""
RMP_ASHMORE scraper — Ashmore SICAV LCBF Ins Inc USD

Responsibility: DOWNLOAD ONLY.
  Navigates to the Ashmore fund page using Playwright (Chrome), handles the
  audience confirmation modal, downloads the Institutional Factsheet PDF,
  and saves it to downloads/.

Source: https://www.ashmoregroup.com/en-gb/our-funds/sicav-lcbf-ins-inc-usd

Usage:
  cd RMP_ASHMORE
  python -c "import scraper; print(scraper.fetch_data())"
"""

import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

PAGE_URL = "https://www.ashmoregroup.com/en-gb/our-funds/sicav-lcbf-ins-inc-usd"


# ── browser helpers ───────────────────────────────────────────────────────────

def _launch_browser(p):
    browser = p.chromium.launch(
        headless=True,
        channel="chrome",
        args=["--start-maximized"],
    )
    context = browser.new_context(
        viewport={"width": 1920, "height": 1080},
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    )
    return browser, context.new_page()


def _handle_cookie(page):
    try:
        btn = page.wait_for_selector("button:has-text('Accept'), button:has-text('accept')", timeout=5000)
        if btn:
            btn.click()
            page.wait_for_timeout(1000)
    except Exception:
        pass


def _handle_modal(page):
    try:
        page.wait_for_timeout(2000)
        uk = page.wait_for_selector("text=United Kingdom", timeout=10000)
        if uk:
            uk.click()
            page.wait_for_timeout(2000)
        try:
            inter = page.wait_for_selector("text=Intermediaries", timeout=5000)
            if inter:
                inter.click()
                page.wait_for_timeout(2000)
        except Exception:
            pass
        try:
            accept = page.wait_for_selector(
                "button:has-text('I Agree'), button:has-text('Accept'), button:has-text('Confirm')",
                timeout=5000,
            )
            if accept:
                accept.click()
                page.wait_for_timeout(2000)
        except Exception:
            pass
    except Exception as e:
        print(f"[scraper] Modal warning: {e}")


def _click_factsheet_tab(page):
    try:
        tab = page.wait_for_selector("text=Factsheet", timeout=10000)
        if tab:
            tab.click()
            page.wait_for_timeout(3000)
    except Exception:
        pass


def _scroll_page(page, steps=5):
    for _ in range(steps):
        page.evaluate("window.scrollBy(0, 500)")
        page.wait_for_timeout(500)
    page.evaluate("window.scrollTo(0, 0)")
    page.wait_for_timeout(1000)


def _find_pdf_url(page) -> str:
    """Return the direct URL of the Institutional Factsheet PDF."""
    try:
        doc_tab = page.get_by_role("tab", name="Document library")
        doc_tab.click()
        page.wait_for_timeout(2000)

        link = page.locator("a:has-text('Institutional Factsheet')").first
        if link.count():
            href = link.get_attribute("href")
            if href:
                return href if href.startswith("http") else "https://www.ashmoregroup.com" + href

        # Fallback: scan all links
        for a in page.locator("a").all():
            href = a.get_attribute("href") or ""
            text = a.inner_text().strip().lower()
            if ("institutional" in text or "institutional" in href.lower()) and "factsheet" in href.lower():
                return href if href.startswith("http") else "https://www.ashmoregroup.com" + href
    except Exception as e:
        print(f"[scraper] PDF URL search error: {e}")
    return ""


def _download_pdf(url: str, downloads_dir: str, timestamp: str) -> str:
    filename = os.path.join(downloads_dir, f"ASHMORE_Factsheet_{timestamp}.pdf")
    resp = requests.get(url, timeout=60)
    if resp.status_code == 200:
        with open(filename, "wb") as f:
            f.write(resp.content)
        print(f"[scraper] PDF saved: {filename} ({len(resp.content):,} bytes)")
        return filename
    raise RuntimeError(f"[scraper] PDF download failed: HTTP {resp.status_code}")


# ── public interface ──────────────────────────────────────────────────────────

def fetch_data(downloads_dir: str = "downloads") -> str:
    """
    Download the Ashmore Institutional Factsheet PDF.

    Saves the PDF to downloads_dir and returns its absolute path.
    """
    from playwright.sync_api import sync_playwright

    os.makedirs(downloads_dir, exist_ok=True)
    abs_dir = os.path.abspath(downloads_dir)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    with sync_playwright() as p:
        browser, page = _launch_browser(p)
        try:
            print(f"[scraper] Navigating to {PAGE_URL}")
            page.goto(PAGE_URL, wait_until="networkidle", timeout=30000)

            _handle_cookie(page)
            _handle_modal(page)
            _click_factsheet_tab(page)
            _scroll_page(page)

            pdf_url = _find_pdf_url(page)
            if not pdf_url:
                raise RuntimeError("[scraper] Could not find Institutional Factsheet PDF link")

            print(f"[scraper] PDF URL: {pdf_url}")
        finally:
            browser.close()

    return _download_pdf(pdf_url, abs_dir, timestamp)
