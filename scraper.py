"""
ADC Membership Scraper
======================
Scrapes membership data from DartsAtlas membership export page,
downloads CSVs for all months from Dec 2021 to present,
and pushes data to Google Sheets (All Memberships + Active Members tabs).

Designed to run daily via GitHub Actions.

CSV format: email, first, last, region, joined
URL patterns:
  All members:    /o/UCypblAwtczg/membership_export.csv?date[month]=M&date[year]=YYYY
  Active only:    /o/UCypblAwtczg/membership_export.csv?active_only=true&date[month]=M&date[year]=YYYY
"""

import os
import sys
import csv
import time
import json
import logging
from datetime import datetime, date
from io import StringIO

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import gspread
from google.oauth2.service_account import Credentials

DARTSATLAS_EMAIL = os.environ.get("DARTSATLAS_EMAIL")
DARTSATLAS_PASSWORD = os.environ.get("DARTSATLAS_PASSWORD")
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON")

ORG_ID = "UCypblAwtczg"
BASE_URL = "https://www.dartsatlas.com"
EXPORT_CSV_PATH = f"/o/{ORG_ID}/membership_export.csv"

START_YEAR = 2021
START_MONTH = 12
CSV_COLUMNS = ["email", "first", "last", "region", "joined"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def get_months_range():
    today = date.today()
    months = []
    year, month = START_YEAR, START_MONTH
    while (year, month) <= (today.year, today.month):
        months.append((year, month))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return months


def month_name(month_num):
    return datetime(2000, month_num, 1).strftime("%B")


def build_csv_url(year, month, active_only=False):
    if active_only:
        return f"{BASE_URL}{EXPORT_CSV_PATH}?active_only=true&date%5Bmonth%5D={month}&date%5Byear%5D={year}"
    else:
        return f"{BASE_URL}{EXPORT_CSV_PATH}?date%5Bmonth%5D={month}&date%5Byear%5D={year}"


def create_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=chrome_options)
    driver.implicitly_wait(10)
    return driver


def login(driver):
    log.info("Navigating to DartsAtlas sign-in page...")
    driver.get(f"{BASE_URL}/sign_in")
    time.sleep(2)
    wait = WebDriverWait(driver, 15)
    try:
        email_field = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "input[type='email'], input[name='email'], input[name='user[email]'], input[name='session[email]']")
        ))
    except Exception:
        email_field = driver.find_element(By.CSS_SELECTOR, "input[type='text'], input[type='email']")
    email_field.clear()
    email_field.send_keys(DARTSATLAS_EMAIL)
    password_field = driver.find_element(By.CSS_SELECTOR, "input[type='password']")
    password_field.clear()
    password_field.send_keys(DARTSATLAS_PASSWORD)
    submit_btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit'], input[type='submit']")
    submit_btn.click()
    log.info("Login form submitted.")
    time.sleep(3)
    driver.get(f"{BASE_URL}/o/{ORG_ID}/membership_export")
    time.sleep(2)
    if "membership_export" in driver.current_url:
        log.info("Login successful!")
    else:
        raise RuntimeError(f"Login failed. Ended up at: {driver.current_url}")


def fetch_csv_via_browser(driver, url):
    script = f"""
    return (async () => {{
        try {{
            const resp = await fetch('{url}');
            if (!resp.ok) {{ return 'ERROR:' + resp.status + ' ' + resp.statusText; }}
            return await resp.text();
        }} catch (e) {{ return 'ERROR:' + e.message; }}
    }})();
    """
    result = driver.execute_script(script)
    if result and result.startswith("ERROR:"):
        log.error("  Fetch failed: %s", result)
        return None
    return result


def parse_csv_text(csv_text):
    if not csv_text or csv_text.strip() == "":
        return [], []
    reader = csv.reader(StringIO(csv_text))
    headers = next(reader, [])
    rows = list(reader)
    rows = [r for r in rows if any(cell.strip() for cell in r)]
    return headers, rows


def deduplicate_by_email(rows, headers):
    if not rows:
        return rows
    email_col = 0
    headers_lower = [h.lower().strip() for h in headers]
    joined_col = headers_lower.index("joined") if "joined" in headers_lower else -1
    seen = {}
    for row in rows:
        if len(row) <= email_col:
            continue
        email = row[email_col].strip().lower()
        if not email:
            continue
        if email in seen:
            if joined_col >= 0 and len(row) > joined_col:
                existing_date = seen[email][joined_col] if len(seen[email]) > joined_col else ""
                if row[joined_col] > existing_date:
                    seen[email] = row
        else:
            seen[email] = row
    return list(seen.values())


def get_google_sheets_client():
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)


def col_letter(n):
    result = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result


def push_to_sheet(client, sheet_id, tab_name, headers, rows):
    spreadsheet = client.open_by_key(sheet_id)
    try:
        worksheet = spreadsheet.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=tab_name, rows=max(len(rows)+1, 100), cols=max(len(headers), 10))
        log.info("Created new worksheet: %s", tab_name)
    worksheet.clear()
    total_rows = len(rows) + 1
    total_cols = len(headers)
    if worksheet.row_count < total_rows:
        worksheet.resize(rows=total_rows)
    if worksheet.col_count < total_cols:
        worksheet.resize(cols=total_cols)
    BATCH_SIZE = 500
    all_data = [headers] + rows
    end_col = col_letter(total_cols)
    for i in range(0, len(all_data), BATCH_SIZE):
        batch = all_data[i : i + BATCH_SIZE]
        start_row = i + 1
        end_row = start_row + len(batch) - 1
        range_name = f"A{start_row}:{end_col}{end_row}"
        worksheet.update(range_name=range_name, values=batch)
        log.info("  Written rows %d-%d to '%s'", start_row, end_row, tab_name)
        time.sleep(1)
    log.info("Pushed %d rows (+ header) to '%s'", len(rows), tab_name)


def main():
    missing = []
    for var in ["DARTSATLAS_EMAIL", "DARTSATLAS_PASSWORD", "GOOGLE_SHEET_ID", "GOOGLE_CREDENTIALS_JSON"]:
        if not os.environ.get(var):
            missing.append(var)
    if missing:
        log.error("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(1)
    months = get_months_range()
    log.info("Processing %d months: %s %d to %s %d", len(months),
             month_name(months[0][1]), months[0][0], month_name(months[-1][1]), months[-1][0])
    driver = create_driver()
    try:
        login(driver)
        driver.get(f"{BASE_URL}/o/{ORG_ID}/membership_export")
        time.sleep(2)
        all_members_rows = []
        active_members_rows = []
        headers = CSV_COLUMNS
        for year, month in months:
            label = f"{month_name(month)} {year}"
            url_all = build_csv_url(year, month, active_only=False)
            log.info("[%s] Fetching all members...", label)
            csv_text = fetch_csv_via_browser(driver, url_all)
            if csv_text:
                h, rows = parse_csv_text(csv_text)
                if h: headers = h
                all_members_rows.extend(rows)
                log.info("[%s]   -> %d rows (total: %d)", label, len(rows), len(all_members_rows))
            else:
                log.warning("[%s]   -> FAILED", label)
            url_active = build_csv_url(year, month, active_only=True)
            log.info("[%s] Fetching active members...", label)
            csv_text = fetch_csv_via_browser(driver, url_active)
            if csv_text:
                h, rows = parse_csv_text(csv_text)
                active_members_rows.extend(rows)
                log.info("[%s]   -> %d rows (total: %d)", label, len(rows), len(active_members_rows))
            else:
                log.warning("[%s]   -> FAILED", label)
            time.sleep(0.5)
    finally:
        driver.quit()
        log.info("Browser closed.")
    log.info("Deduplicating all members: %d rows...", len(all_members_rows))
    all_deduped = deduplicate_by_email(all_members_rows, headers)
    log.info("  -> %d unique members", len(all_deduped))
    log.info("Deduplicating active members: %d rows...", len(active_members_rows))
    active_deduped = deduplicate_by_email(active_members_rows, headers)
    log.info("  -> %d unique active members", len(active_deduped))
    joined_col = headers.index("joined") if "joined" in headers else -1
    if joined_col >= 0:
        all_deduped.sort(key=lambda r: r[joined_col] if len(r) > joined_col else "", reverse=True)
        active_deduped.sort(key=lambda r: r[joined_col] if len(r) > joined_col else "", reverse=True)
    log.info("Connecting to Google Sheets...")
    gs_client = get_google_sheets_client()
    if all_deduped:
        push_to_sheet(gs_client, GOOGLE_SHEET_ID, "All Memberships", headers, all_deduped)
    if active_deduped:
        push_to_sheet(gs_client, GOOGLE_SHEET_ID, "Active Members", headers, active_deduped)
    log.info("=" * 60)
    log.info("COMPLETE")
    log.info("  All-time members:  %d", len(all_deduped))
    log.info("  Active members:    %d", len(active_deduped))
    log.info("=" * 60)


if __name__ == "__main__":
    main()
