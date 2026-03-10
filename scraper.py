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

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DARTSATLAS_EMAIL = os.environ.get("DARTSATLAS_EMAIL")
DARTSATLAS_PASSWORD = os.environ.get("DARTSATLAS_PASSWORD")
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON")

ORG_ID = "UCypblAwtczg"
BASE_URL = "https://www.dartsatlas.com"
EXPORT_CSV_PATH = f"/o/{ORG_ID}/membership_export.csv"

# Start date for backfilling
START_YEAR = 2021
START_MONTH = 12

# CSV columns (known from inspection)
CSV_COLUMNS = ["email", "first", "last", "region", "joined"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def get_months_range():
    """Generate list of (year, month) tuples from START to current month."""
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
    """Return full month name from number."""
    return datetime(2000, month_num, 1).strftime("%B")


def build_csv_url(year, month, active_only=False):
    """Build the direct CSV download URL."""
    if active_only:
        return f"{BASE_URL}{EXPORT_CSV_PATH}?active_only=true&date%5Bmonth%5D={month}&date%5Byear%5D={year}"
    else:
        return f"{BASE_URL}{EXPORT_CSV_PATH}?date%5Bmonth%5D={month}&date%5Byear%5D={year}"


# ---------------------------------------------------------------------------
# Browser setup & login
# ---------------------------------------------------------------------------

def create_driver():
    """Create a headless Chrome driver."""
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
    """
    Log in to DartsAtlas via the sign-in form.
    """
    log.info("Navigating to DartsAtlas sign-in page...")
    driver.get(f"{BASE_URL}/sign_in")
    time.sleep(2)

    wait = WebDriverWait(driver, 15)

    # Find and fill email field
    try:
        email_field = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR,
                 "input[type='email'], input[name='email'], "
                 "input[name='user[email]'], input[name='session[email]']")
            )
        )
    except Exception:
        email_field = driver.find_element(By.CSS_SELECTOR, "input[type='text'], input[type='email']")

    email_field.clear()
    email_field.send_keys(DARTSATLAS_EMAIL)
    log.info("Entered email.")

    # Find and fill password field
    password_field = driver.find_element(By.CSS_SELECTOR, "input[type='password']")
    password_field.clear()
    password_field.send_keys(DARTSATLAS_PASSWORD)
    log.info("Entered password.")

    # Submit the form
    submit_bFâÒG&fW"æfæEöVÆVÖVçB¢'ä555õ4TÄT5Dõ"Â&'WGFöå·GSÒw7V&ÖBuÒÂçWE·GSÒw7V&ÖBuÒ ¢¢7V&ÖEö'Fâæ6Æ6²¢Æörææfò$Æövâf÷&Ò7V&ÖGFVBâ"¢FÖRç6ÆVW2 ¢2fW&gÆövâ'æfvFærFòFRÖVÖ&W'6W÷'BvP¢G&fW"ævWBb'´$4UõU$ÇÒöò÷´õ$uôGÒöÖVÖ&W'6öW÷'B"¢FÖRç6ÆVW" ¢b&ÖVÖ&W'6öW÷'B"âG&fW"æ7W'&VçE÷W&Ã ¢Æörææfò$Æövâ7V66W76gVÂ"¢VÇ6S ¢ÆöræW'&÷"$ÆövâÖfRfÆVBâ7W'&VçBU$Ã¢W2"ÂG&fW"æ7W'&VçE÷W&Â¢&6R'VçFÖTW'&÷"b$ÆövâfÆVBâVæFVBWC¢¶G&fW"æ7W'&VçE÷W&ÇÒ"  ¦FVbfWF6ö77e÷fö'&÷w6W"G&fW"ÂW&Â ¢"" ¢W6RFRWFVçF6FVB6VÆVæVÒ6W76öâFòfWF655bf¦f67&BfWF6à¢F2ÆWfW&vW2FRW7Fær6W76öâ6öö¶W2vF÷WBæVVFærFòæfvFR÷ ¢F÷væÆöBfÆW2(	B×V6f7FW"Fâ6Æ6¶ærF&÷VvFRTf÷"V6ÖöçFà¢"" ¢67&BÒb"" ¢&WGW&â7æ2Óâ·°¢G'·°¢6öç7B&W7ÒvBfWF6w·W&ÇÒr°¢b&W7æö²·°¢&WGW&âtU%$õ#¢r²&W7ç7FGW2²rr²&W7ç7FGW5FWC°¢×Ð¢6öç7BFWBÒvB&W7çFWB°¢&WGW&âFWC°¢×Ò6F6R·°¢&WGW&âtU%$õ#¢r²RæÖW76vS°¢×Ð¢×Ò°¢"" ¢&W7VÇBÒG&fW"æWV7WFU÷67&B67&B ¢b&W7VÇBæB&W7VÇBç7F'G7vF$U%$õ#¢" ¢ÆöræW'&÷""fWF6fÆVC¢W2"Â&W7VÇB¢&WGW&âæöæP ¢&WGW&â&W7VÇ@  ¢2ÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÐ¢255b&ö6W76æp¢2ÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÐ ¦FVb'6Uö77e÷FWB77e÷FWB ¢""%'6R55bFWBæB&WGW&âVFW'2²&÷w2â"" ¢bæ÷B77e÷FWB÷"77e÷FWBç7G&ÓÒ"# ¢&WGW&âµÒÂµÐ ¢&VFW"Ò77bç&VFW"7G&ætò77e÷FWB¢VFW'2ÒæWB&VFW"ÂµÒ¢&÷w2ÒÆ7B&VFW" ¢2fÇFW"÷WBV×G&÷w0¢&÷w2Ò·"f÷""â&÷w2bç6VÆÂç7G&f÷"6VÆÂâ"Ð ¢&WGW&âVFW'2Â&÷w0  ¦FVbFVGWÆ6FUö'öVÖÂ&÷w2ÂVFW'2 ¢"" ¢FVGWÆ6FR&÷w2'VÖÂFG&W72f'7B6öÇVÖâà¢¶VW2FRÄDU5BVçG''¦öæVBFFRf÷"V6VÖÂà¢"" ¢bæ÷B&÷w3 ¢&WGW&â&÷w0 ¢VÖÅö6öÂÒ2VÖÂ2Çv2f'7B6öÇVÖà¢VFW'5öÆ÷vW"Ò¶æÆ÷vW"ç7G&f÷"âVFW'5Ð¢¦öæVEö6öÂÒVFW'5öÆ÷vW"ææFW&¦öæVB"b&¦öæVB"âVFW'5öÆ÷vW"VÇ6RÓ ¢6VVâÒ·Ð¢f÷"&÷râ&÷w3 ¢bÆVâ&÷rÃÒVÖÅö6öÃ ¢6öçFçVP ¢VÖÂÒ&÷u¶VÖÅö6öÅÒç7G&æÆ÷vW"¢bæ÷BVÖÃ ¢6öçFçVP ¢bVÖÂâ6VVã ¢2¶VWFRöæRvFFRÆFW"¦öæVBFFP¢b¦öæVEö6öÂãÒæBÆVâ&÷râ¦öæVEö6öÃ ¢W7FæuöFFRÒ6VVå¶VÖÅÕ¶¦öæVEö6öÅÒbÆVâ6VVå¶VÖÅÒâ¦öæVEö6öÂVÇ6R" ¢æWuöFFRÒ&÷u¶¦öæVEö6öÅÐ¢bæWuöFFRâW7FæuöFFS ¢6VVå¶VÖÅÒÒ&÷p¢VÇ6S ¢6VVå¶VÖÅÒÒ&÷p ¢&WGW&âÆ7B6VVâçfÇVW2  ¢2ÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÐ¢2vöövÆR6VWG0¢2ÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÒÐ ¦FVbvWEövöövÆU÷6VWG5ö6ÆVçB ¢""$WFVçF6FRæB&WGW&âw7&VB6ÆVçBâ"" ¢7&VG5öF7BÒ§6öâæÆöG2tôôtÄUô5$TDTåDÅ5ô¥4ôâ¢66÷W2Ò°¢&GG3¢ò÷wwrævöövÆV2æ6öÒöWF÷7&VG6VWG2"À¢&GG3¢ò÷wwrævöövÆV2æ6öÒöWFöG&fR"À¢Ð¢7&VFVçFÇ2Ò7&VFVçFÇ2æg&öÕ÷6W'f6Uö66÷VçEöæfò7&VG5öF7BÂ66÷W3×66÷W2¢&WGW&âw7&VBæWF÷&¦R7&VFVçFÇ2  ¦FVb6öÅöÆWGFW"â ¢""$6öçfW'BÖ&6VB6öÇVÖâçVÖ&W"FòÆWGFW"2â(i$Â#n(i%¢Â#~(i$â"" ¢&W7VÇBÒ" ¢vÆRââ ¢âÂ&VÖæFW"ÒFfÖöBâÒÂ#b¢&W7VÇBÒ6"cR²&VÖæFW"²&W7VÇ@¢&WGW&â&W7VÇ@  ¦FVbW6÷Fõ÷6VWB6ÆVçBÂ6VWEöBÂF%öæÖRÂVFW'2Â&÷w2 ¢"" ¢w&FRVFW'2²&÷w2Fò7V6f2F"âvöövÆR6VWBà¢7&VFW2FRF"bBFöW6âwBW7Bâ6ÆV'2W7FærFFf'7Bà¢"" ¢7&VG6VWBÒ6ÆVçBæ÷Våö'ö¶W6VWEöB ¢2vWB÷"7&VFRv÷&·6VW@¢G' ¢v÷&·6VWBÒ7&VG6VWBçv÷&·6VWBF%öæÖR¢W6WBw7&VBåv÷&·6VWDæ÷Df÷VæC ¢v÷&·6VWBÒ7&VG6VWBæFE÷v÷&·6VWB¢FFÆS×F%öæÖRÀ¢&÷w3ÖÖÆVâ&÷w2²ÂÀ¢6öÇ3ÖÖÆVâVFW'2ÂÀ¢¢Æörææfò$7&VFVBæWrv÷&·6VWC¢W2"ÂF%öæÖR ¢v÷&·6VWBæ6ÆV" ¢F÷FÅ÷&÷w2ÒÆVâ&÷w2²¢F÷FÅö6öÇ2ÒÆVâVFW'2 ¢bv÷&·6VWBç&÷_count < total_rows:
        worksheet.resize(rows=total_rows)
    if worksheet.col_count < total_cols:
        worksheet.resize(cols=total_cols)

    # Write in batches of 500 rows
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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # Validate env vars
    missing = []
    for var in ["DARTSATLAS_EMAIL", "DARTSATLAS_PASSWORD", "GOOGLE_SHEET_ID", "GOOGLE_CREDENTIALS_JSON"]:
        if not os.environ.get(var):
            missing.append(var)
    if missing:
        log.error("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(1)

    months = get_months_range()
    log.info("Processing %d months: %s %d â %s %d",
             len(months),
             month_name(months[0][1]), months[0][0],
             month_name(months[-1][1]), months[-1][0])

    # ---- Step 1: Login ----
    driver = create_driver()
    try:
        login(driver)

        # Stay on DartsAtlas domain so fetch() has session cookies
        driver.get(f"{BASE_URL}/o/{ORG_ID}/membership_export")
        time.sleep(2)

        # ---- Step 2: Fetch all CSVs ----
        all_members_rows = []
        active_members_rows = []
        headers = CSV_COLUMNS

        for year, month in months:
            label = f"{month_name(month)} {year}"

            # All members
            url_all = build_csv_url(year, month, active_only=False)
            log.info("[%s] Fetching all members...", label)
            csv_text = fetch_csv_via_browser(driver, url_all)
            if csv_text:
                h, rows = parse_csv_text(csv_text)
                if h:
                    headers = h
                all_members_rows.extend(rows)
                log.info("[%s]   â %d rows (total: %d)", label, len(rows), len(all_members_rows))
            else:
                log.warning("[%s]   â FAILED", label)

            # Active members
            url_active = build_csv_url(year, month, active_only=True)
            log.info("[%s] Fetching active members...", label)
            csv_text = fetch_csv_via_browser(driver, url_active)
            if csv_text:
                h, rows = parse_csv_text(csv_text)
                active_members_rows.extend(rows)
                log.info("[%s]   â %d rows (total: %d)", label, len(rows), len(active_members_rows))
            else:
                log.warning("[%s]   â FAILED", label)

            time.sleep(0.5)

    finally:
        driver.quit()
        log.info("Browser closed.")

    # ---- Step 3: Deduplicate ----
    log.info("Deduplicating all members: %d â ...", len(all_members_rows))
    all_deduped = deduplicate_by_email(all_members_rows, headers)
    log.info("  â %d unique members", len(all_deduped))

    log.info("Deduplicating active members: %d â ...", len(active_members_rows))
    active_deduped = deduplicate_by_email(active_members_rows, headers)
    log.info("  â %d unique active members", len(active_deduped))

    # Sort by joined date (newest first)
    joined_col = headers.index("joined") if "joined" in headers else -1
    if joined_col >= 0:
        all_deduped.sort(key=lambda r: r[joined_col] if len(r) > joined_col else "", reverse=True)
        active_deduped.sort(key=lambda r: r[joined_col] if len(r) > joined_col else "", reverse=True)

    # ---- Step 4: Push to Google Sheets ----
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
