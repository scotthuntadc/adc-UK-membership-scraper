"""
DartsAtlas Entry List Fetcher
==============================
Fetches the entry list from a DartsAtlas tournament/event URL.
Uses undetected_chromedriver to bypass Cloudflare.
Pushes results to Supabase for the Admin Hub eligibility checker.

Usage: python fetch_entries.py <dartsatlas_url> [request_id]
"""

import os
import sys
import json
import time
import logging
import urllib.request
import urllib.error
import urllib.parse

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DARTSATLAS_EMAIL = os.environ.get("DARTSATLAS_EMAIL")
DARTSATLAS_PASSWORD = os.environ.get("DARTSATLAS_PASSWORD")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "").strip()
BASE_URL = "https://www.dartsatlas.com"


def create_driver():
    import subprocess
    try:
        result = subprocess.run(["google-chrome", "--version"], capture_output=True, text=True)
        chrome_version = int(result.stdout.strip().split()[-1].split(".")[0])
        log.info("Chrome version: %d", chrome_version)
    except Exception:
        chrome_version = None

    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    driver = uc.Chrome(options=options, version_main=chrome_version)
    driver.implicitly_wait(10)
    return driver


def login(driver):
    log.info("Navigating to sign-in page...")
    driver.get(f"{BASE_URL}/users/sign_in")

    log.info("Waiting for Cloudflare...")
    for attempt in range(30):
        time.sleep(1)
        if "Just a moment" not in driver.title:
            log.info("Cloudflare resolved after %d seconds", attempt + 1)
            break
    else:
        raise RuntimeError("Cloudflare challenge did not resolve")

    time.sleep(2)
    wait = WebDriverWait(driver, 30)

    email_field = wait.until(EC.presence_of_element_located((By.ID, "user_email")))
    email_field.clear()
    email_field.send_keys(DARTSATLAS_EMAIL)

    password_field = driver.find_element(By.ID, "user_password")
    password_field.clear()
    password_field.send_keys(DARTSATLAS_PASSWORD)

    submit_btn = driver.find_element(By.CSS_SELECTOR,
        "#new_user input[type='submit'], #new_user button[type='submit'], #new_user [name='commit']")
    submit_btn.click()
    time.sleep(5)

    log.info("Post-login URL: %s", driver.current_url)
    if "sign_in" in driver.current_url:
        raise RuntimeError("Login failed")
    log.info("Login successful!")


def fetch_entries(driver, url):
    log.info("Fetching entries from: %s", url)

    if not url.rstrip("/").endswith("/entries"):
        url = url.rstrip("/") + "/entries"

    driver.get(url)
    time.sleep(3)

    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "tournament-entry-list"))
        )
    except Exception:
        log.warning("tournament-entry-list not found, trying to parse anyway")

    # Extract title
    try:
        title_el = driver.find_element(By.TAG_NAME, "h1")
        title = title_el.text.strip()
    except Exception:
        title = "Unknown Event"

    log.info("Event title: %s", title)

    # Extract players
    players = []
    seen_ids = set()

    links = driver.find_elements(By.CSS_SELECTOR, "a.user.name-and-photo")
    log.info("Found %d player links", len(links))

    for link in links:
        try:
            href = link.get_attribute("href") or ""
            if "/players/" in href:
                player_id = href.split("/players/")[-1].split("/")[0].split("?")[0]
            else:
                continue

            try:
                name_span = link.find_element(By.TAG_NAME, "span")
                name = name_span.text.strip()
            except Exception:
                try:
                    img = link.find_element(By.TAG_NAME, "img")
                    name = img.get_attribute("alt") or ""
                except Exception:
                    name = ""

            if player_id and name and player_id not in seen_ids:
                seen_ids.add(player_id)
                players.append({"id": player_id, "name": name.strip()})
        except Exception as e:
            log.warning("Error extracting player: %s", e)

    log.info("Extracted %d unique players", len(players))
    return {"title": title, "players": players, "count": len(players), "url": url}


def supabase_request(method, path, body=None):
    """Make a request to Supabase REST API."""
    # Build URL carefully — GitHub Actions masks secrets in strings,
    # so we construct it at runtime to avoid the full URL being masked
    base = SUPABASE_URL.rstrip("/")
    url = base + "/rest/v1/" + path
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": "Bearer " + SUPABASE_KEY,
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        resp = urllib.request.urlopen(req)
        log.info("  Supabase %s → %d", method, resp.status)
        return True
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace")
        log.error("  Supabase %s → %d: %s", method, e.code, body_text[:300])
        return False
    except Exception as e:
        log.error("  Supabase %s → error: %s", method, str(e))
        return False


def push_to_supabase(result, request_id):
    """Push the entry list result to Supabase for the Admin Hub to pick up."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.warning("No Supabase credentials, skipping push")
        return

    log.info("Pushing results to Supabase (request_id=%s)...", request_id)
    log.info("  Supabase URL length: %d, Key length: %d", len(SUPABASE_URL), len(SUPABASE_KEY))

    try:
        # Delete any existing result with this request_id
        encoded_id = urllib.parse.quote(request_id, safe="")
        supabase_request("DELETE", "eligibility_lists?list_type=eq.entry_check&name=eq." + encoded_id)

        # Insert the result
        row = {
            "name": request_id,
            "description": result.get("title", "Unknown Event"),
            "list_type": "entry_check",
            "region": None,
            "player_ids": [p["id"] for p in result.get("players", [])],
            "player_data": result.get("players", []),
        }

        if supabase_request("POST", "eligibility_lists", row):
            log.info("Successfully pushed %d players to Supabase", len(result.get("players", [])))
        else:
            log.error("Failed to push to Supabase — check credentials")
    except Exception as e:
        log.error("Exception pushing to Supabase: %s", e)
        # Don't crash — still print the result to stdout
        log.info("Players were fetched successfully but could not be saved.")


def main():
    if len(sys.argv) < 2:
        print("Usage: python fetch_entries.py <dartsatlas_url> [request_id]")
        sys.exit(1)

    event_url = sys.argv[1]
    request_id = sys.argv[2] if len(sys.argv) > 2 else f"manual-{int(time.time())}"

    log.info("=" * 60)
    log.info("DartsAtlas Entry Fetcher")
    log.info("  URL: %s", event_url)
    log.info("  Request ID: %s", request_id)
    log.info("  Supabase: %s", "configured" if SUPABASE_URL else "NOT configured")
    log.info("=" * 60)

    driver = create_driver()
    try:
        login(driver)
        result = fetch_entries(driver, event_url)
        print(json.dumps(result, indent=2))
        push_to_supabase(result, request_id)
    finally:
        driver.quit()
        log.info("Browser closed.")

    log.info("=" * 60)
    log.info("COMPLETE — %d players found", result.get("count", 0))
    log.info("=" * 60)


if __name__ == "__main__":
    main()
