"""
DartsAtlas Player Denial Script
================================
Logs into DartsAtlas and denies specified players from an event's check-in page.

Usage: python deny_players.py <event_url> <player_ids_comma_separated> <da_email> <da_password>
"""

import os
import sys
import json
import time
import logging

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

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


def login(driver, email, password):
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
    email_field.send_keys(email)

    password_field = driver.find_element(By.ID, "user_password")
    password_field.clear()
    password_field.send_keys(password)

    submit_btn = driver.find_element(By.CSS_SELECTOR,
        "#new_user input[type='submit'], #new_user button[type='submit'], #new_user [name='commit']")
    submit_btn.click()
    time.sleep(5)

    if "sign_in" in driver.current_url:
        raise RuntimeError("Login failed")
    log.info("Login successful!")


def deny_players(driver, event_url, player_ids):
    """Navigate to the event check-in page and deny specified players."""

    # Navigate to check-in page
    checkin_url = event_url.rstrip("/")
    if not checkin_url.endswith("/check_in"):
        checkin_url += "/check_in"

    log.info("Navigating to check-in page: %s", checkin_url)
    driver.get(checkin_url)
    time.sleep(3)

    # Wait for page to load
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".check-in-list, .tournament-check-in, [data-controller]"))
        )
    except Exception:
        log.warning("Check-in list element not found, trying anyway...")

    denied = []
    not_found = []
    errors = []

    for player_id in player_ids:
        try:
            # Look for the player's deny button
            # DartsAtlas check-in pages have player rows with check-in/deny buttons
            # Try multiple selector patterns
            deny_btn = None

            # Pattern 1: Link with player ID in the row
            try:
                player_row = driver.find_element(By.CSS_SELECTOR, f"a[href*='/players/{player_id}']")
                # Find the parent row/container
                row = player_row
                for _ in range(5):
                    row = row.find_element(By.XPATH, "..")
                    try:
                        deny_btn = row.find_element(By.CSS_SELECTOR,
                            "a[data-method='patch'][href*='deny'], "
                            "button[data-action*='deny'], "
                            "a.deny, "
                            "a[href*='deny'], "
                            "input[value*='Deny'], "
                            "button:not(.check-in)")
                        if 'deny' in (deny_btn.get_attribute('class') or '').lower() or \
                           'deny' in (deny_btn.get_attribute('href') or '').lower() or \
                           'deny' in (deny_btn.text or '').lower():
                            break
                        deny_btn = None
                    except Exception:
                        continue
            except Exception:
                pass

            # Pattern 2: Try finding deny link directly associated with player ID
            if not deny_btn:
                try:
                    deny_btn = driver.find_element(By.CSS_SELECTOR, f"a[href*='{player_id}'][href*='deny']")
                except Exception:
                    pass

            # Pattern 3: Use XPath to find deny near player name
            if not deny_btn:
                try:
                    elements = driver.find_elements(By.XPATH,
                        f"//a[contains(@href, '/players/{player_id}')]/ancestor::*[position() <= 5]//a[contains(@href, 'deny') or contains(@class, 'deny')]")
                    if elements:
                        deny_btn = elements[0]
                except Exception:
                    pass

            if deny_btn:
                deny_btn.click()
                time.sleep(1)

                # Handle confirmation dialog if present
                try:
                    confirm = WebDriverWait(driver, 2).until(
                        EC.alert_is_present()
                    )
                    confirm.accept()
                    time.sleep(0.5)
                except Exception:
                    pass

                denied.append(player_id)
                log.info("  DENIED: %s", player_id)
            else:
                not_found.append(player_id)
                log.warning("  NOT FOUND: %s (no deny button)", player_id)

        except Exception as e:
            errors.append({"id": player_id, "error": str(e)})
            log.error("  ERROR denying %s: %s", player_id, e)

    return {
        "denied": denied,
        "not_found": not_found,
        "errors": errors,
        "total_requested": len(player_ids),
    }


def main():
    if len(sys.argv) < 5:
        print("Usage: python deny_players.py <event_url> <player_ids_csv> <da_email> <da_password>")
        sys.exit(1)

    event_url = sys.argv[1]
    player_ids = [pid.strip() for pid in sys.argv[2].split(",") if pid.strip()]
    da_email = sys.argv[3]
    da_password = sys.argv[4]

    log.info("=" * 60)
    log.info("DartsAtlas Player Denial")
    log.info("  Event: %s", event_url)
    log.info("  Players to deny: %d", len(player_ids))
    log.info("=" * 60)

    driver = create_driver()
    try:
        login(driver, da_email, da_password)
        result = deny_players(driver, event_url, player_ids)
    finally:
        driver.quit()
        log.info("Browser closed.")

    # Output result as JSON
    print(json.dumps(result))

    log.info("=" * 60)
    log.info("COMPLETE — %d denied, %d not found, %d errors",
             len(result["denied"]), len(result["not_found"]), len(result["errors"]))
    log.info("=" * 60)


if __name__ == "__main__":
    main()
