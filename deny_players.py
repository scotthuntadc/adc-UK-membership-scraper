"""
DartsAtlas Player Denial Script
================================
Logs into DartsAtlas and clicks the cross (X/deny) button on the check-in page
for specified players.

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
    """Navigate to check-in page and click the cross/deny button for each player."""

    checkin_url = event_url.rstrip("/")
    if not checkin_url.endswith("/check_in"):
        checkin_url += "/check_in"

    log.info("Navigating to check-in page: %s", checkin_url)
    driver.get(checkin_url)
    time.sleep(3)

    # Wait for page to load
    WebDriverWait(driver, 15).until(
        lambda d: len(d.find_elements(By.CSS_SELECTOR, "a[href*='/players/']")) > 0
    )
    time.sleep(1)

    # First: analyze the page structure to understand the deny button
    log.info("Analyzing page structure...")
    page_source = driver.page_source

    # Log what clickable elements exist near player links
    sample_players = driver.find_elements(By.CSS_SELECTOR, "a[href*='/players/']")[:2]
    for sp in sample_players:
        try:
            # Go up to find the row container
            row = sp
            for _ in range(6):
                row = row.find_element(By.XPATH, "..")
                # Check for clickable elements in this container
                clickables = row.find_elements(By.CSS_SELECTOR, "a, button, input, svg, [role='button']")
                if len(clickables) > 2:  # Found a row with multiple interactive elements
                    log.info("  Row tag: %s, class: %s", row.tag_name, (row.get_attribute("class") or "")[:80])
                    for c in clickables:
                        tag = c.tag_name
                        cls = (c.get_attribute("class") or "")[:60]
                        href = (c.get_attribute("href") or "")[:80]
                        method = c.get_attribute("data-method") or ""
                        text = (c.text or "")[:30]
                        log.info("    %s class='%s' href='%s' method='%s' text='%s'", tag, cls, href, method, text)
                    break
        except Exception:
            pass

    denied = []
    not_found = []
    errors = []

    for player_ref in player_ids:
        try:
            is_name_match = player_ref.startswith("name:")
            player_name = player_ref[5:] if is_name_match else None
            player_id = player_ref if not is_name_match else None

            log.info("Denying player: %s", player_ref)

            # Find the player's link on the page
            player_link = None
            if player_id:
                try:
                    player_link = driver.find_element(By.CSS_SELECTOR, f"a[href*='/players/{player_id}']")
                except Exception:
                    pass

            if not player_link and player_name:
                # Find by name text
                links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/players/']")
                name_lower = player_name.lower().strip()
                for link in links:
                    link_text = link.text.strip().lower()
                    alt_text = ""
                    try:
                        img = link.find_element(By.TAG_NAME, "img")
                        alt_text = (img.get_attribute("alt") or "").strip().lower()
                    except Exception:
                        pass
                    if name_lower in link_text or name_lower in alt_text or link_text in name_lower:
                        player_link = link
                        log.info("  Matched by name: %s", link.text.strip())
                        break

            if not player_link:
                not_found.append(player_ref)
                log.warning("  Player not found on check-in page: %s", player_ref)
                continue

            # Find the row/container for this player
            row = player_link
            deny_clicked = False

            for _ in range(8):
                row = row.find_element(By.XPATH, "..")

                # Strategy 1: Look for links with data-method="patch" or "delete" (Rails convention)
                patch_links = row.find_elements(By.CSS_SELECTOR, "a[data-method='patch'], a[data-method='delete']")
                for link in patch_links:
                    href = (link.get_attribute("href") or "").lower()
                    cls = (link.get_attribute("class") or "").lower()
                    # The deny/cross link likely has 'deny', 'reject', 'remove', 'cancel' or is the second action link
                    if any(word in href for word in ['deny', 'reject', 'remove', 'cancel', 'decline']):
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", link)
                        time.sleep(0.3)
                        link.click()
                        deny_clicked = True
                        break
                    if any(word in cls for word in ['deny', 'reject', 'remove', 'cancel', 'decline', 'danger', 'negative', 'cross']):
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", link)
                        time.sleep(0.3)
                        link.click()
                        deny_clicked = True
                        break

                if deny_clicked:
                    break

                # Strategy 2: Look for the cross/X — could be an SVG, a span with X, or a button
                # In many UIs, the deny is the second button (first is tick/check-in)
                action_links = row.find_elements(By.CSS_SELECTOR, "a[data-method], button[data-action], a.btn, button.btn")
                if len(action_links) >= 2:
                    # The cross/deny is typically the second action (first is check-in/tick)
                    deny_link = action_links[1]  # Second action = deny
                    href = (deny_link.get_attribute("href") or "").lower()
                    # Verify it's not the check-in button
                    if 'check_in' not in href and 'approve' not in href:
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", deny_link)
                        time.sleep(0.3)
                        deny_link.click()
                        deny_clicked = True
                        break

                # Strategy 3: Find any element that looks like a cross/X
                cross_elements = row.find_elements(By.CSS_SELECTOR,
                    "[class*='cross'], [class*='deny'], [class*='reject'], "
                    "[class*='remove'], [class*='close'], [class*='cancel'], "
                    "[class*='negative'], [class*='danger']")
                if cross_elements:
                    el = cross_elements[0]
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", el)
                    time.sleep(0.3)
                    try:
                        el.click()
                    except Exception:
                        driver.execute_script("arguments[0].click();", el)
                    deny_clicked = True
                    break

            if deny_clicked:
                time.sleep(1)
                # Handle confirmation dialog
                try:
                    alert = WebDriverWait(driver, 2).until(EC.alert_is_present())
                    alert.accept()
                    time.sleep(0.5)
                except Exception:
                    pass
                denied.append(player_ref)
                log.info("  DENIED: %s", player_ref)
            else:
                not_found.append(player_ref)
                log.warning("  Could not find deny button for: %s", player_ref)

        except Exception as e:
            errors.append({"id": player_ref, "error": str(e)[:200]})
            log.error("  ERROR: %s — %s", player_ref, str(e)[:200])

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

    print(json.dumps(result))

    log.info("=" * 60)
    log.info("COMPLETE — %d denied, %d not found, %d errors",
             len(result["denied"]), len(result["not_found"]), len(result["errors"]))
    log.info("=" * 60)


if __name__ == "__main__":
    main()
