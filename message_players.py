"""
DartsAtlas Player Messaging Script
====================================
Logs into DartsAtlas and sends a message to specified players
via the tournament's message_players page.

Usage: python message_players.py <tournament_url> <player_ids_csv> <message> <da_email> <da_password>
"""

import os
import sys
import json
import time
import logging

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
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


def message_players(driver, tournament_url, player_ids, message):
    """Navigate to message_players page and send message to each player."""

    msg_url = tournament_url.rstrip("/")
    if not msg_url.endswith("/message_players"):
        msg_url += "/message_players"

    sent = []
    not_found = []
    errors = []

    for player_ref in player_ids:
        try:
            # player_ref is either a DartsAtlas ID or "name:Player Name"
            is_name_match = player_ref.startswith("name:")
            player_name = player_ref[5:] if is_name_match else None
            player_id = player_ref if not is_name_match else None

            log.info("Messaging player: %s", player_ref)
            driver.get(msg_url)
            time.sleep(2)

            wait = WebDriverWait(driver, 10)

            # Find the player dropdown/select
            select_el = None
            try:
                select_el = wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "select[name*='player'], select[name*='user'], select[name*='recipient'], select")))
            except Exception:
                selects = driver.find_elements(By.TAG_NAME, "select")
                for s in selects:
                    options = s.find_elements(By.TAG_NAME, "option")
                    if len(options) > 2:
                        select_el = s
                        break

            if not select_el:
                log.error("Could not find player select dropdown")
                errors.append({"id": player_ref, "error": "No select dropdown found"})
                continue

            select = Select(select_el)
            player_selected = False

            if player_id:
                # Try selecting by DartsAtlas ID in option value
                try:
                    select.select_by_value(player_id)
                    player_selected = True
                except Exception:
                    pass

                if not player_selected:
                    for option in select.options:
                        val = option.get_attribute("value") or ""
                        if player_id in val:
                            select.select_by_value(val)
                            player_selected = True
                            break

            if not player_selected and player_name:
                # Match by name in dropdown option text
                name_lower = player_name.lower().strip()
                for option in select.options:
                    option_text = option.text.strip().lower()
                    if name_lower == option_text or name_lower in option_text:
                        option.click()
                        player_selected = True
                        log.info("  Matched by name: %s", option.text.strip())
                        break

            if not player_selected:
                not_found.append(player_ref)
                log.warning("Player %s not found in dropdown", player_ref)
                continue

            time.sleep(1)

            # Find the message textarea
            textarea = None
            try:
                textarea = driver.find_element(By.CSS_SELECTOR,
                    "textarea[name*='message'], textarea[name*='body'], textarea[name*='content'], textarea")
            except Exception:
                log.error("Could not find message textarea")
                errors.append({"id": player_ref, "error": "No textarea found"})
                continue

            # Clear and type the message (280 char limit)
            textarea.clear()
            truncated_msg = message[:280]
            textarea.send_keys(truncated_msg)
            time.sleep(0.5)

            # Find and click the send/submit button
            submit = None
            try:
                submit = driver.find_element(By.CSS_SELECTOR,
                    "input[type='submit'], button[type='submit'], button:not([type])")
            except Exception:
                pass

            if not submit:
                try:
                    buttons = driver.find_elements(By.TAG_NAME, "button")
                    for btn in buttons:
                        txt = btn.text.lower()
                        if 'send' in txt or 'submit' in txt or 'message' in txt:
                            submit = btn
                            break
                except Exception:
                    pass

            if submit:
                submit.click()
                time.sleep(2)
                sent.append(player_ref)
                log.info("  SENT to %s", player_ref)
            else:
                errors.append({"id": player_ref, "error": "No submit button found"})
                log.error("  No submit button for %s", player_ref)

        except Exception as e:
            errors.append({"id": player_ref, "error": str(e)})
            log.error("  ERROR messaging %s: %s", player_ref, e)

    return {
        "sent": sent,
        "not_found": not_found,
        "errors": errors,
        "total_requested": len(player_ids),
    }


def main():
    if len(sys.argv) < 6:
        print("Usage: python message_players.py <tournament_url> <player_ids_csv> <message> <da_email> <da_password>")
        sys.exit(1)

    tournament_url = sys.argv[1]
    player_ids = [pid.strip() for pid in sys.argv[2].split(",") if pid.strip()]
    message = sys.argv[3]
    da_email = sys.argv[4]
    da_password = sys.argv[5]

    log.info("=" * 60)
    log.info("DartsAtlas Player Messaging")
    log.info("  Tournament: %s", tournament_url)
    log.info("  Players to message: %d", len(player_ids))
    log.info("  Message length: %d chars", len(message))
    log.info("=" * 60)

    driver = create_driver()
    try:
        login(driver, da_email, da_password)
        result = message_players(driver, tournament_url, player_ids, message)
    finally:
        driver.quit()
        log.info("Browser closed.")

    print(json.dumps(result))

    log.info("=" * 60)
    log.info("COMPLETE — %d sent, %d not found, %d errors",
             len(result["sent"]), len(result["not_found"]), len(result["errors"]))
    log.info("=" * 60)


if __name__ == "__main__":
    main()
