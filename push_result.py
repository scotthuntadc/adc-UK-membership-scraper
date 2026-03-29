"""Push fetch_entries.py result to Supabase. Called from workflow."""
import json
import sys
import os
import requests

SUPABASE_URL = "https://tlfunituxidxdzxzqcou.supabase.co"
KEY_FILE = os.path.join(os.getcwd(), ".supabase_key")

def main():
    result_file = sys.argv[1]
    request_id = sys.argv[2]

    # Read key from file (written by workflow step)
    if not os.path.exists(KEY_FILE):
        print("ERROR: .supabase_key file not found")
        sys.exit(1)
    key = open(KEY_FILE).read().strip()
    print(f"Key length: {len(key)}")

    # Read result
    text = open(result_file).read().strip()
    if not text:
        print("ERROR: Empty result file")
        sys.exit(1)

    data = json.loads(text)
    players = data.get("players", [])
    title = data.get("title", "Unknown")
    print(f"Parsed: {title} — {len(players)} players")

    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

    # Delete existing
    r = requests.delete(
        f"{SUPABASE_URL}/rest/v1/eligibility_lists?list_type=eq.entry_check&name=eq.{request_id}",
        headers=headers,
    )
    print(f"DELETE: {r.status_code}")

    # Insert
    row = {
        "name": request_id,
        "description": title,
        "list_type": "entry_check",
        "region": None,
        "player_ids": [p["id"] for p in players],
        "player_data": players,
    }
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/eligibility_lists",
        headers=headers,
        json=row,
    )
    print(f"POST: {r.status_code}")
    if r.status_code >= 400:
        print(f"Error: {r.text[:500]}")
        sys.exit(1)
    print(f"SUCCESS — {len(players)} players pushed")

if __name__ == "__main__":
    main()
