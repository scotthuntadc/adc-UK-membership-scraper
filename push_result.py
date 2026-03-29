"""Push fetch result to Supabase using subprocess curl."""
import json
import sys
import os
import subprocess
import tempfile


def main():
    result_file = sys.argv[1]
    request_id = sys.argv[2]
    key_file = sys.argv[3]

    if not os.path.exists(key_file):
        print(f"Key file not found: {key_file}")
        sys.exit(1)
    key = open(key_file).read().strip()
    print(f"Key length: {len(key)}")

    data = json.loads(open(result_file).read().strip())
    players = data.get("players", [])
    print(f"Parsed: {data.get('title', '?')} — {len(players)} players")

    row = {
        "name": request_id,
        "description": data.get("title", ""),
        "list_type": "entry_check",
        "region": None,
        "player_ids": [p["id"] for p in players],
        "player_data": players,
    }

    payload_file = "/tmp/sb_payload.json"
    with open(payload_file, "w") as f:
        json.dump(row, f)
    print(f"Payload: {os.path.getsize(payload_file)} bytes")

    base = "https://tlfunituxidxdzxzqcou.supabase.co/rest/v1"

    # Write curl config with headers
    config_file = "/tmp/curl_cfg.txt"
    with open(config_file, "w") as f:
        f.write(f'header = "apikey: {key}"\n')
        f.write(f'header = "Authorization: Bearer {key}"\n')
        f.write('header = "Content-Type: application/json"\n')
        f.write('header = "Prefer: return=minimal"\n')

    # Delete existing
    subprocess.run([
        "curl", "-s", "-K", config_file, "-X", "DELETE",
        f"{base}/eligibility_lists?list_type=eq.entry_check&name=eq.{request_id}",
    ])
    print("DELETE done")

    # Insert
    r = subprocess.run([
        "curl", "-s", "-K", config_file, "-X", "POST",
        "-d", f"@{payload_file}",
        "-o", "/tmp/sb_resp.txt", "-w", "%{http_code}",
        f"{base}/eligibility_lists",
    ], capture_output=True, text=True)

    code = r.stdout.strip()
    print(f"POST: {code}")

    if code != "201":
        if os.path.exists("/tmp/sb_resp.txt"):
            print(open("/tmp/sb_resp.txt").read()[:500])
        sys.exit(1)

    print(f"SUCCESS — {len(players)} players pushed")

    # Clean up
    os.remove(config_file)
    os.remove(payload_file)


if __name__ == "__main__":
    main()
