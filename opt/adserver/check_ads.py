import sqlite3
import os

DB_PATH = "/opt/adserver/adserver.db"
ADS_PATH = "/srv/vod/ads"

def check():
    print(f"Checking DB at {DB_PATH}")
    if not os.path.exists(DB_PATH):
        print("DB NOT FOUND")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    print("\n--- SETTINGS ---")
    try:
        rows = conn.execute("SELECT * FROM settings").fetchall()
        for r in rows:
            print(f"{r['key']}: {r['value']}")
    except Exception as e:
        print(f"Error reading settings: {e}")

    print("\n--- ADS ---")
    try:
        rows = conn.execute("SELECT * FROM ads").fetchall()
        for r in rows:
            print(f"ID: {r['id']} | Folder: {r['folder_name']} | Active: {r['active']} | Pre: {r['placement_pre']} | Mid: {r['placement_mid']} | Post: {r['placement_post']}")
            ad_path = os.path.join(ADS_PATH, r['folder_name'])
            print(f"  Path: {ad_path} | Exists: {os.path.exists(ad_path)}")
            if os.path.exists(ad_path):
                print(f"  Contents: {os.listdir(ad_path)}")
    except Exception as e:
        print(f"Error reading ads: {e}")

if __name__ == "__main__":
    check()
