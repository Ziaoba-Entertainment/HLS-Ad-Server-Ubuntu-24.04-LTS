import sqlite3
import os
import sys

DB_PATH = "/opt/adserver/adserver.db"
ADS_DIR = "/srv/vod/ads/"

def init_db():
    print(f"[*] Initializing database at {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Create ads table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            folder_name TEXT UNIQUE,
            priority INTEGER DEFAULT 3,
            placement_pre BOOLEAN DEFAULT 1,
            placement_mid BOOLEAN DEFAULT 1,
            placement_post BOOLEAN DEFAULT 1,
            play_count INTEGER DEFAULT 0,
            active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create impressions table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS impressions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ad_id INTEGER,
            content_path TEXT,
            placement TEXT CHECK(placement IN ('pre','mid','post')),
            session_id TEXT,
            played_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(ad_id) REFERENCES ads(id)
        )
    """)

    # Create settings table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            description TEXT,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Insert default settings if not exist
    default_settings = [
        ('mid_roll_interval', '600', 'Interval between mid-roll ads in seconds'),
        ('pre_ad_count', '1', 'Number of ads in pre-roll break'),
        ('mid_ad_count', '1', 'Number of ads in mid-roll break'),
        ('post_ad_count', '1', 'Number of ads in post-roll break')
    ]
    cursor.executemany('INSERT OR IGNORE INTO settings (key, value, description) VALUES (?, ?, ?)', default_settings)

    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_impressions_played_at ON impressions(played_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_impressions_ad_id ON impressions(ad_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_impressions_placement ON impressions(placement)")

    conn.commit()
    print("[+] Schema created successfully.")

    # Scan for ads
    print(f"[*] Scanning {ADS_DIR} for ads...")
    if not os.path.exists(ADS_DIR):
        print(f"[!] Warning: {ADS_DIR} does not exist. Skipping scan.")
    else:
        found_count = 0
        new_count = 0
        for folder in os.listdir(ADS_DIR):
            if folder.startswith("advert") and os.path.isdir(os.path.join(ADS_DIR, folder)):
                found_count += 1
                try:
                    cursor.execute("INSERT INTO ads (folder_name) VALUES (?)", (folder,))
                    new_count += 1
                except sqlite3.IntegrityError:
                    pass # Already exists
        
        conn.commit()
        print(f"[+] Scan complete. Found {found_count} advert folders, added {new_count} new entries to DB.")

    conn.close()

if __name__ == "__main__":
    init_db()
