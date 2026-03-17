import sqlite3
import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "adserver.db")
ADS_DIR = os.path.join(BASE_DIR, "../../srv/vod/ads/")

def init_db():
    print(f"[*] Initializing database at {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Create advertisers table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS advertisers (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            name             TEXT UNIQUE NOT NULL,
            contact_name     TEXT DEFAULT '',
            contact_email    TEXT DEFAULT '',
            phone            TEXT DEFAULT '',
            company          TEXT DEFAULT '',
            notes            TEXT DEFAULT '',
            active           INTEGER DEFAULT 1,
            created_at       TEXT DEFAULT (datetime('now')),
            updated_at       TEXT DEFAULT (datetime('now'))
        )
    """)

    # Create campaigns table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS campaigns (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            name             TEXT NOT NULL,
            advertiser_id    INTEGER REFERENCES advertisers(id),
            description      TEXT DEFAULT '',
            start_date       TEXT DEFAULT '',
            end_date         TEXT DEFAULT '',
            budget_plays     INTEGER DEFAULT 0,
            target_plays     INTEGER DEFAULT 0,
            active           INTEGER DEFAULT 1,
            created_at       TEXT DEFAULT (datetime('now')),
            updated_at       TEXT DEFAULT (datetime('now')),
            UNIQUE(name, advertiser_id)
        )
    """)

    # Create ads table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            folder_name TEXT UNIQUE NOT NULL,
            ad_description TEXT DEFAULT '',
            advertiser_name TEXT DEFAULT '',
            campaign_name TEXT DEFAULT '',
            priority INTEGER NOT NULL DEFAULT 3,
            placement_pre INTEGER NOT NULL DEFAULT 1,
            placement_mid INTEGER NOT NULL DEFAULT 1,
            placement_post INTEGER NOT NULL DEFAULT 1,
            play_count INTEGER NOT NULL DEFAULT 0,
            max_plays INTEGER DEFAULT 0,
            active INTEGER NOT NULL DEFAULT 1,
            duration_seconds REAL DEFAULT 0,
            rendition_count INTEGER DEFAULT 0,
            tags TEXT DEFAULT '',
            contact_email TEXT DEFAULT '',
            start_date TEXT DEFAULT '',
            end_date TEXT DEFAULT '',
            budget_plays INTEGER DEFAULT 0,
            notes TEXT DEFAULT '',
            input_path TEXT DEFAULT '',
            status TEXT DEFAULT 'Ready',
            job_id TEXT,
            advertiser_id INTEGER REFERENCES advertisers(id),
            campaign_id INTEGER REFERENCES campaigns(id),
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)

    # Create impressions table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS impressions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ad_id INTEGER NOT NULL REFERENCES ads(id),
            content_path TEXT NOT NULL,
            placement TEXT NOT NULL CHECK(placement IN ('pre','mid','post')),
            session_id TEXT NOT NULL,
            played_at TEXT DEFAULT (datetime('now'))
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
