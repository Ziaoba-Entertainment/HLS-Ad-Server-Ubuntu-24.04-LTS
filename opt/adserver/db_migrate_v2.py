import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "adserver.db")

def migrate():
    if not os.path.exists(DB_PATH):
        print("[!] Database not found. Run init_db.py first.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("[*] Checking for new columns in 'ads' table...")
    
    # Add status column
    try:
        cursor.execute("ALTER TABLE ads ADD COLUMN status TEXT DEFAULT 'Ready'")
        print("[+] Added 'status' column to 'ads' table.")
    except sqlite3.OperationalError:
        print("[ ] 'status' column already exists.")

    # Add job_id column
    try:
        cursor.execute("ALTER TABLE ads ADD COLUMN job_id TEXT")
        print("[+] Added 'job_id' column to 'ads' table.")
    except sqlite3.OperationalError:
        print("[ ] 'job_id' column already exists.")

    conn.commit()
    conn.close()
    print("[+] Migration complete.")

if __name__ == "__main__":
    migrate()
