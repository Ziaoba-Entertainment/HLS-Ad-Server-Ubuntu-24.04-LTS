#!/bin/bash
set -e

# Configuration
DB_PATH="/opt/adserver/adserver.db"
ARCHIVE_DIR="/opt/adserver/archive"
DATE_STAMP=$(date +%Y-%m)
ARCHIVE_FILE="$ARCHIVE_DIR/impressions_$DATE_STAMP.csv"
DAYS_TO_KEEP=90

mkdir -p "$ARCHIVE_DIR"

echo "[*] Archiving impressions older than $DAYS_TO_KEEP days..."

# Export to CSV
# We use python to handle the CSV export cleanly
/opt/adserver/venv/bin/python3 -c "
import sqlite3
import csv
import datetime

db_path = '$DB_PATH'
archive_file = '$ARCHIVE_FILE'
days = $DAYS_TO_KEEP

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cutoff = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')

cursor.execute('SELECT * FROM impressions WHERE played_at < ?', (cutoff,))
rows = cursor.fetchall()

if rows:
    with open(archive_file, 'w', newline='') as f:
        writer = csv.writer(f)
        # Get headers
        writer.writerow([d[0] for d in cursor.description])
        writer.writerows(rows)
    print(f'[+] Exported {len(rows)} rows to {archive_file}')
    
    # Delete archived rows
    cursor.execute('DELETE FROM impressions WHERE played_at < ?', (cutoff,))
    conn.commit()
    print(f'[+] Deleted {len(rows)} archived rows from database.')
else:
    print('[*] No rows found older than the cutoff date.')

conn.close()
"

# Vacuum the database to reclaim space
echo "[*] Running VACUUM on SQLite database..."
sqlite3 "$DB_PATH" "VACUUM;"

echo "[+] Log rotation complete."
