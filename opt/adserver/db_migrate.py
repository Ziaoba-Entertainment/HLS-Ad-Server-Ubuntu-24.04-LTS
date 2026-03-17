#!/usr/bin/env python3
"""
Ad Server Database Migration
Adds advertiser/campaign support and extended ad metadata.
Safe to run multiple times.
"""

import sqlite3
import os

DB_PATH = "/opt/adserver/adserver.db"

def column_exists(conn, table, column):
    cursor = conn.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cursor.fetchall())

def table_exists(conn, table):
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,)
    )
    return cursor.fetchone() is not None

def run_migration():
    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    
    print(f"Migrating database: {DB_PATH}")
    
    # 1. Create advertisers table
    conn.execute("""
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
    print("  Table 'advertisers' verified.")
    
    # 2. Create campaigns table
    conn.execute("""
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
        updated_at       TEXT DEFAULT (datetime('now'))
    )
    """)
    print("  Table 'campaigns' verified.")
    
    # 3. Add new columns to ads table
    new_ad_columns = [
        ("ad_description",  "TEXT DEFAULT ''"),
        ("advertiser_name", "TEXT DEFAULT ''"),
        ("campaign_name",   "TEXT DEFAULT ''"),
        ("max_plays",       "INTEGER DEFAULT 0"),
        ("tags",            "TEXT DEFAULT ''"),
        ("contact_email",   "TEXT DEFAULT ''"),
        ("start_date",      "TEXT DEFAULT ''"),
        ("end_date",        "TEXT DEFAULT ''"),
        ("budget_plays",    "INTEGER DEFAULT 0"),
        ("notes",           "TEXT DEFAULT ''"),
        ("advertiser_id",   "INTEGER REFERENCES advertisers(id)"),
        ("campaign_id",     "INTEGER REFERENCES campaigns(id)"),
    ]
    for col_name, col_def in new_ad_columns:
        if not column_exists(conn, "ads", col_name):
            conn.execute(f"ALTER TABLE ads ADD COLUMN {col_name} {col_def}")
            print(f"  Added column: ads.{col_name}")
        else:
            print(f"  Skipped (exists): ads.{col_name}")
    
    # 4. Create all indexes
    indexes = [
        ("idx_ads_advertiser", "ads(advertiser_id)"),
        ("idx_ads_campaign", "ads(campaign_id)"),
        ("idx_ads_active_priority", "ads(active, priority)"),
        ("idx_impressions_played_at", "impressions(played_at)"),
        ("idx_impressions_ad_id", "impressions(ad_id)"),
        ("idx_campaigns_advertiser", "campaigns(advertiser_id)"),
    ]
    for idx_name, idx_def in indexes:
        conn.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {idx_def}")
        print(f"  Index '{idx_name}' verified.")
    
    conn.commit()
    conn.close()
    print("Migration complete.")

if __name__ == "__main__":
    run_migration()
