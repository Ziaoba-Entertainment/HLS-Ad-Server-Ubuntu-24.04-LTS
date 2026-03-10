import sqlite3
import os
import random

class AdSelector:
    def __init__(self, db_path: str = "/opt/adserver/adserver.db"):
        self.db_path = db_path

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def select_ad(self, placement: str) -> dict | None:
        col_map = {"pre":"placement_pre","mid":"placement_mid","post":"placement_post"}
        col = col_map.get(placement)
        if not col: return None
        try:
            with self._get_conn() as conn:
                rows = conn.execute(
                    f"SELECT id, folder_name, priority FROM ads "
                    f"WHERE active=1 AND {col}=1"
                ).fetchall()
            if not rows: return None
            ads = [dict(r) for r in rows]
            weights = [max(6 - a["priority"], 1) for a in ads]
            return random.choices(ads, weights=weights, k=1)[0]
        except Exception:
            return None

    def record_impression(self, ad_id: int, content_path: str,
                          placement: str, session_id: str) -> None:
        try:
            with self._get_conn() as conn:
                conn.execute(
                    "INSERT INTO impressions (ad_id,content_path,placement,session_id) "
                    "VALUES (?,?,?,?)",
                    (ad_id, content_path, placement, session_id)
                )
                conn.execute(
                    "UPDATE ads SET play_count=play_count+1,"
                    "updated_at=datetime('now') WHERE id=?",
                    (ad_id,)
                )
                conn.commit()
        except Exception:
            pass

    def get_all_ads(self) -> list:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM ads ORDER BY priority ASC, folder_name ASC"
            ).fetchall()
        return [dict(r) for r in rows]

    def upsert_ad(self, folder_name: str, priority: int = 3,
                  placement_pre: bool = True, placement_mid: bool = True,
                  placement_post: bool = True, active: bool = True) -> dict:
        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO ads (folder_name, priority, placement_pre, placement_mid, placement_post, active)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(folder_name) DO UPDATE SET
                    priority=excluded.priority,
                    placement_pre=excluded.placement_pre,
                    placement_mid=excluded.placement_mid,
                    placement_post=excluded.placement_post,
                    active=excluded.active,
                    updated_at=datetime('now')
            """, (folder_name, priority, 1 if placement_pre else 0, 
                  1 if placement_mid else 0, 1 if placement_post else 0, 
                  1 if active else 0))
            conn.commit()
            row = conn.execute("SELECT * FROM ads WHERE folder_name=?", (folder_name,)).fetchone()
        return dict(row)

    def get_metrics(self, start_date: str, end_date: str) -> list:
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT a.id as ad_id, a.folder_name, i.placement, COUNT(*) as count
                FROM impressions i JOIN ads a ON i.ad_id = a.id
                WHERE date(i.played_at) BETWEEN ? AND ?
                GROUP BY a.id, a.folder_name, i.placement
                ORDER BY count DESC
            """, (start_date, end_date)).fetchall()
        return [dict(r) for r in rows]
