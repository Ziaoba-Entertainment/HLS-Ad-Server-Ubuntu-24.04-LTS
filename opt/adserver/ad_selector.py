import sqlite3
import os
import random
import hashlib
import json
import logging

logger = logging.getLogger("adserver.selector")

class AdSelector:
    def __init__(self, db_path: str = "/opt/adserver/adserver.db", redis_client=None):
        self.db_path = db_path
        self.redis_client = redis_client

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def select_ads(self, placement: str, count: int, seed: str = None) -> list:
        if count <= 0: return []
        
        # Try Redis first
        if self.redis_client:
            try:
                ads = self._select_from_redis(placement, count, seed)
                if ads:
                    return ads
                # If Redis returned empty, it might be because it's not synced yet
                logger.info("Redis returned no ads, falling back to SQLite")
            except Exception as e:
                logger.warning(f"Redis ad selection failed, falling back to SQLite: {e}")
        
        # Fallback to SQLite
        return self._select_from_sqlite(placement, count, seed)

    def _select_from_redis(self, placement: str, count: int, seed: str = None) -> list:
        # Redis Keys (DB 1): 
        # ad:ad_registry (ZSET): ad_id as member, priority as score
        # ad:ad_meta:{ad_id} (HASH): folder_name, placement_pre, placement_mid, placement_post, active
        # ad:ads:disabled (SET): ad_id
        
        # 1. Get all ads from registry
        all_ads_with_scores = self.redis_client.zrange("ad:ad_registry", 0, -1, withscores=True)
        if not all_ads_with_scores:
            return []
            
        # 2. Get disabled ads
        disabled_ads = self.redis_client.smembers("ad:ads:disabled") or set()
        
        # 3. Filter and fetch metadata
        eligible_ads = []
        weights = []
        
        placement_key = f"placement_{placement}"
        
        for ad_id, priority in all_ads_with_scores:
            if ad_id in disabled_ads:
                continue
                
            meta = self.redis_client.hgetall(f"ad:ad_meta:{ad_id}")
            if not meta:
                continue
                
            # Check if active and placement matches
            if meta.get("active") != "1":
                continue
            if meta.get(placement_key) != "1":
                continue
                
            ad_data = {
                "id": ad_id,
                "folder_name": meta.get("folder_name"),
                "priority": int(priority)
            }
            eligible_ads.append(ad_data)
            # Weight is inversely proportional to priority (lower priority number = higher weight)
            # Assuming priority 1 is highest, 5 is lowest
            weights.append(max(6 - int(priority), 1))
            
        if not eligible_ads:
            return []
            
        selected = []
        if seed:
            state = random.getstate()
            for i in range(count):
                combined_seed = f"{seed}:{placement}:{i}"
                random.seed(combined_seed)
                sel = random.choices(eligible_ads, weights=weights, k=1)[0]
                selected.append(sel)
            random.setstate(state)
        else:
            selected = random.choices(eligible_ads, weights=weights, k=count)
            
        return selected

    def _select_from_sqlite(self, placement: str, count: int, seed: str = None) -> list:
        col_map = {"pre":"placement_pre","mid":"placement_mid","post":"placement_post"}
        col = col_map.get(placement)
        if not col: return []
        
        try:
            with self._get_conn() as conn:
                rows = conn.execute(
                    f"SELECT id, folder_name, priority FROM ads "
                    f"WHERE active=1 AND {col}=1"
                ).fetchall()
            if not rows: return []
            ads = [dict(r) for r in rows]
            weights = [max(6 - a["priority"], 1) for a in ads]
            
            selected = []
            if seed:
                state = random.getstate()
                for i in range(count):
                    combined_seed = f"{seed}:{placement}:{i}"
                    random.seed(combined_seed)
                    sel = random.choices(ads, weights=weights, k=1)[0]
                    selected.append(sel)
                random.setstate(state)
            else:
                selected = random.choices(ads, weights=weights, k=count)
            return selected
        except Exception as e:
            logger.error(f"SQLite selection error: {e}")
            return []

    def record_impression(self, ad_id: int, content_path: str,
                          placement: str, session_id: str) -> None:
        # 1. Log to SQLite
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
        except Exception as e:
            logger.error(f"SQLite impression log failed: {e}")

        # 2. Update Redis counter
        if self.redis_client:
            try:
                self.redis_client.incr(f"ad:ad_plays:{ad_id}")
            except Exception as e:
                logger.warning(f"Redis play count update failed: {e}")

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
            ad_data = dict(row)
            
        # Sync to Redis if available
        if self.redis_client:
            try:
                ad_id = ad_data["id"]
                self.redis_client.zadd("ad:ad_registry", {str(ad_id): priority})
                self.redis_client.hset(f"ad:ad_meta:{ad_id}", mapping={
                    "folder_name": folder_name,
                    "input_path": folder_name,
                    "ad_description": ad_data.get("ad_description") or "",
                    "advertiser": ad_data.get("advertiser_name") or "",
                    "max_plays": str(ad_data.get("max_plays") or 0),
                    "placement_pre": "1" if placement_pre else "0",
                    "placement_mid": "1" if placement_mid else "0",
                    "placement_post": "1" if placement_post else "0",
                    "active": "1" if active else "0"
                })
                if not active:
                    self.redis_client.sadd("ad:ads:disabled", str(ad_id))
                else:
                    self.redis_client.srem("ad:ads:disabled", str(ad_id))
            except Exception as e:
                logger.warning(f"Redis sync failed: {e}")
                
        return ad_data

    def update_ad(self, ad_id: int, updates: dict) -> bool:
        allowed = {
            "priority", "placement_pre", "placement_mid", "placement_post", "active",
            "ad_description", "advertiser_name", "campaign_name", "max_plays",
            "tags", "contact_email", "start_date", "end_date", "budget_plays",
            "notes", "advertiser_id", "campaign_id"
        }
        filtered = {k: v for k, v in updates.items() if k in allowed}
        if not filtered: return False
        
        set_clause = ", ".join(f"{k}=?" for k in filtered)
        values = list(filtered.values()) + [ad_id]
        
        try:
            with self._get_conn() as conn:
                conn.execute(f"UPDATE ads SET {set_clause}, updated_at=datetime('now') WHERE id=?", values)
                conn.commit()
                row = conn.execute("SELECT * FROM ads WHERE id=?", (ad_id,)).fetchone()
            
            if row and self.redis_client:
                ad_data = dict(row)
                self.redis_client.zadd("ad:ad_registry", {str(ad_id): ad_data["priority"]})
                self.redis_client.hset(f"ad:ad_meta:{ad_id}", mapping={
                    "folder_name": ad_data["folder_name"],
                    "input_path": ad_data["folder_name"],
                    "ad_description": ad_data.get("ad_description") or "",
                    "advertiser": ad_data.get("advertiser_name") or "",
                    "max_plays": str(ad_data.get("max_plays") or 0),
                    "placement_pre": "1" if ad_data["placement_pre"] else "0",
                    "placement_mid": "1" if ad_data["placement_mid"] else "0",
                    "placement_post": "1" if ad_data["placement_post"] else "0",
                    "active": "1" if ad_data["active"] else "0"
                })
                if not ad_data["active"]:
                    self.redis_client.sadd("ad:ads:disabled", str(ad_id))
                else:
                    self.redis_client.srem("ad:ads:disabled", str(ad_id))
            return True
        except Exception as e:
            logger.error(f"Update ad failed: {e}")
            return False

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
