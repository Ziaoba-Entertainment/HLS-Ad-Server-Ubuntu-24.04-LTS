import sqlite3, os, time, csv, io, re, logging, json, math, hashlib
from datetime import date, datetime, timedelta
from pathlib import Path
from fastapi import FastAPI, Request, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, JSONResponse, StreamingResponse, HTMLResponse
try:
    from starlette.middleware.proxy_headers import ProxyHeadersMiddleware
except ImportError:
    # Starlette 0.36.0+ removed ProxyHeadersMiddleware
    # Uvicorn provides its own version
    try:
        from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
    except ImportError:
        ProxyHeadersMiddleware = None
from jinja2 import Environment, FileSystemLoader, select_autoescape
from starlette.templating import Jinja2Templates
from config import settings

# LOGGING
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("adserver-admin")

# JINJA2 SETUP
_jinja_env = Environment(
    loader=FileSystemLoader("/opt/adserver/templates"),
    extensions=["jinja2.ext.do"],
    autoescape=select_autoescape(["html"])
)

def format_duration(seconds):
    """Format float seconds as M:SS or H:MM:SS"""
    if not seconds: return "—"
    s = int(float(seconds))
    if s >= 3600: return f"{s//3600}:{(s%3600)//60:02d}:{s%60:02d}"
    return f"{s//60}:{s%60:02d}"
_jinja_env.filters["format_duration"] = format_duration

def extract_title(content_path):
    """Extract human-readable title from HLS content path"""
    if not content_path: return "Unknown"
    parts = content_path.strip("/").split("/")
    for part in reversed(parts):
        if part not in ("master.m3u8","hls","movies","tv","vod","srv"):
            return part.replace("_", " ").replace("-", " ")
    return content_path
_jinja_env.filters["extract_title"] = extract_title

def content_type_from_path(path):
    if "/movies/" in (path or ""): return "movie"
    if "/tv/" in (path or ""): return "tv"
    return "unknown"
_jinja_env.filters["content_type_from_path"] = content_type_from_path

def stitched_url(content_path):
    if not content_path: return "#"
    # content_path is like /srv/vod/hls/movies/Title/master.m3u8
    # We want https://stream.ziaoba.com/playlist/movies/Title/master.m3u8
    rel_path = content_path.replace("/srv/vod/hls/", "").replace("/srv/vod/hls", "")
    base = "https://stream.ziaoba.com" # Fallback
    try:
        from config import settings
        base = settings.PUBLIC_BASE_URL.rstrip("/")
    except: pass
    return f"{base}/playlist/{rel_path.lstrip('/')}"
_jinja_env.filters["stitched_url"] = stitched_url

def status_badge(ad):
    """Compute display status for an ad dict"""
    status = ad.get("status", "Ready")
    if status in ("In Queue", "Encoding", "Failed"):
        return status.lower().replace(" ", "_")

    today = date.today().isoformat()
    if not ad.get("active"): return "paused"
    if ad.get("max_plays", 0) > 0 and ad.get("play_count", 0) >= ad.get("max_plays", 0):
        return "budget_reached"
    if ad.get("end_date") and ad["end_date"] < today:
        return "expired"
    if ad.get("start_date") and ad["start_date"] > today:
        return "scheduled"
    return "active"
_jinja_env.filters["status_badge"] = status_badge

templates = Jinja2Templates(env=_jinja_env)

# CONSTANTS
DB_PATH    = settings.DB_PATH
ADS_PATH   = settings.ADS_PATH
HLS_PATH   = settings.HLS_PATH
START_TIME = time.time()

# DATABASE HELPER
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

# AD SELECTOR & REDIS
from ad_selector import AdSelector
try:
    import redis
    redis_client = redis.Redis(host="127.0.0.1", port=6379, db=settings.REDIS_DB, 
                               password=settings.REDIS_PASS, 
                               decode_responses=True, socket_timeout=2)
    redis_client.ping()
    logger.info(f"Admin connected to Redis DB {settings.REDIS_DB}")
except Exception as e:
    logger.warning(f"Admin Redis connection failed: {e}")
    redis_client = None

ad_selector = AdSelector(db_path=DB_PATH, redis_client=redis_client)

def _month_start() -> str:
    return date.today().replace(day=1).isoformat()

def _today() -> str:
    return date.today().isoformat()

def _date_range_from_preset(preset: str) -> tuple[str, str]:
    """Convert preset name to (date_from, date_to) tuple."""
    today = date.today()
    if preset == "today":
        return today.isoformat(), today.isoformat()
    if preset == "yesterday":
        d = today - timedelta(days=1)
        return d.isoformat(), d.isoformat()
    if preset == "7days":
        return (today - timedelta(days=6)).isoformat(), today.isoformat()
    if preset == "month":
        return today.replace(day=1).isoformat(), today.isoformat()
    if preset == "lastmonth":
        last_day_of_prev_month = today.replace(day=1) - timedelta(days=1)
        return last_day_of_prev_month.replace(day=1).isoformat(), last_day_of_prev_month.isoformat()
    # default: this month
    return today.replace(day=1).isoformat(), today.isoformat()

# APP
app = FastAPI(title="Ad Server Admin", version="1.1.0")
if ProxyHeadersMiddleware:
    app.add_middleware(ProxyHeadersMiddleware, trusted_hosts=["*"])
app.add_middleware(CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"], allow_headers=["*"])

@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "adserver-admin"}

# PAGE ROUTES
@app.get("/")
async def root():
    return RedirectResponse("/ads", status_code=302)

@app.get("/ads")
async def ads_page(request: Request):
    advertiser_id = request.query_params.get("advertiser_id")
    campaign_id = request.query_params.get("campaign_id")
    status_filter = request.query_params.get("status")
    placement_filter = request.query_params.get("placement")
    q = request.query_params.get("q")

    filters = {
        "advertiser_id": advertiser_id,
        "campaign_id": campaign_id,
        "status": status_filter,
        "placement": placement_filter,
        "q": q
    }

    ads = _get_all_ads(filters)
    advertisers = _get_all_advertisers()
    campaigns = _get_all_campaigns(advertiser_id) if advertiser_id else _get_all_campaigns()
    
    # Stats for navbar
    status_info = _get_status_info()

    return templates.TemplateResponse("ads.html", {
        "request": request,
        "ads": ads,
        "advertisers": advertisers,
        "campaigns": campaigns,
        "filters": filters,
        "active_page": "ads",
        "status": status_info
    })

@app.get("/metrics")
async def metrics_page(request: Request):
    preset = request.query_params.get("preset", "month")
    date_from = request.query_params.get("date_from") or request.query_params.get("start")
    date_to = request.query_params.get("date_to") or request.query_params.get("end")
    advertiser_id = request.query_params.get("advertiser_id")
    campaign_id = request.query_params.get("campaign_id")
    ad_id = request.query_params.get("ad_id")
    placement = request.query_params.get("placement")
    content_type = request.query_params.get("content_type")

    if preset != "custom" and not (request.query_params.get("start") or request.query_params.get("date_from")):
        date_from, date_to = _date_range_from_preset(preset)
    elif not date_from or not date_to:
        date_from, date_to = _date_range_from_preset("month")

    filters = {
        "date_from": date_from,
        "date_to": date_to,
        "advertiser_id": advertiser_id,
        "campaign_id": campaign_id,
        "ad_id": ad_id,
        "placement": placement,
        "content_type": content_type
    }

    metrics_data = _get_metrics_data(**filters)
    
    # Calculate additional fields for metrics.html
    total = metrics_data["total_impressions"]
    metrics_data["pre_pct"] = round(metrics_data["pre_total"] / total * 100, 1) if total > 0 else 0
    metrics_data["mid_pct"] = round(metrics_data["mid_total"] / total * 100, 1) if total > 0 else 0
    metrics_data["post_pct"] = round(metrics_data["post_total"] / total * 100, 1) if total > 0 else 0
    metrics_data["ad_rows"] = metrics_data["ad_metrics"]
    max_val = max([r["total"] for r in metrics_data["ad_metrics"]]) if metrics_data["ad_metrics"] else 0
    metrics_data["max_total"] = max_val if max_val > 0 else 1
    metrics_data["top_ad"] = metrics_data["ad_metrics"][0]["folder_name"] if metrics_data["ad_metrics"] and metrics_data["ad_metrics"][0]["total"] > 0 else "None"
    metrics_data["date_start"] = date_from
    metrics_data["date_end"] = date_to

    advertisers = _get_all_advertisers()
    campaigns = _get_all_campaigns(advertiser_id) if advertiser_id else _get_all_campaigns()
    ads = _get_all_ads({"advertiser_id": advertiser_id, "campaign_id": campaign_id})
    
    status_info = _get_status_info()

    return templates.TemplateResponse("metrics.html", {
        "request": request,
        "active_page": "metrics",
        "preset": preset,
        "filters": filters,
        "advertisers": advertisers,
        "campaigns": campaigns,
        "ads": ads,
        "status": status_info,
        **metrics_data
    })

@app.get("/activity")
async def activity_page(request: Request):
    advertisers = _get_all_advertisers()
    status_info = _get_status_info()
    recent = _get_recent_impressions(limit=50)
    return templates.TemplateResponse("activity.html", {
        "request": request,
        "active_page": "activity",
        "advertisers": advertisers,
        "status": status_info,
        "impressions": recent["impressions"]
    })

@app.get("/settings")
async def settings_page(request: Request):
    settings = _get_settings()
    advertisers = _get_all_advertisers()
    campaigns = _get_all_campaigns()
    health = _get_system_health()
    
    with get_db() as conn:
        scheduled_ads = [dict(r) for r in conn.execute(
            "SELECT * FROM ads WHERE start_date != '' OR end_date != ''"
        ).fetchall()]

    status_info = _get_status_info()

    return templates.TemplateResponse("settings.html", {
        "request": request,
        "settings": settings,
        "advertisers": advertisers,
        "campaigns": campaigns,
        "health": health,
        "scheduled_ads": scheduled_ads,
        "active_page": "settings",
        "status": status_info
    })

# API ROUTES
@app.get("/api/ads")
async def api_ads(request: Request):
    filters = {
        "advertiser_id": request.query_params.get("advertiser_id"),
        "campaign_id": request.query_params.get("campaign_id"),
        "status": request.query_params.get("status"),
        "placement": request.query_params.get("placement"),
        "q": request.query_params.get("q")
    }
    ads = _get_all_ads(filters)
    return {"ads": ads, "count": len(ads)}

@app.post("/api/ads/scan")
async def api_ads_scan():
    return JSONResponse(_scan_ad_folders())

@app.put("/api/ads/{ad_id}")
async def api_update_ad(ad_id: int, request: Request):
    body = await request.json()
    allowed = {
        "priority", "placement_pre", "placement_mid", "placement_post", "active",
        "ad_description", "advertiser_name", "campaign_name", "max_plays",
        "tags", "contact_email", "start_date", "end_date", "budget_plays",
        "notes", "advertiser_id", "campaign_id", "status"
    }
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates: raise HTTPException(status_code=400, detail="No valid fields")

    if "priority" in updates:
        try:
            p = int(updates["priority"])
            if p < 1 or p > 5: raise ValueError()
            updates["priority"] = p
        except:
            raise HTTPException(status_code=400, detail="Priority must be 1-5")

    set_clause = ", ".join(f"{k}=?" for k in updates)
    values = list(updates.values()) + [ad_id]

    with get_db() as conn:
        conn.execute(
            f"UPDATE ads SET {set_clause}, updated_at=datetime('now') WHERE id=?",
            values
        )
        conn.commit()
        row = conn.execute("SELECT * FROM ads WHERE id=?", (ad_id,)).fetchone()

    if not row: raise HTTPException(status_code=404)
    
    # Sync to Redis
    ad_data = dict(row)
    if redis_client:
        try:
            ad_selector.upsert_ad(
                folder_name=ad_data["folder_name"],
                priority=ad_data["priority"],
                placement_pre=bool(ad_data["placement_pre"]),
                placement_mid=bool(ad_data["placement_mid"]),
                placement_post=bool(ad_data["placement_post"]),
                active=bool(ad_data["active"])
            )
        except Exception as e:
            logger.warning(f"Redis sync failed during update: {e}")
            
    return JSONResponse(ad_data)

@app.delete("/api/ads/{ad_id}")
async def api_delete_ad(ad_id: int):
    with get_db() as conn:
        conn.execute("UPDATE ads SET active=0, updated_at=datetime('now') WHERE id=?", (ad_id,))
        conn.commit()
        row = conn.execute("SELECT * FROM ads WHERE id=?", (ad_id,)).fetchone()
    
    if row and redis_client:
        ad_data = dict(row)
        try:
            ad_selector.upsert_ad(
                folder_name=ad_data["folder_name"],
                priority=ad_data["priority"],
                placement_pre=bool(ad_data["placement_pre"]),
                placement_mid=bool(ad_data["placement_mid"]),
                placement_post=bool(ad_data["placement_post"]),
                active=False
            )
        except Exception as e:
            logger.warning(f"Redis sync failed during delete: {e}")
            
    return JSONResponse({"success": True, "ad_id": ad_id})

@app.post("/api/ads/bulk")
async def api_ads_bulk(request: Request):
    body = await request.json()
    action = body.get("action")
    ad_ids = body.get("ad_ids", [])
    value = body.get("value")

    if not ad_ids: return {"success": False, "error": "No ads selected"}

    with get_db() as conn:
        placeholders = ",".join("?" for _ in ad_ids)
        if action == "activate":
            conn.execute(f"UPDATE ads SET active=1 WHERE id IN ({placeholders})", ad_ids)
        elif action == "deactivate":
            conn.execute(f"UPDATE ads SET active=0 WHERE id IN ({placeholders})", ad_ids)
        elif action == "delete":
            conn.execute(f"UPDATE ads SET active=0 WHERE id IN ({placeholders})", ad_ids)
        elif action == "set_priority":
            conn.execute(f"UPDATE ads SET priority=? WHERE id IN ({placeholders})", [value] + ad_ids)
        conn.commit()
        
        # Sync all affected ads to Redis
        if redis_client:
            rows = conn.execute(f"SELECT * FROM ads WHERE id IN ({placeholders})", ad_ids).fetchall()
            for row in rows:
                ad_data = dict(row)
                try:
                    ad_selector.upsert_ad(
                        folder_name=ad_data["folder_name"],
                        priority=ad_data["priority"],
                        placement_pre=bool(ad_data["placement_pre"]),
                        placement_mid=bool(ad_data["placement_mid"]),
                        placement_post=bool(ad_data["placement_post"]),
                        active=bool(ad_data["active"])
                    )
                except Exception as e:
                    logger.warning(f"Redis sync failed during bulk action: {e}")
                    
    return {"success": True}

@app.get("/api/ads/campaigns")
async def api_ads_campaigns(advertiser_id: int):
    campaigns = _get_all_campaigns(advertiser_id)
    return {"campaigns": campaigns}

# ADVERTISER API
@app.get("/api/advertisers")
async def api_get_advertisers():
    return {"advertisers": _get_all_advertisers()}

@app.post("/api/advertisers")
async def api_create_advertiser(request: Request):
    body = await request.json()
    name = body.get("name")
    if not name: raise HTTPException(status_code=400, detail="Name required")
    
    with get_db() as conn:
        try:
            conn.execute("""
                INSERT INTO advertisers (name, contact_name, contact_email, phone, company, notes)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (name, body.get("contact_name"), body.get("contact_email"), 
                  body.get("phone"), body.get("company"), body.get("notes")))
            conn.commit()
            new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            return {"success": True, "id": new_id}
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=400, detail="Advertiser name already exists")

@app.put("/api/advertisers/{id}")
async def api_update_advertiser(id: int, request: Request):
    body = await request.json()
    allowed = {"name", "contact_name", "contact_email", "phone", "company", "notes", "active"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates: raise HTTPException(status_code=400, detail="No valid fields")
    
    set_clause = ", ".join(f"{k}=?" for k in updates)
    values = list(updates.values()) + [id]
    
    with get_db() as conn:
        conn.execute(f"UPDATE advertisers SET {set_clause}, updated_at=datetime('now') WHERE id=?", values)
        conn.commit()
    return {"success": True}

@app.delete("/api/advertisers/{id}")
async def api_delete_advertiser(id: int):
    with get_db() as conn:
        conn.execute("UPDATE advertisers SET active=0 WHERE id=?", (id,))
        conn.commit()
    return {"success": True}

# CAMPAIGN API
@app.get("/api/campaigns")
async def api_get_campaigns(advertiser_id: int = None):
    return {"campaigns": _get_all_campaigns(advertiser_id)}

@app.post("/api/campaigns")
async def api_create_campaign(request: Request):
    body = await request.json()
    name = body.get("name")
    adv_id = body.get("advertiser_id")
    if not name or not adv_id: raise HTTPException(status_code=400, detail="Name and Advertiser required")
    
    with get_db() as conn:
        conn.execute("""
            INSERT INTO campaigns (name, advertiser_id, description, start_date, end_date, budget_plays, target_plays)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (name, adv_id, body.get("description"), body.get("start_date"), 
              body.get("end_date"), body.get("budget_plays", 0), body.get("target_plays", 0)))
        conn.commit()
        new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    return {"success": True, "id": new_id}

@app.put("/api/campaigns/{id}")
async def api_update_campaign(id: int, request: Request):
    body = await request.json()
    allowed = {"name", "advertiser_id", "description", "start_date", "end_date", "budget_plays", "target_plays", "active"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates: raise HTTPException(status_code=400, detail="No valid fields")
    
    set_clause = ", ".join(f"{k}=?" for k in updates)
    values = list(updates.values()) + [id]
    
    with get_db() as conn:
        conn.execute(f"UPDATE campaigns SET {set_clause}, updated_at=datetime('now') WHERE id=?", values)
        conn.commit()
    return {"success": True}

@app.delete("/api/campaigns/{id}")
async def api_delete_campaign(id: int):
    with get_db() as conn:
        conn.execute("UPDATE campaigns SET active=0 WHERE id=?", (id,))
        conn.commit()
    return {"success": True}

# METRICS API
@app.get("/api/metrics")
async def api_metrics(request: Request):
    preset = request.query_params.get("preset", "month")
    date_from = request.query_params.get("date_from")
    date_to = request.query_params.get("date_to")
    if preset != "custom":
        date_from, date_to = _date_range_from_preset(preset)
    
    data = _get_metrics_data(
        date_from=date_from,
        date_to=date_to,
        advertiser_id=request.query_params.get("advertiser_id"),
        campaign_id=request.query_params.get("campaign_id"),
        ad_id=request.query_params.get("ad_id"),
        placement=request.query_params.get("placement"),
        content_type=request.query_params.get("content_type")
    )
    return JSONResponse(data)

@app.get("/api/metrics/export")
async def api_metrics_export(request: Request):
    preset = request.query_params.get("preset", "month")
    date_from = request.query_params.get("date_from")
    date_to = request.query_params.get("date_to")
    if preset != "custom":
        date_from, date_to = _date_range_from_preset(preset)
    
    adv_id = request.query_params.get("advertiser_id")
    camp_id = request.query_params.get("campaign_id")
    ad_id = request.query_params.get("ad_id")
    placement = request.query_params.get("placement")
    content_type = request.query_params.get("content_type")

    query = """
        SELECT i.played_at as date, a.id as ad_id, a.folder_name, a.ad_description,
               COALESCE(adv.name, a.advertiser_name) as advertiser_name,
               COALESCE(camp.name, a.campaign_name) as campaign_name,
               i.placement, i.content_path, i.session_id
        FROM impressions i
        JOIN ads a ON i.ad_id = a.id
        LEFT JOIN advertisers adv ON a.advertiser_id = adv.id
        LEFT JOIN campaigns camp ON a.campaign_id = camp.id
        WHERE date(i.played_at) BETWEEN ? AND ?
    """
    params = [date_from, date_to]
    if adv_id:
        query += " AND a.advertiser_id = ?"; params.append(adv_id)
    if camp_id:
        query += " AND a.campaign_id = ?"; params.append(camp_id)
    if ad_id:
        query += " AND a.id = ?"; params.append(ad_id)
    if placement and placement != "all":
        query += " AND i.placement = ?"; params.append(placement)
    if content_type and content_type != "all":
        if content_type == "movie": query += " AND i.content_path LIKE '%/movies/%'"
        elif content_type == "tv": query += " AND i.content_path LIKE '%/tv/%'"
    
    query += " ORDER BY i.played_at DESC"

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["date", "ad_id", "folder_name", "ad_description", "advertiser_name",
                     "campaign_name", "placement", "content_path", "content_type",
                     "session_id"])
    
    with get_db() as conn:
        rows = conn.execute(query, params).fetchall()
        for r in rows:
            ct = "movie" if "/movies/" in r["content_path"] else "tv" if "/tv/" in r["content_path"] else "unknown"
            writer.writerow([r["date"], r["ad_id"], r["folder_name"], r["ad_description"], 
                             r["advertiser_name"], r["campaign_name"], r["placement"], 
                             r["content_path"], ct, r["session_id"]])

    adv_name = "all"
    if adv_id:
        with get_db() as conn:
            row = conn.execute("SELECT name FROM advertisers WHERE id=?", (adv_id,)).fetchone()
            if row: adv_name = row["name"].replace(" ", "_")

    filename = f"ad_metrics_{adv_name}_{date_from}_{date_to}.csv"
    return StreamingResponse(iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"})

@app.get("/api/impressions/recent")
async def api_impressions_recent(request: Request):
    limit = int(request.query_params.get("limit", 100))
    filters = {
        "placement": request.query_params.get("placement"),
        "advertiser_id": request.query_params.get("advertiser_id"),
        "content_type": request.query_params.get("content_type")
    }
    return JSONResponse(_get_recent_impressions(limit, filters))

@app.get("/api/impressions/export")
async def api_impressions_export(limit: int = 500):
    data = _get_recent_impressions(limit)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "ad_id", "folder_name", "description", "advertiser", "campaign", "content", "type", "placement", "session_id", "played_at"])
    for i in data["impressions"]:
        ct = "movie" if "/movies/" in i["content_path"] else "tv" if "/tv/" in i["content_path"] else "unknown"
        writer.writerow([i["id"], i["ad_id"], i["folder_name"], i.get("ad_description",""), 
                         i.get("advertiser_display",""), i.get("campaign_name",""),
                         i["content_path"], ct, i["placement"], i["session_id"], i["played_at"]])
    
    return StreamingResponse(iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=recent_impressions.csv"})

@app.get("/api/settings")
async def api_get_settings():
    return JSONResponse(_get_settings())

@app.post("/api/settings")
async def api_update_settings(request: Request):
    body = await request.json()
    allowed = {"mid_roll_interval", "pre_ad_count", "mid_ad_count", "post_ad_count"}
    updates = {k: str(v) for k, v in body.items() if k in allowed}
    if not updates: raise HTTPException(status_code=400, detail="No valid fields")
    with get_db() as conn:
        for k, v in updates.items():
            conn.execute("UPDATE settings SET value=?, updated_at=datetime('now') WHERE key=?", (v, k))
        conn.commit()
    return JSONResponse({"success": True, "settings": _get_settings()})

@app.get("/api/status")
async def api_status_info():
    return JSONResponse(_get_status_info())

@app.get("/health")
async def health():
    return JSONResponse({"status": "ok", "service": "adserver-admin"})

# HELPER FUNCTIONS
def _get_status_info() -> dict:
    try:
        with get_db() as conn:
            active_ads = conn.execute("SELECT COUNT(*) FROM ads WHERE active=1").fetchone()[0]
            imp_today = conn.execute("SELECT COUNT(*) FROM impressions WHERE date(played_at) = date('now')").fetchone()[0]
            total_ads = conn.execute("SELECT COUNT(*) FROM ads").fetchone()[0]
            total_imp = conn.execute("SELECT COUNT(*) FROM impressions").fetchone()[0]
        db_mb = round(os.path.getsize(DB_PATH)/1024/1024, 2) if os.path.exists(DB_PATH) else 0
    except:
        active_ads = imp_today = total_ads = total_imp = db_mb = 0

    return {
        "status": "ok", "service": "adserver-admin",
        "active_ads": active_ads,
        "impressions_today": imp_today,
        "total_ads": total_ads,
        "total_impressions": total_imp,
        "db_size_mb": db_mb,
        "uptime_seconds": int(time.time() - START_TIME)
    }

def _get_all_ads(filters=None) -> list:
    query = """
        SELECT a.*, adv.name as advertiser_display_name, camp.name as campaign_display_name,
               (SELECT COUNT(*) FROM impressions WHERE ad_id = a.id AND date(played_at) = date('now')) as plays_today,
               (SELECT COUNT(*) FROM impressions WHERE ad_id = a.id AND strftime('%Y-%m', played_at) = strftime('%Y-%m', 'now')) as plays_month
        FROM ads a
        LEFT JOIN advertisers adv ON a.advertiser_id = adv.id
        LEFT JOIN campaigns camp ON a.campaign_id = camp.id
        WHERE 1=1
    """
    params = []
    if filters:
        if filters.get("advertiser_id"):
            query += " AND a.advertiser_id = ?"; params.append(filters["advertiser_id"])
        if filters.get("campaign_id"):
            query += " AND a.campaign_id = ?"; params.append(filters["campaign_id"])
        if filters.get("placement"):
            p = filters["placement"]
            if p == "pre": query += " AND a.placement_pre = 1"
            elif p == "mid": query += " AND a.placement_mid = 1"
            elif p == "post": query += " AND a.placement_post = 1"
        if filters.get("q"):
            query += " AND (a.folder_name LIKE ? OR a.ad_description LIKE ?)"
            params.extend([f"%{filters['q']}%", f"%{filters['q']}%"])
        if filters.get("status"):
            s = filters["status"]
            today = date.today().isoformat()
            if s == "paused": query += " AND a.active = 0"
            elif s == "active": query += " AND a.active = 1 AND (a.max_plays = 0 OR a.play_count < a.max_plays) AND (a.end_date = '' OR a.end_date >= ?) AND (a.start_date = '' OR a.start_date <= ?)"; params.extend([today, today])
            elif s == "budget_reached": query += " AND a.max_plays > 0 AND a.play_count >= a.max_plays"
            elif s == "expired": query += " AND a.end_date != '' AND a.end_date < ?"; params.append(today)
            elif s == "scheduled": query += " AND a.start_date != '' AND a.start_date > ?"; params.append(today)

    query += " ORDER BY a.priority ASC, a.folder_name ASC"
    with get_db() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]

def _get_all_advertisers() -> list:
    with get_db() as conn:
        rows = conn.execute("""
            SELECT adv.*, 
                   (SELECT COUNT(*) FROM ads WHERE advertiser_id = adv.id) as ad_count,
                   (SELECT SUM(play_count) FROM ads WHERE advertiser_id = adv.id) as total_plays
            FROM advertisers adv
            ORDER BY adv.name ASC
        """).fetchall()
    return [dict(r) for r in rows]

def _get_all_campaigns(advertiser_id=None) -> list:
    query = """
        SELECT camp.*, adv.name as advertiser_name,
               (SELECT COUNT(*) FROM ads WHERE campaign_id = camp.id) as ad_count,
               (SELECT SUM(play_count) FROM ads WHERE campaign_id = camp.id) as actual_plays
        FROM campaigns camp
        LEFT JOIN advertisers adv ON camp.advertiser_id = adv.id
    """
    params = []
    if advertiser_id:
        query += " WHERE camp.advertiser_id = ?"; params.append(advertiser_id)
    query += " ORDER BY camp.name ASC"
    with get_db() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]

def _get_recent_impressions(limit=100, filters=None) -> dict:
    query = """
        SELECT i.*, a.folder_name, a.ad_description, a.advertiser_name,
               COALESCE(adv.name, a.advertiser_name) as advertiser_display,
               camp.name as campaign_name
        FROM impressions i
        JOIN ads a ON i.ad_id = a.id
        LEFT JOIN advertisers adv ON a.advertiser_id = adv.id
        LEFT JOIN campaigns camp ON a.campaign_id = camp.id
        WHERE 1=1
    """
    params = []
    if filters:
        if filters.get("placement") and filters["placement"] != "all":
            query += " AND i.placement = ?"; params.append(filters["placement"])
        if filters.get("advertiser_id"):
            query += " AND a.advertiser_id = ?"; params.append(filters["advertiser_id"])
        if filters.get("content_type") and filters["content_type"] != "all":
            if filters["content_type"] == "movie": query += " AND i.content_path LIKE '%/movies/%'"
            elif filters["content_type"] == "tv": query += " AND i.content_path LIKE '%/tv/%'"

    query += " ORDER BY i.played_at DESC LIMIT ?"
    params.append(limit)

    with get_db() as conn:
        rows = conn.execute(query, params).fetchall()
        stats = {
            "last_hour": conn.execute("SELECT COUNT(*) FROM impressions WHERE played_at > datetime('now','-1 hour')").fetchone()[0],
            "last_24h": conn.execute("SELECT COUNT(*) FROM impressions WHERE played_at > datetime('now','-24 hours')").fetchone()[0],
            "active_sessions": conn.execute("SELECT COUNT(DISTINCT session_id) FROM impressions WHERE played_at > datetime('now','-5 minutes')").fetchone()[0],
            "top_ad_30min": "None"
        }
        top_ad = conn.execute("""
            SELECT a.folder_name FROM impressions i JOIN ads a ON i.ad_id = a.id 
            WHERE i.played_at > datetime('now','-30 minutes') 
            GROUP BY a.id ORDER BY COUNT(*) DESC LIMIT 1
        """).fetchone()
        if top_ad: stats["top_ad_30min"] = top_ad[0]

    return {"impressions": [dict(r) for r in rows], "stats": stats}

def _get_metrics_data(date_from, date_to, **filters) -> dict:
    where_clause = " WHERE date(i.played_at) BETWEEN ? AND ?"
    params = [date_from, date_to]
    
    if filters.get("advertiser_id"):
        where_clause += " AND a.advertiser_id = ?"; params.append(filters["advertiser_id"])
    if filters.get("campaign_id"):
        where_clause += " AND a.campaign_id = ?"; params.append(filters["campaign_id"])
    if filters.get("ad_id"):
        where_clause += " AND a.id = ?"; params.append(filters["ad_id"])
    if filters.get("placement") and filters["placement"] != "all":
        where_clause += " AND i.placement = ?"; params.append(filters["placement"])
    if filters.get("content_type") and filters["content_type"] != "all":
        if filters["content_type"] == "movie": where_clause += " AND i.content_path LIKE '%/movies/%'"
        elif filters["content_type"] == "tv": where_clause += " AND i.content_path LIKE '%/tv/%'"

    with get_db() as conn:
        total = conn.execute(f"SELECT COUNT(*) FROM impressions i JOIN ads a ON i.ad_id = a.id {where_clause}", params).fetchone()[0]
        
        # Trend calculation
        d1 = datetime.fromisoformat(date_from)
        d2 = datetime.fromisoformat(date_to)
        delta = (d2 - d1).days + 1
        prev_to = (d1 - timedelta(days=1)).isoformat()
        prev_from = (d1 - timedelta(days=delta)).isoformat()
        prev_total = conn.execute(f"SELECT COUNT(*) FROM impressions i JOIN ads a ON i.ad_id = a.id WHERE date(i.played_at) BETWEEN ? AND ?", [prev_from, prev_to]).fetchone()[0]
        trend = round(((total - prev_total) / prev_total * 100), 1) if prev_total > 0 else 0

        # KPI Cards
        pre_total = conn.execute(f"SELECT COUNT(*) FROM impressions i JOIN ads a ON i.ad_id = a.id {where_clause} AND i.placement='pre'", params).fetchone()[0]
        mid_total = conn.execute(f"SELECT COUNT(*) FROM impressions i JOIN ads a ON i.ad_id = a.id {where_clause} AND i.placement='mid'", params).fetchone()[0]
        post_total = conn.execute(f"SELECT COUNT(*) FROM impressions i JOIN ads a ON i.ad_id = a.id {where_clause} AND i.placement='post'", params).fetchone()[0]
        unique_content = conn.execute(f"SELECT COUNT(DISTINCT i.content_path) FROM impressions i JOIN ads a ON i.ad_id = a.id {where_clause}", params).fetchone()[0]
        
        top_adv_row = conn.execute(f"""
            SELECT adv.name, COUNT(*) as count 
            FROM impressions i JOIN ads a ON i.ad_id = a.id 
            LEFT JOIN advertisers adv ON a.advertiser_id = adv.id 
            {where_clause} GROUP BY a.advertiser_id ORDER BY count DESC LIMIT 1
        """, params).fetchone()
        top_advertiser = {"name": top_adv_row[0] if top_adv_row else "None", "count": top_adv_row[1] if top_adv_row else 0}

        # Daily Chart
        daily_rows = conn.execute(f"SELECT date(i.played_at) as day, COUNT(*) as count FROM impressions i JOIN ads a ON i.ad_id = a.id {where_clause} GROUP BY day ORDER BY day ASC", params).fetchall()
        daily_counts = [dict(r) for r in daily_rows]
        max_daily = max([r["count"] for r in daily_counts]) if daily_counts else 1

        # Advertiser Performance
        adv_metrics = [dict(r) for r in conn.execute(f"""
            SELECT adv.id as advertiser_id, adv.name as advertiser_name, COUNT(*) as total,
                   SUM(CASE WHEN i.placement='pre' THEN 1 ELSE 0 END) as pre,
                   SUM(CASE WHEN i.placement='mid' THEN 1 ELSE 0 END) as mid,
                   SUM(CASE WHEN i.placement='post' THEN 1 ELSE 0 END) as post,
                   (SELECT COUNT(*) FROM ads WHERE advertiser_id = adv.id AND active=1) as active_ads
            FROM impressions i JOIN ads a ON i.ad_id = a.id
            LEFT JOIN advertisers adv ON a.advertiser_id = adv.id
            {where_clause} GROUP BY a.advertiser_id ORDER BY total DESC
        """, params).fetchall()]

        # Ad Performance
        ad_metrics = [dict(r) for r in conn.execute(f"""
            SELECT a.folder_name, a.ad_description, adv.name as advertiser_name, camp.name as campaign_name,
                   COUNT(*) as total, a.play_count, a.max_plays, a.active, a.start_date, a.end_date,
                   SUM(CASE WHEN i.placement='pre' THEN 1 ELSE 0 END) as pre,
                   SUM(CASE WHEN i.placement='mid' THEN 1 ELSE 0 END) as mid,
                   SUM(CASE WHEN i.placement='post' THEN 1 ELSE 0 END) as post,
                   MAX(i.played_at) as last_played
            FROM impressions i JOIN ads a ON i.ad_id = a.id
            LEFT JOIN advertisers adv ON a.advertiser_id = adv.id
            LEFT JOIN campaigns camp ON a.campaign_id = camp.id
            {where_clause} GROUP BY a.id ORDER BY total DESC
        """, params).fetchall()]

        # Content Breakdown
        content_breakdown = [dict(r) for r in conn.execute(f"""
            SELECT i.content_path, COUNT(*) as total,
                   SUM(CASE WHEN i.placement='pre' THEN 1 ELSE 0 END) as pre,
                   SUM(CASE WHEN i.placement='mid' THEN 1 ELSE 0 END) as mid,
                   SUM(CASE WHEN i.placement='post' THEN 1 ELSE 0 END) as post
            FROM impressions i JOIN ads a ON i.ad_id = a.id
            {where_clause} GROUP BY i.content_path ORDER BY total DESC LIMIT 20
        """, params).fetchall()]

    return {
        "total_impressions": total, "trend": trend, "pre_total": pre_total, "mid_total": mid_total, "post_total": post_total,
        "unique_content_count": unique_content, "top_advertiser": top_advertiser,
        "daily_counts": daily_counts, "max_daily": max_daily,
        "advertiser_metrics": adv_metrics, "ad_metrics": ad_metrics, "content_breakdown": content_breakdown
    }

def _get_settings() -> dict:
    defaults = {"mid_roll_interval": 600, "pre_ad_count": 1, "mid_ad_count": 1, "post_ad_count": 1}
    with get_db() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT, description TEXT, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
        for k, v in defaults.items():
            conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (k, str(v)))
        conn.commit()
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
        for row in rows:
            if row["key"] in defaults:
                try: defaults[row["key"]] = int(row["value"])
                except: pass
    return defaults

def _get_system_health() -> dict:
    def count_dirs(p): return len([d for d in os.scandir(p) if d.is_dir()]) if os.path.exists(p) else 0
    
    health = {
        "hls_movies": count_dirs(os.path.join(HLS_PATH, "movies")),
        "hls_tv": count_dirs(os.path.join(HLS_PATH, "tv")),
        "ads_live": count_dirs(ADS_PATH),
        "ads_incoming": count_dirs(os.path.join(ADS_PATH, "incoming")),
        "ads_rejected": count_dirs(os.path.join(ADS_PATH, "rejected")),
        "db_size_mb": round(os.path.getsize(DB_PATH)/1024/1024, 2) if os.path.exists(DB_PATH) else 0,
    }
    with get_db() as conn:
        health["total_ads"] = conn.execute("SELECT COUNT(*) FROM ads").fetchone()[0]
        health["total_impressions"] = conn.execute("SELECT COUNT(*) FROM impressions").fetchone()[0]
        health["oldest_imp"] = conn.execute("SELECT MIN(played_at) FROM impressions").fetchone()[0]
        health["newest_imp"] = conn.execute("SELECT MAX(played_at) FROM impressions").fetchone()[0]
    
    # Simple pings
    import urllib.request
    try:
        with urllib.request.urlopen("http://127.0.0.1:8083/health", timeout=1) as r:
            health["adserver_status"] = "online" if r.status == 200 else "error"
    except: health["adserver_status"] = "offline"
    
    try:
        import redis
        r = redis.Redis(host="127.0.0.1", port=6379, db=settings.REDIS_DB, password=settings.REDIS_PASS, socket_timeout=1)
        health["redis_status"] = "online" if r.ping() else "error"
    except: health["redis_status"] = "offline"
    
    return health

def _scan_ad_folders() -> dict:
    scanned = new = already = invalid = 0
    try: entries = [e for e in os.scandir(ADS_PATH) if e.is_dir() and e.name.startswith("advert")]
    except: return {"error": f"{ADS_PATH} not found"}
    
    for entry in entries:
        scanned += 1
        if not os.path.exists(os.path.join(ADS_PATH, entry.name, "master.m3u8")):
            invalid += 1; continue
            
        with get_db() as conn:
            exists = conn.execute("SELECT id FROM ads WHERE folder_name=?", (entry.name,)).fetchone()
            
        if exists:
            already += 1
        else:
            try:
                ad_selector.upsert_ad(folder_name=entry.name)
                new += 1
            except Exception as e:
                logger.error(f"Failed to upsert ad {entry.name}: {e}")
                invalid += 1
                
    return {"scanned": scanned, "new": new, "already": already, "invalid": invalid}

if __name__ == "__main__":
    import uvicorn
    # Port 8089 is proxied by Nginx on port 8082 for local access
    uvicorn.run(app, host="127.0.0.1", port=8089, log_level="info")
