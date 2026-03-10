import sqlite3, os, time, csv, io, re, logging
from datetime import date
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, JSONResponse, StreamingResponse
from jinja2 import Environment, FileSystemLoader, select_autoescape
from starlette.templating import Jinja2Templates

# LOGGING
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("adserver-admin")

# JINJA2 SETUP
_jinja_env = Environment(
    loader=FileSystemLoader("/opt/adserver/templates"),
    extensions=["jinja2.ext.do"],
    autoescape=select_autoescape(["html"])
)
templates = Jinja2Templates(env=_jinja_env)

# CONSTANTS
DB_PATH    = "/opt/adserver/adserver.db"
ADS_PATH   = "/srv/vod/ads"
START_TIME = time.time()

# APP
app = FastAPI(title="Ad Server Admin", version="1.0.0")
app.add_middleware(CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"], allow_headers=["*"])

# DATABASE HELPER
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

# DATE HELPERS
def _month_start() -> str:
    return date.today().replace(day=1).isoformat()

def _today() -> str:
    return date.today().isoformat()

# PAGE ROUTES
@app.get("/")
async def root():
    return RedirectResponse("/ads", status_code=302)

@app.get("/ads")
async def ads_page(request: Request):
    ads = _get_all_ads()
    return templates.TemplateResponse("ads.html", {
        "request": request, "ads": ads,
        "total": len(ads),
        "active_count": sum(1 for a in ads if a["active"]),
        "active_page": "ads"
    })

@app.get("/metrics")
async def metrics_page(request: Request):
    start = request.query_params.get("start") or _month_start()
    end   = request.query_params.get("end")   or _today()
    raw   = _get_metrics_raw(start, end)

    # Aggregate in Python
    per_ad = {}
    for m in raw:
        fn = m["folder_name"]
        if fn not in per_ad:
            per_ad[fn] = {"folder_name":fn,"ad_id":m["ad_id"],
                          "pre":0,"mid":0,"post":0,"total":0}
        per_ad[fn][m["placement"]] += m["count"]
        per_ad[fn]["total"] += m["count"]

    ad_rows   = sorted(per_ad.values(), key=lambda x: x["total"], reverse=True)
    total     = sum(r["total"] for r in ad_rows)
    pre_t     = sum(r["pre"]   for r in ad_rows)
    mid_t     = sum(r["mid"]   for r in ad_rows)
    post_t    = sum(r["post"]  for r in ad_rows)
    max_t     = ad_rows[0]["total"] if ad_rows else 1
    
    def pct(n): return round(n/total*100,1) if total else 0.0

    return templates.TemplateResponse("metrics.html", {
        "request":           request,
        "ad_rows":           ad_rows,
        "total_impressions": total,
        "pre_total":  pre_t,   "mid_total":  mid_t,   "post_total": post_t,
        "pre_pct":    pct(pre_t), "mid_pct": pct(mid_t),"post_pct": pct(post_t),
        "top_ad":     ad_rows[0]["folder_name"] if ad_rows else "None",
        "max_total":  max_t,
        "date_start": start, "date_end": end,
        "active_page": "metrics"
    })

@app.get("/activity")
async def activity_page(request: Request):
    impressions = _get_recent_impressions(50)
    return templates.TemplateResponse("activity.html", {
        "request": request, "impressions": impressions, "active_page": "activity"
    })

@app.get("/settings")
async def settings_page(request: Request):
    settings = _get_settings()
    return templates.TemplateResponse("settings.html", {
        "request": request, "settings": settings, "active_page": "settings"
    })

# API ROUTES
@app.get("/api/ads")
async def api_ads():
    ads = _get_all_ads()
    return {"ads": ads, "count": len(ads)}

@app.post("/api/ads/scan")
async def api_ads_scan():
    return JSONResponse(_scan_ad_folders())

@app.put("/api/ads/{ad_id}")
async def api_update_ad(ad_id: int, request: Request):
    body = await request.json()
    allowed = {"priority","placement_pre","placement_mid","placement_post","active"}
    updates = {k:v for k,v in body.items() if k in allowed}
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
    return JSONResponse(dict(row))

@app.delete("/api/ads/{ad_id}")
async def api_delete_ad(ad_id: int):
    with get_db() as conn:
        conn.execute("UPDATE ads SET active=0,updated_at=datetime('now') WHERE id=?",
                     (ad_id,))
        conn.commit()
    return JSONResponse({"success":True,"ad_id":ad_id})

@app.get("/api/metrics")
async def api_metrics(request: Request):
    start = request.query_params.get("start") or _month_start()
    end   = request.query_params.get("end")   or _today()
    raw   = _get_metrics_raw(start, end)
    
    per_ad = {}
    for m in raw:
        fn = m["folder_name"]
        if fn not in per_ad:
            per_ad[fn] = {"folder_name":fn,"ad_id":m["ad_id"],
                          "pre":0,"mid":0,"post":0,"total":0}
        per_ad[fn][m["placement"]] += m["count"]
        per_ad[fn]["total"] += m["count"]
    
    ad_rows = sorted(per_ad.values(), key=lambda x: x["total"], reverse=True)
    total = sum(r["total"] for r in ad_rows)
    
    return JSONResponse({"metrics":ad_rows,"total":total,
                         "period":{"start":start,"end":end}})

@app.get("/api/metrics/export")
async def api_metrics_export(request: Request):
    start = request.query_params.get("start") or _month_start()
    end   = request.query_params.get("end")   or _today()
    
    raw = _get_metrics_raw(start, end)
    per_ad = {}
    for m in raw:
        fn = m["folder_name"]
        if fn not in per_ad:
            per_ad[fn] = {"folder_name":fn,"ad_id":m["ad_id"],
                          "pre":0,"mid":0,"post":0,"total":0}
        per_ad[fn][m["placement"]] += m["count"]
        per_ad[fn]["total"] += m["count"]
    ad_rows = sorted(per_ad.values(), key=lambda x: x["total"], reverse=True)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ad_id","folder_name","pre_roll_plays","mid_roll_plays",
                     "post_roll_plays","total_plays","period_start","period_end"])
    for r in ad_rows:
        writer.writerow([r["ad_id"],r["folder_name"],r["pre"],r["mid"],
                         r["post"],r["total"],start,end])
    
    filename = f"ad_metrics_{start}_{end}.csv"
    return StreamingResponse(iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"})

@app.get("/api/impressions/recent")
async def api_impressions_recent():
    impressions = _get_recent_impressions(50)
    return JSONResponse({"impressions": impressions, "count": len(impressions)})

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
    try:
        with get_db() as conn:
            total_ads = conn.execute("SELECT COUNT(*) FROM ads").fetchone()[0]
            total_imp = conn.execute("SELECT COUNT(*) FROM impressions").fetchone()[0]
        db_mb = round(os.path.getsize(DB_PATH)/1024/1024, 2)
    except:
        total_ads = 0
        total_imp = 0
        db_mb = 0

    return JSONResponse({
        "status":"ok","service":"adserver-admin",
        "db_size_mb":db_mb,"total_ads":total_ads,
        "total_impressions":total_imp,
        "uptime_seconds":int(time.time()-START_TIME)
    })

@app.get("/health")
async def health():
    return JSONResponse({"status":"ok","service":"adserver-admin"})

# HELPER FUNCTIONS
def _get_all_ads() -> list:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM ads ORDER BY priority ASC, folder_name ASC"
        ).fetchall()
    return [dict(r) for r in rows]

def _get_metrics_raw(start: str, end: str) -> list:
    with get_db() as conn:
        rows = conn.execute("""
            SELECT a.id as ad_id, a.folder_name, i.placement, COUNT(*) as count
            FROM impressions i JOIN ads a ON i.ad_id = a.id
            WHERE date(i.played_at) BETWEEN ? AND ?
            GROUP BY a.id, a.folder_name, i.placement
            ORDER BY count DESC
        """, (start, end)).fetchall()
    return [dict(r) for r in rows]

def _get_recent_impressions(limit: int = 50) -> list:
    with get_db() as conn:
        rows = conn.execute("""
            SELECT i.id, i.ad_id, a.folder_name, i.content_path,
                   i.placement, i.session_id, i.played_at
            FROM impressions i JOIN ads a ON i.ad_id = a.id
            ORDER BY i.played_at DESC LIMIT ?
        """, (limit,)).fetchall()
    return [dict(r) for r in rows]

def _get_settings() -> dict:
    defaults = {
        "mid_roll_interval": 600,
        "pre_ad_count": 1,
        "mid_ad_count": 1,
        "post_ad_count": 1
    }
    with get_db() as conn:
        # Check if table exists and has the old schema
        try:
            conn.execute("SELECT key FROM settings LIMIT 1")
        except sqlite3.OperationalError:
            # Table might not exist OR it has the old schema (no 'key' column)
            try:
                # Check if it's the old schema (has 'id' column)
                conn.execute("SELECT id FROM settings LIMIT 1")
                conn.execute("DROP TABLE settings")
                logger.info("Dropped old settings table schema")
            except:
                pass

        # Ensure table exists (matches installer)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                description TEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Insert defaults if missing
        for k, v in defaults.items():
            conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (k, str(v)))
        conn.commit()
        
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
        for row in rows:
            if row["key"] in defaults:
                try:
                    defaults[row["key"]] = int(row["value"])
                except:
                    pass
    return defaults

def _scan_ad_folders() -> dict:
    scanned = new = already = invalid = 0
    details = []
    try:
        entries = [e for e in os.scandir(ADS_PATH)
                   if e.is_dir() and e.name.startswith("advert")]
    except FileNotFoundError:
        return {"error": f"{ADS_PATH} not found"}
    
    with get_db() as conn:
        for entry in sorted(entries, key=lambda x: x.name):
            scanned += 1
            master = os.path.join(ADS_PATH, entry.name, "master.m3u8")
            if not os.path.exists(master):
                invalid += 1
                details.append({"folder":entry.name,"status":"invalid",
                                 "reason":"master.m3u8 missing"})
                continue
            
            cur = conn.execute("SELECT id FROM ads WHERE folder_name=?",
                               (entry.name,)).fetchone()
            if cur:
                already += 1
                details.append({"folder":entry.name,"status":"already_registered"})
            else:
                conn.execute(
                    "INSERT OR IGNORE INTO ads (folder_name) VALUES (?)",
                    (entry.name,)
                )
                new += 1
                details.append({"folder":entry.name,"status":"registered"})
        conn.commit()
    return {"scanned":scanned,"new":new,"already_registered":already,
            "invalid":invalid,"details":details}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8089, log_level="info")
