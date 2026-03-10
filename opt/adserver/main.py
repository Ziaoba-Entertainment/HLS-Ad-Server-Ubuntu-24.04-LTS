import os, re, time, hashlib, random, logging, sqlite3
from logging.handlers import RotatingFileHandler
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, JSONResponse
import aiofiles

try:
    import redis as redis_lib
except ImportError:
    redis_lib = None

# CONSTANTS
DB_PATH           = "/opt/adserver/adserver.db"
MOVIES_PATH       = "/srv/vod/hls/movies"
TV_PATH           = "/srv/vod/hls/tv"
ADS_PATH          = "/srv/vod/ads"
LOG_PATH          = "/var/log/adserver/adserver.log"
BIND_HOST         = "127.0.0.1"
BIND_PORT         = 8083
MID_ROLL_INTERVAL = 600

# LOGGING
os.makedirs("/var/log/adserver", exist_ok=True)
logger = logging.getLogger("adserver")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s %(levelname)-8s: %(message)s")
file_handler = RotatingFileHandler(LOG_PATH, maxBytes=10*1024*1024, backupCount=5)
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# REDIS
redis_client = None
if redis_lib:
    try:
        _r = redis_lib.Redis(host="127.0.0.1", port=6379, db=1,
                              decode_responses=True, socket_timeout=2)
        _r.ping()
        redis_client = _r
        logger.info("Redis connected — db=1 (isolated from transcoding pipeline db=0)")
    except Exception as e:
        logger.warning(f"Redis unavailable: {e} — running without cache")

# APP
app = FastAPI(title="HLS Ad Stitching Middleware", version="1.0.0")
app.add_middleware(CORSMiddleware,
    allow_origins=["*"], allow_methods=["GET","OPTIONS"], allow_headers=["*"])

# DATABASE
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def _get_settings() -> dict:
    defaults = {
        "mid_roll_interval": MID_ROLL_INTERVAL,
        "pre_ad_count": 1,
        "mid_ad_count": 1,
        "post_ad_count": 1
    }
    try:
        with get_db() as conn:
            rows = conn.execute("SELECT key, value FROM settings").fetchall()
            for row in rows:
                if row["key"] in defaults:
                    try:
                        defaults[row["key"]] = int(row["value"])
                    except:
                        pass
    except Exception as e:
        logger.error(f"Error fetching settings: {e}")
    return defaults

@app.get("/health")
async def health_check():
    redis_status = "ok"
    if redis_client:
        try:
            redis_client.ping()
        except:
            redis_status = "unavailable"
    else:
        redis_status = "unavailable"

    db_status = "ok"
    active_ads = 0
    try:
        with get_db() as conn:
            active_ads = conn.execute("SELECT COUNT(*) FROM ads WHERE active=1").fetchone()[0]
    except:
        db_status = "error"

    return {
        "status": "ok",
        "service": "adserver",
        "version": "1.0.0",
        "bind_port": BIND_PORT,
        "redis_status": redis_status,
        "redis_db": 1,
        "db_status": db_status,
        "paths": {
            "movies_exists": os.path.exists(MOVIES_PATH),
            "tv_exists": os.path.exists(TV_PATH),
            "ads_exists": os.path.exists(ADS_PATH)
        },
        "active_ads": active_ads
    }

@app.get("/api/status")
async def api_status():
    return await health_check()

@app.get("/api/ads")
async def api_ads():
    try:
        with get_db() as conn:
            rows = conn.execute("""
                SELECT id, folder_name, priority, placement_pre, placement_mid,
                       placement_post, play_count, active, duration_seconds
                FROM ads ORDER BY priority ASC, folder_name ASC
            """).fetchall()
            ads = [dict(r) for r in rows]
            active_count = sum(1 for a in ads if a["active"] == 1)
            return JSONResponse({"ads": ads, "count": len(ads), "active_count": active_count})
    except Exception as e:
        logger.error(f"Error fetching ads: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/playlist/{content_type}/{path:path}")
async def get_playlist(content_type: str, path: str, request: Request):
    if content_type not in ["movies", "tv"]:
        raise HTTPException(status_code=400, detail="Invalid content type")

    base = MOVIES_PATH if content_type == "movies" else TV_PATH
    fs_path = os.path.join(base, path.lstrip("/"))
    
    if not os.path.exists(fs_path):
        parent = os.path.dirname(fs_path)
        exists_str = "exists" if os.path.exists(parent) else "NOT FOUND"
        logger.warning(f"Content not found: {fs_path} | Parent dir {parent} {exists_str}")
        if os.path.exists(parent):
            try:
                files = os.listdir(parent)
                logger.info(f"Files in {parent}: {files[:10]}{'...' if len(files)>10 else ''}")
            except: pass
        raise HTTPException(status_code=404, detail=f"Content not found: {path}")

    # Session ID generation
    client_ip = (request.headers.get("X-Real-IP") or
                 request.headers.get("X-Forwarded-For","").split(",")[0].strip() or
                 (request.client.host if request.client else "unknown"))
    
    # Use the movie folder as the session key to ensure consistency across variants
    movie_id = os.path.dirname(path) or path
    session_key = f"{client_ip}:{movie_id}:{int(time.time() // 3600)}"
    session_id = hashlib.md5(session_key.encode()).hexdigest()

    # Redis cache check
    cache_key = f"adserver:pl:{hashlib.md5(f'{session_id}:{path}'.encode()).hexdigest()}"
    if redis_client:
        try:
            cached = redis_client.get(cache_key)
            if cached:
                return PlainTextResponse(cached, media_type="application/vnd.apple.mpegurl")
        except Exception:
            pass

    # Read original playlist
    try:
        async with aiofiles.open(fs_path, 'r', encoding='utf-8') as f:
            original_content = await f.read()
    except Exception as e:
        logger.error(f"Cannot read playlist {fs_path}: {e}")
        raise HTTPException(status_code=500)

    # Handle Master Playlist vs Media Playlist
    is_master = "#EXT-X-STREAM-INF" in original_content
    
    if is_master:
        logger.info(f"Master playlist detected: {path}. Rewriting variants...")
        lines = []
        base_dir = os.path.dirname(path)
        
        for line in original_content.splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                # It's a variant URI
                if not line.startswith("http"):
                    if line.startswith("/"):
                        # If it's an absolute path like /hls/movies/..., we need to strip the prefix
                        # and turn it into a /playlist/movies/... path
                        # This is tricky without knowing the exact mount points, but we can try to be smart
                        for prefix in ["/hls/movies/", "/hls/tv/"]:
                            if line.startswith(prefix):
                                rel = line[len(prefix):].lstrip("/")
                                ctype = "movies" if "movies" in prefix else "tv"
                                line = f"/playlist/{ctype}/{rel}"
                                break
                    else:
                        # Relative path
                        line = f"/playlist/{content_type}/{base_dir}/{line}".replace("//", "/")
                logger.debug(f"Rewrote variant: {line}")
            lines.append(line)
        return PlainTextResponse("\n".join(lines), media_type="application/vnd.apple.mpegurl")

    # Select ads deterministically for this session
    settings = _get_settings()
    pre_ads  = _select_weighted_ads("pre", settings["pre_ad_count"], session_id)
    mid_ads  = _select_weighted_ads("mid", settings["mid_ad_count"], session_id)
    post_ads = _select_weighted_ads("post", settings["post_ad_count"], session_id)

    if not any([pre_ads, mid_ads, post_ads]):
        logger.info(f"No active ads selected for {path}")
        return PlainTextResponse(original_content, media_type="application/vnd.apple.mpegurl")

    raw_host = request.headers.get("host", "localhost")
    host = raw_host.split(":")[0]
    proto = request.headers.get("X-Forwarded-Proto", request.url.scheme)

    try:
        stitched = await _build_stitched_playlist(
            content_master_path=fs_path,
            content_type=content_type,
            content_subpath=path,
            pre_ads=pre_ads, mid_ads=mid_ads, post_ads=post_ads,
            mid_roll_interval=settings["mid_roll_interval"],
            host=host,
            proto=proto
        )
    except Exception as e:
        logger.error(f"Stitching failed for {path}: {e}", exc_info=True)
        stitched = None

    result = stitched if stitched else original_content

    if redis_client and stitched:
        try:
            redis_client.setex(cache_key, 300, stitched)
        except Exception:
            pass

    for placement, ads in [("pre",pre_ads),("mid",mid_ads),("post",post_ads)]:
        for ad in ads:
            _log_impression(ad["id"], path, placement, session_id)

    return PlainTextResponse(result, media_type="application/vnd.apple.mpegurl")

def _select_weighted_ads(placement: str, count: int, seed: str = None) -> list:
    if count <= 0: return []
    col_map = {"pre":"placement_pre","mid":"placement_mid","post":"placement_post"}
    col = col_map.get(placement)
    if not col: return []
    try:
        with get_db() as conn:
            rows = conn.execute(
                f"SELECT id, folder_name, priority FROM ads "
                f"WHERE active=1 AND {col}=1"
            ).fetchall()
        if not rows: return []
        ads = [dict(r) for r in rows]
        weights = [max(6 - a["priority"], 1) for a in ads]
        
        selected_ads = []
        if seed:
            state = random.getstate()
            for i in range(count):
                combined_seed = f"{seed}:{placement}:{i}"
                random.seed(combined_seed)
                sel = random.choices(ads, weights=weights, k=1)[0]
                selected_ads.append(sel)
            random.setstate(state)
        else:
            selected_ads = random.choices(ads, weights=weights, k=count)
            
        return selected_ads
    except Exception as e:
        logger.error(f"Ad selection error ({placement}): {e}")
        return []

def _log_impression(ad_id, content_path, placement, session_id) -> None:
    try:
        with get_db() as conn:
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
        logger.error(f"Impression log failed: {e}")

async def _build_stitched_playlist(content_master_path, content_type, content_subpath, pre_ads, mid_ads, post_ads, mid_roll_interval, host, proto) -> str:
    
    # State tracking for seamless transitions
    movie_state = {
        "key": None,  # Last #EXT-X-KEY seen in movie
        "map": None,  # Last #EXT-X-MAP seen in movie
    }
    
    def _parse_media_playlist(m3u8_path: str) -> list:
        if not os.path.exists(m3u8_path): return []
        items = []
        with open(m3u8_path, 'r') as f:
            lines = f.readlines()
        
        current_extinf = None
        for line in lines:
            line = line.strip()
            if not line: continue
            if line.startswith("#EXTINF:"):
                current_extinf = {"type": "extinf", "content": line, "tags": []}
            elif line.startswith("#"):
                # Skip tags we handle ourselves
                if any(line.startswith(t) for t in [
                    "#EXTM3U", "#EXT-X-VERSION", "#EXT-X-TARGETDURATION",
                    "#EXT-X-MEDIA-SEQUENCE", "#EXT-X-PLAYLIST-TYPE", "#EXT-X-ENDLIST"
                ]):
                    continue
                
                if current_extinf:
                    # Tags that belong to the current segment (like BYTERANGE)
                    current_extinf["tags"].append(line)
                else:
                    items.append({"type": "tag", "content": line})
            else:
                # It's a URI
                if current_extinf:
                    current_extinf["uri"] = line
                    items.append(current_extinf)
                    current_extinf = None
                else:
                    items.append({"type": "uri", "content": line})
        return items

    def _get_best_rendition(master_path: str, target_bandwidth: int = None) -> str | None:
        if not os.path.exists(master_path): return None
        with open(master_path, 'r') as f:
            content = f.read()
        
        if "#EXT-X-STREAM-INF" not in content:
            # It's already a media playlist
            return master_path
            
        renditions = []
        lines = content.splitlines()
        for i, line in enumerate(lines):
            if line.startswith("#EXT-X-STREAM-INF"):
                bandwidth = 0
                match = re.search(r'BANDWIDTH=(\d+)', line)
                if match:
                    bandwidth = int(match.group(1))
                
                if i + 1 < len(lines):
                    uri = lines[i+1].strip()
                    if uri and not uri.startswith("#"):
                        renditions.append((bandwidth, uri))
        
        if not renditions: return None
        
        if target_bandwidth:
            # Find rendition closest to target bandwidth
            renditions.sort(key=lambda x: abs(x[0] - target_bandwidth))
        else:
            # Default to highest
            renditions.sort(key=lambda x: x[0], reverse=True)
            
        best_uri = renditions[0][1]
        
        if best_uri.startswith("http"):
            return best_uri
        
        return os.path.join(os.path.dirname(master_path), best_uri)

    def _extinf_dur(line: str) -> float:
        m = re.search(r'#EXTINF:([\d.]+)', line)
        return float(m.group(1)) if m else 6.0

    def _content_ts_url(ts: str, content_dir: str) -> str:
        if ts.startswith("http"): return ts
        
        # Handle relative vs absolute paths in the original playlist
        if not ts.startswith("/"):
            full_path = os.path.join(content_dir, ts)
        else:
            full_path = ts
            
        full_path = os.path.normpath(full_path)
        
        # Check both physical path and common URL prefixes
        if full_path.startswith(MOVIES_PATH):
            rel = full_path[len(MOVIES_PATH):].lstrip("/")
            return f"{proto}://{host}:8081/segments/hls/movies/{rel}"
        elif full_path.startswith("/hls/movies/"):
            rel = full_path[len("/hls/movies/"):].lstrip("/")
            return f"{proto}://{host}:8081/segments/hls/movies/{rel}"
        elif full_path.startswith(TV_PATH):
            rel = full_path[len(TV_PATH):].lstrip("/")
            return f"{proto}://{host}:8081/segments/hls/tv/{rel}"
        elif full_path.startswith("/hls/tv/"):
            rel = full_path[len("/hls/tv/"):].lstrip("/")
            return f"{proto}://{host}:8081/segments/hls/tv/{rel}"
        else:
            # If we can't map it, try to be smart about the relative path
            basename = os.path.basename(ts.split("?")[0])
            return f"{proto}://{host}:8081/segments/hls/{content_type}/{os.path.dirname(content_subpath)}/{basename}"

    def _ad_ts_url(ts: str, ad_folder: str) -> str:
        if ts.startswith("http"): return ts
        basename = os.path.basename(ts.split("?")[0])
        return f"{proto}://{host}:8081/segments/ads/{ad_folder}/{basename}"

    def _get_ad_segments(ad_folder: str) -> list:
        master = os.path.join(ADS_PATH, ad_folder, "master.m3u8")
        if not os.path.exists(master):
            logger.warning(f"Ad folder or master playlist missing: {master}")
            return []
        rendition = _get_best_rendition(master, target_bandwidth=movie_bandwidth)
        if not rendition or not os.path.exists(rendition): return []
        return _parse_media_playlist(rendition)

    # Try to determine movie bandwidth to match ads
    movie_bandwidth = None
    try:
        # The content_master_path is the variant playlist. 
        # We need the master playlist to find the bandwidth for this variant.
        parent_dir = os.path.dirname(content_master_path)
        master_m3u8 = os.path.join(parent_dir, "master.m3u8")
        if os.path.exists(master_m3u8):
            with open(master_m3u8, 'r') as f:
                m_cont = f.read()
            m_lines = m_cont.splitlines()
            variant_filename = os.path.basename(content_master_path)
            for i, ml in enumerate(m_lines):
                if variant_filename in ml:
                    # Found our rendition, look at the line before for bandwidth
                    if i > 0 and m_lines[i-1].startswith("#EXT-X-STREAM-INF"):
                        match = re.search(r'BANDWIDTH=(\d+)', m_lines[i-1])
                        if match:
                            movie_bandwidth = int(match.group(1))
                            break
    except Exception as e:
        logger.debug(f"Could not determine movie bandwidth: {e}")

    content_rendition = _get_best_rendition(content_master_path)
    if not content_rendition:
        async with aiofiles.open(content_master_path, 'r') as f:
            return await f.read()
            
    content_dir = os.path.dirname(content_rendition)
    content_items = _parse_media_playlist(content_rendition)
    if not content_items:
        async with aiofiles.open(content_master_path, 'r') as f:
            return await f.read()

    # Collect all durations to find max target duration
    all_durs = []
    for item in content_items:
        if item["type"] == "extinf":
            all_durs.append(_extinf_dur(item["content"]))
    
    # Pre-scan for initial movie state (Key and Map)
    # This ensures we can restore the movie's encryption/init state after a pre-roll
    for item in content_items:
        if item["type"] == "tag":
            if item["content"].startswith("#EXT-X-KEY"):
                if movie_state["key"] is None: movie_state["key"] = item["content"]
            elif item["content"].startswith("#EXT-X-MAP"):
                if movie_state["map"] is None: movie_state["map"] = item["content"]
        elif item["type"] == "extinf":
            break

    # Check ad durations too
    for ads in [pre_ads, mid_ads, post_ads]:
        for ad in ads:
            ad_items = _get_ad_segments(ad["folder_name"])
            for item in ad_items:
                if item["type"] == "extinf":
                    all_durs.append(_extinf_dur(item["content"]))

    max_dur = max(all_durs) if all_durs else 6

    # Extract version and other global tags from original
    hls_version = 3
    for item in content_items:
        if item["type"] == "tag" and item["content"].startswith("#EXT-X-VERSION:"):
            try: hls_version = int(item["content"].split(":")[1])
            except: pass

    lines = [
        "#EXTM3U",
        f"#EXT-X-VERSION:{hls_version}",
        "#EXT-X-PLAYLIST-TYPE:VOD",
        f"#EXT-X-TARGETDURATION:{int(max_dur)+1}",
        "#EXT-X-MEDIA-SEQUENCE:0",
        "#EXT-X-INDEPENDENT-SEGMENTS",
    ]
    
    lines.append("")

    def _append_ads(ads_list, label):
        if not ads_list: return
        for ad in ads_list:
            ad_items = _get_ad_segments(ad["folder_name"])
            if not ad_items: continue
            
            lines.append(f"# AD-BREAK: {label} - {ad['folder_name']}")
            lines.append("#EXT-X-DISCONTINUITY")
            # Ads are assumed to be clear. If they are encrypted, their own tags will override this.
            lines.append("#EXT-X-KEY:METHOD=NONE")
            
            for item in ad_items:
                if item["type"] == "extinf":
                    lines.append(item["content"])
                    for tag in item.get("tags", []): lines.append(tag)
                    lines.append(_ad_ts_url(item["uri"], ad["folder_name"]))
                elif item["type"] == "tag":
                    # Preserve important tags from the ad rendition
                    if any(item["content"].startswith(t) for t in ["#EXT-X-MAP", "#EXT-X-KEY", "#EXT-X-BYTERANGE", "#EXT-X-DISCONTINUITY"]):
                        lines.append(item["content"])

    def _restore_content_state():
        lines.append("#EXT-X-DISCONTINUITY")
        if movie_state["key"]:
            lines.append(movie_state["key"])
        else:
            # Explicitly reset key if movie is clear but ad was encrypted or set METHOD=NONE
            lines.append("#EXT-X-KEY:METHOD=NONE")
        
        if movie_state["map"]:
            lines.append(movie_state["map"])

    # PRE-ROLL
    if pre_ads:
        _append_ads(pre_ads, "PRE")
        _restore_content_state()

    # CONTENT WITH MID-ROLLS
    cumulative = 0.0
    next_mid = mid_roll_interval
    mid_count = 0
    for item in content_items:
        if item["type"] == "extinf":
            dur = _extinf_dur(item["content"])
            if mid_ads and cumulative > 0 and cumulative >= next_mid:
                _append_ads(mid_ads, f"MID-{mid_count+1}")
                _restore_content_state()
                
                mid_count += 1
                next_mid += mid_roll_interval
            
            lines.append(item["content"])
            for tag in item.get("tags", []): lines.append(tag)
            lines.append(_content_ts_url(item["uri"], content_dir))
            cumulative += dur
        elif item["type"] == "tag":
            if item["content"].startswith("#EXT-X-KEY"):
                movie_state["key"] = item["content"]
            elif item["content"].startswith("#EXT-X-MAP"):
                movie_state["map"] = item["content"]
            lines.append(item["content"])
        elif item["type"] == "uri":
            lines.append(_content_ts_url(item["content"], content_dir))

    # POST-ROLL
    if post_ads:
        _append_ads(post_ads, "POST")

    lines.append("#EXT-X-ENDLIST")

    logger.info(
        f"Stitched OK: {content_type}/{content_subpath} | "
        f"pre={len(pre_ads)} | "
        f"mids={mid_count} (x{len(mid_ads)}) @ {mid_roll_interval}s intervals | "
        f"post={len(post_ads)} | "
        f"total={cumulative:.0f}s"
    )
    return "\n".join(lines)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=BIND_HOST, port=BIND_PORT, log_level="info")
