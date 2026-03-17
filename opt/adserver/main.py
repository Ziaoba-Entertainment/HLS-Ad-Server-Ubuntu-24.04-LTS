import os, re, time, hashlib, random, logging, sqlite3
from datetime import datetime
from logging.handlers import RotatingFileHandler
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
try:
    from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
except ImportError:
    ProxyHeadersMiddleware = None
from fastapi.responses import PlainTextResponse, JSONResponse
import m3u8
import aiofiles

try:
    import redis as redis_lib
except ImportError:
    redis_lib = None

# CONFIG
from config import settings

# CONSTANTS
BASE_DIR          = settings.BASE_DIR
DB_PATH           = settings.DB_PATH
HLS_PATH          = settings.get_hls_path()
ADS_PATH          = settings.get_ads_path()
LOG_PATH          = os.path.join(BASE_DIR, "adserver.log")
BIND_HOST         = settings.FASTAPI_HOST
BIND_PORT         = settings.FASTAPI_PORT
MID_ROLL_INTERVAL = settings.MID_ROLL_INTERVAL
REDIS_PASS        = settings.REDIS_PASS
REDIS_DB          = settings.REDIS_DB
REDIS_PREFIX      = settings.REDIS_PREFIX
TRANSCODER_API    = settings.TRANSCODER_API_URL

# LOGGING
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
logger = logging.getLogger("adserver")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s %(levelname)-8s: %(message)s")
file_handler = RotatingFileHandler(LOG_PATH, maxBytes=10*1024*1024, backupCount=5)
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# AD SELECTOR
from ad_selector import AdSelector
ad_selector = None

# REDIS
redis_client = None
if redis_lib:
    try:
        _r = redis_lib.Redis(host="127.0.0.1", port=6379, db=REDIS_DB,
                              password=REDIS_PASS,
                              decode_responses=True, socket_timeout=2)
        _r.ping()
        redis_client = _r
        logger.info(f"Redis connected — db={REDIS_DB} (prefix={REDIS_PREFIX})")
    except Exception as e:
        logger.warning(f"Redis unavailable: {e} — running without cache")

ad_selector = AdSelector(db_path=DB_PATH, redis_client=redis_client)

# APP
app = FastAPI(title="HLS Ad Stitching Middleware", version="1.2.0")
if ProxyHeadersMiddleware:
    app.add_middleware(ProxyHeadersMiddleware, trusted_hosts=["*"])
app.add_middleware(CORSMiddleware,
    allow_origins=["*"], allow_methods=["GET","OPTIONS"], allow_headers=["*"])

@app.on_event("startup")
async def startup_event():
    logger.info(f"INFO: Base URL will be generated as: {settings.PUBLIC_BASE_URL}")
    logger.info(f"INFO: FastAPI bound to: {BIND_HOST}:{BIND_PORT}")
    logger.info("INFO: Trusted proxy headers: enabled")

@app.get("/debug/baseurl")
async def debug_baseurl(request: Request):
    x_forwarded_host = request.headers.get("x-forwarded-host")
    x_forwarded_proto = request.headers.get("x-forwarded-proto")
    host_header = request.headers.get("host")
    
    computed_host = x_forwarded_host or host_header or "stream.ziaoba.com"
    computed_proto = x_forwarded_proto or "https"
    computed_base_url = f"{computed_proto}://{computed_host}"
    
    expected = "https://stream.ziaoba.com"
    status = "OK" if computed_base_url == expected else "MISMATCH"
    
    return {
        "x_forwarded_host": x_forwarded_host,
        "x_forwarded_proto": x_forwarded_proto,
        "host_header": host_header,
        "computed_base_url": computed_base_url,
        "expected": expected,
        "status": status
    }

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
        "version": "1.3.0",
        "bind_port": BIND_PORT,
        "redis_status": redis_status,
        "redis_db": 1,
        "db_status": db_status,
        "base_dir": BASE_DIR,
        "paths": {
            "hls_path": HLS_PATH,
            "ads_path": ADS_PATH,
            "hls_exists": os.path.exists(HLS_PATH),
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

    fs_path = os.path.join(HLS_PATH, content_type, path.lstrip("/"))
    
    # Directory handling: if path is a directory, look for master.m3u8
    if os.path.isdir(fs_path):
        for index_file in ["master.m3u8", "index.m3u8"]:
            test_path = os.path.join(fs_path, index_file)
            if os.path.exists(test_path):
                fs_path = test_path
                path = os.path.join(path, index_file).replace("//", "/")
                break

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

    # Select ads deterministically for this session
    settings = _get_settings()
    
    # Create a settings hash to include in cache key
    settings_str = f"{settings['mid_roll_interval']}:{settings['pre_ad_count']}:{settings['mid_ad_count']}:{settings['post_ad_count']}"
    settings_hash = hashlib.md5(settings_str.encode()).hexdigest()

    # Redis cache check
    cache_key = f"{REDIS_PREFIX}manifest_cache:{hashlib.md5(f'{session_id}:{path}:{settings_hash}'.encode()).hexdigest()}"
    if redis_client:
        try:
            cached = redis_client.get(cache_key)
            if cached:
                logger.debug(f"Cache hit for {path} (session: {session_id})")
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
    pre_ads  = ad_selector.select_ads("pre", settings["pre_ad_count"], session_id)
    mid_ads  = ad_selector.select_ads("mid", settings["mid_ad_count"], session_id)
    post_ads = ad_selector.select_ads("post", settings["post_ad_count"], session_id)

    logger.info(f"Ad selection for {session_id}: pre={len(pre_ads)}, mid={len(mid_ads)}, post={len(post_ads)}")

    if not any([pre_ads, mid_ads, post_ads]):
        logger.info(f"No active ads selected for {path}")
        return PlainTextResponse(original_content, media_type="application/vnd.apple.mpegurl")

    # Get host and proto, prioritizing forwarded headers
    # ProxyHeadersMiddleware handles X-Forwarded-Host and X-Forwarded-Proto
    # We construct base_url without port as per requirements
    forwarded_host = request.headers.get("x-forwarded-host", request.headers.get("host", "stream.ziaoba.com"))
    forwarded_proto = request.headers.get("x-forwarded-proto", "https")
    
    # Use the full host header (including port if present)
    host = forwarded_host
    proto = forwarded_proto

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
            _log_impression(ad["id"], ad["folder_name"], path, placement, session_id)

    return PlainTextResponse(result, media_type="application/vnd.apple.mpegurl")

def _log_impression(ad_id, folder_name, content_path, placement, session_id) -> None:
    try:
        ad_selector.record_impression(ad_id, content_path, placement, session_id)
        
        # Report to Transcoder API
        import urllib.request
        import json
        try:
            req = urllib.request.Request(f"{TRANSCODER_API}/api/ad/{folder_name}/play", method="POST")
            with urllib.request.urlopen(req, timeout=1) as response:
                pass
        except Exception as e:
            logger.warning(f"Failed to report play to Transcoder API: {e}")

    except Exception as e:
        logger.error(f"Impression log failed: {e}")

async def _build_stitched_playlist(content_master_path, content_type, content_subpath, pre_ads, mid_ads, post_ads, mid_roll_interval, host, proto) -> str:
    
    # State tracking for seamless transitions
    movie_state = {
        "key": None,  # Last #EXT-X-KEY seen in movie
        "map": None,  # Last #EXT-X-MAP seen in movie
    }
    
    def _map_to_public_url(full_path):
        full_path = os.path.normpath(full_path)
        if full_path.startswith(HLS_PATH):
            rel = full_path[len(HLS_PATH):].lstrip("/")
            return f"{proto}://{host}/segments/hls/{rel}"
        if full_path.startswith(ADS_PATH):
            rel = full_path[len(ADS_PATH):].lstrip("/")
            return f"{proto}://{host}/segments/ads/{rel}"
        return full_path

    def _get_best_rendition(master_path: str, target_bandwidth: int = None) -> str | None:
        if not os.path.exists(master_path): return None
        try:
            m = m3u8.load(master_path)
            if not m.is_variant:
                return master_path
            
            if not m.playlists: return None
            
            playlists = m.playlists
            if target_bandwidth:
                playlists.sort(key=lambda x: abs((x.stream_info.bandwidth or 0) - target_bandwidth))
            else:
                playlists.sort(key=lambda x: x.stream_info.bandwidth or 0, reverse=True)
            
            best = playlists[0]
            if best.uri.startswith("http"):
                return best.uri
            return os.path.join(os.path.dirname(master_path), best.uri)
        except Exception as e:
            logger.error(f"Error selecting rendition from {master_path}: {e}")
            return None

    async def _get_content_profile(subpath: str) -> dict:
        """Query Transcoder API for content encoding profile"""
        try:
            import urllib.request
            import json
            # Extract title/id from path
            title = os.path.dirname(subpath).split("/")[-1] or subpath.split("/")[-1]
            url = f"{TRANSCODER_API}/profile/{title}"
            with urllib.request.urlopen(url, timeout=1) as response:
                return json.loads(response.read().decode())
        except Exception as e:
            logger.debug(f"Failed to fetch content profile from Transcoder: {e}")
            return {}

    profile = await _get_content_profile(content_subpath)
    target_resolution = profile.get("resolution", "1080p")
    
    # Try to determine movie bandwidth to match ads
    movie_bandwidth = None
    try:
        parent_dir = os.path.dirname(content_master_path)
        master_m3u8 = os.path.join(parent_dir, "master.m3u8")
        if os.path.exists(master_m3u8):
            m = m3u8.load(master_m3u8)
            variant_filename = os.path.basename(content_master_path)
            for p in m.playlists:
                if variant_filename in p.uri:
                    movie_bandwidth = p.stream_info.bandwidth
                    break
    except Exception as e:
        logger.debug(f"Could not determine movie bandwidth: {e}")

    content_rendition = _get_best_rendition(content_master_path)
    if not content_rendition:
        async with aiofiles.open(content_master_path, 'r') as f:
            return await f.read()
            
    try:
        content_playlist = m3u8.load(content_rendition)
    except Exception as e:
        logger.error(f"Failed to load content playlist {content_rendition}: {e}")
        async with aiofiles.open(content_master_path, 'r') as f:
            return await f.read()

    # Collect all durations to find max target duration
    all_durs = [s.duration for s in content_playlist.segments]
    
    def _get_ad_playlist(ad_folder: str):
        master = os.path.join(ADS_PATH, ad_folder, "master.m3u8")
        if not os.path.exists(master):
            logger.warning(f"Ad folder or master playlist missing: {master}")
            return None
        
        rendition = None
        if target_resolution:
            res_path = os.path.join(ADS_PATH, ad_folder, f"{target_resolution}.m3u8")
            if os.path.exists(res_path):
                rendition = res_path
        
        if not rendition:
            rendition = _get_best_rendition(master, target_bandwidth=movie_bandwidth)
            
        if not rendition or not os.path.exists(rendition): return None
        try:
            return m3u8.load(rendition)
        except:
            return None

    # Check ad durations for target duration
    for ads in [pre_ads, mid_ads, post_ads]:
        for ad in ads:
            ap = _get_ad_playlist(ad["folder_name"])
            if ap:
                all_durs.extend([s.duration for s in ap.segments])

    max_dur = max(all_durs) if all_durs else 6
    hls_version = content_playlist.version or 3

    lines = [
        "#EXTM3U",
        f"#EXT-X-VERSION:{hls_version}",
        "#EXT-X-PLAYLIST-TYPE:VOD",
        f"#EXT-X-TARGETDURATION:{int(max_dur)+1}",
        "#EXT-X-MEDIA-SEQUENCE:0",
        "#EXT-X-INDEPENDENT-SEGMENTS",
        ""
    ]

    def _append_ad_segments(ad_p, label, folder_name):
        lines.append(f"# AD-BREAK: {label} - {folder_name}")
        lines.append("#EXT-X-DISCONTINUITY")
        lines.append(f"#EXT-X-PROGRAM-DATE-TIME:{datetime.now().isoformat()}Z")
        
        # Track if ad has its own key/map
        ad_has_key = False
        ad_has_map = False
        
        for segment in ad_p.segments:
            # Handle tags before segment
            if segment.key:
                # Resolve key URI if relative
                key_uri = segment.key.uri
                if not key_uri.startswith("http"):
                    key_uri = _map_to_public_url(os.path.join(os.path.dirname(ad_p.base_uri), key_uri))
                
                key_line = f"#EXT-X-KEY:METHOD={segment.key.method}"
                if segment.key.uri: key_line += f',URI="{key_uri}"'
                if segment.key.iv: key_line += f',IV={segment.key.iv}'
                lines.append(key_line)
                ad_has_key = True
            elif not ad_has_key:
                # If no key yet, ensure it's clear
                lines.append("#EXT-X-KEY:METHOD=NONE")
                ad_has_key = True

            if segment.init_section:
                map_uri = segment.init_section.uri
                if not map_uri.startswith("http"):
                    map_uri = _map_to_public_url(os.path.join(os.path.dirname(ad_p.base_uri), map_uri))
                
                map_line = f'#EXT-X-MAP:URI="{map_uri}"'
                if segment.init_section.byterange: map_line += f',BYTERANGE="{segment.init_section.byterange}"'
                lines.append(map_line)
                ad_has_map = True

            # Segment itself
            lines.append(f"#EXTINF:{segment.duration:.3f},")
            if segment.byterange:
                lines.append(f"#EXT-X-BYTERANGE:{segment.byterange}")
            
            # Resolve segment URI
            seg_uri = segment.uri
            if not seg_uri.startswith("http"):
                seg_uri = _map_to_public_url(os.path.join(os.path.dirname(ad_p.base_uri), seg_uri))
            lines.append(seg_uri)

    def _restore_content_state():
        lines.append("#EXT-X-DISCONTINUITY")
        lines.append(f"#EXT-X-PROGRAM-DATE-TIME:{datetime.now().isoformat()}Z")
        if movie_state["key"]:
            lines.append(movie_state["key"])
        else:
            lines.append("#EXT-X-KEY:METHOD=NONE")
        
        if movie_state["map"]:
            lines.append(movie_state["map"])

    # PRE-ROLL
    if pre_ads:
        for ad in pre_ads:
            ap = _get_ad_playlist(ad["folder_name"])
            if ap: _append_ad_segments(ap, "PRE", ad["folder_name"])
        _restore_content_state()

    # CONTENT WITH MID-ROLLS
    cumulative = 0.0
    interval = float(mid_roll_interval)
    next_mid = interval
    mid_count = 0
    
    for segment in content_playlist.segments:
        # Update movie state if segment has key/map
        if segment.key:
            key_uri = segment.key.uri
            if key_uri and not key_uri.startswith("http"):
                key_uri = _map_to_public_url(os.path.join(os.path.dirname(content_playlist.base_uri), key_uri))
            
            key_line = f"#EXT-X-KEY:METHOD={segment.key.method}"
            if segment.key.uri: key_line += f',URI="{key_uri}"'
            if segment.key.iv: key_line += f',IV={segment.key.iv}'
            movie_state["key"] = key_line
            lines.append(key_line)
        
        if segment.init_section:
            map_uri = segment.init_section.uri
            if not map_uri.startswith("http"):
                map_uri = _map_to_public_url(os.path.join(os.path.dirname(content_playlist.base_uri), map_uri))
            
            map_line = f'#EXT-X-MAP:URI="{map_uri}"'
            if segment.init_section.byterange: map_line += f',BYTERANGE="{segment.init_section.byterange}"'
            movie_state["map"] = map_line
            lines.append(map_line)

        # Check for mid-roll
        if mid_ads and interval > 0 and cumulative > 0 and cumulative >= next_mid:
            while cumulative >= next_mid:
                for ad in mid_ads:
                    ap = _get_ad_playlist(ad["folder_name"])
                    if ap: _append_ad_segments(ap, f"MID-{mid_count+1}", ad["folder_name"])
                _restore_content_state()
                mid_count += 1
                next_mid += interval

        # Add content segment
        lines.append(f"#EXTINF:{segment.duration:.3f},")
        if segment.byterange:
            lines.append(f"#EXT-X-BYTERANGE:{segment.byterange}")
        
        seg_uri = segment.uri
        if not seg_uri.startswith("http"):
            seg_uri = _map_to_public_url(os.path.join(os.path.dirname(content_playlist.base_uri), seg_uri))
        lines.append(seg_uri)
        
        cumulative += segment.duration

    # POST-ROLL
    if post_ads:
        for ad in post_ads:
            ap = _get_ad_playlist(ad["folder_name"])
            if ap: _append_ad_segments(ap, "POST", ad["folder_name"])

    lines.append("#EXT-X-ENDLIST")
    
    logger.info(f"Stitched {content_subpath}: pre={len(pre_ads)}, mid={mid_count}, post={len(post_ads)}")
    return "\n".join(lines)

if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting Ad Stitcher on {BIND_HOST}:{BIND_PORT}")
    uvicorn.run(app, host=BIND_HOST, port=BIND_PORT, log_level="info")
