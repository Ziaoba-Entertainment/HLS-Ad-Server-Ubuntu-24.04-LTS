# HLS Ad Server Technical Brief

## 1. System Architecture Overview
The system is a high-performance HLS ad-stitching middleware. It intercepts `.m3u8` playlist requests, deterministically injects pre-roll, mid-roll, and post-roll ads based on session IDs, and serves the modified manifests.

## 2. Port Mapping & Services

| Port | Service | Description |
| :--- | :--- | :--- |
| **80** | **Nginx (Public)** | Entry point for HLS Delivery (`stream.ziaoba.com`) |
| **88** | **Nginx (Admin)** | Entry point for Admin WebUI (Basic Auth protected) |
| **8083** | **FastAPI (Middleware)** | Core ad-stitching logic (`main.py`) |
| **8089** | **FastAPI (Admin)** | Admin Backend & Dashboard (`admin_app.py`) |
| **6379** | **Redis** | Caching layer for stitched manifests (DB 1) |
| **3000** | **Development** | Default port for local development |

## 3. Nginx Configuration Logic
Nginx acts as both a reverse proxy and a high-speed static file server.

### HLS Delivery (Port 80)
- **Playlists**: Requests for `*.m3u8` are proxied to the Middleware on `127.0.0.1:8083`.
- **Segments**: `.ts` files are served directly from the filesystem for zero-overhead delivery:
  - `/segments/hls/movies/` -> `/srv/vod/hls/movies/`
  - `/segments/hls/tv/` -> `/srv/vod/hls/tv/`
  - `/segments/ads/` -> `/srv/vod/ads/`
- **Proxy Headers**: Crucial for correct URL generation in manifests:
  - `X-Forwarded-Host`: Passes the public domain (`stream.ziaoba.com`).
  - `X-Forwarded-Proto`: Passes the protocol (`https` from Cloudflare).

### Admin UI (Port 88)
- Proxies all requests to the Admin Backend on `127.0.0.1:8089`.
- Serves static assets (CSS/JS) from `/opt/adserver/static/`.

## 4. API Endpoints

### Middleware (Port 8083)
- `GET /playlist/{movies|tv}/{path}`: Returns a stitched HLS manifest.
- `GET /health`: Returns system health, Redis status, and DB status.

### Admin Backend (Port 8089)
- `GET /api/ads`: List all ads with filters.
- `GET /api/advertisers`: Manage advertisers.
- `GET /api/campaigns`: Manage campaigns.
- `GET /api/metrics`: Aggregated impression data for charts.
- `GET /api/impressions/recent`: Real-time impression log.

## 5. URL Generation & Proxy Handling
The application uses `ProxyHeadersMiddleware`. When generating absolute URLs for segments inside the `.m3u8` files, the app **must not** use the local request host.

**Correct Logic:**
```python
# The middleware handles this automatically
host = request.url.hostname  # Returns 'stream.ziaoba.com'
proto = request.url.scheme   # Returns 'https'
```
This ensures that even though Nginx talks to FastAPI over `http://127.0.0.1:8083`, the manifest contains public `https://stream.ziaoba.com/...` links.

## 6. Maintenance Commands
The `install_ad_server.sh` script is the primary maintenance tool:
- `sudo ./install_ad_server.sh update`: Deploys code and runs migrations.
- `sudo ./install_ad_server.sh status`: Checks all ports and service health.
- `sudo ./install_ad_server.sh logs`: Combined log stream.
