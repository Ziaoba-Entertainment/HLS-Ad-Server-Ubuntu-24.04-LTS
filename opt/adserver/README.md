# ZiaOba Ad Server

Ad stitching middleware for HLS streams.

## Port Map (Authoritative)

| Port | Service | Access |
|------|---------|--------|
| 80 | Nginx Public HLS | Public (Cloudflare) |
| 8081 | Transcoder Web UI | Local Only |
| 8089 | Ad Server Admin UI | Localhost/SSH Only |
| 8083 | FastAPI Ad Stitcher | Internal Only |
| 8080 | qBittorrent | Local Only (Stand-alone) |
| 7878 | Radarr | Local Only (Stand-alone) |
| 8989 | Sonarr | Local Only (Stand-alone) |
| 9696 | Prowlarr | Local Only (Stand-alone) |

## Architecture

```text
Internet -> [Cloudflare Tunnel] -> [Nginx Port 80]
                                        |
                                        +-- /playlist/ -> [FastAPI Port 8083]
                                        +-- /segments/ -> [Static Files]
                                        +-- /health    -> [FastAPI Port 8083]

SSH Tunnel -> [Localhost:8089] -> [Ad Server Admin UI]
```

## Admin API Documentation

The Admin interface provides several machine-readable endpoints for monitoring and integration.

### Authentication
All admin endpoints require **Basic Authentication** (if accessed via Nginx) or are restricted to **localhost** access.

### Endpoints

#### GET /health
- **Description:** Health check for the admin service.
- **Response:** `{ "status": "ok", "service": "adserver-admin" }`

#### GET /api/metrics
- **Description:** Returns ad performance metrics.
- **Response:** JSON object containing impressions, fill rate, and distribution data.

#### GET /api/impressions/recent
- **Description:** Returns the most recent ad impressions.
- **Response:** JSON list of impression objects.

#### GET /api/status
- **Description:** Returns the overall status of the ad server components.
- **Response:** JSON object with counts and service statuses.

## Quick Start

```bash
sudo ./install.sh install
```

## Verification

```bash
# Public manifest
curl -si 'https://stream.ziaoba.com/playlist/movies/Baby_Boy_(2001)/master.m3u8'

# Admin Health (Localhost only)
curl -si 'http://localhost:8089/health'
```

## Redis Health Check

A monitoring script is available to verify Redis connectivity and queue health:

```bash
python3 opt/adserver/check_redis_health.py
```

This script validates:
1. Redis connection using standardized credentials from `/etc/ziaoba/redis.env`.
2. Transcoder event channel activity.
3. Ad registration queue health.
4. Ad registry size.
