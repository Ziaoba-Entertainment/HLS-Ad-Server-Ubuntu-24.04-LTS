# ZiaOba Ad Server

Ad stitching middleware for HLS streams.

## Port Map (Authoritative)

| Port | Service | Access |
|------|---------|--------|
| 80 | Nginx Public HLS | Public (Cloudflare) |
| 8081 | Transcoder Web UI | Local Only |
| 8082 | Ad Server Web UI | Local Only |
| 8083 | FastAPI Ad Stitcher | Internal Only |
| 8080 | qBittorrent | Local Only (Stand-alone) |
| 7878 | Radarr | Local Only (Stand-alone) |
| 8989 | Sonarr | Local Only (Stand-alone) |
| 9696 | Prowlarr | Local Only (Stand-alone) |

## Nginx Configuration

The authoritative Nginx configuration is stored in a single file named `mediaserver`.

**CRITICAL:** This file has **NO FILE EXTENSION**. This is intentional and follows Ubuntu Nginx conventions. Do not rename it to `mediaserver.conf`.

- **Available:** `/etc/nginx/sites-available/mediaserver`
- **Enabled:** `/etc/nginx/sites-enabled/mediaserver`

## Architecture

```text
Internet -> [Cloudflare Tunnel] -> [Nginx Port 80]
                                        |
                                        +-- /playlist/ -> [FastAPI Port 8083]
                                        +-- /segments/ -> [Static Files]
                                        +-- /health    -> [FastAPI Port 8083]

LAN Only -> [Nginx Port 8081] -> [Transcoder UI Port 6666]
LAN Only -> [Nginx Port 8082] -> [Ad Server UI Port 8089]
LAN Only -> [Port 8080] -> [qBittorrent]
```

## Quick Start

```bash
sudo ./install.sh install
```

## Verification

```bash
# Public manifest
curl -si 'https://stream.ziaoba.com/playlist/movies/Baby_Boy_(2001)/master.m3u8'

# Local UI
curl -si 'http://192.168.0.103:8082/'
```
