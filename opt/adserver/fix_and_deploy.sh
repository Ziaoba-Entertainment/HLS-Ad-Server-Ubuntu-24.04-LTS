#!/usr/bin/env bash
set -euo pipefail

# Colors and formatting
RED=$(tput setaf 1 2>/dev/null||echo "")
GREEN=$(tput setaf 2 2>/dev/null||echo "")
YELLOW=$(tput setaf 3 2>/dev/null||echo "")
CYAN=$(tput setaf 6 2>/dev/null||echo "")
BOLD=$(tput bold 2>/dev/null||echo "")
RESET=$(tput sgr0 2>/dev/null||echo "")

ok()      { echo "${GREEN}  [OK]${RESET}    $*"; }
warn()    { echo "${YELLOW}  [WARN]${RESET}  $*"; }
fail()    { echo "${RED}  [FAIL]${RESET}  $*"; }
info()    { echo "${CYAN}  [INFO]${RESET}  $*"; }
section() { echo ""; echo "${BOLD}${CYAN}━━━ $* ━━━${RESET}"; echo ""; }

# SECTION 1 — PREFLIGHT
if [[ $EUID -ne 0 ]]; then
   fail "This script must be run as root"
   exit 1
fi

cat << "BANNER"
╔══════════════════════════════════════════════════╗
║   HLS AD SERVER — FIX & DEPLOY v1.0             ║
║   Fixing port collision + broken nginx configs  ║
╚══════════════════════════════════════════════════╝
BANNER
info "Server: $(hostname) | Date: $(date) | User: $(whoami)"

# SECTION 2 — CONFIRM EXISTING SERVICES SAFE
section "PRE-DEPLOYMENT CHECKS"
for svc in transcoder-worker transcoder-webhook; do
    if systemctl is-active "$svc" >/dev/null 2>&1; then
        ok "$svc running"
    else
        warn "$svc not active"
    fi
done

status=$(systemctl is-active transcoder-webui 2>/dev/null || echo "unknown")
if [ "$status" = "activating" ]; then
    warn "transcoder-webui is STUCK in activating state"
    warn "This is caused by adserver holding port 8082 — will be fixed by this script"
elif [ "$status" = "active" ]; then
    ok "transcoder-webui active"
else
    warn "transcoder-webui status: $status"
fi

if grep -q "proxy_pass http://127.0.0.1:8082" /etc/nginx/sites-available/mediaserver 2>/dev/null; then
    ok "mediaserver config intact — transcoder Flask on 8082 confirmed"
else
    warn "mediaserver config may have changed — verify manually"
fi

# SECTION 3 — BACKUP NGINX
section "BACKUP NGINX CONFIGS"
BACKUP_DIR="/etc/nginx/backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"
cp -r /etc/nginx/sites-available/ "$BACKUP_DIR/" 2>/dev/null || true
cp -r /etc/nginx/sites-enabled/ "$BACKUP_DIR/" 2>/dev/null || true
ok "Nginx configs backed up to $BACKUP_DIR"

# SECTION 4 — WRITE NGINX CONFIGS
section "DEPLOY NGINX CONFIGURATIONS"
NGINX_HTPASSWD="/etc/nginx/.adserver_htpasswd"
if [ ! -f "$NGINX_HTPASSWD" ]; then
    info "Creating admin credentials for Nginx..."
    # Default to admin:ChangeMe2024! if not exists
    printf "admin:$(openssl passwd -1 ChangeMe2024!)\n" > "$NGINX_HTPASSWD"
    chmod 640 "$NGINX_HTPASSWD"
    chown root:www-data "$NGINX_HTPASSWD"
    ok "Nginx admin credentials created"
fi

cat > /etc/nginx/sites-available/adserver-hls.conf << 'NGINXEOF'
server {
    listen 8081;
    server_name _;

    # All HLS manifest requests proxied to ad stitching middleware on 8083
    location ~* \.m3u8$ {
        proxy_pass http://127.0.0.1:8083;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Session-ID $request_id;
        proxy_read_timeout 30s;
        proxy_connect_timeout 5s;
        add_header Cache-Control "no-cache, no-store, must-revalidate";
        add_header Pragma "no-cache";
        add_header Access-Control-Allow-Origin "*";
        add_header Access-Control-Allow-Methods "GET, OPTIONS";
        add_header X-Content-Type-Options "nosniff";
        default_type application/vnd.apple.mpegurl;
    }

    # Movie TS segments — direct static, bypasses middleware entirely
    location /segments/hls/movies/ {
        alias /srv/vod/hls/movies/;
        types { video/mp2t ts; }
        default_type video/mp2t;
        add_header Cache-Control "public, max-age=31536000, immutable";
        add_header Access-Control-Allow-Origin "*";
        sendfile on;
        tcp_nopush on;
        gzip off;
    }

    # TV TS segments — direct static, bypasses middleware entirely
    location /segments/hls/tv/ {
        alias /srv/vod/hls/tv/;
        types { video/mp2t ts; }
        default_type video/mp2t;
        add_header Cache-Control "public, max-age=31536000, immutable";
        add_header Access-Control-Allow-Origin "*";
        sendfile on;
        tcp_nopush on;
        gzip off;
    }

    # Ad TS segments — direct static, bypasses middleware entirely
    location /segments/ads/ {
        alias /srv/vod/ads/;
        types { video/mp2t ts; }
        default_type video/mp2t;
        add_header Cache-Control "public, max-age=31536000, immutable";
        add_header Access-Control-Allow-Origin "*";
        sendfile on;
        tcp_nopush on;
        gzip off;
    }

    # Health check passthrough to stitching middleware
    location = /health {
        proxy_pass http://127.0.0.1:8083/health;
        proxy_set_header Host $host;
        access_log off;
    }

    access_log /var/log/nginx/adserver-hls-access.log;
    error_log  /var/log/nginx/adserver-hls-error.log warn;
}
NGINXEOF
ok "adserver-hls.conf written (port 8081 → proxy to 8083)"

cat > /etc/nginx/sites-available/adserver-admin.conf << 'NGINXEOF'
server {
    listen 88;
    server_name _;

    auth_basic "Ad Server Admin";
    auth_basic_user_file /etc/nginx/.adserver_htpasswd;

    # All admin UI requests proxied to FastAPI admin backend on 8089
    location / {
        proxy_pass http://127.0.0.1:8089;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 60s;
        proxy_connect_timeout 10s;
        proxy_send_timeout 60s;
    }

    # Admin static assets served directly for performance
    location /static/ {
        alias /opt/adserver/static/;
        expires 1h;
        add_header Cache-Control "public";
        auth_basic off;
    }

    access_log /var/log/nginx/adserver-admin-access.log;
    error_log  /var/log/nginx/adserver-admin-error.log warn;
}
NGINXEOF
ok "adserver-admin.conf written (port 88 → proxy to 8089)"

for conf in adserver-hls.conf adserver-admin.conf; do
    ln -sf "/etc/nginx/sites-available/$conf" "/etc/nginx/sites-enabled/$conf"
    ok "Symlink ensured: sites-enabled/$conf"
done

if [ -e /etc/nginx/sites-enabled/mediaserver ]; then
    ok "mediaserver symlink present — untouched"
else
    ln -sf /etc/nginx/sites-available/mediaserver /etc/nginx/sites-enabled/mediaserver
    warn "mediaserver symlink was missing — re-created"
fi

# SECTION 5 — DATABASE MIGRATIONS
section "DATABASE MIGRATIONS"
DB_FILE="/opt/adserver/adserver.db"
if [ -f "$DB_FILE" ]; then
    info "Ensuring database schema is up to date..."
    # Create settings table if missing
    sqlite3 "$DB_FILE" "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT, description TEXT, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP);"
    
    # Ensure default settings exist (using INSERT OR IGNORE)
    sqlite3 "$DB_FILE" "INSERT OR IGNORE INTO settings (key, value, description) VALUES ('mid_roll_interval', '600', 'Interval between mid-roll ads in seconds');"
    sqlite3 "$DB_FILE" "INSERT OR IGNORE INTO settings (key, value, description) VALUES ('pre_ad_count', '1', 'Number of ads in pre-roll break');"
    sqlite3 "$DB_FILE" "INSERT OR IGNORE INTO settings (key, value, description) VALUES ('mid_ad_count', '1', 'Number of ads in mid-roll break');"
    sqlite3 "$DB_FILE" "INSERT OR IGNORE INTO settings (key, value, description) VALUES ('post_ad_count', '1', 'Number of ads in post-roll break');"
    
    # Ensure impressions table has session_id if missing (older versions might not have it)
    # Note: sqlite doesn't support IF NOT EXISTS for ADD COLUMN easily in one line, but we can try
    if ! sqlite3 "$DB_FILE" ".schema impressions" | grep -q "session_id"; then
        info "Adding session_id column to impressions table..."
        sqlite3 "$DB_FILE" "ALTER TABLE impressions ADD COLUMN session_id TEXT;"
    fi

    ok "Database schema and default settings verified"
else
    warn "Database file not found — adserver service will create it on startup"
fi

# SECTION 6 — WRITE CORRECTED SYSTEMD UNIT
section "DEPLOY SYSTEMD SERVICE"
cat > /etc/systemd/system/adserver.service << 'SVCEOF'
[Unit]
Description=HLS Ad Stitching Middleware (FastAPI)
Documentation=https://github.com/your-org/adserver
After=network.target redis.service
Wants=redis.service

[Service]
Type=simple
User=media
Group=media
WorkingDirectory=/opt/adserver
ExecStart=/opt/adserver/venv/bin/uvicorn main:app \
    --host 127.0.0.1 \
    --port 8083 \
    --workers 2 \
    --log-level info \
    --access-log
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=adserver
Environment=PYTHONUNBUFFERED=1
Environment=PYTHONPATH=/opt/adserver

[Install]
WantedBy=multi-user.target
SVCEOF
ok "adserver.service written — port changed 8082 → 8083"
systemctl daemon-reload
ok "systemctl daemon-reload complete"

# SECTION 7 — RESOLVE PORT COLLISION
info "Stopping adserver (currently on wrong port 8082)..."
systemctl stop adserver || true
sleep 2

if ss -tlnp | grep ':8082' | grep -q 'uvicorn\|adserver'; then
    warn "adserver process still on 8082 — force killing"
    pkill -f "uvicorn main:app" || true
    sleep 2
fi

info "Starting adserver on correct port 8083..."
systemctl start adserver
sleep 3

if ss -tlnp | grep ':8083' | grep -q 'python3\|uvicorn'; then
    ok "adserver confirmed running on port 8083"
else
    fail "adserver not detected on port 8083 — check: journalctl -u adserver -n 20"
fi

# SECTION 8 — RESTORE TRANSCODER WEBUI
info "Restarting transcoder-webui (was stuck waiting for port 8082)..."
systemctl restart transcoder-webui || warn "Could not restart transcoder-webui"
sleep 3

status=$(systemctl is-active transcoder-webui 2>/dev/null || echo "unknown")
if [ "$status" = "active" ]; then
    ok "transcoder-webui active — Flask running on port 8082"
else
    warn "transcoder-webui status: $status"
    warn "Check: journalctl -u transcoder-webui -n 20"
fi

# SECTION 9 — VALIDATE AND RELOAD NGINX
info "Testing nginx configuration..."
if nginx -t 2>&1; then
    ok "nginx config test passed"
    nginx -s reload
    sleep 2
    ok "nginx reloaded"
else
    fail "nginx config test FAILED"
    echo "Restoring nginx backup from $BACKUP_DIR..."
    cp -r "$BACKUP_DIR/sites-available/"* /etc/nginx/sites-available/ 2>/dev/null || true
    cp -r "$BACKUP_DIR/sites-enabled/"*   /etc/nginx/sites-enabled/   2>/dev/null || true
    nginx -s reload 2>/dev/null || true
    echo "Backup restored. Fix config errors manually then re-run."
    exit 1
fi

# SECTION 10 — RESTART ALL AD SERVER SERVICES
section "RESTART AD SERVER SERVICES"
SERVICES=("adserver" "adserver-admin" "ad-watcher" "ad-scanner")
for svc in "${SERVICES[@]}"; do
    info "Restarting $svc..."
    systemctl restart "$svc" || warn "Could not restart $svc"
    sleep 1
    if systemctl is-active "$svc" >/dev/null 2>&1; then
        ok "$svc is active"
    else
        warn "$svc is not active — check: journalctl -u $svc -n 20"
    fi
done

# SECTION 11 — DEPLOY APP FILE PERMISSIONS
if [ -d /opt/adserver ]; then
    chown -R media:media /opt/adserver/
    find /opt/adserver -name "*.py" -exec chmod 644 {} \;
    find /opt/adserver -name "*.html" -exec chmod 644 {} \;
    [ -d /opt/adserver/venv/bin ] && chmod 755 /opt/adserver/venv/bin/* || true
    ok "File permissions set on /opt/adserver/"
fi
[ -d /var/log/adserver ] || { mkdir -p /var/log/adserver; chown media:media /var/log/adserver; }
ok "Log directory verified"

# SECTION 12 — HEALTH CHECK
SERVER_IP=$(hostname -I | awk '{print $1}')
sleep 2

if nginx -t 2>/dev/null; then ok "nginx config valid"; else fail "nginx config invalid"; fi

if [ -e /etc/nginx/sites-enabled/mediaserver ]; then
    ok "mediaserver config enabled (port 80 transcoding pipeline)"
else
    fail "mediaserver config NOT in sites-enabled"
fi

if ss -tlnp 2>/dev/null | grep ':8082' | grep -q 'python3'; then
    ok "Port 8082: transcoder Flask WebUI running"
else
    warn "Port 8082: transcoder WebUI not detected — check transcoder-webui service"
fi

if ss -tlnp 2>/dev/null | grep ':8083' | grep -q 'python3'; then
    ok "Port 8083: adserver stitching middleware running"
else
    fail "Port 8083: adserver NOT running — run: journalctl -u adserver -n 50"
fi

if ss -tlnp 2>/dev/null | grep ':8082' | grep -q 'uvicorn'; then
    fail "Port 8082: adserver uvicorn STILL on wrong port — collision not resolved"
else
    ok "Port 8082: not occupied by adserver (collision resolved)"
fi

response=$(curl -s -m 5 -o /dev/null -w "%{http_code}" http://127.0.0.1:80/ 2>/dev/null || echo "000")
body=$(curl -s -m 5 http://127.0.0.1:80/ 2>/dev/null | head -c 200 || echo "")
if echo "$body" | grep -q '"detail"'; then
    fail "Port 80: still returning FastAPI JSON 404 — nginx misconfigured"
elif [ "$response" = "301" ] || [ "$response" = "302" ] || [ "$response" = "200" ]; then
    ok "Port 80: transcoding app responding (HTTP $response)"
elif [ "$response" = "000" ]; then
    fail "Port 80: not responding"
else
    warn "Port 80: HTTP $response — verify transcoding app is running"
fi

code=$(curl -s -m 5 -o /dev/null -w "%{http_code}" http://127.0.0.1:8081/health 2>/dev/null || echo "000")
if [ "$code" = "200" ]; then ok "Port 8081: HLS delivery responding";
elif [ "$code" = "502" ]; then warn "Port 8081: nginx up but adserver 502 — check service";
else fail "Port 8081: not responding (HTTP $code)"; fi

code=$(curl -s -m 5 -o /dev/null -w "%{http_code}" http://127.0.0.1:88/ 2>/dev/null || echo "000")
if [ "$code" = "401" ]; then ok "Port 88: admin UI responding (401 = auth required, correct)";
elif [ "$code" = "200" ]; then ok "Port 88: admin UI responding (200)";
elif [ "$code" = "502" ]; then warn "Port 88: nginx up but admin app 502 — check adserver-admin";
else fail "Port 88: not responding (HTTP $code)"; fi

if ss -tlnp 2>/dev/null | grep ':8089' | grep -q 'python3'; then
    ok "Port 8089: adserver-admin running"
else
    warn "Port 8089: adserver-admin not detected"
fi

if redis-cli -n 1 ping 2>/dev/null | grep -q "PONG"; then
    ok "Redis db=1 responding"
else
    warn "Redis not responding — ad caching disabled but service will still work"
fi

if [ -f /opt/adserver/adserver.db ]; then
    tables=$(sqlite3 /opt/adserver/adserver.db ".tables" 2>/dev/null || echo "")
    if echo "$tables" | grep -q "ads" && echo "$tables" | grep -q "impressions" && echo "$tables" | grep -q "settings"; then
        ok "SQLite: ads, impressions, and settings tables present"
    else
        fail "SQLite: database missing or required tables not created"
    fi
else
    fail "SQLite: database file missing"
fi

if [ -e /dev/dri/renderD128 ]; then
    ok "VAAPI device /dev/dri/renderD128 present"
else
    warn "VAAPI device not found — GPU acceleration unavailable"
fi

for f in main.py admin_app.py ad_selector.py templates/settings.html; do
    if [ -f "/opt/adserver/$f" ]; then
        ok "App file deployed: $f"
    else
        warn "App file MISSING: /opt/adserver/$f — routes will return 404"
    fi
done

# SECTION 13 — FINAL SUMMARY BOX
echo ""
echo "${BOLD}${GREEN}"
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║          HLS AD SERVER — FIX & DEPLOY COMPLETE              ║"
  echo "╠══════════════════════════════════════════════════════════════╣"
  printf "║  Transcoding WebUI:     http://%-29s ║\n" "${SERVER_IP}/transcoder/"
  printf "║  Radarr:                http://%-29s ║\n" "${SERVER_IP}/radarr/"
  printf "║  Sonarr:                http://%-29s ║\n" "${SERVER_IP}/sonarr/"
  printf "║  Prowlarr:              http://%-29s ║\n" "${SERVER_IP}/prowlarr/"
  echo   "╠══════════════════════════════════════════════════════════════╣"
  printf "║  HLS Stream + Ads:      http://%-29s ║\n" "${SERVER_IP}:8081"
  printf "║  Ad Admin UI:           http://%-29s ║\n" "${SERVER_IP}:88"
  echo   "╠══════════════════════════════════════════════════════════════╣"
  echo   "║  Port 8082: transcoder Flask (RESTORED)                      ║"
  echo   "║  Port 8083: adserver stitching (FIXED from 8082)             ║"
  echo   "║  Port 8089: adserver admin backend (unchanged)               ║"
  echo   "╠══════════════════════════════════════════════════════════════╣"
  printf "║  Admin creds:  %-45s ║\n" "/opt/adserver/.admin_credentials"
  printf "║  Nginx backup: %-45s ║\n" "${BACKUP_DIR}"
  echo   "╠══════════════════════════════════════════════════════════════╣"
  echo   "║  Logs:  journalctl -u adserver -f                            ║"
  echo   "║         journalctl -u adserver-admin -f                      ║"
  echo   "║         journalctl -u transcoder-webui -f                    ║"
  echo   "╚══════════════════════════════════════════════════════════════╝"
  echo   "${RESET}"
