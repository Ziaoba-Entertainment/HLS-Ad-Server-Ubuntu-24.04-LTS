#!/usr/bin/env bash

# ==============================================================================
# AD SERVER INSTALLATION SCRIPT
# Version: 1.0.0
# Target OS: Ubuntu Server 24.04 LTS
# ==============================================================================

set -euo pipefail

# --- Configuration Constants ---
INSTALL_DIR="/opt/adserver"
VOD_ROOT="/srv/vod"
LOG_DIR="/var/log/adserver"
DB_PATH="$INSTALL_DIR/adserver.db"
VENV_PATH="$INSTALL_DIR/venv"
ADMIN_CRED_FILE="$INSTALL_DIR/.admin_credentials"
HTPASSWD_FILE="/etc/nginx/.adserver_htpasswd"

# --- UI / Formatting ---
BOLD=$(tput bold)
GREEN=$(tput setaf 2)
YELLOW=$(tput setaf 3)
RED=$(tput setaf 1)
CYAN=$(tput setaf 6)
RESET=$(tput sgr0)

# --- Banner ---
print_banner() {
    echo "${CYAN}${BOLD}"
    echo "####################################################"
    echo "#                                                  #"
    echo "#              AD SERVER INSTALLER                 #"
    echo "#                 Version 1.0.0                    #"
    echo "#          Date: $(date +'%Y-%m-%d %H:%M:%S')           #"
    echo "#                                                  #"
    echo "####################################################"
    echo "${RESET}"
}

# --- Helper Functions ---
section() {
    echo -e "\n${BOLD}${CYAN}>>> $1${RESET}"
}

check_cmd() {
    local msg="$1"
    shift
    echo -n "${BOLD}$msg... ${RESET}"
    local tmp_log=$(mktemp)
    if "$@" > "$tmp_log" 2>&1; then
        echo "${GREEN}[OK]${RESET}"
        rm "$tmp_log"
    else
        echo "${RED}[FAIL]${RESET}"
        echo "${RED}${BOLD}Error Output:${RESET}"
        cat "$tmp_log"
        rm "$tmp_log"
        return 1
    fi
}

# --- Pre-flight Checks ---
if [[ $EUID -ne 0 ]]; then
   echo "${RED}${BOLD}ERROR: This script must be run as root.${RESET}"
   exit 1
fi

print_banner

# ==============================================================================
# 1. SYSTEM DEPENDENCIES
# ==============================================================================
section "Installing System Dependencies"

check_cmd "Updating apt cache" apt-get update
check_cmd "Installing required packages" \
    apt-get install -y python3-venv python3-pip sqlite3 redis-server nginx openssl curl psmisc

# ==============================================================================
# 2. USERS AND GROUPS
# ==============================================================================
section "Configuring Users and Groups"

if id "media" &>/dev/null; then
    echo "${YELLOW}[SKIP] User 'media' already exists.${RESET}"
else
    check_cmd "Creating system user 'media'" \
        useradd -r -m -d "$INSTALL_DIR" -s /usr/sbin/nologin media
fi

check_cmd "Adding 'www-data' to 'media' group" usermod -aG media www-data
check_cmd "Adding 'media' to 'render' group" usermod -aG render media
check_cmd "Adding 'media' to 'video' group" usermod -aG video media

# ==============================================================================
# 2.5 DEPLOY APPLICATION FILES
# ==============================================================================
section "Deploying Application Files"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

FILES_TO_COPY=(
    "main.py"
    "admin_app.py"
    "ad_selector.py"
    "playlist_builder.py"
    "scan_ads.py"
    "watch_ads.py"
    "verify_ad_segments.py"
)

for file in "${FILES_TO_COPY[@]}"; do
    if [[ -f "$SCRIPT_DIR/$file" ]]; then
        check_cmd "Deploying $file" cp "$SCRIPT_DIR/$file" "$INSTALL_DIR/"
    else
        echo "${YELLOW}[WARN] Source file $file not found in $SCRIPT_DIR${RESET}"
    fi
done

if [[ -d "$SCRIPT_DIR/templates" ]]; then
    check_cmd "Deploying templates directory" cp -r "$SCRIPT_DIR/templates"/* "$INSTALL_DIR/templates/"
fi

if [[ -d "$SCRIPT_DIR/static" ]]; then
    check_cmd "Deploying static directory" cp -r "$SCRIPT_DIR/static"/* "$INSTALL_DIR/static/"
fi

# ==============================================================================
# 3. DIRECTORY STRUCTURE
# ==============================================================================
section "Creating Directory Structure"

DIRS=(
    "$INSTALL_DIR"
    "$INSTALL_DIR/templates"
    "$INSTALL_DIR/static"
    "$VOD_ROOT"
    "$VOD_ROOT/hls"
    "$VOD_ROOT/hls/movies"
    "$VOD_ROOT/hls/tv"
    "$VOD_ROOT/ads"
    "$VOD_ROOT/ads/incoming"
    "$VOD_ROOT/ads/rejected"
    "$VOD_ROOT/output"
    "$LOG_DIR"
)

for dir in "${DIRS[@]}"; do
    if [[ ! -d "$dir" ]]; then
        check_cmd "Creating $dir" mkdir -p "$dir"
    else
        echo "${YELLOW}[SKIP] $dir already exists.${RESET}"
    fi
done

section "Setting Ownership and Permissions"
check_cmd "chown $INSTALL_DIR" chown -R media:media "$INSTALL_DIR"
check_cmd "chmod $INSTALL_DIR" chmod 755 "$INSTALL_DIR"

check_cmd "chown $VOD_ROOT" chown media:media "$VOD_ROOT"
check_cmd "chmod $VOD_ROOT" chmod 755 "$VOD_ROOT"

check_cmd "chown $VOD_ROOT/hls" chown -R media:media "$VOD_ROOT/hls"
check_cmd "chmod $VOD_ROOT/hls" chmod 755 "$VOD_ROOT/hls"

check_cmd "chown $VOD_ROOT/ads" chown -R media:media "$VOD_ROOT/ads"
check_cmd "chmod $VOD_ROOT/ads" chmod 755 "$VOD_ROOT/ads"

check_cmd "chown $VOD_ROOT/output" chown -R media:media "$VOD_ROOT/output"
check_cmd "chmod $VOD_ROOT/output" chmod 755 "$VOD_ROOT/output"

check_cmd "chown $LOG_DIR" chown -R media:media "$LOG_DIR"
check_cmd "chmod $LOG_DIR" chmod 755 "$LOG_DIR"

section "Recursive Permission Enforcement"
check_cmd "Setting directory permissions (755)" find "$VOD_ROOT" -type d -exec chmod 755 {} +
check_cmd "Setting file permissions (644)" find "$VOD_ROOT" -type f -exec chmod 644 {} +
check_cmd "Ensuring media ownership on $VOD_ROOT" chown -R media:media "$VOD_ROOT"

# ==============================================================================
# 3. PYTHON VIRTUAL ENVIRONMENT
# ==============================================================================
section "Setting up Python Virtual Environment"

if [[ ! -d "$VENV_PATH" ]]; then
    check_cmd "Creating venv at $VENV_PATH" /usr/bin/python3 -m venv "$VENV_PATH"
else
    echo "${YELLOW}[SKIP] Virtual environment already exists.${RESET}"
fi

check_cmd "Upgrading pip in venv" "$VENV_PATH/bin/pip" install --upgrade pip

check_cmd "Installing Python packages" \
    "$VENV_PATH/bin/pip" install \
    fastapi==0.115.6 \
    uvicorn[standard]==0.32.1 \
    aiofiles==24.1.0 \
    jinja2==3.1.5 \
    redis==5.2.1 \
    inotify-simple==1.3.5 \
    aiosqlite==0.20.0 \
    httpx==0.27.2 \
    m3u8==4.1.0

check_cmd "Fixing venv ownership" chown -R media:media "$VENV_PATH"

# ==============================================================================
# 4. SQLITE DATABASE
# ==============================================================================
section "Initializing SQLite Database"

DB_INIT_SCRIPT=$(cat <<EOF
import sqlite3
import os

db_path = "$DB_PATH"

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Table: ads
cursor.execute('''
CREATE TABLE IF NOT EXISTS ads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    folder_name TEXT UNIQUE NOT NULL,
    priority INTEGER NOT NULL DEFAULT 3,
    placement_pre INTEGER NOT NULL DEFAULT 1,
    placement_mid INTEGER NOT NULL DEFAULT 1,
    placement_post INTEGER NOT NULL DEFAULT 1,
    play_count INTEGER NOT NULL DEFAULT 0,
    active INTEGER NOT NULL DEFAULT 1,
    duration_seconds REAL DEFAULT 0,
    rendition_count INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
)
''')

# Table: impressions
cursor.execute('''
CREATE TABLE IF NOT EXISTS impressions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ad_id INTEGER NOT NULL REFERENCES ads(id),
    content_path TEXT NOT NULL,
    placement TEXT NOT NULL CHECK(placement IN ('pre','mid','post')),
    session_id TEXT NOT NULL,
    played_at TEXT DEFAULT (datetime('now'))
)
''')

# Table: settings
cursor.execute('''
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT,
    description TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
''')

# Default settings
default_settings = [
    ('mid_roll_interval', '600', 'Interval between mid-roll ads in seconds'),
    ('pre_ad_count', '1', 'Number of ads in pre-roll break'),
    ('mid_ad_count', '1', 'Number of ads in mid-roll break'),
    ('post_ad_count', '1', 'Number of ads in post-roll break')
]
cursor.executemany('INSERT OR IGNORE INTO settings (key, value, description) VALUES (?, ?, ?)', default_settings)

# Indexes
cursor.execute('CREATE INDEX IF NOT EXISTS idx_impressions_played_at ON impressions(played_at)')
cursor.execute('CREATE INDEX IF NOT EXISTS idx_impressions_ad_id ON impressions(ad_id)')
cursor.execute('CREATE INDEX IF NOT EXISTS idx_impressions_placement ON impressions(placement)')
cursor.execute('CREATE INDEX IF NOT EXISTS idx_ads_active ON ads(active, priority)')

# Migration: Add session_id to impressions if missing
try:
    cursor.execute("SELECT session_id FROM impressions LIMIT 1")
except sqlite3.OperationalError:
    print("Adding session_id column to impressions table...")
    cursor.execute("ALTER TABLE impressions ADD COLUMN session_id TEXT NOT NULL DEFAULT 'unknown'")

conn.commit()
cursor.execute("PRAGMA user_version = 1")
conn.commit()
conn.close()
EOF
)

check_cmd "Creating database schema" sudo -u media "$VENV_PATH/bin/python3" -c "$DB_INIT_SCRIPT"
check_cmd "Setting database permissions" chmod 664 "$DB_PATH"

# ==============================================================================
# 5. NGINX CONFIGURATION
# ==============================================================================
section "Configuring Nginx"

# --- HLS Delivery Site ---
cat <<EOF > /etc/nginx/sites-available/adserver-hls.conf
server {
    listen 8081;
    server_name _;

    # Proxy all .m3u8 playlist requests to the ad stitching FastAPI middleware
    location ~* \.m3u8$ {
        proxy_pass http://127.0.0.1:8083;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Session-ID \$request_id;
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
        proxy_set_header Host \$host;
        access_log off;
    }

    access_log /var/log/nginx/adserver-hls-access.log;
    error_log /var/log/nginx/adserver-hls-error.log warn;
}
EOF

# --- Admin UI Site ---
cat <<EOF > /etc/nginx/sites-available/adserver-admin.conf
server {
    listen 88;
    server_name _;

    auth_basic "Ad Server Admin";
    auth_basic_user_file /etc/nginx/.adserver_htpasswd;

    # All admin UI requests proxied to FastAPI admin backend on 8089
    location / {
        proxy_pass http://127.0.0.1:8089;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
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
    error_log /var/log/nginx/adserver-admin-error.log warn;
}
EOF

# --- Admin Credentials ---
if [[ ! -f "$ADMIN_CRED_FILE" ]]; then
    ADMIN_PASS=$(head /dev/urandom | tr -dc A-Za-z0-9 | head -c 16)
    echo "admin:$ADMIN_PASS" > "$ADMIN_CRED_FILE"
    chmod 600 "$ADMIN_CRED_FILE"
    chown root:root "$ADMIN_CRED_FILE"
    
    HASHED_PASS=$(openssl passwd -apr1 "$ADMIN_PASS")
    echo "admin:$HASHED_PASS" > "$HTPASSWD_FILE"
    chmod 644 "$HTPASSWD_FILE"
    echo "${GREEN}[OK] Admin credentials generated.${RESET}"
else
    echo "${YELLOW}[SKIP] Admin credentials already exist.${RESET}"
fi

# --- Enable Sites ---
check_cmd "Enabling HLS site" ln -sf /etc/nginx/sites-available/adserver-hls.conf /etc/nginx/sites-enabled/
check_cmd "Enabling Admin site" ln -sf /etc/nginx/sites-available/adserver-admin.conf /etc/nginx/sites-enabled/

check_cmd "Validating Nginx config" nginx -t
check_cmd "Reloading Nginx" nginx -s reload

# ==============================================================================
# 6. SYSTEMD SERVICES
# ==============================================================================
section "Configuring Systemd Services"

# --- adserver.service ---
cat <<EOF > /etc/systemd/system/adserver.service
[Unit]
Description=HLS Ad Injection Middleware (FastAPI)
After=network.target redis.service
Wants=redis.service

[Service]
Type=simple
User=media
Group=media
UMask=0022
WorkingDirectory=$INSTALL_DIR
Environment="PATH=$VENV_PATH/bin"
ExecStart=$VENV_PATH/bin/python3 -m uvicorn main:app --host 127.0.0.1 --port 8083 --workers 2 --log-level info --access-log
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=adserver
Environment=PYTHONUNBUFFERED=1
Environment=PYTHONPATH=$INSTALL_DIR

[Install]
WantedBy=multi-user.target
EOF

# --- adserver-admin.service ---
cat <<EOF > /etc/systemd/system/adserver-admin.service
[Unit]
Description=HLS Ad Server Admin WebUI (FastAPI)
After=network.target redis.service adserver.service

[Service]
Type=simple
User=media
Group=media
UMask=0022
WorkingDirectory=$INSTALL_DIR
Environment="PATH=$VENV_PATH/bin"
ExecStart=$VENV_PATH/bin/python3 -m uvicorn admin_app:app --host 127.0.0.1 --port 8089 --workers 1 --log-level info
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=adserver-admin
Environment=PYTHONUNBUFFERED=1
Environment=PYTHONPATH=$INSTALL_DIR

[Install]
WantedBy=multi-user.target
EOF

# --- ad-watcher.service ---
cat <<EOF > /etc/systemd/system/ad-watcher.service
[Unit]
Description=HLS Ad Folder Watcher
After=network.target redis.service adserver.service

[Service]
Type=simple
User=media
Group=media
UMask=0022
WorkingDirectory=$INSTALL_DIR
Environment="PATH=$VENV_PATH/bin"
ExecStart=$VENV_PATH/bin/python3 $INSTALL_DIR/watch_ads.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=ad-watcher
Environment=PYTHONUNBUFFERED=1
Environment=PYTHONPATH=$INSTALL_DIR

[Install]
WantedBy=multi-user.target
EOF

# --- ad-scanner.service ---
cat <<EOF > /etc/systemd/system/ad-scanner.service
[Unit]
Description=HLS Ad Scanner (one-shot on boot)
After=network.target adserver.service

[Service]
Type=oneshot
User=media
Group=media
UMask=0022
WorkingDirectory=$INSTALL_DIR
Environment="PATH=$VENV_PATH/bin"
ExecStart=$VENV_PATH/bin/python3 $INSTALL_DIR/scan_ads.py
StandardOutput=journal
StandardError=journal
SyslogIdentifier=ad-scanner
Environment=PYTHONUNBUFFERED=1
Environment=PYTHONPATH=$INSTALL_DIR
RemainAfterExit=no

[Install]
WantedBy=multi-user.target
EOF

check_cmd "Reloading systemd" systemctl daemon-reload

# Final ownership fix
check_cmd "Final ownership fix" chown -R media:media "$INSTALL_DIR"

SERVICES=("adserver" "adserver-admin" "ad-watcher" "ad-scanner")
for svc in "${SERVICES[@]}"; do
    check_cmd "Enabling $svc" systemctl enable "$svc"
    
    # Kill any zombie processes on the ports before starting
    if [[ "$svc" == "adserver" ]]; then
        echo -n "${BOLD}Cleaning up port 8083... ${RESET}"
        fuser -k 8083/tcp > /dev/null 2>&1 || true
        # Also clean up 8082 if adserver was previously there
        fuser -k 8082/tcp > /dev/null 2>&1 || true
        echo "${GREEN}[OK]${RESET}"
    elif [[ "$svc" == "adserver-admin" ]]; then
        echo -n "${BOLD}Cleaning up port 8089... ${RESET}"
        fuser -k 8089/tcp > /dev/null 2>&1 || true
        echo "${GREEN}[OK]${RESET}"
    fi

    echo -n "${BOLD}Starting $svc... ${RESET}"
    systemctl restart "$svc" > /dev/null 2>&1 || echo "${YELLOW}[WARN] Service $svc failed to start${RESET}"
done

# ==============================================================================
# 7. LOGROTATE
# ==============================================================================
section "Configuring Logrotate"

cat <<EOF > /etc/logrotate.d/adserver
/var/log/adserver/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
    su media media
}
EOF
echo "${GREEN}[OK] Logrotate configured.${RESET}"

# ==============================================================================
# 8. FIREWALL
# ==============================================================================
section "Configuring Firewall"

if ufw status | grep -q "Status: active"; then
    check_cmd "Allowing port 8081 (HLS)" ufw allow 8081/tcp comment "HLS Ad Stream Delivery"
    check_cmd "Allowing port 88 (Admin)" ufw allow 88/tcp comment "Ad Server Admin UI"
    check_cmd "Reloading UFW" ufw reload
else
    echo "${YELLOW}[NOTICE] UFW is not active. Please ensure ports 8081 and 88 are open.${RESET}"
fi

# ==============================================================================
# 9. HEALTH CHECK
# ==============================================================================
section "Post-Install Health Check"

health_check() {
    local status_code
    
    # 1. Services
    for svc in adserver adserver-admin ad-watcher; do
        if systemctl is-active "$svc" &>/dev/null; then
            echo "${GREEN}[OK] Service $svc is active${RESET}"
        else
            echo "${YELLOW}[WARN] Service $svc is not active (app files may be missing)${RESET}"
        fi
    done
    
    # 2. Redis
    if redis-cli ping | grep -q "PONG"; then
        echo "${GREEN}[OK] Redis is responding${RESET}"
    else
        echo "${RED}[FAIL] Redis is not responding${RESET}"
    fi
    
    # 3. Nginx
    if nginx -t &>/dev/null; then
        echo "${GREEN}[OK] Nginx configuration is valid${RESET}"
    else
        echo "${RED}[FAIL] Nginx configuration is invalid${RESET}"
    fi
    
    # 4. Port 8081 (HLS Delivery + Health)
    status_code=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8081/health || echo "000")
    if [[ "$status_code" == "200" ]]; then
        echo "${GREEN}[OK] Port 8081 HLS Health is responding (HTTP 200)${RESET}"
    else
        echo "${RED}[FAIL] Port 8081 HLS Health is not responding (HTTP $status_code)${RESET}"
    fi
    
    # 5. Port 88
    status_code=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:88/ || echo "000")
    if [[ "$status_code" == "401" ]]; then
        echo "${GREEN}[OK] Port 88 is responding (HTTP 401 Unauthorized - Expected)${RESET}"
    else
        echo "${YELLOW}[WARN] Port 88 returned HTTP $status_code (Expected 401)${RESET}"
    fi
    
    # 6. SQLite
    if sqlite3 "$DB_PATH" ".tables" | grep -q "settings"; then
        echo "${GREEN}[OK] SQLite database initialized correctly (settings table present)${RESET}"
    else
        echo "${RED}[FAIL] SQLite database initialization failed (settings table missing)${RESET}"
    fi
    
    # 7. VAAPI
    if [[ -e "/dev/dri/renderD128" ]]; then
        echo "${GREEN}[OK] VAAPI device /dev/dri/renderD128 exists${RESET}"
    else
        echo "${RED}[FAIL] VAAPI device /dev/dri/renderD128 not found${RESET}"
    fi
    
    # 8. Media User
    if id "media" &>/dev/null; then
        echo "${GREEN}[OK] User 'media' exists${RESET}"
    else
        echo "${RED}[FAIL] User 'media' does not exist${RESET}"
    fi
}

health_check

# ==============================================================================
# 10. SUMMARY
# ==============================================================================
IP_ADDR=$(hostname -I | awk '{print $1}')

echo -e "\n${BOLD}${GREEN}####################################################"
echo "#                                                  #"
echo "#            INSTALLATION COMPLETE!                #"
echo "#                                                  #"
echo "####################################################${RESET}"
echo ""
echo "${BOLD}Admin UI URL:${RESET}      http://$IP_ADDR:88"
echo "${BOLD}HLS Delivery URL:${RESET}  http://$IP_ADDR:8081"
echo ""
echo "${BOLD}Credentials:${RESET}       $ADMIN_CRED_FILE"
echo "${BOLD}Log Directory:${RESET}     $LOG_DIR"
echo ""
echo "${BOLD}Next Steps:${RESET}"
echo "1. Deploy Python application files to $INSTALL_DIR"
echo "2. Ensure main.py, admin_app.py, watch_ads.py, and scan_ads.py are present"
echo "3. Restart services: systemctl restart adserver adserver-admin ad-watcher"
echo ""
echo "${CYAN}Installation finished at $(date)${RESET}"
