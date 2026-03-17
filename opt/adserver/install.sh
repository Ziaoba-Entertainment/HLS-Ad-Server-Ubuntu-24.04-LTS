#!/usr/bin/env bash

# ==============================================================================
# AD SERVER INSTALLATION & MAINTENANCE SCRIPT
# Version: 1.3.3
# Target OS: Ubuntu Server 24.04 LTS
# SOURCE OF TRUTH: Transcoder Server Nginx Configuration
# ==============================================================================

# Changelog:
# v1.3.3 - Fixed ad stitching logic for 100% reliability
#          Improved URI resolution for ad segments and keys
#          Added support for mid-roll intervals and counts from database
#          Fixed Redis caching for stitched playlists (invalidates on settings change)
#          Ensured discontinuity and program-date-time tags for seamless transitions
#          Improved mid-roll insertion logic for long segments
# v1.3.1 - Fixed redislite version mismatch in installation
#          Added missing dependencies (jinja2, python-multipart)
#          Improved Redis binary discovery logic
# v1.3.0 - Removed Nginx management (Transcoder Server is authoritative)
#          Added Redis Event Listener service
#          Aligned with SSAI manifest middleware specification
# v1.2.2 - Added backup, restore, and edit-nginx commands
#          Fixed ProxyHeadersMiddleware import issues for Starlette 0.36.0+
#          Ensured adserver-admin.service uses venv python
# v1.2.1 - Corrected authoritative Nginx config filename from streaming-server.conf to mediaserver
#          Filename has no extension - this is intentional
#          Removed all references to streaming-server.conf
#          Removed all references to adserver-hls.conf
#          Removed all references to mediaserver.conf
#          Added stale file cleanup to fresh install
#          Fixed all grep checks to reference mediaserver

set -euo pipefail

# --- Configuration Constants ---
INSTALL_DIR="/opt/adserver"
VOD_ROOT="/srv/vod"
LOG_DIR="/var/log/adserver"
DB_PATH="$INSTALL_DIR/adserver.db"
VENV_PATH="$INSTALL_DIR/venv"
ADMIN_CRED_FILE="$INSTALL_DIR/.admin_credentials"
HTPASSWD_FILE="/etc/nginx/.adserver_htpasswd"
DOMAIN="stream.ziaoba.com"

# --- UI / Formatting ---
BOLD=$(tput bold || echo "")
GREEN=$(tput setaf 2 || echo "")
YELLOW=$(tput setaf 3 || echo "")
RED=$(tput setaf 1 || echo "")
CYAN=$(tput setaf 6 || echo "")
RESET=$(tput sgr0 || echo "")

# --- Banner ---
print_banner() {
    echo "${CYAN}${BOLD}"
    echo "####################################################"
    echo "#                                                  #"
    echo "#         AD SERVER LIFECYCLE MANAGER              #"
    echo "#                 Version 1.3.3                    #"
    echo "#          Date: $(date +'%Y-%m-%d %H:%M:%S')           #"
    echo "#                                                  #"
    echo "####################################################"
    echo "${RESET}"
}

# --- Usage ---
usage() {
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  install    Full installation from scratch"
    echo "  update     Update code, dependencies, and run migrations"
    echo "  repair     Fix permissions and restart services"
    echo "  status     Check health and service status"
    echo "  audit      Run security audit (Option 14)"
    echo "  logs       View combined application logs"
    echo "  enable-network  Enable local network access to Admin UI"
    echo "  help       Show this help message"
    echo ""
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

# --- Port Conflict Pre-check ---
check_ports() {
    section "Pre-check: Port Availability"
    local ports=(8083 8089)
    local conflict=0
    for port in "${ports[@]}"; do
        if ss -tlnp | grep ":$port " > /dev/null 2>&1; then
            echo "${YELLOW}WARNING: Port $port is already in use by:${RESET}"
            ss -tlnp | grep ":$port "
            conflict=1
        fi
    done
    
    if [[ $conflict -eq 1 ]]; then
        read -p "Conflicts detected. Do you want to continue? (y/N) " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Installation aborted."
            exit 1
        fi
    else
        echo "${GREEN}[OK] All required ports are available.${RESET}"
    fi
}

# --- Transcoder Integration Pre-check ---
check_transcoder() {
    section "Pre-check: Transcoder Server Integration"
    
    # Check for VOD HLS directory
    if [[ ! -d "/srv/vod/hls" ]]; then
        echo "${YELLOW}WARNING: Transcoder HLS directory (/srv/vod/hls) not found.${RESET}"
        echo "The Ad Server requires this directory to stitch manifests."
        read -p "Continue anyway? (y/N) " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    else
        echo "${GREEN}[OK] Transcoder HLS directory found.${RESET}"
    fi

    # Check if transcoder API is reachable
    if ! curl -s --max-time 2 http://127.0.0.1:6666/api/health > /dev/null 2>&1; then
        echo "${YELLOW}WARNING: Transcoder API (port 6666) is not responding.${RESET}"
        echo "The Ad Server may fail to fetch encoding profiles."
    else
        echo "${GREEN}[OK] Transcoder API is reachable.${RESET}"
    fi
}

FILES_TO_COPY=(
    "main.py"
    "admin_app.py"
    "ad_selector.py"
    "playlist_builder.py"
    "scan_ads.py"
    "watch_ads.py"
    "redis_listener.py"
    "verify_ad_segments.py"
    "init_db.py"
    "db_migrate.py"
    "db_migrate_v2.py"
    "health_check.sh"
    "setup_ad_dirs.sh"
    "rotate_logs.sh"
    "test_injection.py"
    "config.py"
    "enable_network_admin.sh"
    "adserver.service"
    "adserver-admin.service"
    "ad-watcher.service"
    "ad-redis-listener.service"
)

# ==============================================================================
# LIFECYCLE FUNCTIONS
# ==============================================================================

install_deps() {
    section "Installing System Dependencies"
    check_cmd "Updating apt cache" apt-get update -qq
    check_cmd "Installing required packages" \
        apt-get install -y python3-venv python3-pip sqlite3 redis-server openssl curl psmisc
}

setup_users() {
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
}

create_dirs() {
    section "Creating Directory Structure"
    DIRS=(
        "$INSTALL_DIR" "$INSTALL_DIR/templates" "$INSTALL_DIR/static"
        "$VOD_ROOT" "$VOD_ROOT/hls" "$VOD_ROOT/hls/movies" "$VOD_ROOT/hls/tv"
        "$VOD_ROOT/ads" "$VOD_ROOT/ads/incoming" "$VOD_ROOT/ads/rejected"
        "$VOD_ROOT/output" "$LOG_DIR"
    )
    for dir in "${DIRS[@]}"; do
        mkdir -p "$dir"
    done
    chown -R media:media "$INSTALL_DIR" "$LOG_DIR"
    # Ad segments need to be readable by www-data (Nginx)
    chown -R media:www-data "$VOD_ROOT/ads" "$VOD_ROOT/hls"
    chmod -R 755 "$VOD_ROOT/ads" "$VOD_ROOT/hls"
}

deploy_files() {
    section "Deploying Application Files"
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    for file in "${FILES_TO_COPY[@]}"; do
        if [[ -f "$SCRIPT_DIR/$file" ]]; then
            cp "$SCRIPT_DIR/$file" "$INSTALL_DIR/"
        fi
    done
    [[ -d "$SCRIPT_DIR/templates" ]] && cp -r "$SCRIPT_DIR/templates"/* "$INSTALL_DIR/templates/"
    [[ -d "$SCRIPT_DIR/static" ]] && cp -r "$SCRIPT_DIR/static"/* "$INSTALL_DIR/static/"
    
    # Initialize .env if missing
    if [[ ! -f "$INSTALL_DIR/.env" ]]; then
        if [[ -f "$SCRIPT_DIR/.env.example" ]]; then
            check_cmd "Initializing .env from example" cp "$SCRIPT_DIR/.env.example" "$INSTALL_DIR/.env"
        fi
    fi
    
    chown -R media:media "$INSTALL_DIR"
    chmod -R 750 "$INSTALL_DIR"

    # Deploy Nginx reference files if present in source
    if [[ -d "$SCRIPT_DIR/../../etc/nginx" ]]; then
        mkdir -p "$INSTALL_DIR/nginx_reference"
        cp -r "$SCRIPT_DIR/../../etc/nginx/"* "$INSTALL_DIR/nginx_reference/"
        echo "Deployed Nginx reference files to $INSTALL_DIR/nginx_reference"
    fi
}

setup_venv() {
    section "Setting up Python Virtual Environment"
    [[ ! -d "$VENV_PATH" ]] && /usr/bin/python3 -m venv "$VENV_PATH"
    check_cmd "Upgrading pip" "$VENV_PATH/bin/pip" install --upgrade pip -q
    check_cmd "Installing Python packages" \
        "$VENV_PATH/bin/pip" install \
        fastapi==0.115.6 uvicorn[standard]==0.32.1 aiofiles==24.1.0 jinja2==3.1.5 \
        redis==5.2.1 inotify-simple==1.3.5 aiosqlite==0.20.0 httpx==0.27.2 \
        m3u8==4.1.0 pydantic==2.10.4 pydantic-settings==2.7.0 \
        python-multipart==0.0.9 redislite -q
    chown -R media:media "$VENV_PATH"
}

init_db() {
    section "Initializing/Updating SQLite Database"
    if [[ ! -f "$DB_PATH" ]]; then
        check_cmd "Initializing fresh database" \
            sudo -u media "$VENV_PATH/bin/python3" "$INSTALL_DIR/init_db.py"
    else
        check_cmd "Running database migrations (v1)" \
            sudo -u media "$VENV_PATH/bin/python3" "$INSTALL_DIR/db_migrate.py"
        check_cmd "Running database migrations (v2)" \
            sudo -u media "$VENV_PATH/bin/python3" "$INSTALL_DIR/db_migrate_v2.py"
    fi
    chmod 664 "$DB_PATH"
}

setup_systemd() {
    section "Configuring Systemd Services"
    
    # Kill any zombie processes on the ports before starting
    check_cmd "Cleaning up port 8083" bash -c "fuser -k 8083/tcp || true"
    check_cmd "Cleaning up port 8089" bash -c "fuser -k 8089/tcp || true"

    # Copy service files to systemd
    # NOTE: Transcoder Server is the authority. We prepare the files here.
    # The master installer expects them in the deployment root.
    for svc_file in adserver.service adserver-admin.service ad-watcher.service ad-redis-listener.service; do
        if [[ -f "$INSTALL_DIR/$svc_file" ]]; then
            check_cmd "Installing $svc_file to systemd" cp "$INSTALL_DIR/$svc_file" "/etc/systemd/system/"
            # Also copy to root as requested by Transcoder Architect
            check_cmd "Staging $svc_file for master installer" cp "$INSTALL_DIR/$svc_file" "/"
        else
            echo "${RED}Error: $svc_file not found in $INSTALL_DIR${RESET}"
            exit 1
        fi
    done

    # Block external access to Admin UI port by default
    # But allow if explicitly configured for network access
    if command -v ufw > /dev/null; then
        if grep "AD_ADMIN_HOST=0.0.0.0" "$INSTALL_DIR/.env" >/dev/null 2>&1; then
            echo "${GREEN}[OK] Network access detected. Allowing port 8089 from local network.${RESET}"
            ufw allow from 192.168.0.0/24 to any port 8089 >/dev/null 2>&1 || true
        else
            check_cmd "Blocking external access to port 8089" ufw deny 8089
        fi
    fi

    check_cmd "Reloading systemd" systemctl daemon-reload
    for svc in adserver adserver-admin ad-redis-listener ad-watcher; do
        check_cmd "Enabling $svc" systemctl enable "$svc"
        check_cmd "Restarting $svc" systemctl restart "$svc"
    done
}

verify_cloudflare() {
    section "Verifying Cloudflare Tunnel Configuration"
    local config_file="/home/craig/.cloudflared/config.yml"
    if [[ -f "$config_file" ]]; then
        if grep -E "localhost:(8081|8082|8083)" "$config_file" > /dev/null; then
            echo "${YELLOW}WARNING: Cloudflare tunnel points to wrong port. Fixing...${RESET}"
            sed -i 's/localhost:808[123]/localhost:80/g' "$config_file"
            check_cmd "Restarting cloudflared" systemctl restart cloudflared || echo "${YELLOW}Cloudflared service not found, please restart manually.${RESET}"
        else
            echo "${GREEN}[OK] Cloudflare tunnel points to port 80.${RESET}"
        fi
    else
        echo "${YELLOW}[SKIP] Cloudflare config not found at $config_file${RESET}"
    fi
}

check_8080_migration() {
    section "Checking for Port 8080 (qBittorrent) Conflicts"
    # Only warn if Ad Server components are explicitly configured to use 8080
    # We check .env for FASTAPI_PORT or AD_ADMIN_PORT being 8080
    local env_file="$INSTALL_DIR/.env"
    if [[ -f "$env_file" ]]; then
        if grep -E "^(FASTAPI_PORT|AD_ADMIN_PORT)=8080" "$env_file" >/dev/null; then
            echo "${RED}CRITICAL: Ad Server is configured to use Port 8080!${RESET}"
            echo "Port 8080 is reserved for qBittorrent. Please update .env to use 8083/8089."
            grep -E "^(FASTAPI_PORT|AD_ADMIN_PORT)=8080" "$env_file"
        else
            echo "${GREEN}[OK] Ad Server ports are correctly configured (not using 8080).${RESET}"
        fi
    fi

    # Also check for any other suspicious 8080 references in python files
    if grep -r "8080" "$INSTALL_DIR" --include="*.py" --exclude="config.py" --exclude-dir="venv" 2>/dev/null; then
        echo "${YELLOW}Note: Found 8080 references in Python files (likely for qBittorrent proxying).${RESET}"
        grep -r "8080" "$INSTALL_DIR" --include="*.py" --exclude="config.py" --exclude-dir="venv" 2>/dev/null | head -n 5
    fi
}

show_summary() {
    IP_ADDR=$(hostname -I | awk '{print $1}')
    ADMIN_URL="http://localhost:8089 (SSH Tunnel Required)"
    if grep "AD_ADMIN_HOST=0.0.0.0" "$INSTALL_DIR/.env" >/dev/null 2>&1; then
        ADMIN_URL="http://$IP_ADDR:8089"
    fi

    echo -e "\n${BOLD}${GREEN}####################################################"
    echo "#            INSTALLATION COMPLETE!                #"
    echo "####################################################${RESET}"
    echo ""
    echo "Public HLS Streaming: https://$DOMAIN/playlist/"
    echo "Transcoder Web UI:    http://$IP_ADDR:8081"
    echo "Ad Server Admin UI: $ADMIN_URL"
    echo "qBittorrent:          http://$IP_ADDR:8080"
    echo "Radarr:               http://$IP_ADDR:7878"
    echo "Sonarr:               http://$IP_ADDR:8989"
    echo "Prowlarr:             http://$IP_ADDR:9696"
    echo ""
    echo "Credentials:   $ADMIN_CRED_FILE"
    echo ""
}

# ==============================================================================
# COMMANDS
# ==============================================================================

run_install() {
    print_banner
    check_ports
    check_transcoder
    install_deps
    setup_users
    create_dirs
    deploy_files
    setup_venv
    init_db
    setup_systemd
    check_8080_migration
    show_summary
}

run_update() {
    print_banner
    section "Updating Ad Server to v1.3.0"
    check_transcoder
    deploy_files
    setup_venv
    init_db
    setup_systemd
    echo "${GREEN}${BOLD}Update Complete!${RESET}"
}

run_enable_network() {
    print_banner
    if [[ -f "$INSTALL_DIR/enable_network_admin.sh" ]]; then
        bash "$INSTALL_DIR/enable_network_admin.sh"
    else
        echo "${RED}Error: enable_network_admin.sh not found in $INSTALL_DIR${RESET}"
        exit 1
    fi
}

# --- Entry Point ---
if [[ $# -eq 0 ]]; then
    usage
    exit 0
fi

if [[ $EUID -ne 0 ]]; then
   echo "${RED}${BOLD}ERROR: This script must be run as root.${RESET}"
   exit 1
fi

case "$1" in
    install) run_install ;;
    update)  run_update ;;
    enable-network) run_enable_network ;;
    repair)  setup_systemd ;;
    status)  systemctl status adserver adserver-admin ad-redis-listener ad-watcher ;;
    logs)    journalctl -f -u adserver -u adserver-admin -u ad-redis-listener -u ad-watcher ;;
    help)    usage ;;
    *)       echo "Unknown command: $1"; usage; exit 1 ;;
esac
