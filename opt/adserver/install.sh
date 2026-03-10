#!/bin/bash
set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Starting HLS Ad Injection System Installation ===${NC}"

# 1. Install System Dependencies
echo -e "${BLUE}[1/8] Installing system dependencies...${NC}"
apt-get update
apt-get install -y python3.11 python3.11-venv python3-pip redis-server nginx ffmpeg apache2-utils curl

# 2. Create Service User
echo -e "${BLUE}[2/8] Creating 'media' service user...${NC}"
if ! id "media" &>/dev/null; then
    useradd -m -s /usr/sbin/nologin -d /opt/adserver media
    echo -e "${GREEN}✓ User 'media' created.${NC}"
else
    echo -e "${GREEN}✓ User 'media' already exists.${NC}"
fi

# 3. Create Directory Tree
echo -e "${BLUE}[3/8] Creating directory tree...${NC}"
mkdir -p /opt/adserver/templates /opt/adserver/archive
mkdir -p /srv/vod/ads /srv/vod/hls /srv/vod/output

# 4. Set Ownership and Permissions
echo -e "${BLUE}[4/8] Setting ownership and permissions...${NC}"
chown -R media:media /opt/adserver
chown -R media:media /srv/vod/ads
chown -R www-data:www-data /srv/vod/hls
chown -R www-data:www-data /srv/vod/output
chmod -R 755 /srv/vod/output
chmod -R 755 /srv/vod/hls
chmod -R 755 /srv/vod/ads

# 5. Setup Python Virtual Environment
echo -e "${BLUE}[5/8] Setting up Python virtual environment...${NC}"
sudo -u media python3 -m venv /opt/adserver/venv
sudo -u media /opt/adserver/venv/bin/pip install --upgrade pip
if [ -f "/opt/adserver/requirements.txt" ]; then
    sudo -u media /opt/adserver/venv/bin/pip install -r /opt/adserver/requirements.txt
else
    echo -e "${RED}✗ requirements.txt not found in /opt/adserver!${NC}"
    exit 1
fi

# 6. Initialize Database
echo -e "${BLUE}[6/8] Initializing SQLite database...${NC}"
sudo -u media /opt/adserver/venv/bin/python3 /opt/adserver/init_db.py

# 7. Configure and Start Services
echo -e "${BLUE}[7/8] Configuring and starting services...${NC}"
systemctl enable redis-server
systemctl start redis-server

# Assuming systemd units were already created in previous steps
if [ -f "/etc/systemd/system/adserver.service" ]; then
    systemctl daemon-reload
    systemctl enable adserver
    systemctl restart adserver
    echo -e "${GREEN}✓ adserver service started.${NC}"
fi

if [ -f "/etc/systemd/system/adserver-admin.service" ]; then
    systemctl enable adserver-admin
    systemctl restart adserver-admin
    echo -e "${GREEN}✓ adserver-admin service started.${NC}"
fi

# 8. Setup Nginx
echo -e "${BLUE}[8/8] Finalizing Nginx setup...${NC}"
if [ -f "/opt/adserver/setup_nginx.sh" ]; then
    bash /opt/adserver/setup_nginx.sh
fi

echo -e "${BLUE}=== Installation Complete ===${NC}"
echo -e "${GREEN}HLS Delivery Port: 8081${NC}"
echo -e "${GREEN}Admin Dashboard Port: 88 (User: admin, Pass: ChangeMe2024!)${NC}"
echo -e "${GREEN}Ad Injection API: http://127.0.0.1:8082${NC}"
echo -e "${BLUE}Run 'bash /opt/adserver/health_check.sh' to verify.${NC}"
