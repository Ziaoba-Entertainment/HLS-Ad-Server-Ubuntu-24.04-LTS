#!/bin/bash

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

check_service() {
    if systemctl is-active --quiet "$1"; then
        echo -e "${GREEN}✓ Service $1 is running${NC}"
        return 0
    else
        echo -e "${RED}✗ Service $1 is NOT running${NC}"
        return 1
    fi
}

check_port() {
    if curl -s --head "http://127.0.0.1:$1" | grep "200\|401\|404" > /dev/null; then
        echo -e "${GREEN}✓ Port $1 is responding${NC}"
        return 0
    else
        echo -e "${RED}✗ Port $1 is NOT responding${NC}"
        return 1
    fi
}

check_api_health() {
    local url=$1
    local name=$2
    if curl -s "$url" | grep "ok" > /dev/null; then
        echo -e "${GREEN}✓ $name API Health check passed${NC}"
        return 0
    else
        echo -e "${RED}✗ $name API Health check FAILED${NC}"
        return 1
    fi
}

FAILED=0

echo "--- System Health Check ---"

# Services
check_service "adserver" || FAILED=1
check_service "adserver-admin" || FAILED=1
check_service "nginx" || FAILED=1
check_service "redis-server" || FAILED=1

# Ports
check_port "8083" || FAILED=1 # Ad Stitcher
check_port "8089" || FAILED=1 # Admin UI

# API Health
check_api_health "http://127.0.0.1:8083/health" "Ad Stitcher" || FAILED=1
check_api_health "http://127.0.0.1:8089/health" "Admin UI" || FAILED=1

# Redis
REDIS_PING_CMD="redis-cli"
if [[ -f "/etc/ziaoba/redis.env" ]]; then
    source "/etc/ziaoba/redis.env"
    export REDISCLI_AUTH="$REDIS_PASSWORD"
    REDIS_PING_CMD="redis-cli -h $REDIS_HOST -p $REDIS_PORT"
fi

if $REDIS_PING_CMD ping | grep "PONG" > /dev/null; then
    echo -e "${GREEN}✓ Redis is responding to PING${NC}"
else
    echo -e "${RED}✗ Redis PING failed${NC}"
    FAILED=1
fi

# SQLite
if [ -f "/opt/adserver/adserver.db" ] && [ -s "/opt/adserver/adserver.db" ]; then
    echo -e "${GREEN}✓ SQLite database exists and is not empty${NC}"
else
    echo -e "${RED}✗ SQLite database missing or empty${NC}"
    FAILED=1
fi

# Ad Folders
AD_COUNT=$(find /srv/vod/ads/ -maxdepth 1 -type d -name "advert*" | wc -l)
if [ "$AD_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✓ Found $AD_COUNT ad folders in /srv/vod/ads/${NC}"
else
    echo -e "${RED}✗ No ad folders found in /srv/vod/ads/${NC}"
    FAILED=1
fi

echo "---------------------------"
if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}ALL CHECKS PASSED${NC}"
    exit 0
else
    echo -e "${RED}SOME CHECKS FAILED${NC}"
    exit 1
fi
