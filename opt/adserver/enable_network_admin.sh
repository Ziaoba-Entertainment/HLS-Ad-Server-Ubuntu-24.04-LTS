#!/bin/bash
# Helper to enable internal network access for Ad Admin UI

INSTALL_DIR="/opt/adserver"
ENV_FILE="$INSTALL_DIR/.env"

echo "Updating .env to listen on all interfaces..."
if [[ -f "$ENV_FILE" ]]; then
    sed -i 's/AD_ADMIN_HOST=127.0.0.1/AD_ADMIN_HOST=0.0.0.0/' "$ENV_FILE"
    echo "AD_ADMIN_HOST set to 0.0.0.0"
else
    echo "Error: .env file not found at $ENV_FILE"
    exit 1
fi

echo "Restarting adserver-admin service..."
systemctl restart adserver-admin

if command -v ufw > /dev/null; then
    echo "Allowing port 8089 from local network (192.168.0.0/24)..."
    ufw allow from 192.168.0.0/24 to any port 8089
    ufw status | grep 8089
fi

echo "Done. You should now be able to reach the UI at http://192.168.0.103:8089/"
