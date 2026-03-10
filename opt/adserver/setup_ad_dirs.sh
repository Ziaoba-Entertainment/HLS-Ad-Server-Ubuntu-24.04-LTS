#!/bin/bash
set -e

# Ensure script is run as root
if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root" 
   exit 1
fi

echo "[*] Creating ad server directories..."
mkdir -p /srv/vod/ads/incoming
mkdir -p /srv/vod/ads/rejected
mkdir -p /var/log/adserver

echo "[*] Setting ownership to media:media..."
chown -R media:media /srv/vod/ads
chown -R media:media /var/log/adserver

echo "[*] Setting permissions (755)..."
chmod -R 755 /srv/vod/ads
chmod -R 755 /var/log/adserver

touch /var/log/adserver/.gitkeep

echo "[*] Verifying dependencies..."
if ! command -v inotifywait &> /dev/null; then
    echo "[!] inotifywait not found. You should install inotify-tools:"
    echo "    apt-get update && apt-get install -y inotify-tools"
else
    echo "[PASS] inotify-tools is installed."
fi

if [[ ! -f "/usr/bin/ffprobe" ]]; then
    echo "[FAIL] ffprobe not found at /usr/bin/ffprobe. Please install ffmpeg:"
    echo "    apt-get update && apt-get install -y ffmpeg"
    exit 1
else
    echo "[PASS] ffprobe found at /usr/bin/ffprobe."
fi

echo "[*] Current directory structure of /srv/vod/ads/:"
find /srv/vod/ads -maxdepth 2 -type d

echo "[SUCCESS] Ad directories setup complete."
