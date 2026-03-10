#!/bin/bash
set -e

# Ensure script is run as root
if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root" 
   exit 1
fi

echo "Enabling Nginx configurations..."
ln -sf /etc/nginx/sites-available/hls-delivery.conf /etc/nginx/sites-enabled/
ln -sf /etc/nginx/sites-available/admin-ui.conf /etc/nginx/sites-enabled/

# Remove default if it exists
rm -f /etc/nginx/sites-enabled/default

echo "Creating .htpasswd for admin user..."
# Install apache2-utils if htpasswd command is missing
if ! command -v htpasswd &> /dev/null; then
    apt-get update && apt-get install -y apache2-utils
fi

# Create .htpasswd with password 'ChangeMe2024!'
# -b flag allows passing password in command line
# -c creates a new file
htpasswd -b -c /etc/nginx/.htpasswd admin ChangeMe2024!
chmod 640 /etc/nginx/.htpasswd
chown root:www-data /etc/nginx/.htpasswd

echo "Setting up directory permissions..."
mkdir -p /srv/vod/hls /srv/vod/ads /srv/vod/output
chown -R www-data:www-data /srv/vod
find /srv/vod -type d -exec chmod 755 {} +
find /srv/vod -type f -exec chmod 644 {} +

# Ensure output dir is specifically writable
chmod -R 755 /srv/vod/output

echo "Testing Nginx configuration..."
nginx -t

echo "Reloading Nginx..."
systemctl reload nginx

echo "Nginx setup complete."
