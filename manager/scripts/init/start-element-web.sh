#!/bin/bash
# start-element-web.sh - Generate Element Web config and start Nginx

MATRIX_DOMAIN="${AGENTTEAMS_MATRIX_DOMAIN:-matrix-local.agentteams.io:8080}"
# Browser-facing homeserver URL (may differ from internal domain in embedded mode)
ELEMENT_HOMESERVER_URL="${AGENTTEAMS_ELEMENT_HOMESERVER_URL:-http://${MATRIX_DOMAIN}}"
# Brand name for Element Web (defaults to "Element" if not set)
ELEMENT_BRAND="${AGENTTEAMS_ELEMENT_BRAND:-Element}"

# Generate Element Web config.json pointing to local Matrix Homeserver
cat > /opt/element-web/config.json << EOF
{
    "default_server_config": {
        "m.homeserver": {
            "base_url": "${ELEMENT_HOMESERVER_URL}"
        }
    },
    "brand": "${ELEMENT_BRAND}",
    "disable_guests": true,
    "disable_custom_urls": false
}
EOF

# Configure nginx worker processes (default is auto, which uses CPU core count)
sed -i 's/worker_processes.*auto;/worker_processes 2;/' /etc/nginx/nginx.conf 2>/dev/null || \
sed -i 's/^worker_processes [0-9]*;/worker_processes 2;/' /etc/nginx/nginx.conf 2>/dev/null || \
grep -q '^worker_processes' /etc/nginx/nginx.conf || \
sed -i '1i worker_processes 2;' /etc/nginx/nginx.conf

# Create browser bypass script as external JS file (allowed by CSP script-src 'self')
# This avoids adding 'unsafe-inline' to CSP, preserving XSS protection
echo 'window.localStorage.setItem("mx_accepts_unsupported_browser","true");' > /opt/element-web/browser-bypass.js

# Generate Nginx config for Element Web
# Note: We inject an external script tag to automatically accept unsupported browsers
# This bypasses the browser version check in Element Web's SupportedBrowser.ts
cat > /etc/nginx/conf.d/element-web.conf << 'NGINX'
server {
    listen 8088;
    root /opt/element-web;
    index index.html;

    # Inject external script to bypass browser compatibility check
    # Uses external JS file instead of inline script to comply with CSP (script-src 'self')
    sub_filter '</head>' '<script src="browser-bypass.js"></script></head>';
    sub_filter_once on;
    sub_filter_types text/html;

    location / {
        try_files $uri $uri/ /index.html;
    }

    location ~* ^/(config.*\.json|index\.html|i18n|version)$ {
        add_header Cache-Control "no-cache";
    }
}
NGINX

# Generate Nginx config for Higress WASM plugin server (port 8002).
# This serves /usr/share/nginx/html/plugins/* to Envoy so it can fetch
# WASM modules (ai-proxy, key-auth, ai-statistics, etc.). Without this,
# Envoy fails to load AI plugins and forwards requests to upstream LLMs
# without Host header rewrite, resulting in 404s from the LLM backend.
# The base higress/all-in-one image normally runs this as a separate
# `plugin-server` supervisord program with its own nginx instance, but
# our embedded supervisord overrides that config — so we serve it from
# the same nginx as Element Web instead, listening on both v4 and v6
# loopback (Envoy's wasm fetcher uses `localhost` which may resolve to ::1).
cat > /etc/nginx/conf.d/plugin-server.conf << 'NGINX'
server {
    listen 8002;
    listen [::]:8002;
    server_name localhost;

    root /usr/share/nginx/html;
    server_tokens off;

    location = /healthz {
        return 200 'ok';
        add_header Content-Type text/plain;
    }

    error_page 500 502 503 504 /50x.html;
    location = /50x.html {
        root /usr/share/nginx/html;
    }
}
NGINX

# Remove default nginx site if exists
rm -f /etc/nginx/sites-enabled/default

exec nginx -g 'daemon off;'
