#!/bin/bash
# start-cinny.sh - Generate Cinny config and start Nginx.

set -euo pipefail

MATRIX_DOMAIN="${AGENTTEAMS_MATRIX_DOMAIN:-matrix-local.agentteams.io:8080}"
# The old Element variable is read only so an existing env file can be
# upgraded without losing its browser-facing Matrix address.
CINNY_HOMESERVER_URL="${AGENTTEAMS_CINNY_HOMESERVER_URL:-${AGENTTEAMS_ELEMENT_HOMESERVER_URL:-http://${MATRIX_DOMAIN}}}"
CINNY_HOMESERVER_JSON="$(printf '%s' "${CINNY_HOMESERVER_URL}" | jq -Rs .)"
CINNY_PUBLIC_URL="${AGENTTEAMS_CINNY_PUBLIC_URL:-http://127.0.0.1:${AGENTTEAMS_PORT_CINNY:-8088}}"
CINNY_PUBLIC_JSON="$(printf '%s' "${CINNY_PUBLIC_URL}" | jq -Rs .)"

# Cinny performs Matrix discovery against each homeserverList entry. Embedded
# deployments expose Cinny and the Matrix gateway on different host ports, so
# discovery starts at Cinny's public URL and its well-known response points to
# the actual Matrix gateway.
cat > /opt/cinny/config.json << EOF
{
  "defaultHomeserver": 0,
  "homeserverList": [${CINNY_PUBLIC_JSON}],
  "allowCustomHomeservers": true,
  "featuredCommunities": {
    "openAsDefault": false,
    "spaces": [],
    "rooms": [],
    "servers": []
  },
  "hashRouter": {
    "enabled": true,
    "basename": "/"
  }
}
EOF

mkdir -p /opt/cinny/.well-known/matrix
cat > /opt/cinny/.well-known/matrix/client << EOF
{"m.homeserver":{"base_url":${CINNY_HOMESERVER_JSON}}}
EOF

# Keep Nginx bounded on large hosts instead of spawning one worker per CPU.
sed -i 's/worker_processes.*auto;/worker_processes 2;/' /etc/nginx/nginx.conf 2>/dev/null || \
sed -i 's/^worker_processes [0-9]*;/worker_processes 2;/' /etc/nginx/nginx.conf 2>/dev/null || \
grep -q '^worker_processes' /etc/nginx/nginx.conf || \
sed -i '1i worker_processes 2;' /etc/nginx/nginx.conf

cat > /etc/nginx/conf.d/cinny.conf << 'NGINX'
server {
    listen 8088;
    root /opt/cinny;
    index index.html;

    location = /config.json {
        add_header Cache-Control "no-store, no-cache, must-revalidate";
        try_files $uri =404;
    }

    location = /.well-known/matrix/client {
        default_type application/json;
        add_header Access-Control-Allow-Origin "*";
        add_header Cache-Control "no-store, no-cache, must-revalidate";
        try_files $uri =404;
    }

    location = /index.html {
        add_header Cache-Control "no-store, no-cache, must-revalidate";
        try_files $uri =404;
    }

    # Ubuntu 22.04's Nginx MIME table does not include WebAssembly. Cinny's
    # Matrix crypto module requires this exact type for streaming compilation.
    location ~ \.wasm$ {
        default_type application/wasm;
        try_files $uri =404;
    }

    location / {
        try_files $uri $uri/ /index.html;
    }
}
NGINX

# Higress' Envoy process fetches WASM plugins from localhost:8002. The
# embedded image replaces the base Supervisor config, so this Nginx process
# also preserves the plugin server that the Higress all-in-one image expects.
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

rm -f /etc/nginx/sites-enabled/default

exec nginx -g 'daemon off;'
