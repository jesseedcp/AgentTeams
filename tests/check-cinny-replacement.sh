#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

grep -Fq 'FROM ghcr.io/cinnyapp/cinny:v4.12.3 AS cinny' \
    "${ROOT_DIR}/agentteams-controller/Dockerfile.embedded"
grep -Fq 'COPY --from=cinny /app /opt/cinny' \
    "${ROOT_DIR}/agentteams-controller/Dockerfile.embedded"
grep -Fq '[program:cinny]' \
    "${ROOT_DIR}/agentteams-controller/supervisord.embedded.conf"
grep -Fq 'command=/opt/agentteams/scripts/init/start-cinny.sh' \
    "${ROOT_DIR}/agentteams-controller/supervisord.embedded.conf"
grep -Fq 'AGENTTEAMS_CINNY_HOMESERVER_URL' \
    "${ROOT_DIR}/manager/scripts/init/start-cinny.sh"
grep -Fq 'AGENTTEAMS_CINNY_PUBLIC_URL' \
    "${ROOT_DIR}/manager/scripts/init/start-cinny.sh"
grep -Fq '"defaultHomeserver": 0' \
    "${ROOT_DIR}/manager/scripts/init/start-cinny.sh"
grep -Fq '"hashRouter"' \
    "${ROOT_DIR}/manager/scripts/init/start-cinny.sh"
grep -Fq 'location = /.well-known/matrix/client' \
    "${ROOT_DIR}/manager/scripts/init/start-cinny.sh"
grep -Fq '"m.homeserver"' \
    "${ROOT_DIR}/manager/scripts/init/start-cinny.sh"
grep -Fq 'default_type application/wasm' \
    "${ROOT_DIR}/manager/scripts/init/start-cinny.sh"
grep -Fq 'listen 8088;' \
    "${ROOT_DIR}/manager/scripts/init/start-cinny.sh"
grep -Fq 'listen 8002;' \
    "${ROOT_DIR}/manager/scripts/init/start-cinny.sh"

test ! -e "${ROOT_DIR}/manager/scripts/init/start-element-web.sh"
! grep -qi 'element-web' "${ROOT_DIR}/agentteams-controller/Dockerfile.embedded"
! grep -qi 'element-web' "${ROOT_DIR}/agentteams-controller/supervisord.embedded.conf"

echo "PASS: embedded image contains Cinny and no Element runtime"
