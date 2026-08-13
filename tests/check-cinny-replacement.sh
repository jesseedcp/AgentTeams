#!/usr/bin/env bash

# 静态替换门禁：检查构建、Supervisor、Nginx 和 Helm 都已从 Element 切换到固定版本的 Cinny。
# grep 成功只证明“接线文本存在”，不证明页面可交互；真实 UI/Matrix 验收仍由端到端测试承担。
# 固定字符串刻意形成发布契约，若升级 Cinny 或改变启动方式，必须同步审查并更新本测试。

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
