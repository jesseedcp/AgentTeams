#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

legacy_brand='hi''claw'
archive_paths=(
    ':(exclude)blog/**'
    ':(exclude)changelog/**'
)

if matches="$(git grep -nIi "${legacy_brand}" -- . "${archive_paths[@]}" 2>/dev/null)"; then
    echo "FAIL: active files still contain the retired brand:" >&2
    echo "${matches}" >&2
    exit 1
fi

if paths="$(git ls-files | grep -i "${legacy_brand}" || true)" && [ -n "${paths}" ]; then
    echo "FAIL: tracked paths still use the retired brand:" >&2
    echo "${paths}" >&2
    exit 1
fi

if matches="$(git grep -nF 'agents/manager/openclaw.json' -- \
    'tests/*.sh' \
    'tests/**/*.sh' \
    ':(exclude)tests/check-agentteams-rename-defaults.sh' \
    2>/dev/null)"; then
    echo "FAIL: tests still depend on the retired Manager OpenClaw config:" >&2
    echo "${matches}" >&2
    exit 1
fi

required_paths=(
    agentteams-controller/go.mod
    helm/agentteams/Chart.yaml
    install/agentteams-install.sh
    install/agentteams-install.ps1
    shared/lib/agentteams-env.sh
)
for path in "${required_paths[@]}"; do
    if [ ! -e "${path}" ]; then
        echo "FAIL: canonical AgentTeams path is missing: ${path}" >&2
        exit 1
    fi
done

grep -Fq 'module github.com/agentscope-ai/AgentTeams/agentteams-controller' agentteams-controller/go.mod
grep -Fq 'name: agentteams' helm/agentteams/Chart.yaml
grep -Fq 'define "agentteams.name"' helm/agentteams/templates/_helpers.tpl
grep -Fq 'manager/Dockerfile' hack/local-k8s-up.sh
grep -Fq 'QWENPAW_WORKER_IMAGE=' hack/local-k8s-up.sh
grep -Fq 'COPY agent/ /opt/agentteams/agent/' agentteams-controller/Dockerfile
grep -Fq 'COPY manager/agent/worker-skills/ /opt/agentteams/agent/worker-skills/' \
    agentteams-controller/Dockerfile.embedded
grep -Fq 'cp -r ./manager/agent ./agentteams-controller/agent' Makefile

if grep -Eq 'manager-copaw|Dockerfile\.(copaw|k8s)|AGENTTEAMS_BUILD_K8S_IMAGE' \
    hack/local-k8s-up.sh; then
    echo "FAIL: local Kubernetes bootstrap still selects a retired Manager image" >&2
    exit 1
fi

if grep -nF 'push-worker-skills.sh' \
    agentteams-controller/internal/service/deployer.go \
    manager/agent/worker-skills/README.md \
    manager/scripts/init/start-mc-mirror.sh \
    docs/import-worker.md \
    docs/zh-cn/import-worker.md \
    docs/declarative-resource-management.md \
    docs/zh-cn/declarative-resource-management.md; then
    echo "FAIL: active distribution paths still invoke the deleted Manager skill script" >&2
    exit 1
fi

for readme in README.md README.zh-CN.md README.ja-JP.md; do
    grep -Fq 'AgentScope 2.0' "${readme}"
    grep -Fq 'OpenHuman' "${readme}"
done

if grep -Eq 'Manager \((OpenClaw|CoPaw|QwenPaw)' \
    README.md README.zh-CN.md README.ja-JP.md; then
    echo "FAIL: a primary README still presents a Worker engine as the Manager runtime" >&2
    exit 1
fi

echo "PASS: active source tree uses AgentTeams-only names and contracts"
