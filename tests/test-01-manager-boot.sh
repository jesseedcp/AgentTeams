#!/bin/bash
# test-01-manager-boot.sh - AgentScope Manager production boot contract
# 测试意图：先确认真实容器中的 Matrix、Cinny、Higress、MinIO 和 Manager 健康端点都已就绪。
# 这是后续用例的前置门禁；如果本测试失败，后面的“创建 Worker”失败通常只是基础设施未启动的连锁结果。
# 通过标准不仅是进程存在，还包括生产入口、运行时配置与持久化目录符合 AgentScope Manager 契约。

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/test-helpers.sh"
source "${SCRIPT_DIR}/lib/matrix-client.sh"
source "${SCRIPT_DIR}/lib/higress-client.sh"
source "${SCRIPT_DIR}/lib/minio-client.sh"

test_setup "01-manager-boot"

log_section "Infrastructure Health"

GATEWAY_CODE=$(curl -s -o /dev/null -w '%{http_code}' \
    "http://${TEST_MANAGER_HOST}:${TEST_GATEWAY_PORT}/" 2>/dev/null)
if [ "${GATEWAY_CODE}" != "000" ]; then
    log_pass "Higress Gateway is accessible (HTTP ${GATEWAY_CODE})"
else
    log_fail "Higress Gateway is accessible"
fi

assert_http_code "http://${TEST_MANAGER_HOST}:${TEST_CONSOLE_PORT}/" "200" \
    "Higress Console is accessible"
assert_http_code "http://${TEST_MANAGER_HOST}:${TEST_CINNY_PORT}/" "200" \
    "Cinny is accessible"

_INFRA_CTR="${TEST_CONTROLLER_CONTAINER:-agentteams-controller}"
_AGENT_CTR="${TEST_AGENT_CONTAINER:-agentteams-manager}"

MATRIX_CODE=$(docker exec "${_INFRA_CTR}" curl -s -o /dev/null -w '%{http_code}' \
    "http://127.0.0.1:6167/_matrix/client/versions" 2>/dev/null || echo "000")
assert_eq "200" "${MATRIX_CODE}" "Tuwunel Matrix is healthy"

MINIO_CODE=$(docker exec "${_INFRA_CTR}" curl -s -o /dev/null -w '%{http_code}' \
    "http://127.0.0.1:9000/minio/health/live" 2>/dev/null || echo "000")
assert_eq "200" "${MINIO_CODE}" "MinIO API is healthy"

log_section "AgentScope Manager Runtime"

if docker exec "${_AGENT_CTR}" python -c \
    'import urllib.request; urllib.request.urlopen("http://127.0.0.1:18799/readyz", timeout=2).read()' \
    >/dev/null 2>&1; then
    log_pass "AgentScope Manager /readyz is healthy"
else
    log_fail "AgentScope Manager /readyz is healthy"
fi

AGENTSCOPE_VERSION=$(docker exec "${_AGENT_CTR}" python -c \
    'import agentscope; print(agentscope.__version__)' 2>/dev/null || true)
assert_eq "2.0.4.post1" "${AGENTSCOPE_VERSION}" "AgentScope is pinned to 2.0.4.post1"

MANAGER_RUNTIME=$(docker exec "${_AGENT_CTR}" printenv AGENTTEAMS_MANAGER_RUNTIME 2>/dev/null || \
    docker exec "${_INFRA_CTR}" printenv AGENTTEAMS_MANAGER_RUNTIME 2>/dev/null || true)
assert_eq "agentscope" "${MANAGER_RUNTIME}" "Manager runtime is agentscope"

MANAGER_RESOURCE=$(docker exec "${_AGENT_CTR}" agt get managers default -o json 2>/dev/null || true)
assert_eq "agentscope" "$(echo "${MANAGER_RESOURCE}" | jq -r '.runtime // empty')" \
    "Controller reports the agentscope Manager runtime"

for asset in SOUL.md AGENTS.md TOOLS.md HEARTBEAT.md; do
    if docker exec "${_AGENT_CTR}" test -f "/opt/agentteams/manager/${asset}" 2>/dev/null; then
        log_pass "Declared Manager asset exists: ${asset}"
    else
        log_fail "Declared Manager asset exists: ${asset}"
    fi
done

SKILL_COUNT=$(docker exec "${_AGENT_CTR}" sh -c \
    'find /opt/agentteams/manager/skills -mindepth 1 -maxdepth 1 -type d | wc -l' \
    2>/dev/null | tr -d '[:space:]')
assert_eq "19" "${SKILL_COUNT}" "Exactly 19 Manager skills are installed"

STALE_PROCESSES=$(docker exec "${_AGENT_CTR}" python -c \
    'import os,pathlib; pats=("open"+"claw gateway","co"+"paw app","redis"+"-server"); print("\n".join(x for p in pathlib.Path("/proc").glob("[0-9]*/cmdline") if int(p.parent.name)!=os.getpid() for x in [p.read_bytes().replace(b"\0",b" ").decode(errors="ignore")] if any(q in x for q in pats)))' \
    2>/dev/null || true)
assert_eq "" "${STALE_PROCESSES}" "No legacy Manager or Redis process is running"

log_section "Control Plane Access"

ADMIN_LOGIN=$(matrix_login "${TEST_ADMIN_USER}" "${TEST_ADMIN_PASSWORD}")
ADMIN_TOKEN=$(echo "${ADMIN_LOGIN}" | jq -r '.access_token')
assert_not_empty "${ADMIN_TOKEN}" "Admin Matrix login returns an access token"

HIGRESS_SESSION=$(higress_login "${TEST_ADMIN_USER}" "${TEST_ADMIN_PASSWORD}" 2>/dev/null || true)
assert_not_empty "${HIGRESS_SESSION}" "Higress Console login succeeds"

CONSUMERS=$(higress_get_consumers 2>/dev/null || true)
if echo "${CONSUMERS}" | grep -q "manager" 2>/dev/null; then
    log_pass "Manager consumer exists in Higress"
else
    log_fail "Manager consumer exists in Higress"
fi

if minio_setup 2>/dev/null; then
    log_pass "MinIO client credentials work"
else
    log_fail "MinIO client credentials work"
fi

log_section "Matrix Agent Turn"

if require_llm_key; then
    MANAGER_USER_ID="@manager:${TEST_MATRIX_DOMAIN}"
    MANAGER_ROOM=$(matrix_find_dm_room "${ADMIN_TOKEN}" "${MANAGER_USER_ID}" 2>/dev/null || true)
    if [ -z "${MANAGER_ROOM}" ]; then
        MANAGER_ROOM=$(matrix_create_dm_room "${ADMIN_TOKEN}" "${MANAGER_USER_ID}" 2>/dev/null || true)
    fi
    assert_not_empty "${MANAGER_ROOM}" "Manager DM room exists"

    if [ -n "${MANAGER_ROOM}" ] && \
        wait_for_manager_agent_ready 300 "${MANAGER_ROOM}" "${ADMIN_TOKEN}"; then
        log_pass "Manager joined the DM room"
        matrix_send_message "${ADMIN_TOKEN}" "${MANAGER_ROOM}" \
            "Reply with exactly: AGENTSCOPE_MANAGER_READY" >/dev/null
        MANAGER_REPLY=$(matrix_wait_for_reply_matching \
            "${ADMIN_TOKEN}" "${MANAGER_ROOM}" "@manager:" \
            "AGENTSCOPE_MANAGER_READY" 180 2>/dev/null || true)
        assert_contains "${MANAGER_REPLY}" "AGENTSCOPE_MANAGER_READY" \
            "AgentScope Manager completes a streamed Matrix turn"
    fi
else
    log_info "Matrix model turn skipped; runtime and infrastructure checks still executed"
fi

test_teardown "01-manager-boot"
test_summary
