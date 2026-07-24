#!/bin/bash
# test-05-heartbeat.sh - AgentScope deterministic heartbeat and snapshot gate

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/test-helpers.sh"
source "${SCRIPT_DIR}/lib/minio-client.sh"

test_setup "05-heartbeat"

_AGENT_CTR="${TEST_AGENT_CONTAINER:-agentteams-manager}"

log_section "Heartbeat Runtime"

if docker exec "${_AGENT_CTR}" python -c \
    'import urllib.request; urllib.request.urlopen("http://127.0.0.1:18799/readyz",timeout=2).read()' \
    >/dev/null 2>&1; then
    log_pass "AgentScope Manager is ready after startup heartbeat"
else
    log_fail "AgentScope Manager is ready after startup heartbeat"
fi

METRICS=$(docker exec "${_AGENT_CTR}" python -c \
    'import urllib.request; print(urllib.request.urlopen("http://127.0.0.1:18799/metrics",timeout=2).read().decode())' \
    2>/dev/null || true)

_metric_value() {
    local name="$1"
    echo "${METRICS}" | awk -v metric="${name}" \
        '$1 == metric {print $2; exit}'
}

HEARTBEATS=$(_metric_value agentteams_manager_heartbeats_total)
RECOVERED=$(_metric_value agentteams_manager_recovery_reconciled_total)
RECOVERY_ERRORS=$(_metric_value agentteams_manager_recovery_errors_total)
MANAGER_UP=$(_metric_value agentteams_manager_up)

if awk "BEGIN {exit !(${HEARTBEATS:-0} >= 1)}"; then
    log_pass "Startup executes at least one deterministic heartbeat"
else
    log_fail "Startup executes at least one deterministic heartbeat (got ${HEARTBEATS:-empty})"
fi
assert_not_empty "${RECOVERED}" "Heartbeat exports recovery reconciliation count"
assert_not_empty "${RECOVERY_ERRORS}" "Heartbeat exports recovery error count"
assert_eq "1" "${MANAGER_UP}" "Manager up metric remains healthy"

log_section "Verified SQLite Snapshot"

minio_setup
if minio_wait_for_file "manager/snapshots/latest.json" 60; then
    log_pass "Heartbeat publishes the latest SQLite snapshot pointer"
else
    log_fail "Heartbeat publishes the latest SQLite snapshot pointer"
fi

SNAPSHOT_META=$(minio_read_file "manager/snapshots/latest.json" 2>/dev/null || true)
if echo "${SNAPSHOT_META}" | jq -e '
    (.sequence | type == "number") and
    (.sequence >= 0) and
    (.key | test("^manager/snapshots/[0-9]{20}\\.db$")) and
    (.sha256 | test("^[0-9a-f]{64}$")) and
    (.size > 0)
' >/dev/null 2>&1; then
    log_pass "Snapshot pointer has a sequence, checksum, size, and immutable key"
else
    log_fail "Snapshot pointer has a sequence, checksum, size, and immutable key"
fi

SNAPSHOT_KEY=$(echo "${SNAPSHOT_META}" | jq -r '.key // empty')
if [ -n "${SNAPSHOT_KEY}" ] && \
    minio_file_exists "${SNAPSHOT_KEY}"; then
    log_pass "Snapshot database object exists"
else
    log_fail "Snapshot database object exists"
fi

log_section "Heartbeat Policy"

HEARTBEAT_DOC=$(docker exec "${_AGENT_CTR}" \
    cat /opt/agentteams/manager/HEARTBEAT.md 2>/dev/null || true)
assert_contains "${HEARTBEAT_DOC}" \
    "Recover task, project, artifact, and Git operations" \
    "Heartbeat starts with deterministic operation recovery"
assert_contains "${HEARTBEAT_DOC}" \
    "Create a verified SQLite snapshot" \
    "Heartbeat ends with verified SQLite snapshot creation"
assert_contains "${HEARTBEAT_DOC}" \
    "without a model call" \
    "Heartbeat does not depend on an LLM turn"

test_teardown "05-heartbeat"
test_summary
