#!/bin/bash
# test-28-agentscope-manager-parity.sh - Static and running-image parity gate

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/lib/test-helpers.sh"

test_setup "28-agentscope-manager-parity"

log_section "Declared Skill Parity"

if PROJECT_ROOT="${PROJECT_ROOT}" python3 - <<'PY'
import json
import os
import pathlib
import re

root = pathlib.Path(os.environ["PROJECT_ROOT"])
manifest = json.loads(
    (root / "tests/manager-skill-parity.json").read_text(encoding="utf-8")
)
skills = manifest["skills"]
assert manifest["schemaVersion"] == 1
assert manifest["managerRuntime"] == "agentscope"
assert manifest["agentScopeVersion"] == "2.0.4.post1"
assert len(skills) == 16
names = {item["name"] for item in skills}
disk = {
    path.name
    for path in (root / "manager/agent/skills").iterdir()
    if path.is_dir()
}
assert names == disk
for item in skills:
    skill_file = root / item["skillFile"]
    text = skill_file.read_text(encoding="utf-8")
    match = re.search(r"(?m)^name:\s*(\S+)\s*$", text)
    assert match and match.group(1) == item["name"]
    docs = "\n".join(
        path.read_text(encoding="utf-8")
        for path in skill_file.parent.rglob("*.md")
    )
    for tool in item["tools"]:
        assert f"`{tool}`" in docs, (item["name"], tool)
    for evidence in item["evidence"]:
        assert (root / evidence).is_file(), (item["name"], evidence)
PY
then
    log_pass "All 16 retained skills have typed-tool documents and evidence"
else
    log_fail "All 16 retained skills have typed-tool documents and evidence"
fi

STALE_SOURCE_PATHS=(
    "manager/scripts/systemctl-shim.sh"
    "manager/scripts/setup-host-symlinks.sh"
    "manager/agent/copaw-manager-agent"
    "manager/agent/skills-alpha"
)
STALE_SOURCE_FOUND=""
for path in "${STALE_SOURCE_PATHS[@]}"; do
    if [ -f "${PROJECT_ROOT}/${path}" ] || \
        find "${PROJECT_ROOT}/${path}" -type f -print -quit 2>/dev/null | grep -q .; then
        STALE_SOURCE_FOUND="${STALE_SOURCE_FOUND} ${path}"
    fi
done
assert_eq "" "${STALE_SOURCE_FOUND}" "Legacy Manager runtime source payload is absent"

log_section "Running AgentScope Image"

_AGENT_CTR="${TEST_AGENT_CONTAINER:-agentteams-manager}"
RUNTIME=$(docker exec "${_AGENT_CTR}" printenv AGENTTEAMS_MANAGER_RUNTIME 2>/dev/null || true)
assert_eq "agentscope" "${RUNTIME}" "Running Manager runtime is agentscope"

VERSION=$(docker exec "${_AGENT_CTR}" python -c \
    'import agentscope; print(agentscope.__version__)' 2>/dev/null || true)
assert_eq "2.0.4.post1" "${VERSION}" "Running AgentScope version is pinned"

if docker exec "${_AGENT_CTR}" python -c \
    'import urllib.request; urllib.request.urlopen("http://127.0.0.1:18799/readyz",timeout=2).read()' \
    >/dev/null 2>&1; then
    log_pass "AgentScope Manager readiness contract passes"
else
    log_fail "AgentScope Manager readiness contract passes"
fi

IMAGE_SKILLS=$(docker exec "${_AGENT_CTR}" sh -c \
    'find /opt/agentteams/manager/skills -mindepth 1 -maxdepth 1 -type d | wc -l' \
    2>/dev/null | tr -d '[:space:]')
assert_eq "16" "${IMAGE_SKILLS}" "Running image contains exactly 16 Manager skills"

LEGACY_BINARIES=$(docker exec "${_AGENT_CTR}" sh -c '
    for path in \
      /usr/local/bin/openclaw \
      /usr/local/bin/copaw \
      /usr/local/bin/redis-server \
      /opt/agentteams/scripts/init/start-manager-agent.sh \
      /opt/agentteams/scripts/init/start-copaw-manager.sh; do
        [ -e "$path" ] && printf "%s\n" "$path"
    done
' 2>/dev/null || true)
assert_eq "" "${LEGACY_BINARIES}" "Running image contains no legacy Manager runtime binary"

STALE_PROCESSES=$(docker exec "${_AGENT_CTR}" python -c \
    'import os,pathlib; pats=("open"+"claw gateway","co"+"paw app","redis"+"-server"); print("\n".join(x for p in pathlib.Path("/proc").glob("[0-9]*/cmdline") if int(p.parent.name)!=os.getpid() for x in [p.read_bytes().replace(b"\0",b" ").decode(errors="ignore")] if any(q in x for q in pats)))' \
    2>/dev/null || true)
assert_eq "" "${STALE_PROCESSES}" "No legacy Manager process is running"

log_section "Operational Metrics Contract"

METRICS=$(docker exec "${_AGENT_CTR}" python -c \
    'import urllib.request; print(urllib.request.urlopen("http://127.0.0.1:18799/metrics",timeout=2).read().decode())' \
    2>/dev/null || true)

REQUIRED_METRICS=(
    agentteams_manager_up
    agentteams_manager_runtime_revision
    agentteams_manager_runtime_reloads_total
    agentteams_manager_matrix_events_total
    agentteams_manager_matrix_turns_total
    agentteams_manager_model_turns_total
    agentteams_manager_tool_calls_total
    agentteams_manager_tool_errors_total
    agentteams_manager_recovery_reconciled_total
    agentteams_manager_recovery_errors_total
    agentteams_manager_errors_total
)
for metric in "${REQUIRED_METRICS[@]}"; do
    if echo "${METRICS}" | grep -q "^${metric} "; then
        log_pass "Metric exported: ${metric}"
    else
        log_fail "Metric exported: ${metric}"
    fi
done

test_teardown "28-agentscope-manager-parity"
test_summary
