#!/bin/bash
# test-28-agentscope-manager-parity.sh - Static and running-image parity gate
# 测试意图：保证源码、Manager Skill、注册工具、镜像入口与运行中容器暴露的能力是同一套契约。
# `--static-only` 适合快速 CI，不启动镜像；完整模式还进入真实容器检查，能发现 COPY/entrypoint 遗漏。
# 通过标准是保留能力存在、已删除的旧 Manager 分支不再出现，且 Skill parity 清单没有漂移。

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

STATIC_ONLY=false
while [ "$#" -gt 0 ]; do
    case "$1" in
        --static-only)
            STATIC_ONLY=true
            ;;
        *)
            echo "Unknown argument: $1" >&2
            echo "Usage: $0 [--static-only]" >&2
            exit 2
            ;;
    esac
    shift
done

# Avoid probing Docker merely by sourcing the shared helpers during a static
# release check. Runtime mode still auto-detects the real containers.
if [ "${STATIC_ONLY}" = "true" ]; then
    export TEST_CONTROLLER_CONTAINER="${TEST_CONTROLLER_CONTAINER:-static-only}"
    export TEST_AGENT_CONTAINER="${TEST_AGENT_CONTAINER:-static-only}"
fi

source "${SCRIPT_DIR}/lib/test-helpers.sh"

if [ "${STATIC_ONLY}" = "true" ]; then
    log_section "Starting: 28-agentscope-manager-parity (static only)"
else
    test_setup "28-agentscope-manager-parity"
fi

log_section "Declared Skill Parity"

PYTHON_BIN="${PYTHON_BIN:-}"
if [ -z "${PYTHON_BIN}" ] && command -v python3 >/dev/null 2>&1 && \
    python3 -c 'import sys; assert sys.version_info >= (3, 11)' \
    >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
elif [ -z "${PYTHON_BIN}" ] && command -v python >/dev/null 2>&1 && \
    python -c 'import sys; assert sys.version_info >= (3, 11)' \
    >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python)"
fi

if [ -z "${PYTHON_BIN}" ]; then
    log_fail "A host Python interpreter is available for the static parity gate"
elif PROJECT_ROOT="${PROJECT_ROOT}" "${PYTHON_BIN}" - <<'PY'
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
assert len(skills) == 19
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
    log_pass "All 19 retained skills have typed-tool documents and evidence"
else
    log_fail "All 19 retained skills have typed-tool documents and evidence"
fi

log_section "Pinned Upstream Baseline"

if [ -z "${PYTHON_BIN}" ]; then
    log_fail "Latest upstream baseline and parity report are pinned"
elif PROJECT_ROOT="${PROJECT_ROOT}" "${PYTHON_BIN}" - <<'PY'
import json
import os
import pathlib

root = pathlib.Path(os.environ["PROJECT_ROOT"])
expected = "fb3a40be1f005bd584f45544fc73bd4601d5c52a"
fixture = json.loads(
    (
        root
        / "manager-agentscope/tests/contract/fixtures"
        / "upstream-agentteams.json"
    ).read_text(encoding="utf-8")
)
assert fixture["commit"] == expected
assert fixture["latestUpstreamDelta"]["commit"] == expected
report = (
    root / "docs/parity/upstream-agentteams-fb3a40b.md"
).read_text(encoding="utf-8")
assert expected in report
for category in ("已实现", "有意替换", "有意删除", "外部未验证"):
    assert category in report
for evidence in (
    "manager-agentscope/tests/e2e/test_k8s_admin_and_console.py",
    "manager-agentscope/tests/e2e/test_k8s_matrix_commands.py",
):
    assert (root / evidence).is_file()
PY
then
    log_pass "Latest upstream baseline and parity report are pinned"
else
    log_fail "Latest upstream baseline and parity report are pinned"
fi

if [ -z "${PYTHON_BIN}" ]; then
    log_fail "Current Manager docs contain no retired runtime instructions"
elif PROJECT_ROOT="${PROJECT_ROOT}" "${PYTHON_BIN}" - <<'PY'
import os
import pathlib
import re

root = pathlib.Path(os.environ["PROJECT_ROOT"])

production_files = [
    root / "README.md",
    root / "README.zh-CN.md",
    root / "README.ja-JP.md",
    root / "AGENTS.md",
    root / "Makefile",
    root / "changelog/current.md",
]
for relative in ("install", "helm", ".github"):
    production_files.extend(
        path
        for path in (root / relative).rglob("*")
        if path.is_file()
    )
for relative in (
    "architecture.md",
    "zh-cn/architecture.md",
    "manager-guide.md",
    "zh-cn/manager-guide.md",
    "agentscope-manager-operations.md",
    "zh-cn/agentscope-manager-operations.md",
    "development.md",
    "zh-cn/development.md",
    "quickstart.md",
    "zh-cn/quickstart.md",
    "faq.md",
    "zh-cn/faq.md",
    "cms-integration.md",
    "zh-cn/cms-integration.md",
    "import-worker.md",
    "zh-cn/import-worker.md",
    "declarative-resource-management.md",
    "zh-cn/declarative-resource-management.md",
):
    production_files.append(root / "docs" / relative)

retired_claims = (
    "manager runtime: openclaw",
    "manager runtime: copaw",
    "agentteams-manager-copaw",
    "agentteams_force_legacy",
    "openclaw gateway restart",
)
violations: list[str] = []
for path in production_files:
    if not path.is_file():
        continue
    text = path.read_text(encoding="utf-8", errors="replace").casefold()
    for claim in retired_claims:
        if claim in text:
            violations.append(f"{path.relative_to(root)}: {claim}")

manager_docs = [
    root / "manager/agent/AGENTS.md",
    root / "manager/agent/SOUL.md",
    root / "manager/agent/TOOLS.md",
    *(root / "manager/agent/skills").rglob("*.md"),
]
retired_manager_literals = (
    "openclaw gateway",
    "copaw channels send",
    "state.json",
    "workers-registry.json",
    "pending-workers.json",
    "worker-openclaw.json.tmpl",
)
script_path = re.compile(
    r"(?:/opt/agentteams/agent/skills|manager/agent/skills)/[^\s`]+/scripts",
    re.IGNORECASE,
)
mcporter_command = re.compile(
    r"(?im)^\s*(?:`{0,3})mcporter\s+(?:list|call)\b"
)
for path in manager_docs:
    text = path.read_text(encoding="utf-8", errors="replace")
    folded = text.casefold()
    for literal in retired_manager_literals:
        if literal in folded:
            violations.append(f"{path.relative_to(root)}: {literal}")
    if script_path.search(text):
        violations.append(f"{path.relative_to(root)}: retired Manager skill script path")
    if mcporter_command.search(text):
        violations.append(f"{path.relative_to(root)}: mcporter executable command")

controller_owned_distribution_docs = [
    root / "manager/agent/worker-skills/README.md",
    root / "manager/scripts/init/start-mc-mirror.sh",
    root / "agentteams-controller/internal/service/deployer.go",
    root / "docs/import-worker.md",
    root / "docs/zh-cn/import-worker.md",
    root / "docs/declarative-resource-management.md",
    root / "docs/zh-cn/declarative-resource-management.md",
]
for path in controller_owned_distribution_docs:
    text = path.read_text(encoding="utf-8", errors="replace").casefold()
    if "push-worker-skills.sh" in text:
        violations.append(
            f"{path.relative_to(root)}: deleted Manager skill distribution script"
        )
    if "manager's workers-registry.json" in text:
        violations.append(
            f"{path.relative_to(root)}: Manager registry presented as desired state"
        )
    if "manager 的 workers-registry.json" in text:
        violations.append(
            f"{path.relative_to(root)}: Manager registry presented as desired state"
        )

exporter_source = (root / "scripts/export-debug-log.py").read_text(
    encoding="utf-8",
)
exporter_options = set(
    re.findall(
        r'parser\.add_argument\(\s*"(--[a-z0-9-]+)"',
        exporter_source,
    )
)
for relative in (
    "docs/agentscope-manager-operations.md",
    "docs/zh-cn/agentscope-manager-operations.md",
):
    path = root / relative
    text = path.read_text(encoding="utf-8")
    command = re.search(
        r"python scripts/export-debug-log\.py\s*\\\n.*?\n```",
        text,
        re.DOTALL,
    )
    if command is None:
        violations.append(f"{relative}: debug export command is missing")
        continue
    for option in re.findall(r"--[a-z0-9-]+", command.group(0)):
        if option not in exporter_options:
            violations.append(
                f"{relative}: unsupported debug export option {option}"
            )

if violations:
    raise AssertionError("\n".join(sorted(set(violations))))
PY
then
    log_pass "Current Manager docs contain no retired runtime instructions"
else
    log_fail "Current Manager docs contain no retired runtime instructions"
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

if [ "${STATIC_ONLY}" = "true" ]; then
    test_teardown "28-agentscope-manager-parity (static only)"
    test_summary
    exit $?
fi

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
assert_eq "19" "${IMAGE_SKILLS}" "Running image contains exactly 19 Manager skills"

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
