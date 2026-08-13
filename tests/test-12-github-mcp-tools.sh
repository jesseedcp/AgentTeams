#!/bin/bash
# test-12-github-mcp-tools.sh - Native AgentScope GitHub MCP parity
#
# Verifies that installation-time GitHub bootstrap produces one secret-free
# Manager descriptor, that all retained GitHub operations are discovered by
# AgentScope MCPClient, and that representative read-only calls work without
# the deleted mcporter sidecar or a model round trip.
# 初学者提示：这是确定性的 MCP 契约测试，直接检查描述符和工具调用，不依赖模型“猜测”应该调用哪个工具。
# 通过标准包括 Secret 不出现在描述符、保留工具集合完整、只读调用成功，以及旧 sidecar 已不再是运行前置条件。
# 它与 test-08 的区别是：这里验证 Manager 的原生 AgentScope MCP 接线，test-08 验证 Worker 的真实 GitHub 流程。

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/test-helpers.sh"

test_setup "12-github-mcp-tools"

if [ -z "${AGENTTEAMS_GITHUB_TOKEN}" ] && [ -z "${TEST_GITHUB_TOKEN}" ]; then
    log_info "SKIP: No GitHub token configured (set AGENTTEAMS_GITHUB_TOKEN or TEST_GITHUB_TOKEN)"
    test_teardown "12-github-mcp-tools"
    test_summary
    exit 0
fi

if ! wait_for_manager 300; then
    test_teardown "12-github-mcp-tools"
    test_summary
    exit 1
fi

native_mcp() {
    local action="$1"
    local tool_name="${2:-}"
    local arguments="${3:-{}}"
    local output status result

    output=$(docker exec -i \
        -e "AGENTTEAMS_TEST_MCP_ACTION=${action}" \
        -e "AGENTTEAMS_TEST_MCP_TOOL=${tool_name}" \
        -e "AGENTTEAMS_TEST_MCP_ARGS_JSON=${arguments}" \
        "${TEST_AGENT_CONTAINER}" python - 2>&1 <<'PY'
import asyncio
import json
import os
from enum import Enum

from pydantic import BaseModel

from agentteams_manager.config import ManagerConfig, RuntimeDocument
from agentteams_manager.runtime.mcp import MCPRegistry


def plain(value):
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(key): plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [plain(item) for item in value]
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return plain(model_dump(mode="json"))
    return str(value)


def logical_tool_name(name):
    prefix = "mcp__github__"
    if name.startswith(prefix):
        return name[len(prefix):]
    if name.startswith("github."):
        return name[len("github."):]
    return name


async def main():
    config = ManagerConfig.from_env()
    runtime = RuntimeDocument.load(config.runtime_document_path)
    if not any(server.name == "github" for server in runtime.mcp_servers):
        raise RuntimeError("Manager runtime has no GitHub MCP descriptor")

    registry = MCPRegistry(gateway_key=config.gateway_key)
    try:
        await registry.prepare(runtime)
        tools = await registry.list_server_tools(
            "github",
            revision=runtime.revision,
        )
        action = os.environ["AGENTTEAMS_TEST_MCP_ACTION"]
        if action == "list":
            payload = sorted(logical_tool_name(tool.name) for tool in tools)
        elif action == "call":
            requested = os.environ["AGENTTEAMS_TEST_MCP_TOOL"]
            selected = next(
                (
                    tool.name
                    for tool in tools
                    if logical_tool_name(tool.name) == requested
                ),
                None,
            )
            if selected is None:
                raise RuntimeError(f"GitHub MCP tool is missing: {requested}")
            arguments = json.loads(
                os.environ["AGENTTEAMS_TEST_MCP_ARGS_JSON"],
            )
            payload = plain(
                await registry.call_server_tool(
                    "github",
                    selected,
                    arguments,
                    revision=runtime.revision,
                ),
            )
        else:
            raise RuntimeError(f"unsupported MCP test action: {action}")
        print(
            "AGENTTEAMS_MCP_RESULT="
            + json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ),
        )
    finally:
        await registry.close()


asyncio.run(main())
PY
    )
    status=$?
    if [ "${status}" -ne 0 ]; then
        log_info "Native AgentScope MCP command failed: ${output}"
        return "${status}"
    fi
    result=$(printf '%s\n' "${output}" | sed -n 's/^AGENTTEAMS_MCP_RESULT=//p' | tail -1)
    if [ -z "${result}" ]; then
        log_info "Native AgentScope MCP command returned no result marker: ${output}"
        return 1
    fi
    printf '%s\n' "${result}"
}

log_section "Verify Native Runtime"

if docker exec "${TEST_AGENT_CONTAINER}" sh -c 'command -v mcporter >/dev/null 2>&1'; then
    log_fail "Legacy mcporter executable is still present in Manager image"
else
    log_pass "Manager image has no mcporter sidecar executable"
fi

TOOLS=$(native_mcp list)
if [ -n "${TOOLS}" ]; then
    log_pass "AgentScope discovered GitHub MCP tools"
else
    log_fail "AgentScope did not discover GitHub MCP tools"
fi

REQUIRED_TOOLS="
get_me
list_branches
delete_file
update_pull_request
list_tags
list_releases
get_latest_release
get_commit
list_issue_comments
get_label
list_labels
list_teams
list_team_members
list_notifications
request_reviewers
list_commits
get_repo
list_pull_requests
get_pull_request_comments
get_pull_request_reviews
search_code
search_repositories
"

while IFS= read -r tool_name; do
    [ -n "${tool_name}" ] || continue
    if printf '%s' "${TOOLS}" | jq -e --arg name "${tool_name}" \
        'index($name) != null' >/dev/null 2>&1; then
        log_pass "GitHub MCP exposes ${tool_name}"
    else
        log_fail "GitHub MCP is missing ${tool_name}"
    fi
done <<EOF
${REQUIRED_TOOLS}
EOF

log_section "Call Representative Read-only Tools"

RESULT=$(native_mcp call get_me '{}')
assert_contains_i "${RESULT}" "login" "get_me returns authenticated GitHub identity"

RESULT=$(native_mcp call get_repo \
    '{"owner":"agentscope-ai","repo":"AgentTeams"}')
assert_contains_i "${RESULT}" "AgentTeams" "get_repo returns upstream repository"

RESULT=$(native_mcp call list_branches \
    '{"owner":"agentscope-ai","repo":"AgentTeams","per_page":5,"page":1}')
assert_contains_i "${RESULT}" "main" "list_branches returns main"

RESULT=$(native_mcp call get_commit \
    '{"owner":"agentscope-ai","repo":"AgentTeams","ref":"main"}')
assert_contains_i "${RESULT}" "sha" "get_commit returns commit metadata"

test_teardown "12-github-mcp-tools"
test_summary
