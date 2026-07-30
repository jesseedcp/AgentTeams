from __future__ import annotations

import re
from pathlib import Path

import pytest

from agentteams_manager.tools.tasks import TASK_TOOL_NAMES

ROOT = Path("manager/agent/skills")
FAMILIES = {
    "file-sync-management": {"sync_files", "read_task_file"},
    "git-delegation-management": {
        "inspect_git_request",
        "git_delegate",
        "git_delegate_high_risk",
    },
    "project-management": {
        "create_project",
        "confirm_project_plan",
        "list_projects",
        "get_project",
        "update_project",
        "delete_project",
    },
    "task-coordination": {"sync_files", "inspect_git_request"},
    "task-management": {
        "list_tasks",
        "get_task",
        "create_task",
        "delegate_task",
        "delegate_team_task",
        "inspect_task_result",
        "complete_task",
        "schedule_task",
        "update_task",
        "delete_task",
    },
}
FORBIDDEN = re.compile(
    r"scripts/|/opt/agentteams|"
    r"\b(?:mc|curl|bash|copaw|agt|agentteams)\s+|"
    r"(?:state|tasks-registry|projects-registry)\.json|"
    r"manage-state\.sh|"
    r"model[- ]driven (?:recovery|reconciliation)",
    re.IGNORECASE,
)


@pytest.mark.parametrize(
    ("family", "required_tools"),
    tuple(FAMILIES.items()),
)
def test_task_skills_map_to_registered_typed_tools(
    family: str,
    required_tools: set[str],
) -> None:
    files = sorted((ROOT / family).rglob("*.md"))
    assert files, family
    combined = "\n".join(
        path.read_text(encoding="utf-8")
        for path in files
    )
    assert not FORBIDDEN.search(combined), family
    for tool_name in required_tools:
        assert f"`{tool_name}`" in combined, (family, tool_name)
        assert tool_name in TASK_TOOL_NAMES


def test_heartbeat_documents_deterministic_task_order() -> None:
    heartbeat = Path("manager/agent/HEARTBEAT.md").read_text(
        encoding="utf-8",
    )
    expected = (
        "Recover task, project, artifact, and Git operations",
        "Reclaim only provably expired processing leases",
        "Dispatch due recurring-task occurrences exactly once",
        "Reconcile finite-task completion artifacts",
        "Deliver unsent terminal notifications exactly once",
        "Create a verified SQLite snapshot",
    )

    positions = tuple(heartbeat.index(item) for item in expected)

    assert positions == tuple(sorted(positions))
    assert "without a model call" in heartbeat
    assert not FORBIDDEN.search(heartbeat)


def test_team_leader_closes_manager_parent_tasks_in_leader_room() -> None:
    manager_contract = Path(
        "manager/agent/team-leader-agent/AGENTS.md",
    ).read_text(encoding="utf-8")
    controller_contract = Path(
        "agentteams-controller/agent/team-leader-agent/AGENTS.md",
    ).read_text(encoding="utf-8")

    assert controller_contract == manager_contract
    assert "Manager parent-task completion boundary" in manager_contract
    assert "shared/tasks/{parent-task-id}/result.md" in manager_contract
    assert "TASK_COMPLETED: {parent-task-id}" in manager_contract
    assert "do not send it directly to the Admin room" in manager_contract


def test_team_leader_project_request_fast_path_is_bounded() -> None:
    leader_prompt = Path(
        "plugins/teamharness/prompts/agent/leader.md",
    ).read_text(encoding="utf-8")
    teams_prompt = Path(
        "plugins/teamharness/prompts/team/TEAMS.md",
    ).read_text(encoding="utf-8")
    project_skill = Path(
        "plugins/teamharness/skills/team/project-management/SKILL.md",
    ).read_text(encoding="utf-8")
    normalized = " ".join(leader_prompt.split())

    assert "PROJECT_REQUESTED fast path" in leader_prompt
    assert "Do not end the turn with only analysis" in leader_prompt
    assert "projectId` exactly equal to the Manager parent task id" in (
        normalized
    )
    assert "must be distinct from that parent id" in normalized
    assert "always wins over the generic single-Worker Quick Task" in (
        leader_prompt
    )
    assert "do not debate Quick Task versus Project Work" in normalized
    assert "Execute the sequence as tool calls" in leader_prompt
    assert "This rule does not apply to a Manager parent task" in teams_prompt
    assert "Never use this shortcut for a Manager parent task" in project_skill
