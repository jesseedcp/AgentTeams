from __future__ import annotations

import re
from pathlib import Path

import pytest

from agentteams_manager.tools.tasks import TASK_TOOL_NAMES

ROOT = Path("manager/agent/skills")
FAMILIES = {
    "file-sync-management": {"sync_files"},
    "git-delegation-management": {
        "inspect_git_request",
        "git_delegate",
        "git_delegate_high_risk",
    },
    "project-management": {
        "create_project",
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
