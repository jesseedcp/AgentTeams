from __future__ import annotations

import re
from pathlib import Path

import pytest

from agentteams_manager.tools.resources import RESOURCE_TOOL_NAMES

ROOT = Path("manager/agent/skills")
FAMILIES = {
    "agentteams-find-worker": {"find_worker", "import_worker"},
    "channel-management": {
        "list_channels",
        "create_channel",
        "update_channel",
        "delete_channel",
        "send_notification",
    },
    "human-management": {
        "list_humans",
        "get_human",
        "create_human",
        "update_human",
        "delete_human",
    },
    "matrix-server-management": {
        "list_matrix_rooms",
        "list_matrix_members",
        "lookup_matrix_user",
        "get_matrix_room_state",
        "upload_matrix_media",
        "download_matrix_media",
        "invite_matrix_user",
        "kick_matrix_user",
        "ban_matrix_user",
        "unban_matrix_user",
    },
    "team-management": {
        "list_teams",
        "get_team",
        "create_team",
        "update_team",
        "delete_team",
    },
    "worker-management": {
        "list_workers",
        "get_worker",
        "create_worker",
        "update_worker",
        "sleep_worker",
        "wake_worker",
        "delete_worker",
    },
}
FORBIDDEN = re.compile(
    r"scripts/|/opt/agentteams|"
    r"\b(?:agt|agentteams)\s+(?:create|update|delete|get|apply|worker)|"
    r"\bcurl\b|"
    r"(?:state|workers-registry|pending-workers|humans-registry)\.json|"
    r"manual Worker JSON",
    re.IGNORECASE,
)


@pytest.mark.parametrize(
    ("family", "required_tools"),
    tuple(FAMILIES.items()),
)
def test_resource_skills_map_to_registered_typed_tools(
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
        assert f"`{tool_name}`" in combined, (
            family,
            tool_name,
        )
        assert tool_name in RESOURCE_TOOL_NAMES
