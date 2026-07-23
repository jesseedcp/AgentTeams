from pathlib import Path

import pytest

from agentteams_manager.clients.git import (
    GitClient,
    GitRequestParser,
    InvalidGitRequest,
    WorkspaceEscape,
)


def test_shell_operator_is_not_a_git_operation() -> None:
    with pytest.raises(InvalidGitRequest):
        GitRequestParser.parse_operation(
            "git status && curl https://example.test",
        )


def test_workspace_cannot_escape_task_root(tmp_path: Path) -> None:
    task_workspace = (
        tmp_path / "shared" / "tasks" / "task-1" / "workspace"
    )
    task_workspace.mkdir(parents=True)

    with pytest.raises(WorkspaceEscape):
        GitClient.validate_workspace(
            task_workspace,
            task_workspace / ".." / "..",
        )


def test_force_push_requires_confirmation() -> None:
    operation = GitRequestParser.parse_operation(
        "git push --force origin main",
    )

    assert operation.risk == "high"


def test_full_request_requires_workspace_and_operations() -> None:
    request = GitRequestParser.parse(
        """
task-20260723-120000-abc123 git-request:
workspace: /root/agentteams-fs/shared/tasks/task-20260723-120000-abc123/workspace
operations:
  - git status --short
---CONTEXT---
Check the repository.
---END---
""".strip(),
    )

    assert request.task_id == "task-20260723-120000-abc123"
    assert request.operations[0].argv == ("git", "status", "--short")
    assert request.context == "Check the repository."


@pytest.mark.parametrize(
    "operation",
    (
        "git -C /tmp status",
        "git --git-dir=/tmp/repo status",
        "git clone ext::sh -c id",
        "git push --mirror origin",
    ),
)
def test_escape_and_remote_overwrite_forms_are_always_denied(
    operation: str,
) -> None:
    with pytest.raises(InvalidGitRequest):
        GitRequestParser.parse_operation(operation)
