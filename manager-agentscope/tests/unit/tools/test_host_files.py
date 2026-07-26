from pathlib import Path

import pytest

from agentteams_manager.tools.host_files import HostFileAccess


def test_host_file_access_is_disabled_without_mount(tmp_path: Path) -> None:
    access = HostFileAccess(
        root=None,
        read_allowlist=("docs/**",),
        write_allowlist=(),
    )
    with pytest.raises(PermissionError, match="disabled"):
        access.read_text("docs/readme.md")


def test_host_file_access_enforces_separate_allowlists(
    tmp_path: Path,
) -> None:
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "readme.md").write_text(
        "hello",
        encoding="utf-8",
    )
    access = HostFileAccess(
        root=tmp_path,
        read_allowlist=("docs/**",),
        write_allowlist=("output/**",),
    )

    assert access.read_text("docs/readme.md")["content"] == "hello"
    with pytest.raises(PermissionError):
        access.read_text("../secret")
    with pytest.raises(PermissionError):
        access.write_text("docs/readme.md", "changed")
    assert access.write_text("output/result.txt", "done")["bytes"] == 4
    assert (tmp_path / "output" / "result.txt").read_text() == "done"
