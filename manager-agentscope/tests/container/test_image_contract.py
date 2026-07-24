from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DOCKERFILE = ROOT / "manager" / "Dockerfile"
ENTRYPOINT = ROOT / "manager" / "entrypoint.sh"


def test_manager_image_contains_agentscope_and_no_legacy_gateway() -> None:
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")

    assert "manager-agentscope" in dockerfile
    assert "agentscope" in dockerfile.casefold()
    assert "agentscope[s3]==2.0.4.post1" in (
        ROOT / "manager-agentscope" / "pyproject.toml"
    ).read_text(encoding="utf-8")
    assert "manager/agent/skills" in dockerfile
    assert "manager/configs/known-models.json" in dockerfile
    for forbidden in (
        "openclaw gateway",
        "copaw app",
        "Dockerfile.copaw",
        "redis-server",
        "supervisord",
    ):
        assert forbidden not in dockerfile


def test_manager_image_has_only_operational_http() -> None:
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")

    assert "EXPOSE 18799" in dockerfile
    assert "http://127.0.0.1:18799/readyz" in dockerfile
    assert (
        'ENTRYPOINT ["/usr/bin/tini", "--", '
        '"/opt/agentteams/entrypoint.sh"]'
    ) in dockerfile


def test_entrypoint_execs_one_python_manager() -> None:
    script = ENTRYPOINT.read_text(encoding="utf-8")

    assert "exec agentteams-manager" in script
    assert "AGENTTEAMS_MANAGER_RUNTIME_DOCUMENT_KEY" in script
    assert "AGENTTEAMS_MANAGER_MATRIX_TOKEN" in script
    assert "AGENTTEAMS_FS_SECRET_KEY" in script
    assert 'mkdir -p "${workspace}/state"' in script
    assert 'mkdir -p "${workspace}/media"' in script
    assert 'mkdir -p "${workspace}/matrix-e2ee"' in script
    for forbidden in (
        "openclaw",
        "copaw",
        "redis",
        "supervisord",
        "password login",
    ):
        assert forbidden not in script.casefold()
