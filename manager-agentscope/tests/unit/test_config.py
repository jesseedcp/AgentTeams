from pathlib import Path

import pytest

from agentteams_manager.config import ManagerConfig, RuntimeDocument


def test_manager_config_reads_secret_values_without_exposing_them(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    values = {
        "AGENTTEAMS_MANAGER_NAME": "default",
        "AGENTTEAMS_MANAGER_MATRIX_USER_ID": "@manager:matrix.local",
        "AGENTTEAMS_MANAGER_MATRIX_TOKEN": "matrix-secret",
        "AGENTTEAMS_MANAGER_GATEWAY_KEY": "gateway-secret",
        "AGENTTEAMS_MANAGER_RUNTIME_DOCUMENT_KEY":
            "manager/agentscope-manager.json",
        "AGENTTEAMS_MANAGER_WORKSPACE": str(tmp_path),
        "AGENTTEAMS_MATRIX_URL": "http://matrix:6167",
        "AGENTTEAMS_MATRIX_DOMAIN": "matrix.local",
        "AGENTTEAMS_CONTROLLER_URL": "http://controller:8080",
        "AGENTTEAMS_AI_GATEWAY_URL": "http://higress:8080",
        "AGENTTEAMS_FS_ENDPOINT": "http://minio:9000",
        "AGENTTEAMS_FS_BUCKET": "agentteams",
        "AGENTTEAMS_FS_ACCESS_KEY": "default",
        "AGENTTEAMS_FS_SECRET_KEY": "minio-secret",
        "AGENTTEAMS_DEFAULT_MODEL": "qwen3.6-plus",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)

    config = ManagerConfig.from_env()

    assert config.session_database == tmp_path / "state" / "manager.db"
    assert config.matrix_access_token.get_secret_value() == "matrix-secret"
    assert "matrix-secret" not in repr(config)
    assert "gateway-secret" not in repr(config)
    assert "minio-secret" not in repr(config)


def test_runtime_document_rejects_unsupported_schema(tmp_path: Path) -> None:
    path = tmp_path / "agentscope-manager.json"
    path.write_text(
        '{"schema_version": 2, "revision": 1, "model": "x"}',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="schema_version 1"):
        RuntimeDocument.load(path)
