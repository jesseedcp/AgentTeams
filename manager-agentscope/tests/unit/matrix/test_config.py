from pathlib import Path

from pydantic import SecretStr

from agentteams_manager.config import ManagerConfig
from agentteams_manager.matrix.client import MatrixClientConfig
from tests.fixtures.matrix_events import matrix_text_event


def _manager_config(tmp_path: Path) -> ManagerConfig:
    return ManagerConfig(
        manager_name="default",
        manager_user_id="@manager:matrix.local",
        matrix_url="http://matrix:6167",
        matrix_domain="matrix.local",
        matrix_access_token=SecretStr("matrix-secret"),
        controller_url="http://controller:8080",
        controller_auth_token=None,
        ai_gateway_url="http://higress:8080",
        gateway_key=SecretStr("gateway-secret"),
        fs_endpoint="http://minio:9000",
        fs_bucket="agentteams",
        fs_access_key="default",
        fs_secret_key=SecretStr("minio-secret"),
        storage_prefix="agentteams",
        default_model="qwen3.6-plus",
        workspace=tmp_path,
        runtime_document_path=tmp_path / "agentscope-manager.json",
        runtime_document_key="manager/agentscope-manager.json",
        session_database=tmp_path / "state" / "manager.db",
    )


def test_matrix_config_uses_manager_identity(tmp_path: Path) -> None:
    config = MatrixClientConfig.from_manager_config(
        _manager_config(tmp_path),
    )

    assert config.user_id == "@manager:matrix.local"
    assert config.sync_timeout_ms == 30_000
    assert config.history_limit == 50
    assert config.crypto_store.name == "matrix-e2ee"


def test_event_fixture_keeps_real_sender() -> None:
    event = matrix_text_event(
        room_id="!room:local",
        event_id="$one",
        sender="@alice:local",
        body="finished",
    )

    assert event.sender_id == "@alice:local"
    assert event.room_id == "!room:local"
