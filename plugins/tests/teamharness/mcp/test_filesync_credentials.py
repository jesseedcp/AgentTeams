from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SERVER_PATH = ROOT / "plugins" / "teamharness" / "mcp" / "server.py"


def _load_server():
    sys.path.insert(0, str(SERVER_PATH.parent))
    spec = importlib.util.spec_from_file_location(
        "_teamharness_filesync_credentials",
        SERVER_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class _Response:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self) -> bytes:
        return json.dumps(
            {
                "access_key_id": "STS.TEST",
                "access_key_secret": "secret+/=",
                "security_token": "token+/=",
                "oss_endpoint": "oss.example.test",
            },
        ).encode("utf-8")


def test_filesync_uses_controller_sts_without_persisting_static_keys(
    monkeypatch,
    tmp_path: Path,
) -> None:
    module = _load_server()
    token_file = tmp_path / "token"
    token_file.write_text("worker-bearer", encoding="utf-8")
    monkeypatch.setenv(
        "AGENTTEAMS_CONTROLLER_URL",
        "http://controller.example.test",
    )
    monkeypatch.setenv("AGENTTEAMS_AUTH_TOKEN_FILE", str(token_file))
    for name in (
        "AGENTTEAMS_FS_ACCESS_KEY",
        "AGENTTEAMS_FS_SECRET_KEY",
        "MC_HOST_agentteams",
    ):
        monkeypatch.delenv(name, raising=False)
    observed = {}

    def urlopen(request, timeout):
        observed["url"] = request.full_url
        observed["authorization"] = request.headers["Authorization"]
        observed["timeout"] = timeout
        return _Response()

    monkeypatch.setattr(module.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(
        module,
        "_mc_alias_configured",
        lambda _env: False,
    )

    env, error = module._filesync_mc_env(
        "agentteams/agentteams-storage/shared/tasks/demo/",
    )

    assert error is None
    assert observed == {
        "url": (
            "http://controller.example.test/api/v1/credentials/sts"
        ),
        "authorization": "Bearer worker-bearer",
        "timeout": 30,
    }
    assert env["MC_HOST_agentteams"] == (
        "https://STS.TEST:secret+/=:token+/=@oss.example.test"
    )
