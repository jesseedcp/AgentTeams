"""Live Kind acceptance with a deterministic manifest fallback for CI."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
import uuid
from pathlib import Path
from urllib.request import ProxyHandler, build_opener

ROOT = Path(".")
NAMESPACE = os.environ.get(
    "AGENTTEAMS_E2E_NAMESPACE",
    "agentteams-k8s-b35deb9",
)
GATEWAY_URL = os.environ.get(
    "AGENTTEAMS_E2E_GATEWAY_URL",
    "http://127.0.0.1:18388",
).rstrip("/")


def test_k8s_manager_has_cinny_route_service_and_persistent_state() -> None:
    if not _live_acceptance_enabled():
        _assert_static_k8s_contract()
        return
    pod = _json("get", "pod", "agentteams-manager")
    service = _json("get", "service", "agentteams-manager")
    claim = _json("get", "pvc", "agentteams-manager-data")
    ingress = _json("get", "ingress", "manager-admin")

    assert pod["status"]["phase"] == "Running"
    assert _condition(pod, "Ready") == "True"
    mounts = pod["spec"]["containers"][0]["volumeMounts"]
    assert any(
        item["mountPath"] == "/var/lib/agentteams-manager"
        for item in mounts
    )
    assert service["spec"]["ports"][0]["port"] == 18799
    assert claim["status"]["phase"] == "Bound"
    paths = ingress["spec"]["rules"][0]["http"]["paths"]
    assert any(item["path"] == "/manager-admin" for item in paths)

    status, body = _gateway_get("/manager-admin/")
    assert status == 200
    assert b"AgentTeams Manager" in body
    status, _ = _gateway_get("/")
    assert status == 200
    status, body = _gateway_get("/config.json")
    assert status == 200
    cinny_config = json.loads(body)
    assert cinny_config["homeserverList"] == [GATEWAY_URL]
    status, body = _gateway_get("/.well-known/matrix/client")
    assert status == 200
    matrix_client = json.loads(body)
    assert matrix_client["m.homeserver"]["base_url"] == GATEWAY_URL


def test_manager_sqlite_survives_live_pod_restart_when_enabled() -> None:
    if not _live_acceptance_enabled() or os.environ.get(
        "AGENTTEAMS_E2E_RESTART",
    ) != "1":
        _assert_static_k8s_contract()
        return
    before = _manager_sqlite_identity()
    sentinel = uuid.uuid4().hex
    _write_manager_sqlite_sentinel(sentinel)
    try:
        _run("delete", "pod", "agentteams-manager")
        _wait_for_manager_pod()
        _run(
            "wait",
            "--for=condition=Ready",
            "pod/agentteams-manager",
            "--timeout=180s",
        )
        deadline = time.monotonic() + 60
        while True:
            try:
                after = _manager_sqlite_identity()
                persisted = _read_manager_sqlite_sentinel()
                break
            except subprocess.CalledProcessError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(2)
        assert after == before
        assert persisted == sentinel
        recreated = _json("get", "pod", "agentteams-manager")
        recreated_uid = recreated["metadata"]["uid"]
        time.sleep(25)
        stable = _json("get", "pod", "agentteams-manager")
        assert stable["metadata"]["uid"] == recreated_uid
        assert _condition(stable, "Ready") == "True"
    finally:
        _delete_manager_sqlite_sentinel()


def _wait_for_manager_pod(*, timeout: float = 60) -> None:
    """Wait for the controller to recreate the stable Manager pod name."""
    deadline = time.monotonic() + timeout
    while True:
        result = subprocess.run(
            [
                "kubectl",
                "-n",
                NAMESPACE,
                "get",
                "pod",
                "agentteams-manager",
            ],
            capture_output=True,
            check=False,
            encoding="utf-8",
        )
        if result.returncode == 0:
            return
        if time.monotonic() >= deadline:
            raise TimeoutError("Manager pod was not recreated")
        time.sleep(2)


def _gateway_get(
    path: str,
    *,
    timeout: float = 60,
) -> tuple[int, bytes]:
    """Wait for one local Higress route without inheriting host proxies."""
    opener = build_opener(ProxyHandler({}))
    deadline = time.monotonic() + timeout
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        try:
            with opener.open(
                f"{GATEWAY_URL}{path}",
                timeout=10,
            ) as page:
                return page.status, page.read()
        except OSError as error:
            last_error = error
            time.sleep(2)
    raise TimeoutError(
        f"timed out waiting for gateway route {path}: {last_error}",
    )


def _manager_sqlite_identity() -> str:
    script = (
        "import sqlite3;"
        "p='/var/lib/agentteams-manager/state/manager.db';"
        "c=sqlite3.connect(p);"
        "v=c.execute('pragma user_version').fetchone()[0];"
        "n=c.execute('select count(*) from sqlite_master').fetchone()[0];"
        "print(str(v)+':'+str(n))"
    )
    return _run(
        "exec",
        "agentteams-manager",
        "--",
        "python",
        "-c",
        script,
    ).strip().splitlines()[-1]


def _write_manager_sqlite_sentinel(value: str) -> None:
    script = (
        "import sqlite3;"
        "p='/var/lib/agentteams-manager/state/manager.db';"
        "c=sqlite3.connect(p);"
        "c.execute("
        "\"insert or replace into key_values(key,value,updated_at) "
        "values('e2e:pvc-sentinel',?,'e2e')\","
        f"({value!r},));"
        "c.commit()"
    )
    _exec_manager_python(script)


def _read_manager_sqlite_sentinel() -> str:
    script = (
        "import sqlite3;"
        "p='/var/lib/agentteams-manager/state/manager.db';"
        "c=sqlite3.connect(p);"
        "r=c.execute("
        "\"select value from key_values where key='e2e:pvc-sentinel'\""
        ").fetchone();"
        "print(r[0] if r else '')"
    )
    return _exec_manager_python(script).strip().splitlines()[-1]


def _delete_manager_sqlite_sentinel() -> None:
    script = (
        "import sqlite3;"
        "p='/var/lib/agentteams-manager/state/manager.db';"
        "c=sqlite3.connect(p);"
        "c.execute("
        "\"delete from key_values where key='e2e:pvc-sentinel'\""
        ");c.commit()"
    )
    _exec_manager_python(script)


def _exec_manager_python(script: str) -> str:
    return _run(
        "exec",
        "agentteams-manager",
        "--",
        "python",
        "-c",
        script,
    )


def _assert_static_k8s_contract() -> None:
    values = (ROOT / "helm" / "agentteams" / "values.yaml").read_text(
        encoding="utf-8",
    )
    deployment = (
        ROOT
        / "helm"
        / "agentteams"
        / "templates"
        / "controller"
        / "deployment.yaml"
    ).read_text(encoding="utf-8")
    pvc = (
        ROOT / "helm" / "agentteams" / "templates" / "manager-pvc.yaml"
    ).read_text(encoding="utf-8")
    kind_config = (ROOT / "hack" / "kind-config.yaml").read_text(
        encoding="utf-8",
    )
    assert "persistence:\n    enabled: true" in values
    assert "AGENTTEAMS_MANAGER_DATA_CLAIM" in deployment
    assert "kind: PersistentVolumeClaim" in pvc
    assert "hostPort: 18388" in kind_config
    assert "codingCLI:" in values
    assert "enabled: false" in values
    assert (
        ROOT
        / "manager-agentscope"
        / "src"
        / "agentteams_manager"
        / "admin"
        / "commands.py"
    ).is_file()
    assert (
        ROOT
        / "docs"
        / "parity"
        / "upstream-agentteams-8de237d.md"
    ).is_file()


def _live_namespace() -> bool:
    if shutil.which("kubectl") is None:
        return False
    result = subprocess.run(
        ["kubectl", "get", "namespace", NAMESPACE],
        capture_output=True,
        check=False,
        encoding="utf-8",
    )
    return result.returncode == 0


def _live_acceptance_enabled() -> bool:
    return (
        os.environ.get("AGENTTEAMS_E2E_K8S") == "1"
        and _live_namespace()
    )


def _run(*arguments: str) -> str:
    return subprocess.run(
        ["kubectl", "-n", NAMESPACE, *arguments],
        capture_output=True,
        check=True,
        encoding="utf-8",
    ).stdout


def _json(*arguments: str) -> dict[str, object]:
    return json.loads(_run(*arguments, "-o", "json"))


def _condition(pod: dict[str, object], kind: str) -> str:
    return next(
        item["status"]
        for item in pod["status"]["conditions"]
        if item["type"] == kind
    )
