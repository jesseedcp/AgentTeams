"""Live Kind acceptance with a deterministic manifest fallback for CI."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
import uuid
from pathlib import Path
from urllib.request import urlopen

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
    if not _live_namespace():
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

    with urlopen(f"{GATEWAY_URL}/manager-admin/", timeout=10) as page:
        assert page.status == 200
        assert b"AgentTeams Manager" in page.read()
    with urlopen(f"{GATEWAY_URL}/", timeout=10) as page:
        assert page.status == 200
    with urlopen(f"{GATEWAY_URL}/config.json", timeout=10) as page:
        cinny_config = json.load(page)
    assert cinny_config["homeserverList"] == [GATEWAY_URL]
    with urlopen(
        f"{GATEWAY_URL}/.well-known/matrix/client",
        timeout=10,
    ) as page:
        matrix_client = json.load(page)
    assert matrix_client["m.homeserver"]["base_url"] == GATEWAY_URL


def test_manager_sqlite_survives_live_pod_restart_when_enabled() -> None:
    if not _live_namespace() or os.environ.get(
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
            text=True,
        )
        if result.returncode == 0:
            return
        if time.monotonic() >= deadline:
            raise TimeoutError("Manager pod was not recreated")
        time.sleep(2)


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


def _live_namespace() -> bool:
    if shutil.which("kubectl") is None:
        return False
    result = subprocess.run(
        ["kubectl", "get", "namespace", NAMESPACE],
        capture_output=True,
        check=False,
        text=True,
    )
    return result.returncode == 0


def _run(*arguments: str) -> str:
    return subprocess.run(
        ["kubectl", "-n", NAMESPACE, *arguments],
        capture_output=True,
        check=True,
        text=True,
    ).stdout


def _json(*arguments: str) -> dict[str, object]:
    return json.loads(_run(*arguments, "-o", "json"))


def _condition(pod: dict[str, object], kind: str) -> str:
    return next(
        item["status"]
        for item in pod["status"]["conditions"]
        if item["type"] == kind
    )
