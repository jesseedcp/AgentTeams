"""Behavioral acceptance for the writable Manager admin surface."""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

from conftest import K8sHarness

ROOT = Path(__file__).resolve().parents[3]


def test_admin_crud_confirmations_and_worker_console(
    k8s_harness: K8sHarness,
) -> None:
    if not k8s_harness.enabled:
        _assert_static_admin_contract()
        return

    suffix = uuid.uuid4().hex[:8]
    leader = f"e2e-lead-{suffix}"
    member = f"e2e-work-{suffix}"
    team = f"e2e-team-{suffix}"
    project_id: str | None = None
    default_model = str(
        k8s_harness.kubectl_json("get", "manager", "default")["spec"][
            "model"
        ],
    )

    unauthorized = k8s_harness.admin_request(
        "GET",
        "workers",
        token="definitely-invalid",
    )
    assert unauthorized.status == 401
    assert unauthorized.payload["error"]["code"] == "unauthorized"

    try:
        for name in (leader, member):
            created = k8s_harness.admin_request(
                "POST",
                "workers",
                {
                    "name": name,
                    "runtime": "qwenpaw",
                    "model": default_model,
                },
            )
            assert created.status == 201, created.payload
            assert created.payload["item"]["name"] == name

        patched = k8s_harness.admin_request(
            "PATCH",
            f"workers/{leader}",
            {"identity": "Kubernetes parity test leader"},
        )
        assert patched.status == 200, patched.payload
        assert patched.payload["item"]["name"] == leader

        _wait_for_ready_worker_pod(k8s_harness, leader)
        first_uid = _worker_pod(k8s_harness, leader)["metadata"]["uid"]

        enabled = k8s_harness.admin_request(
            "PATCH",
            f"workers/{leader}",
            {"console_enabled": True, "console_port": 8088},
        )
        assert enabled.status == 200, enabled.payload
        assert enabled.payload["item"]["spec"]["console"] == {
            "enabled": True,
            "port": 8088,
        }
        enabled_pod = _wait_for_console_state(
            k8s_harness,
            leader,
            enabled=True,
            previous_uid=str(first_uid),
        )

        disabled = k8s_harness.admin_request(
            "PATCH",
            f"workers/{leader}",
            {"console_enabled": False},
        )
        assert disabled.status == 200, disabled.payload
        assert disabled.payload["item"]["spec"]["console"]["enabled"] is False
        _wait_for_console_state(
            k8s_harness,
            leader,
            enabled=False,
            previous_uid=str(enabled_pod["metadata"]["uid"]),
        )

        created_team = k8s_harness.admin_request(
            "POST",
            "teams",
            {
                "name": team,
                "leader_name": leader,
                "worker_names": [member],
                "description": "Kubernetes parity acceptance",
            },
        )
        assert created_team.status == 201, created_team.payload
        assert created_team.payload["item"]["leader"] == leader
        assert created_team.payload["item"]["workers"] == [member]

        patched_team = k8s_harness.admin_request(
            "PATCH",
            f"teams/{team}",
            {"description": "Kubernetes parity acceptance updated"},
        )
        assert patched_team.status == 200, patched_team.payload

        refused_team_delete = k8s_harness.admin_request(
            "DELETE",
            f"teams/{team}",
            {},
        )
        _assert_confirmation_required(refused_team_delete)
        deleted_team = k8s_harness.admin_request(
            "DELETE",
            f"teams/{team}",
            {"confirmed": True},
        )
        assert deleted_team.status == 200, deleted_team.payload
        assert set(deleted_team.payload["preserved_workers"]) == {
            leader,
            member,
        }
        assert (
            k8s_harness.kubectl_json("get", "worker", leader)["metadata"][
                "name"
            ]
            == leader
        )

        created_project = k8s_harness.admin_request(
            "POST",
            "projects",
            {
                "title": f"Kubernetes parity {suffix}",
                "description": "Exercise project lifecycle through admin API",
                "plan": "1. Verify the Manager admin workflow.",
                "participants": [leader, member],
            },
        )
        assert created_project.status == 201, created_project.payload
        project_id = str(created_project.payload["item"]["project_id"])

        revised_project = k8s_harness.admin_request(
            "PATCH",
            f"projects/{project_id}",
            {
                "plan": (
                    "1. Verify the Manager admin workflow.\n"
                    "2. Record the behavioral evidence."
                ),
                "change_kind": "minor",
                "reason": "acceptance coverage",
            },
        )
        assert revised_project.status == 200, revised_project.payload

        refused_project_close = k8s_harness.admin_request(
            "DELETE",
            f"projects/{project_id}",
            {"force": True},
        )
        _assert_confirmation_required(refused_project_close)
        closed_project = k8s_harness.admin_request(
            "DELETE",
            f"projects/{project_id}",
            {"confirmed": True, "force": True},
        )
        assert closed_project.status == 200, closed_project.payload
        project_id = None

        for name in (member, leader):
            refused_worker_delete = k8s_harness.admin_request(
                "DELETE",
                f"workers/{name}",
                {},
            )
            _assert_confirmation_required(refused_worker_delete)
            deleted_worker = k8s_harness.admin_request(
                "DELETE",
                f"workers/{name}",
                {"confirmed": True},
            )
            assert deleted_worker.status == 200, deleted_worker.payload
    finally:
        if project_id is not None:
            k8s_harness.admin_request(
                "DELETE",
                f"projects/{project_id}",
                {"confirmed": True, "force": True},
            )
        k8s_harness.kubectl(
            "delete",
            "team",
            team,
            "--ignore-not-found=true",
            check=False,
        )
        for name in (member, leader):
            k8s_harness.kubectl(
                "delete",
                "worker",
                name,
                "--ignore-not-found=true",
                check=False,
            )


def _assert_confirmation_required(result: Any) -> None:
    assert result.status == 409, result.payload
    assert result.payload["error"]["code"] == "confirmation_required"


def _worker_pod(
    harness: K8sHarness,
    worker_name: str,
) -> dict[str, Any]:
    pod = harness.try_kubectl_json(
        "get",
        "pod",
        f"agentteams-worker-{worker_name}",
    )
    return pod or {}


def _wait_for_ready_worker_pod(
    harness: K8sHarness,
    worker_name: str,
) -> dict[str, Any]:
    def ready() -> dict[str, Any] | None:
        pod = _worker_pod(harness, worker_name)
        conditions = pod.get("status", {}).get("conditions", ())
        return (
            pod
            if any(
                item.get("type") == "Ready"
                and item.get("status") == "True"
                for item in conditions
            )
            else None
        )

    return harness.wait(
        ready,
        timeout=300,
        description=f"worker/{worker_name} pod readiness",
    )


def _wait_for_console_state(
    harness: K8sHarness,
    worker_name: str,
    *,
    enabled: bool,
    previous_uid: str,
) -> dict[str, Any]:
    def converged() -> dict[str, Any] | None:
        pod = _worker_pod(harness, worker_name)
        if not pod or pod["metadata"]["uid"] == previous_uid:
            return None
        conditions = pod.get("status", {}).get("conditions", ())
        if not any(
            item.get("type") == "Ready" and item.get("status") == "True"
            for item in conditions
        ):
            return None
        env = {
            item["name"]: item.get("value", "")
            for item in pod["spec"]["containers"][0].get("env", ())
        }
        actual = env.get("AGENTTEAMS_CONSOLE_PORT")
        if enabled and actual == "8088":
            return pod
        if not enabled and actual is None:
            return pod
        return None

    return harness.wait(
        converged,
        timeout=300,
        description=(
            f"worker/{worker_name} console "
            f"{'enable' if enabled else 'disable'} rollout"
        ),
    )


def _assert_static_admin_contract() -> None:
    commands = (
        ROOT
        / "manager-agentscope"
        / "src"
        / "agentteams_manager"
        / "admin"
        / "commands.py"
    ).read_text(encoding="utf-8")
    worker_env = (
        ROOT
        / "agentteams-controller"
        / "internal"
        / "service"
        / "worker_env.go"
    ).read_text(encoding="utf-8")
    assert 'AdminResource = Literal["workers", "teams", "projects"]' in commands
    assert '"confirmation_required"' in commands
    assert "AGENTTEAMS_CONSOLE_PORT" in worker_env
