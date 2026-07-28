from __future__ import annotations

import httpx
import pytest
from agentteams_manager.admin.commands import (
    AdminAPIError,
    AdminCommand,
)
from agentteams_manager.health import HealthServer, ReadinessState
from agentteams_manager.observability.metrics import MetricsRegistry
from pydantic import SecretStr


@pytest.mark.asyncio
async def test_versioned_admin_resource_routes_are_authenticated_and_typed() -> None:
    calls: list[AdminCommand] = []

    async def execute(command: AdminCommand) -> dict[str, object]:
        calls.append(command)
        if command.resource == "workers" and command.name == "missing":
            raise AdminAPIError(404, "not_found", "worker does not exist")
        if command.method == "GET":
            return {"items": [], "total": 0}
        return {
            "item": {"name": command.name or command.payload.get("name")},
            "operation_id": "a" * 32,
        }

    server = HealthServer(
        readiness=ReadinessState(),
        metrics=MetricsRegistry(),
        host="127.0.0.1",
        port=0,
        admin_token=SecretStr("admin-token"),
        admin_command=execute,
    )
    await server.start()
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{server.bound_port}",
            trust_env=False,
        ) as client:
            denied = await client.get("/manager-admin/api/v1/workers")
            assert denied.status_code == 401
            assert denied.json()["error"]["code"] == "unauthorized"

            auth = {"Authorization": "Bearer admin-token"}
            listed = await client.get(
                "/manager-admin/api/v1/workers",
                headers=auth,
            )
            assert listed.status_code == 200
            assert calls[-1] == AdminCommand(
                method="GET",
                resource="workers",
            )

            created = await client.post(
                "/manager-admin/api/v1/workers",
                headers={**auth, "Idempotency-Key": "create-alice"},
                json={
                    "name": "alice",
                    "runtime": "copaw",
                    "model": "qwen3.6-plus",
                },
            )
            assert created.status_code == 201
            assert calls[-1].idempotency_key == "create-alice"

            patched = await client.patch(
                "/manager-admin/api/v1/workers/alice",
                headers={**auth, "Idempotency-Key": "patch-alice"},
                json={"model": "new-model"},
            )
            assert patched.status_code == 200
            assert calls[-1].name == "alice"

            deleted = await client.request(
                "DELETE",
                "/manager-admin/api/v1/workers/alice",
                headers={**auth, "Idempotency-Key": "delete-alice"},
                json={"confirmed": True},
            )
            assert deleted.status_code == 200
            assert calls[-1].payload == {"confirmed": True}

            missing = await client.get(
                "/manager-admin/api/v1/workers/missing",
                headers=auth,
            )
            assert missing.status_code == 404
            assert missing.json()["error"]["code"] == "not_found"
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_admin_resource_api_rejects_bad_bodies_before_dispatch() -> None:
    calls: list[AdminCommand] = []

    async def execute(command: AdminCommand) -> dict[str, object]:
        calls.append(command)
        return {}

    server = HealthServer(
        readiness=ReadinessState(),
        metrics=MetricsRegistry(),
        host="127.0.0.1",
        port=0,
        admin_token=SecretStr("admin-token"),
        admin_command=execute,
    )
    await server.start()
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{server.bound_port}",
            trust_env=False,
        ) as client:
            auth = {
                "Authorization": "Bearer admin-token",
                "Idempotency-Key": "bad-body",
            }
            wrong_type = await client.post(
                "/manager-admin/api/v1/workers",
                headers={**auth, "Content-Type": "text/plain"},
                content=b"{}",
            )
            assert wrong_type.status_code == 415
            assert wrong_type.json()["error"]["code"] == (
                "unsupported_media_type"
            )

            invalid = await client.post(
                "/manager-admin/api/v1/workers",
                headers={**auth, "Content-Type": "application/json"},
                content=b"{",
            )
            assert invalid.status_code == 400
            assert invalid.json()["error"]["code"] == "invalid_json"

            array = await client.post(
                "/manager-admin/api/v1/workers",
                headers=auth,
                json=[],
            )
            assert array.status_code == 400
            assert array.json()["error"]["code"] == "invalid_body"

            too_large = await client.post(
                "/manager-admin/api/v1/workers",
                headers={**auth, "Content-Type": "application/json"},
                content=b'{"padding":"' + (b"x" * 70000) + b'"}',
            )
            assert too_large.status_code == 413
            assert too_large.json()["error"]["code"] == "payload_too_large"

            missing_key = await client.post(
                "/manager-admin/api/v1/workers",
                headers={"Authorization": "Bearer admin-token"},
                json={
                    "name": "alice",
                    "runtime": "copaw",
                    "model": "qwen3.6-plus",
                },
            )
            assert missing_key.status_code == 400
            assert missing_key.json()["error"]["code"] == (
                "idempotency_key_required"
            )
            assert calls == []
    finally:
        await server.stop()
