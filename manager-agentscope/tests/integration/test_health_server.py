import httpx
import pytest
from agentteams_manager.health import HealthServer, ReadinessState
from agentteams_manager.observability.metrics import MetricsRegistry
from pydantic import SecretStr


@pytest.mark.asyncio
async def test_ready_endpoint_is_503_until_dependencies_are_ready() -> None:
    readiness = ReadinessState()
    metrics = MetricsRegistry()
    metrics.increment("agentteams_matrix_events_total")
    server = HealthServer(
        readiness=readiness,
        metrics=metrics,
        host="127.0.0.1",
        port=0,
    )
    await server.start()

    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{server.bound_port}",
            trust_env=False,
        ) as client:
            response = await client.get("/readyz")
            assert response.status_code == 503

            readiness.database_ready = True
            readiness.recovery_ready = True
            readiness.config_ready = True
            readiness.matrix_ready = True
            readiness.heartbeat_ready = True
            response = await client.get("/readyz")
            assert response.status_code == 200

            health = await client.get("/healthz")
            assert health.status_code == 200
            metric_response = await client.get("/metrics")
            assert "agentteams_matrix_events_total 1" in metric_response.text
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_health_server_rejects_non_get_requests() -> None:
    server = HealthServer(
        readiness=ReadinessState(),
        metrics=MetricsRegistry(),
        host="127.0.0.1",
        port=0,
    )
    await server.start()
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{server.bound_port}",
            trust_env=False,
        ) as client:
            response = await client.post("/healthz")
            assert response.status_code == 405
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_health_endpoint_fails_when_critical_supervisor_exits() -> None:
    live = True
    server = HealthServer(
        readiness=ReadinessState(),
        metrics=MetricsRegistry(),
        host="127.0.0.1",
        port=0,
        liveness_probe=lambda: live,
    )
    await server.start()
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{server.bound_port}",
            trust_env=False,
        ) as client:
            response = await client.get("/healthz")
            assert response.status_code == 200
            assert response.json()["status"] == "ok"

            live = False
            response = await client.get("/healthz")
            assert response.status_code == 503
            assert response.json()["status"] == "unhealthy"
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_health_reports_optional_capability_configuration_separately(
) -> None:
    server = HealthServer(
        readiness=ReadinessState(),
        metrics=MetricsRegistry(),
        host="127.0.0.1",
        port=0,
        capability_snapshot=lambda: {
            "coding_cli": {
                "enabled": True,
                "providers": {
                    "claude": {
                        "configured": True,
                        "available": False,
                    },
                },
            },
        },
    )
    await server.start()
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{server.bound_port}",
            trust_env=False,
        ) as client:
            payload = (await client.get("/healthz")).json()
            coding = payload["capabilities"]["coding_cli"]
            assert coding["enabled"] is True
            assert coding["providers"]["claude"] == {
                "configured": True,
                "available": False,
            }
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_admin_console_requires_bearer_token_for_data() -> None:
    async def snapshot(section: str) -> dict[str, object]:
        return {"section": section, "items": [{"name": "default"}]}

    server = HealthServer(
        readiness=ReadinessState(),
        metrics=MetricsRegistry(),
        host="127.0.0.1",
        port=0,
        admin_token=SecretStr("admin-console-token"),
        admin_snapshot=snapshot,
    )
    await server.start()
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{server.bound_port}",
            trust_env=False,
        ) as client:
            page = await client.get("/manager-admin/")
            assert page.status_code == 200
            assert "AgentTeams Manager" in page.text
            assert "admin-console-token" not in page.text

            denied = await client.get(
                "/manager-admin/api/projects",
            )
            assert denied.status_code == 401

            response = await client.get(
                "/manager-admin/api/projects",
                headers={
                    "Authorization": "Bearer admin-console-token",
                },
            )
            assert response.status_code == 200
            assert response.json()["section"] == "projects"
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_signed_channel_webhook_is_forwarded_to_handler() -> None:
    calls: list[tuple[str, bytes]] = []

    async def webhook(provider, request):
        assert request.headers["x-agentteams-signature"] == "signed"
        calls.append((provider, request.body))
        from agentteams_manager.channels.base import ChannelWebhookResponse

        return ChannelWebhookResponse(
            status_code=202,
            body=b'{"status":"pending_approval"}',
        )

    server = HealthServer(
        readiness=ReadinessState(),
        metrics=MetricsRegistry(),
        host="127.0.0.1",
        port=0,
        webhook_handler=webhook,
    )
    await server.start()
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{server.bound_port}",
            trust_env=False,
        ) as client:
            response = await client.post(
                "/manager-admin/hooks/telegram",
                content=b'{"message":"hello"}',
                headers={"X-AgentTeams-Signature": "signed"},
            )
            assert response.status_code == 202
            assert calls == [
                ("telegram", b'{"message":"hello"}'),
            ]
    finally:
        await server.stop()
