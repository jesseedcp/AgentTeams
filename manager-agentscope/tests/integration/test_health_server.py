import httpx
import pytest

from agentteams_manager.health import HealthServer, ReadinessState
from agentteams_manager.observability.metrics import MetricsRegistry


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
