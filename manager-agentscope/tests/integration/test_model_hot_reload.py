from __future__ import annotations

from datetime import UTC, datetime

import pytest

from agentteams_manager.clients.agt import ManagerResource
from agentteams_manager.clients.model_gateway import ModelCapabilities
from agentteams_manager.config import PromptSources, RuntimeDocument
from agentteams_manager.runtime.config_watcher import (
    ConfigChange,
    RuntimeRegistry,
)
from agentteams_manager.workflows.integrations import (
    IntegrationService,
    ModelSwitchRequest,
)
from agentteams_manager.workflows.resources import MutationContext
from tests.fixtures.task_workflow import TaskSupervisor


class Clock:
    def now(self) -> datetime:
        return datetime(2026, 7, 23, 12, tzinfo=UTC)


def _runtime(revision: int, model: str) -> RuntimeDocument:
    return RuntimeDocument(
        revision=revision,
        manager_name="manager",
        model=model,
        prompt_sources=PromptSources(
            soul="SOUL.md",
            agents="AGENTS.md",
            tools="TOOLS.md",
            heartbeat="HEARTBEAT.md",
        ),
    )


class Gateway:
    async def preflight(self, request):
        return ModelCapabilities(
            model=request.model,
            context_window=150_000,
            max_tokens=128_000,
            reasoning=True,
        )


class Agt:
    async def update_manager_model(self, name: str, model: str):
        return ManagerResource(
            name=name,
            phase="Running",
            model=model,
            runtime="qwenpaw",
        )


class Watcher:
    def __init__(self, registry: RuntimeRegistry) -> None:
        self.registry = registry
        self.calls = 0

    async def poll_once(self):
        self.calls += 1
        runtime = _runtime(2, "new")
        change = ConfigChange(
            revision=2,
            digest=self.registry.current.digest,
            document=runtime,
            etag='"new"',
        )
        # Test the workflow contract without duplicating watcher hashing.
        change = change.__class__(
            revision=2,
            digest=__import__("hashlib").sha256(
                __import__("json").dumps(
                    runtime.model_dump(mode="json"),
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode(),
            ).hexdigest(),
            document=runtime,
            etag='"new"',
        )
        self.registry.activate(change)
        return change


@pytest.mark.asyncio
async def test_manager_switch_waits_for_higher_runtime_generation() -> None:
    registry = RuntimeRegistry(_runtime(1, "old"))
    watcher = Watcher(registry)
    service = IntegrationService(
        agt=Agt(),
        gateway=Gateway(),
        supervisor=TaskSupervisor(Clock()),
        clock=Clock(),
        manager_name="manager",
        registry=registry,
        watcher=watcher,
        sleep=lambda _: None,
    )

    receipt = await service.switch_manager_model(
        ModelSwitchRequest(model="new"),
        context=MutationContext(
            room_id="!admin:example",
            event_id="$switch",
            tool_call_id="model",
        ),
    )

    assert receipt.model == "new"
    assert receipt.runtime_revision == 2
    assert receipt.active_turns_preserved is True
    assert watcher.calls == 1
