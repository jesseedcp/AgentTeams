"""Generation-scoped native AgentScope MCP clients."""

from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol
from urllib.parse import urlsplit

from agentscope.mcp import HttpMCPConfig, MCPClient
from agentscope.message import ToolResultState
from agentscope.tool import ToolBase
from pydantic import SecretStr

from agentteams_manager.config import RuntimeDocument
from agentteams_manager.domain.models import RoomKind, RoomPolicy


class MCPPreparationError(RuntimeError):
    """A proposed MCP generation failed safe discovery."""


class MCPClientPort(Protocol):
    name: str
    is_stateful: bool
    mcp_config: object

    async def list_tools(self) -> list[ToolBase]: ...

    async def close(self) -> None: ...


MCPClientFactory = Callable[..., MCPClientPort]


@dataclass(frozen=True, slots=True)
class MCPGeneration:
    revision: int
    digest: str
    tool_names: frozenset[str]
    clients: tuple[MCPClientPort, ...] = field(repr=False)


class MCPRegistry:
    """Prepare, authorize, and retire MCP clients by runtime revision."""

    def __init__(
        self,
        *,
        gateway_key: SecretStr,
        client_factory: MCPClientFactory = MCPClient,
        reserved_tool_names: frozenset[str] = frozenset(),
        discovery_timeout: float = 30,
        execution_timeout: float = 30,
    ) -> None:
        if discovery_timeout <= 0 or execution_timeout <= 0:
            raise ValueError("MCP timeouts must be positive")
        self._gateway_key = gateway_key
        self._client_factory = client_factory
        self._reserved_tool_names = reserved_tool_names
        self._discovery_timeout = discovery_timeout
        self._execution_timeout = execution_timeout
        self._generations: dict[int, MCPGeneration] = {}
        self._references: dict[int, int] = {}

    async def prepare(
        self,
        runtime: RuntimeDocument,
    ) -> MCPGeneration:
        """Discover every proposed MCP before making it available."""
        digest = _mcp_digest(runtime)
        existing = self._generations.get(runtime.revision)
        if existing is not None:
            if existing.digest != digest:
                raise MCPPreparationError(
                    "runtime revision already has different MCP descriptors",
                )
            return existing

        clients: list[MCPClientPort] = []
        tool_names: set[str] = set()
        current_name = ""
        try:
            for descriptor in runtime.mcp_servers:
                current_name = descriptor.name
                _validate_transport(
                    descriptor.transport,
                    descriptor.url,
                )
                client = self._client_factory(
                    name=descriptor.name,
                    is_stateful=False,
                    mcp_config=HttpMCPConfig(
                        url=descriptor.url,
                        headers={
                            "Authorization": (
                                "Bearer "
                                + self._gateway_key.get_secret_value()
                            ),
                        },
                        timeout=self._discovery_timeout,
                    ),
                    execution_timeout=self._execution_timeout,
                )
                clients.append(client)
                for tool in await client.list_tools():
                    if (
                        tool.name in tool_names
                        or tool.name in self._reserved_tool_names
                    ):
                        raise MCPPreparationError(
                            "duplicate or reserved MCP tool name "
                            f"{tool.name!r}",
                        )
                    tool_names.add(tool.name)
        except MCPPreparationError:
            await _close_clients(clients)
            raise
        except Exception as exc:
            await _close_clients(clients)
            raise MCPPreparationError(
                f"MCP {current_name!r} discovery failed "
                f"({type(exc).__name__})",
            ) from None

        generation = MCPGeneration(
            revision=runtime.revision,
            digest=digest,
            tool_names=frozenset(tool_names),
            clients=tuple(clients),
        )
        self._generations[runtime.revision] = generation
        self._references[runtime.revision] = 0
        return generation

    def clients_for(
        self,
        policy: RoomPolicy,
        *,
        revision: int | None = None,
    ) -> tuple[MCPClientPort, ...]:
        """Return only clients authorized for one immutable room policy."""
        if revision is None:
            if not self._generations:
                return ()
            revision = max(self._generations)
        generation = self._generations[revision]
        if policy.kind is RoomKind.ADMIN_DM:
            return generation.clients
        allowed = policy.allowed_mcp_names
        return tuple(
            client
            for client in generation.clients
            if client.name in allowed
        )

    async def list_server_tools(
        self,
        server_name: str,
        *,
        revision: int | None = None,
    ) -> tuple[ToolBase, ...]:
        """Rediscover one configured server through its AgentScope client."""
        client = self._client_for(server_name, revision=revision)
        try:
            return tuple(await client.list_tools())
        except Exception as exc:
            raise MCPPreparationError(
                f"MCP {server_name!r} discovery failed "
                f"({type(exc).__name__})",
            ) from None

    async def call_server_tool(
        self,
        server_name: str,
        tool_name: str,
        arguments: dict[str, object],
        *,
        revision: int | None = None,
    ) -> object:
        """Invoke a discovered tool without a sidecar or model round trip."""
        tools = await self.list_server_tools(
            server_name,
            revision=revision,
        )
        tool = next(
            (candidate for candidate in tools if candidate.name == tool_name),
            None,
        )
        if tool is None:
            raise MCPPreparationError(
                f"MCP tool {tool_name!r} is not exposed by {server_name!r}",
            )
        try:
            result = tool.call(**arguments)
            if inspect.isawaitable(result):
                result = await result
        except Exception as exc:
            raise MCPPreparationError(
                f"MCP tool {tool_name!r} failed "
                f"({type(exc).__name__})",
            ) from None
        if getattr(result, "state", None) is ToolResultState.ERROR:
            raise MCPPreparationError(
                f"MCP tool {tool_name!r} returned an error result",
            )
        return result

    def retain(self, revision: int) -> None:
        if revision not in self._generations:
            raise KeyError(f"MCP generation {revision} is not prepared")
        self._references[revision] += 1

    async def release(
        self,
        revision: int,
        *,
        active_revision: int,
    ) -> None:
        references = self._references.get(revision)
        if references is None:
            return
        if references < 1:
            raise RuntimeError(
                f"MCP generation {revision} has no active lease",
            )
        references -= 1
        self._references[revision] = references
        if references == 0 and revision != active_revision:
            await self.close_generation(revision)

    async def close_generation(self, revision: int) -> None:
        """Close a generation only when no room Agent still owns it."""
        generation = self._generations.get(revision)
        if generation is None:
            return
        if self._references[revision] != 0:
            raise RuntimeError(
                f"MCP generation {revision} is still in use",
            )
        await _close_clients(generation.clients, ignore_errors=False)
        del self._generations[revision]
        del self._references[revision]

    def _client_for(
        self,
        server_name: str,
        *,
        revision: int | None,
    ) -> MCPClientPort:
        if revision is None:
            if not self._generations:
                raise MCPPreparationError("no MCP generation is prepared")
            revision = max(self._generations)
        generation = self._generations.get(revision)
        if generation is None:
            raise MCPPreparationError(
                f"MCP generation {revision} is not prepared",
            )
        client = next(
            (
                candidate
                for candidate in generation.clients
                if candidate.name == server_name
            ),
            None,
        )
        if client is None:
            raise MCPPreparationError(
                f"MCP server {server_name!r} is not in generation "
                f"{revision}",
            )
        return client


def _validate_transport(transport: str, url: str) -> None:
    path = urlsplit(url).path.rstrip("/")
    looks_like_sse = path.endswith("/sse") or path.endswith("/messages")
    if transport == "sse" and not looks_like_sse:
        raise MCPPreparationError(
            "SSE MCP URLs must end in /sse or /messages/",
        )
    if transport == "http" and looks_like_sse:
        raise MCPPreparationError(
            "HTTP MCP descriptor points at an SSE endpoint",
        )


def _mcp_digest(runtime: RuntimeDocument) -> str:
    data = json.dumps(
        [
            descriptor.model_dump(mode="json")
            for descriptor in runtime.mcp_servers
        ],
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


async def _close_clients(
    clients: tuple[MCPClientPort, ...] | list[MCPClientPort],
    *,
    ignore_errors: bool = True,
) -> None:
    first_error: Exception | None = None
    for client in reversed(clients):
        try:
            await client.close()
        except Exception as exc:
            if first_error is None:
                first_error = exc
    if first_error is not None and not ignore_errors:
        raise MCPPreparationError(
            "MCP generation cleanup failed "
            f"({type(first_error).__name__})",
        ) from None
