"""Generation-scoped native AgentScope MCP clients.

按 runtime generation 创建、预热并回收 AgentScope MCP client。

配置更新时先在旁路建立新连接并验证工具，再激活 generation；旧 Agent 正在运行时继续
持有旧 client，turn 结束后才 retire。这样避免热更新把半途中的 MCP 调用断开，也防止
新旧配置共享一个可变 client 而产生权限串线。
"""

from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol, cast
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
        client_factory: MCPClientFactory = cast(
            MCPClientFactory,
            MCPClient,
        ),
        reserved_tool_names: frozenset[str] = frozenset(),
        discovery_timeout: float = 30,
        execution_timeout: float = 30,
    ) -> None:
        # 逻辑说明：校验发现与执行超时均为正数后保存 gateway 凭据、client factory 和保留工具名，并初始化 generation/租约表；构造阶段不读取 secret 明文、不创建或连接 MCP client。
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
        # 逻辑说明：按 runtime revision/digest 幂等复用已准备 generation，否则逐个校验传输、创建 client 并发现工具，拒绝跨 server 重名或保留名；全部成功才登记 generation，任何失败先逆序关闭临时 clients 再抛 MCPPreparationError。
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
        # 逻辑说明：从指定或最新已准备 revision 读取 generation；Admin DM 获得全部 clients，其他房间仅返回名称位于 policy.allowed_mcp_names 的 clients，缺失显式 revision 由映射访问报错且不做隐式准备。
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
        # 逻辑说明：定位指定或最新 generation 中的 server client 并重新调用 list_tools，成功时复制为不可变元组；发现异常被压缩成只含 server 名和异常类型的 MCPPreparationError，避免泄露底层响应。
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
        # 逻辑说明：先在指定 MCP server 重新发现工具并按 tool_name 精确查找，再以 arguments 调用同步或异步 ToolBase；未暴露、调用异常或 ERROR 结果统一转成不泄露内部详情的 MCPPreparationError，成功返回原结果。
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
            raw_result = tool.call(**arguments)
            if inspect.isawaitable(raw_result):
                result = await raw_result
            else:
                result = raw_result
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
        # 逻辑说明：确认 revision 已完成 prepare 后将其 Agent 租约计数加一；未知 generation 抛 KeyError，确保未准备的 MCP client 不会被会话声明为在用。
        if revision not in self._generations:
            raise KeyError(f"MCP generation {revision} is not prepared")
        self._references[revision] += 1

    async def release(
        self,
        revision: int,
        *,
        active_revision: int,
    ) -> None:
        # 逻辑说明：将指定 revision 的租约减一；未知 revision 为空操作、计数已为零则报状态错误，减到零且不是 active_revision 时立即关闭该旧 generation，关闭失败向上传播。
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
        # 逻辑说明：仅当 generation 存在且租约计数为零时严格关闭其全部 clients，成功后才删除 generation 与引用表；仍在使用或关闭失败时保留登记状态并抛错。
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

    async def close(self) -> None:
        """Close every generation after room sessions have been retired."""
        # 逻辑说明：按 revision 从新到旧调用 close_generation 清理所有已准备 MCP generations；任一仍有租约或 client 关闭失败即停止并传播，防止静默遗留活连接。
        for revision in sorted(self._generations, reverse=True):
            await self.close_generation(revision)

    def _client_for(
        self,
        server_name: str,
        *,
        revision: int | None,
    ) -> MCPClientPort:
        # 逻辑说明：在指定或最新已准备 generation 中按 server_name 精确查找 client；无 generation、revision 未准备或 server 不存在均抛带稳定上下文的 MCPPreparationError。
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
    # 逻辑说明：根据 URL path 是否以 /sse 或 /messages 结尾，校验 descriptor 的 sse/http transport 与端点形态一致；不匹配时拒绝准备，匹配时无返回和副作用。
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
    # 逻辑说明：仅将 runtime.mcp_servers 描述符按稳定 JSON 规则编码并计算 SHA-256，用于判定同一 revision 的 MCP 配置是否相同；序列化错误直接传播。
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
    # 逻辑说明：按创建顺序的逆序尝试关闭全部 MCP clients，并记住首个异常但继续清理其余项；默认忽略清理错误，严格模式则在全部尝试后将首错归一为 MCPPreparationError。
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
