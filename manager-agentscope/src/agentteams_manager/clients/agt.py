"""Typed command boundary for the AgentTeams Controller CLI.

通过 ``agt`` CLI 与 AgentTeams Controller 进行类型化通信。

Manager 查询或修改 Worker、Team、Human、Project 时，只在本模块把明确参数转换为
argv，并校验 CLI 返回的 JSON。workflow 不拼接 shell 字符串，也不直接访问 Controller
HTTP API：这样既减少命令注入风险，又让所有资源协议集中在一个边界。CLI 超时仍可能
表示 Controller 已执行操作但回执丢失，上层必须进入 reconciliation，而非盲目重试。
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Annotated, Any, Literal, Protocol
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    field_validator,
    model_validator,
)

from agentteams_manager.config import MCPServerDocument
from agentteams_manager.domain.models import (
    HumanResource,
    TeamResource,
    WorkerResource,
)

from .process import ProcessResult, ProcessTimeout

WorkerRuntime = Literal[
    "openclaw",
    "copaw",
    "hermes",
    "qwenpaw",
]
Port = Annotated[int, Field(ge=1, le=65535)]
ResourceName = Annotated[
    str,
    StringConstraints(
        pattern=r"^[a-z0-9][a-z0-9-]*$",
        min_length=1,
    ),
]


class AgtError(RuntimeError):
    """Base typed CLI error."""


class AgtCommandError(AgtError):
    """The CLI returned a non-success exit status."""


class AgtProtocolError(AgtError):
    """The CLI returned output outside its declared JSON contract."""


class ProcessPort(Protocol):
    async def run(
        self,
        argv: tuple[str, ...],
        *,
        stdin: bytes | None = None,
        cwd: Path | None = None,
        timeout: float | None = None,
    ) -> ProcessResult: ...


class _Request(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class WorkerCreateRequest(_Request):
    name: ResourceName
    runtime: WorkerRuntime
    model: str = Field(min_length=1)
    image: str | None = None
    identity: str | None = None
    soul: str | None = None
    skills: tuple[str, ...] = ()
    package_uri: str | None = None
    expose: tuple[Port, ...] = ()
    console_enabled: bool = False
    console_port: Port = 8088
    team: ResourceName | None = None
    role: Literal["team_leader", "worker"] | None = None

    @field_validator("package_uri")
    @classmethod
    def package_reference_has_no_credentials(
        cls,
        value: str | None,
    ) -> str | None:
        # 逻辑说明：创建请求进入 CLI 前统一拒绝 URI 中的凭据，Secret 必须来自受控运行时配置。
        return _safe_package_reference(value)

    @field_validator("expose")
    @classmethod
    def require_unique_expose_ports(
        cls,
        value: tuple[int, ...],
    ) -> tuple[int, ...]:
        # 逻辑说明：复用端口去重校验，并收窄创建场景不会返回 None 的类型。
        validated = _unique_ports(value)
        assert validated is not None
        return validated

    @model_validator(mode="after")
    def require_supported_console_runtime(self) -> WorkerCreateRequest:
        # 逻辑说明：交叉检查 console 与 runtime，避免 Controller 收到单字段合法但组合不受支持的请求。
        if self.console_enabled and self.runtime not in {"copaw", "qwenpaw"}:
            raise ValueError(
                "Worker console is supported only by copaw and qwenpaw",
            )
        return self


class WorkerUpdateRequest(_Request):
    name: ResourceName
    model: str | None = None
    runtime: WorkerRuntime | None = None
    image: str | None = None
    identity: str | None = None
    soul: str | None = None
    skills: tuple[str, ...] | None = None
    package_uri: str | None = None
    expose: tuple[Port, ...] | None = None
    console_enabled: bool | None = None
    console_port: Port | None = None

    @field_validator("package_uri")
    @classmethod
    def package_reference_has_no_credentials(
        cls,
        value: str | None,
    ) -> str | None:
        # 逻辑说明：更新请求也走同一无凭据 URI 规则，防止 PATCH 路径绕过创建时的 Secret 边界。
        return _safe_package_reference(value)

    @field_validator("expose")
    @classmethod
    def require_unique_expose_ports(
        cls,
        value: tuple[int, ...] | None,
    ) -> tuple[int, ...] | None:
        # 逻辑说明：保留 None 表示“不修改”，但显式端口列表仍必须去重。
        return _unique_ports(value)

    @model_validator(mode="after")
    def require_change(self) -> WorkerUpdateRequest:
        # 逻辑说明：验证 console/runtime 组合并拒绝空更新，使 argv 的每次执行都代表明确状态变更。
        if self.console_enabled is False and self.console_port is not None:
            raise ValueError(
                "console_port cannot be set when console_enabled is false",
            )
        if (
            self.runtime in {"openclaw", "hermes"}
            and (
                self.console_enabled is True
                or self.console_port is not None
            )
        ):
            raise ValueError(
                "Worker console is supported only by copaw and qwenpaw",
            )
        changed = (
            self.model,
            self.runtime,
            self.image,
            self.identity,
            self.soul,
            self.skills,
            self.package_uri,
            self.expose,
            self.console_enabled,
            self.console_port,
        )
        if all(value is None for value in changed):
            raise ValueError("at least one Worker field must change")
        return self


class TeamCreateRequest(_Request):
    name: ResourceName
    leader_name: ResourceName
    worker_names: tuple[ResourceName, ...] = ()
    team_name: ResourceName | None = None
    description: str | None = None
    heartbeat_every: str | None = None
    admin_name: ResourceName | None = None
    admin_matrix_id: str | None = None
    peer_mentions: bool = True


class HumanCreateRequest(_Request):
    name: ResourceName
    display_name: str = Field(min_length=1)
    email: str | None = None
    permission_level: int = Field(ge=1, le=3)
    accessible_teams: tuple[ResourceName, ...] = ()
    accessible_workers: tuple[ResourceName, ...] = ()
    note: str | None = None


class HumanUpdateRequest(_Request):
    name: ResourceName
    display_name: str | None = None
    email: str | None = None
    permission_level: int | None = Field(default=None, ge=1, le=3)
    accessible_teams: tuple[ResourceName, ...] | None = None
    accessible_workers: tuple[ResourceName, ...] | None = None
    note: str | None = None

    @model_validator(mode="after")
    def require_change(self) -> HumanUpdateRequest:
        # 逻辑说明：None 表示字段未提供；全部未提供时拒绝请求，避免发送无意义的 update 命令。
        changed = (
            self.display_name,
            self.email,
            self.permission_level,
            self.accessible_teams,
            self.accessible_workers,
            self.note,
        )
        if all(value is None for value in changed):
            raise ValueError("at least one Human field must change")
        return self


class _ExposePayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    port: int = Field(ge=1, le=65535)


class _ExposedPortPayload(_ExposePayload):
    domain: str = Field(min_length=1)


class _ConsolePayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    enabled: bool = False
    port: Port = 8088


class _WorkerPayload(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str
    phase: str
    model: str = ""
    runtime: WorkerRuntime = "openclaw"
    image: str = ""
    identity: str = ""
    soul: str = ""
    skills: tuple[str, ...] = ()
    mcp_servers: tuple[MCPServerDocument, ...] = Field(
        default=(),
        alias="mcpServers",
    )
    package_uri: str = Field(default="", alias="package")
    expose: tuple[_ExposePayload, ...] = ()
    console: _ConsolePayload | None = None
    exposed_ports: tuple[_ExposedPortPayload, ...] = Field(
        default=(),
        alias="exposedPorts",
    )
    container_state: str = Field(default="", alias="containerState")
    matrix_user_id: str = Field(default="", alias="matrixUserID")
    room_id: str = Field(default="", alias="roomID")
    message: str = ""
    team: str = ""
    role: str = ""

    def domain(self) -> WorkerResource:
        # 逻辑说明：把 Controller 的扁平/别名响应分成稳定 domain spec 与 status，空字符串规范化为 None。
        return WorkerResource(
            name=self.name,
            phase=self.phase,
            model=self.model or None,
            runtime=self.runtime,
            room_id=self.room_id or None,
            matrix_user_id=self.matrix_user_id or None,
            team=self.team or None,
            role=self.role or None,
            skills=self.skills,
            spec={
                "image": self.image,
                "identity": self.identity,
                "soul": self.soul,
                "package": self.package_uri,
                "expose": [
                    item.port
                    for item in self.expose
                ],
                "console": (
                    self.console.model_dump(mode="json")
                    if self.console is not None
                    else None
                ),
                "mcpServers": [
                    server.model_dump(mode="json")
                    for server in self.mcp_servers
                ],
            },
            status={
                "containerState": self.container_state,
                "message": self.message,
                "exposedPorts": [
                    item.model_dump(mode="json")
                    for item in self.exposed_ports
                ],
            },
        )


class _TeamPayload(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str
    team_name: str = Field(default="", alias="teamName")
    phase: str = "Pending"
    description: str = ""
    admin: dict[str, Any] | None = None
    heartbeat_every: str = Field(default="", alias="heartbeatEvery")
    leader_name: str = Field(alias="leaderName")
    worker_names: tuple[str, ...] = Field(
        default=(),
        alias="workerNames",
    )
    team_room_id: str = Field(default="", alias="teamRoomID")
    leader_dm_room_id: str = Field(default="", alias="leaderDMRoomID")
    leader_ready: bool = Field(default=False, alias="leaderReady")
    ready_workers: int = Field(default=0, alias="readyWorkers")
    total_workers: int = Field(default=0, alias="totalWorkers")
    message: str = ""
    peer_mentions: bool = Field(default=True, alias="peerMentions")

    def domain(self) -> TeamResource:
        # 逻辑说明：将 CLI payload 映射为团队领域模型，并区分期望配置与就绪观测状态。
        return TeamResource(
            name=self.name,
            leader=self.leader_name,
            workers=self.worker_names,
            phase=self.phase,
            spec={
                "teamName": self.team_name or self.name,
                "description": self.description,
                "admin": self.admin,
                "heartbeatEvery": self.heartbeat_every,
                "teamRoomID": self.team_room_id,
                "leaderDMRoomID": self.leader_dm_room_id,
                "peerMentions": self.peer_mentions,
            },
            status={
                "leaderReady": self.leader_ready,
                "readyWorkers": self.ready_workers,
                "totalWorkers": self.total_workers,
                "message": self.message,
            },
        )


class _HumanPayload(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str
    phase: str = "Pending"
    display_name: str = Field(alias="displayName")
    email: str = ""
    matrix_user_id: str = Field(default="", alias="matrixUserID")
    rooms: tuple[str, ...] = ()
    permission_level: int = Field(alias="permissionLevel", ge=1, le=3)
    accessible_teams: tuple[str, ...] = Field(
        default=(),
        alias="accessibleTeams",
    )
    accessible_workers: tuple[str, ...] = Field(
        default=(),
        alias="accessibleWorkers",
    )
    note: str = ""
    message: str = ""

    def domain(self) -> HumanResource:
        # 逻辑说明：把 Controller 字段归一化为权限领域模型，原始 phase/message 只保留在 status 中。
        return HumanResource(
            name=self.name,
            matrix_user_id=self.matrix_user_id,
            permission_level=self.permission_level,
            allowed_rooms=self.rooms,
            spec={
                "displayName": self.display_name,
                "email": self.email,
                "accessibleTeams": list(self.accessible_teams),
                "accessibleWorkers": list(self.accessible_workers),
                "note": self.note,
            },
            status={"phase": self.phase, "message": self.message},
        )


class ManagerResource(BaseModel):
    """Secret-free Controller view of one Manager."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    phase: str
    model: str
    runtime: str
    room_id: str | None = None
    matrix_user_id: str | None = None
    version: str | None = None
    message: str | None = None
    identity: str = ""
    mcp_servers: tuple[MCPServerDocument, ...] = ()


class _ManagerPayload(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str
    phase: str = "Pending"
    model: str = ""
    runtime: str = ""
    room_id: str = Field(default="", alias="roomID")
    matrix_user_id: str = Field(default="", alias="matrixUserID")
    version: str = ""
    message: str = ""
    identity: str = ""
    mcp_servers: tuple[MCPServerDocument, ...] = Field(
        default=(),
        alias="mcpServers",
    )

    def domain(self) -> ManagerResource:
        # 逻辑说明：将允许公开的 Manager 状态规范化为空值可选字段，不携带任何 Controller Secret。
        return ManagerResource(
            name=self.name,
            phase=self.phase,
            model=self.model,
            runtime=self.runtime,
            room_id=self.room_id or None,
            matrix_user_id=self.matrix_user_id or None,
            version=self.version or None,
            message=self.message or None,
            identity=self.identity,
            mcp_servers=self.mcp_servers,
        )


class _WorkerList(BaseModel):
    model_config = ConfigDict(extra="forbid")
    workers: tuple[_WorkerPayload, ...]
    total: int


class _TeamList(BaseModel):
    model_config = ConfigDict(extra="forbid")
    teams: tuple[_TeamPayload, ...]
    total: int


class _HumanList(BaseModel):
    model_config = ConfigDict(extra="forbid")
    humans: tuple[_HumanPayload, ...]
    total: int


class _ManagerList(BaseModel):
    model_config = ConfigDict(extra="forbid")
    managers: tuple[_ManagerPayload, ...]
    total: int


class AgtClient:
    """The only Python module authorized to invoke ``agt``."""

    def __init__(
        self,
        process: ProcessPort,
        *,
        timeout: float = 30,
    ) -> None:
        # 逻辑说明：保存唯一获准执行 agt 的进程边界及统一超时，上层 workflow 不接触 shell 拼接。
        self._process = process
        self._timeout = timeout

    async def get_worker(self, name: str) -> WorkerResource | None:
        # 逻辑说明：校验名称后查询单个 Worker；404 规范化为 None，成功响应必须通过 schema 转换。
        raw = await self._get(("workers",), name)
        return self._parse(_WorkerPayload, raw).domain() if raw else None

    async def list_workers(self) -> tuple[WorkerResource, ...]:
        # 逻辑说明：执行固定 argv，先校验列表 envelope，再逐项转换为不可变领域资源。
        raw = await self._json(("agt", "get", "workers", "-o", "json"))
        return tuple(item.domain() for item in self._parse(_WorkerList, raw).workers)

    async def create_worker(
        self,
        request: WorkerCreateRequest,
    ) -> WorkerResource | None:
        # 逻辑说明：从已校验请求逐项构造 argv，以 --no-wait 异步创建；旧 CLI 无完整对象时允许返回 None。
        argv = ["agt", "create", "worker", "--name", request.name]
        _optional_flag(argv, "--model", request.model)
        _optional_flag(argv, "--runtime", request.runtime)
        _optional_flag(argv, "--image", request.image)
        _optional_flag(argv, "--identity", request.identity)
        _optional_flag(argv, "--soul", request.soul)
        _csv_flag(argv, "--skills", request.skills)
        _flag(argv, "--package", request.package_uri)
        if request.expose:
            _flag(argv, "--expose", ",".join(map(str, request.expose)))
        if request.console_enabled:
            argv.extend(
                (
                    "--console",
                    "--console-port",
                    str(request.console_port),
                ),
            )
        _flag(argv, "--team", request.team)
        _flag(argv, "--role", request.role)
        argv.extend(("--no-wait", "-o", "json"))
        raw = await self._json(tuple(argv))
        try:
            return self._parse(_WorkerPayload, raw).domain()
        except AgtProtocolError:
            return None

    async def update_worker(
        self,
        request: WorkerUpdateRequest,
    ) -> WorkerResource:
        # 逻辑说明：只为显式字段生成参数；命令成功后重新读取权威资源，确认 Worker 仍可见。
        argv = ["agt", "update", "worker", "--name", request.name]
        _flag(argv, "--model", request.model)
        _flag(argv, "--runtime", request.runtime)
        _flag(argv, "--image", request.image)
        _flag(argv, "--identity", request.identity)
        _flag(argv, "--soul", request.soul)
        if request.skills is not None:
            _csv_flag(argv, "--skills", request.skills, allow_empty=True)
        _optional_flag(argv, "--package", request.package_uri)
        if request.expose is not None:
            if request.expose:
                _optional_flag(
                    argv,
                    "--expose",
                    ",".join(map(str, request.expose)),
                )
            else:
                argv.append("--clear-expose")
        if request.console_enabled is False:
            argv.append("--no-console")
        elif (
            request.console_enabled is True
            or request.console_port is not None
        ):
            argv.append("--console")
            if request.console_port is not None:
                argv.extend(("--console-port", str(request.console_port)))
        await self._command(tuple(argv))
        worker = await self.get_worker(request.name)
        if worker is None:
            raise AgtProtocolError(
                f"updated worker {request.name!r} is not readable",
            )
        return worker

    async def update_worker_expose(
        self,
        name: str,
        ports: tuple[int, ...],
    ) -> WorkerResource:
        """Replace every desired exposed port and return observed status."""
        # 逻辑说明：把“整体替换端口”收敛到通用更新模型，保留空元组表示清空的语义。
        return await self.update_worker(
            WorkerUpdateRequest(name=name, expose=ports),
        )

    async def apply_worker_package(
        self,
        *,
        name: str,
        package_uri: str,
        expected_digest: str,
        runtime: WorkerRuntime,
    ) -> WorkerResource:
        """Apply one digest-bound Nacos package and prove it is readable."""
        # 逻辑说明：验证资源名、摘要和 nacos URI，将确认过的摘要绑定进 URI；apply 后回读证明可观察。
        _validate_name(name)
        if re.fullmatch(r"sha256:[0-9a-f]{64}", expected_digest) is None:
            raise ValueError("expected_digest must be a sha256 digest")
        parsed = urlsplit(package_uri)
        if parsed.scheme != "nacos" or not parsed.hostname:
            raise ValueError("Worker packages must use a valid nacos:// URI")
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        existing = query.get("expectedDigest")
        if existing is not None and existing != expected_digest:
            raise ValueError(
                "package URI expectedDigest conflicts with confirmation",
            )
        query["expectedDigest"] = expected_digest
        bound_uri = urlunsplit(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                urlencode(query),
                "",
            ),
        )
        await self._command(
            (
                "agt",
                "apply",
                "worker",
                "--name",
                name,
                "--package",
                bound_uri,
                "--runtime",
                runtime,
            ),
        )
        worker = await self.get_worker(name)
        if worker is None:
            raise AgtProtocolError(
                f"applied worker {name!r} is not readable",
            )
        return worker

    async def worker_status(self, name: str) -> WorkerResource | None:
        # 逻辑说明：用专用 status 子命令读取实时状态，合法 404 返回 None，其余响应必须满足 Worker schema。
        _validate_name(name)
        raw = await self._json_or_none(
            (
                "agt",
                "worker",
                "status",
                "--name",
                name,
                "-o",
                "json",
            ),
        )
        return self._parse(_WorkerPayload, raw).domain() if raw else None

    async def sleep_worker(self, name: str) -> WorkerResource:
        # 逻辑说明：复用统一生命周期边界执行 sleep 并回读最终资源。
        return await self._lifecycle(name, "sleep")

    async def wake_worker(self, name: str) -> WorkerResource:
        # 逻辑说明：复用统一生命周期边界执行 wake 并回读最终资源。
        return await self._lifecycle(name, "wake")

    async def _lifecycle(
        self,
        name: str,
        action: str,
    ) -> WorkerResource:
        # 逻辑说明：校验名字、执行固定生命周期动作，再回读 Controller；动作后消失视为协议错误。
        _validate_name(name)
        await self._command(
            ("agt", "worker", action, "--name", name),
        )
        worker = await self.get_worker(name)
        if worker is None:
            raise AgtProtocolError(
                f"worker {name!r} disappeared after {action}",
            )
        return worker

    async def delete_worker(self, name: str) -> None:
        # 逻辑说明：通过统一删除边界验证名称并执行固定资源类型命令。
        await self._delete("worker", name)

    async def get_team(self, name: str) -> TeamResource | None:
        # 逻辑说明：按合法名称读取团队，404 映射 None，存在时转换为 TeamResource。
        raw = await self._get(("teams",), name)
        return self._parse(_TeamPayload, raw).domain() if raw else None

    async def list_teams(self) -> tuple[TeamResource, ...]:
        # 逻辑说明：校验 Controller 的团队列表 envelope 后逐项拆分 spec/status。
        raw = await self._json(("agt", "get", "teams", "-o", "json"))
        return tuple(item.domain() for item in self._parse(_TeamList, raw).teams)

    async def create_team(self, request: TeamCreateRequest) -> None:
        # 逻辑说明：由类型化请求构造无 shell 的 argv，仅追加已提供的可选团队配置后执行。
        argv = [
            "agt",
            "create",
            "team",
            "--name",
            request.name,
            "--leader-name",
            request.leader_name,
        ]
        _csv_flag(argv, "--workers", request.worker_names)
        _flag(argv, "--team-name", request.team_name)
        _flag(argv, "--description", request.description)
        _flag(
            argv,
            "--leader-heartbeat-every",
            request.heartbeat_every,
        )
        _flag(argv, "--admin", request.admin_name)
        _flag(argv, "--admin-matrix-id", request.admin_matrix_id)
        _flag(argv, "--peer-mentions", str(request.peer_mentions).lower())
        await self._command(tuple(argv))

    async def apply_team(self, name: str, document: bytes) -> TeamResource:
        # 逻辑说明：校验目标名，将 YAML/JSON 文档经 stdin 交给 agt apply，随后回读确认资源可见。
        _validate_name(name)
        await self._command(
            ("agt", "apply", "-f", "-"),
            stdin=document,
        )
        team = await self.get_team(name)
        if team is None:
            raise AgtProtocolError(
                f"applied team {name!r} is not readable",
            )
        return team

    async def delete_team(self, name: str) -> None:
        # 逻辑说明：复用统一删除边界，避免各资源方法产生不一致的名称校验和错误处理。
        await self._delete("team", name)

    async def get_human(self, name: str) -> HumanResource | None:
        # 逻辑说明：查询稳定 Human 名称；不存在是正常空结果，畸形 JSON 则是协议错误。
        raw = await self._get(("humans",), name)
        return self._parse(_HumanPayload, raw).domain() if raw else None

    async def list_humans(self) -> tuple[HumanResource, ...]:
        # 逻辑说明：先验证列表总结构，再把每项转换为不暴露底层 payload 细节的 HumanResource。
        raw = await self._json(("agt", "get", "humans", "-o", "json"))
        return tuple(
            item.domain()
            for item in self._parse(_HumanList, raw).humans
        )

    async def create_human(self, request: HumanCreateRequest) -> None:
        # 逻辑说明：将经过 Pydantic 校验的权限字段编码为精确 argv，空可选字段不会误覆盖默认值。
        argv = [
            "agt",
            "create",
            "human",
            "--name",
            request.name,
            "--display-name",
            request.display_name,
        ]
        _flag(argv, "--email", request.email)
        _flag(argv, "--permission-level", str(request.permission_level))
        _csv_flag(argv, "--accessible-teams", request.accessible_teams)
        _csv_flag(
            argv,
            "--accessible-workers",
            request.accessible_workers,
        )
        _flag(argv, "--note", request.note)
        await self._command(tuple(argv))

    async def update_human(
        self,
        request: HumanUpdateRequest,
    ) -> HumanResource:
        # 逻辑说明：区分未提供与显式空列表生成更新参数，执行后回读以确认 Human 仍可观察。
        argv = ["agt", "update", "human", "--name", request.name]
        _optional_flag(argv, "--display-name", request.display_name)
        _optional_flag(argv, "--email", request.email)
        _flag(argv, "--permission-level", request.permission_level)
        if request.accessible_teams is not None:
            _csv_flag(
                argv,
                "--accessible-teams",
                request.accessible_teams,
                allow_empty=True,
            )
        if request.accessible_workers is not None:
            _csv_flag(
                argv,
                "--accessible-workers",
                request.accessible_workers,
                allow_empty=True,
            )
        _optional_flag(argv, "--note", request.note)
        await self._command(tuple(argv))
        human = await self.get_human(request.name)
        if human is None:
            raise AgtProtocolError(
                f"updated human {request.name!r} is not readable",
            )
        return human

    async def delete_human(self, name: str) -> None:
        # 逻辑说明：经统一资源删除入口执行，错误会保留为类型化 AgtCommandError。
        await self._delete("human", name)

    async def get_manager(self, name: str) -> ManagerResource | None:
        # 逻辑说明：读取指定 Manager 并校验公开响应；合法不存在映射为 None，便于 workflow 做重建判断。
        raw = await self._get(("managers",), name)
        return (
            self._parse(_ManagerPayload, raw).domain()
            if raw
            else None
        )

    async def list_managers(self) -> tuple[ManagerResource, ...]:
        # 逻辑说明：验证 Manager 列表 envelope 后逐项转换，避免未校验 dict 流入业务层。
        raw = await self._json(
            ("agt", "get", "managers", "-o", "json"),
        )
        parsed = self._parse(_ManagerList, raw)
        return tuple(item.domain() for item in parsed.managers)

    async def update_manager_model(
        self,
        name: str,
        model: str,
    ) -> ManagerResource:
        # 逻辑说明：验证名字与非空模型后执行更新，再回读权威状态；不可读视为更新协议失败。
        _validate_name(name)
        if not model:
            raise ValueError("Manager model must not be empty")
        await self._command(
            (
                "agt",
                "update",
                "manager",
                "--name",
                name,
                "--model",
                model,
            ),
        )
        manager = await self.get_manager(name)
        if manager is None:
            raise AgtProtocolError(
                f"updated Manager {name!r} is not readable",
            )
        return manager

    async def update_manager_identity(
        self,
        name: str,
        identity: str,
    ) -> ManagerResource:
        # 逻辑说明：写入非空身份后回读并比较原文，只有 Controller 已收敛到目标值才报告成功。
        _validate_name(name)
        if not identity.strip():
            raise ValueError("Manager identity must not be empty")
        await self._command(
            (
                "agt",
                "update",
                "manager",
                "--name",
                name,
                "--identity",
                identity,
            ),
        )
        manager = await self.get_manager(name)
        if manager is None:
            raise AgtProtocolError(
                f"updated Manager {name!r} is not readable",
            )
        if manager.identity != identity:
            raise AgtProtocolError(
                f"Manager {name!r} identity did not converge",
            )
        return manager

    async def replace_manager_mcp_servers(
        self,
        name: str,
        servers: tuple[MCPServerDocument, ...],
    ) -> ManagerResource:
        # 逻辑说明：超时被视为结果不明；无论是否超时都回读比较完整 MCP 集合，已收敛即可成功。
        _validate_name(name)
        timeout_error: ProcessTimeout | None = None
        try:
            await self._command(
                (
                    "agt",
                    "update",
                    "manager",
                    "--name",
                    name,
                    "--mcp-servers-file",
                    "-",
                ),
                stdin=_mcp_servers_json(servers),
            )
        except ProcessTimeout as exc:
            timeout_error = exc
        manager = await self.get_manager(name)
        if manager is not None and manager.mcp_servers == servers:
            return manager
        if timeout_error is not None:
            raise timeout_error
        raise AgtProtocolError(
            f"Manager {name!r} MCP descriptors did not converge",
        )

    async def replace_worker_mcp_servers(
        self,
        name: str,
        servers: tuple[MCPServerDocument, ...],
    ) -> WorkerResource:
        # 逻辑说明：整体替换 Worker MCP 描述；命令超时后先核对外部状态，避免盲目重放副作用。
        _validate_name(name)
        timeout_error: ProcessTimeout | None = None
        try:
            await self._command(
                (
                    "agt",
                    "update",
                    "worker",
                    "--name",
                    name,
                    "--mcp-servers-file",
                    "-",
                ),
                stdin=_mcp_servers_json(servers),
            )
        except ProcessTimeout as exc:
            timeout_error = exc
        worker = await self.get_worker(name)
        if worker is not None and _worker_mcp_servers(worker) == servers:
            return worker
        if timeout_error is not None:
            raise timeout_error
        raise AgtProtocolError(
            f"Worker {name!r} MCP descriptors did not converge",
        )

    async def _delete(self, kind: str, name: str) -> None:
        # 逻辑说明：统一校验资源名并以 argv 执行删除，kind 只由内部固定调用点传入。
        _validate_name(name)
        await self._command(("agt", "delete", kind, name))

    async def _get(
        self,
        resource_parts: tuple[str, ...],
        name: str,
    ) -> dict[str, Any] | None:
        # 逻辑说明：在拼接 argv 前验证用户可控名称，并复用“404 为 None”的 JSON 边界。
        _validate_name(name)
        return await self._json_or_none(
            ("agt", "get", *resource_parts, name, "-o", "json"),
        )

    async def _json_or_none(
        self,
        argv: tuple[str, ...],
    ) -> dict[str, Any] | None:
        # 逻辑说明：运行命令并仅把明确的 not-found 归一为空；其他失败脱敏后抛错，成功必须是 JSON object。
        result = await self._process.run(argv, timeout=self._timeout)
        if result.returncode:
            error = _safe_error(result.stderr)
            if result.returncode == 1 and _is_not_found(error):
                return None
            raise AgtCommandError(
                f"agt command failed ({result.returncode}): {error}",
            )
        return _decode_json(result.stdout)

    async def _json(self, argv: tuple[str, ...]) -> dict[str, Any]:
        # 逻辑说明：要求命令成功且 stdout 是 JSON object；stderr 先脱敏再进入异常，防止 Secret 泄漏。
        result = await self._process.run(argv, timeout=self._timeout)
        if result.returncode:
            raise AgtCommandError(
                f"agt command failed ({result.returncode}): "
                f"{_safe_error(result.stderr)}",
            )
        return _decode_json(result.stdout)

    async def _command(
        self,
        argv: tuple[str, ...],
        *,
        stdin: bytes | None = None,
    ) -> ProcessResult:
        # 逻辑说明：以统一超时执行无 shell argv/可选 stdin，并把非零退出集中翻译为安全异常。
        result = await self._process.run(
            argv,
            stdin=stdin,
            timeout=self._timeout,
        )
        if result.returncode:
            raise AgtCommandError(
                f"agt command failed ({result.returncode}): "
                f"{_safe_error(result.stderr)}",
            )
        return result

    @staticmethod
    def _parse(model: type[BaseModel], raw: object) -> Any:
        # 逻辑说明：在外部 JSON 进入领域层前执行 schema 校验，并隐藏 Pydantic 内部错误细节。
        try:
            return model.model_validate(raw)
        except ValidationError as exc:
            raise AgtProtocolError(
                f"agt JSON does not match {model.__name__}",
            ) from exc


def _decode_json(stdout: bytes) -> dict[str, Any]:
    # 逻辑说明：严格按 UTF-8 解码并要求根节点为对象，拒绝 CLI 的文本、数组或截断响应。
    try:
        value = json.loads(stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AgtProtocolError("agt returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise AgtProtocolError("agt JSON root must be an object")
    return value


def _mcp_servers_json(
    servers: tuple[MCPServerDocument, ...],
) -> bytes:
    # 逻辑说明：把已验证描述符稳定排序序列化为 UTF-8 stdin，禁用 NaN 以保持跨语言 JSON 合法。
    return json.dumps(
        [
            server.model_dump(mode="json")
            for server in servers
        ],
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _worker_mcp_servers(
    worker: WorkerResource,
) -> tuple[MCPServerDocument, ...]:
    # 逻辑说明：从 Worker spec 读取数组并逐项重验 MCP schema，畸形 Controller 状态不会被当作收敛。
    value = worker.spec.get("mcpServers", [])
    if not isinstance(value, list):
        raise AgtProtocolError(
            "Worker mcpServers is not an array",
        )
    try:
        return tuple(
            MCPServerDocument.model_validate(item)
            for item in value
        )
    except ValidationError as exc:
        raise AgtProtocolError(
            "Worker mcpServers is invalid",
        ) from exc


_SENSITIVE_VALUE = re.compile(
    r"(?i)(token|password|secret|authorization|api[_-]?key)"
    r"(\s*[=:]\s*)([^,\s\"'}]+)",
)
_URI_USERINFO = re.compile(
    r"(?i)(nacos://)[^/@\s]+:[^/@\s]+@",
)


def _safe_error(stderr: bytes) -> str:
    # 逻辑说明：容错解码后遮蔽键值凭据和 URI userinfo，再截断，确保外部命令诊断可记录但不泄密。
    text = stderr.decode("utf-8", errors="replace").strip()
    text = _SENSITIVE_VALUE.sub(r"\1\2[REDACTED]", text)
    text = _URI_USERINFO.sub(r"\1[REDACTED]@", text)
    return text[:1000] or "no diagnostic output"


def _is_not_found(error: str) -> bool:
    # 逻辑说明：把 agt 的不同 404 文案归一成同一“资源不存在”判定，供幂等删除和查询分支使用；不吞掉其他错误。
    normalized = error.casefold()
    return "http 404" in normalized or "not found" in normalized


def _validate_name(name: str) -> None:
    # 逻辑说明：在名称进入 argv 前限制为 Controller 资源语法，阻断选项注入和模糊资源引用。
    if re.fullmatch(r"[a-z0-9][a-z0-9-]*", name) is None:
        raise ValueError(f"invalid resource name {name!r}")


def _unique_ports(
    ports: tuple[int, ...] | None,
) -> tuple[int, ...] | None:
    # 逻辑说明：保留 None 的“不变更”语义，但拒绝会让 Controller 配置含义重复的端口集合。
    if ports is not None and len(ports) != len(set(ports)):
        raise ValueError("exposed ports must be unique")
    return ports


def _flag(argv: list[str], name: str, value: object | None) -> None:
    # 逻辑说明：只追加实际非空值，并作为两个独立 argv 元素传递，不经过 shell 字符串插值。
    if value is not None and value != "":
        argv.extend((name, str(value)))


def _safe_package_reference(value: str | None) -> str | None:
    # 逻辑说明：解析包 URI，拒绝控制字符、userinfo 和敏感 query key，确保凭据仅来自运行环境。
    if value is None or value == "":
        return value
    if any(ord(character) < 32 for character in value):
        raise ValueError("package reference contains control characters")
    parsed = urlsplit(value)
    if parsed.username is not None or parsed.password is not None:
        raise ValueError(
            "package credentials must come from runtime configuration",
        )
    sensitive = re.compile(
        r"(?:token|secret|password|credential|authorization|"
        r"api[_-]?key|signature|sig)$",
        re.IGNORECASE,
    )
    if any(sensitive.search(key) for key, _ in parse_qsl(parsed.query)):
        raise ValueError(
            "package credentials must come from runtime configuration",
        )
    return value


def _optional_flag(
    argv: list[str],
    name: str,
    value: object | None,
) -> None:
    # 逻辑说明：只区分 None 与显式值，因此空字符串可用于需要“清空字段”的 CLI 选项。
    if value is not None:
        argv.extend((name, str(value)))


def _csv_flag(
    argv: list[str],
    name: str,
    values: tuple[object, ...],
    *,
    allow_empty: bool = False,
) -> None:
    # 逻辑说明：将受控元组编码为单个逗号参数；allow_empty 用于显式清空而非省略更新。
    if values or allow_empty:
        argv.extend((name, ",".join(map(str, values))))
