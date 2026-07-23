"""Typed command boundary for the AgentTeams Controller CLI."""

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
    model_validator,
)

from agentteams_manager.domain.models import (
    HumanResource,
    TeamResource,
    WorkerResource,
)

from .process import ProcessResult

WorkerRuntime = Literal[
    "openclaw",
    "copaw",
    "hermes",
    "qwenpaw",
    "openhuman",
]
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
    expose: tuple[int, ...] = ()
    team: ResourceName | None = None
    role: Literal["team_leader", "worker"] | None = None


class WorkerUpdateRequest(_Request):
    name: ResourceName
    model: str | None = None
    runtime: WorkerRuntime | None = None
    image: str | None = None
    identity: str | None = None
    soul: str | None = None
    skills: tuple[str, ...] | None = None
    package_uri: str | None = None
    expose: tuple[int, ...] | None = None

    @model_validator(mode="after")
    def require_change(self) -> WorkerUpdateRequest:
        changed = (
            self.model,
            self.runtime,
            self.image,
            self.identity,
            self.soul,
            self.skills,
            self.package_uri,
            self.expose,
        )
        if all(value is None for value in changed):
            raise ValueError("at least one Worker field must change")
        return self


class TeamCreateRequest(_Request):
    name: ResourceName
    leader_name: ResourceName
    workers: tuple[ResourceName, ...] = ()
    team_name: ResourceName | None = None
    leader_model: str | None = None
    description: str | None = None
    leader_heartbeat_every: str | None = None
    worker_idle_timeout: str | None = None


class HumanCreateRequest(_Request):
    name: ResourceName
    display_name: str = Field(min_length=1)
    email: str | None = None
    permission_level: int = Field(ge=1, le=3)
    accessible_teams: tuple[ResourceName, ...] = ()
    accessible_workers: tuple[ResourceName, ...] = ()
    note: str | None = None


class _WorkerPayload(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str
    phase: str
    model: str = ""
    runtime: WorkerRuntime = "openclaw"
    image: str = ""
    container_state: str = Field(default="", alias="containerState")
    matrix_user_id: str = Field(default="", alias="matrixUserID")
    room_id: str = Field(default="", alias="roomID")
    message: str = ""
    team: str = ""
    role: str = ""

    def domain(self) -> WorkerResource:
        return WorkerResource(
            name=self.name,
            phase=self.phase,
            model=self.model or None,
            runtime=self.runtime,
            room_id=self.room_id or None,
            matrix_user_id=self.matrix_user_id or None,
            team=self.team or None,
            role=self.role or None,
            spec={"image": self.image} if self.image else {},
            status={
                "containerState": self.container_state,
                "message": self.message,
            },
        )


class _TeamPayload(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str
    phase: str = "Pending"
    description: str = ""
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

    def domain(self) -> TeamResource:
        return TeamResource(
            name=self.name,
            leader=self.leader_name,
            workers=self.worker_names,
            phase=self.phase,
            spec={
                "description": self.description,
                "teamRoomID": self.team_room_id,
                "leaderDMRoomID": self.leader_dm_room_id,
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


class AgtClient:
    """The only Python module authorized to invoke ``agt``."""

    def __init__(
        self,
        process: ProcessPort,
        *,
        timeout: float = 30,
    ) -> None:
        self._process = process
        self._timeout = timeout

    async def get_worker(self, name: str) -> WorkerResource | None:
        raw = await self._get(("workers",), name)
        return self._parse(_WorkerPayload, raw).domain() if raw else None

    async def list_workers(self) -> tuple[WorkerResource, ...]:
        raw = await self._json(("agt", "get", "workers", "-o", "json"))
        return tuple(item.domain() for item in self._parse(_WorkerList, raw).workers)

    async def create_worker(
        self,
        request: WorkerCreateRequest,
    ) -> WorkerResource | None:
        argv = ["agt", "create", "worker", "--name", request.name]
        _flag(argv, "--model", request.model)
        _flag(argv, "--runtime", request.runtime)
        _flag(argv, "--image", request.image)
        _flag(argv, "--identity", request.identity)
        _flag(argv, "--soul", request.soul)
        _csv_flag(argv, "--skills", request.skills)
        _flag(argv, "--package", request.package_uri)
        if request.expose:
            _flag(argv, "--expose", ",".join(map(str, request.expose)))
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
        argv = ["agt", "update", "worker", "--name", request.name]
        _flag(argv, "--model", request.model)
        _flag(argv, "--runtime", request.runtime)
        _flag(argv, "--image", request.image)
        _flag(argv, "--identity", request.identity)
        _flag(argv, "--soul", request.soul)
        if request.skills is not None:
            _csv_flag(argv, "--skills", request.skills, allow_empty=True)
        _flag(argv, "--package", request.package_uri)
        if request.expose is not None:
            _flag(argv, "--expose", ",".join(map(str, request.expose)))
        await self._command(tuple(argv))
        worker = await self.get_worker(request.name)
        if worker is None:
            raise AgtProtocolError(
                f"updated worker {request.name!r} is not readable",
            )
        return worker

    async def apply_worker_package(
        self,
        *,
        name: str,
        package_uri: str,
        expected_digest: str,
        runtime: WorkerRuntime,
    ) -> WorkerResource:
        """Apply one digest-bound Nacos package and prove it is readable."""
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
        return await self._lifecycle(name, "sleep")

    async def wake_worker(self, name: str) -> WorkerResource:
        return await self._lifecycle(name, "wake")

    async def _lifecycle(
        self,
        name: str,
        action: str,
    ) -> WorkerResource:
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
        await self._delete("worker", name)

    async def get_team(self, name: str) -> TeamResource | None:
        raw = await self._get(("teams",), name)
        return self._parse(_TeamPayload, raw).domain() if raw else None

    async def list_teams(self) -> tuple[TeamResource, ...]:
        raw = await self._json(("agt", "get", "teams", "-o", "json"))
        return tuple(item.domain() for item in self._parse(_TeamList, raw).teams)

    async def create_team(self, request: TeamCreateRequest) -> None:
        argv = [
            "agt",
            "create",
            "team",
            "--name",
            request.name,
            "--leader-name",
            request.leader_name,
        ]
        _csv_flag(argv, "--workers", request.workers)
        _flag(argv, "--team-name", request.team_name)
        _flag(argv, "--leader-model", request.leader_model)
        _flag(argv, "--description", request.description)
        _flag(
            argv,
            "--leader-heartbeat-every",
            request.leader_heartbeat_every,
        )
        _flag(
            argv,
            "--worker-idle-timeout",
            request.worker_idle_timeout,
        )
        await self._command(tuple(argv))

    async def apply_team(self, name: str, document: bytes) -> TeamResource:
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
        await self._delete("team", name)

    async def get_human(self, name: str) -> HumanResource | None:
        raw = await self._get(("humans",), name)
        return self._parse(_HumanPayload, raw).domain() if raw else None

    async def list_humans(self) -> tuple[HumanResource, ...]:
        raw = await self._json(("agt", "get", "humans", "-o", "json"))
        return tuple(
            item.domain()
            for item in self._parse(_HumanList, raw).humans
        )

    async def create_human(self, request: HumanCreateRequest) -> None:
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

    async def delete_human(self, name: str) -> None:
        await self._delete("human", name)

    async def _delete(self, kind: str, name: str) -> None:
        _validate_name(name)
        await self._command(("agt", "delete", kind, name))

    async def _get(
        self,
        resource_parts: tuple[str, ...],
        name: str,
    ) -> dict[str, Any] | None:
        _validate_name(name)
        return await self._json_or_none(
            ("agt", "get", *resource_parts, name, "-o", "json"),
        )

    async def _json_or_none(
        self,
        argv: tuple[str, ...],
    ) -> dict[str, Any] | None:
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
        try:
            return model.model_validate(raw)
        except ValidationError as exc:
            raise AgtProtocolError(
                f"agt JSON does not match {model.__name__}",
            ) from exc


def _decode_json(stdout: bytes) -> dict[str, Any]:
    try:
        value = json.loads(stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AgtProtocolError("agt returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise AgtProtocolError("agt JSON root must be an object")
    return value


_SENSITIVE_VALUE = re.compile(
    r"(?i)(token|password|secret|authorization|api[_-]?key)"
    r"(\s*[=:]\s*)([^,\s\"'}]+)",
)
_URI_USERINFO = re.compile(
    r"(?i)(nacos://)[^/@\s]+:[^/@\s]+@",
)


def _safe_error(stderr: bytes) -> str:
    text = stderr.decode("utf-8", errors="replace").strip()
    text = _SENSITIVE_VALUE.sub(r"\1\2[REDACTED]", text)
    text = _URI_USERINFO.sub(r"\1[REDACTED]@", text)
    return text[:1000] or "no diagnostic output"


def _is_not_found(error: str) -> bool:
    normalized = error.casefold()
    return "http 404" in normalized or "not found" in normalized


def _validate_name(name: str) -> None:
    if re.fullmatch(r"[a-z0-9][a-z0-9-]*", name) is None:
        raise ValueError(f"invalid resource name {name!r}")


def _flag(argv: list[str], name: str, value: object | None) -> None:
    if value is not None and value != "":
        argv.extend((name, str(value)))


def _csv_flag(
    argv: list[str],
    name: str,
    values: tuple[object, ...],
    *,
    allow_empty: bool = False,
) -> None:
    if values or allow_empty:
        argv.extend((name, ",".join(map(str, values))))
