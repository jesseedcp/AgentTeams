"""Validated, secret-safe Manager configuration.

定义并校验 Manager 启动配置和 Controller 下发的运行配置。

外部输入不能直接散落到业务代码中：环境变量先被解析成 ``ManagerConfig``，远端
运行文档先被解析成 ``RuntimeDocument``。Pydantic 在边界处拒绝缺失、格式错误或
越界的数据；敏感值使用 ``SecretStr``，避免普通日志和对象打印意外泄露密钥。
这些模型通过校验并不代表拥有权限，真正的房间和工具授权仍由 policy 决定。
"""

from __future__ import annotations

import json
import os
import re
import warnings
from pathlib import Path
from typing import Any, Literal, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    model_validator,
)

CodingCLIProviderName = Literal["claude", "gemini", "qodercli"]


class MCPServerDocument(BaseModel):
    """A secret-free MCP endpoint descriptor."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(
        pattern=r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$",
    )
    url: str = Field(pattern=r"^https?://")
    transport: Literal["http", "sse"] = "http"


class PromptSources(BaseModel):
    """Object keys for the Manager's ordered prompt sources."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    soul: str = Field(min_length=1)
    agents: str = Field(min_length=1)
    tools: str = Field(min_length=1)
    heartbeat: str = Field(min_length=1)


class RuntimeDocument(BaseModel):
    """Controller-generated, secret-free desired runtime state."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    revision: int = Field(ge=0)
    manager_name: str = Field(min_length=1)
    model: str = Field(min_length=1)
    context_window: int = Field(default=150_000, gt=0)
    max_tokens: int = Field(default=128_000, gt=0)
    reasoning: bool = True
    input_modalities: tuple[str, ...] = ("text",)
    skills: tuple[str, ...] = ()
    mcp_servers: tuple[MCPServerDocument, ...] = ()
    prompt_sources: PromptSources
    heartbeat_interval_seconds: int = Field(default=1_800, gt=0)
    worker_idle_timeout_seconds: int = Field(default=43_200, gt=0)

    @model_validator(mode="after")
    def require_unique_mcp_names(self) -> RuntimeDocument:
        names = [server.name for server in self.mcp_servers]
        if len(names) != len(set(names)):
            raise ValueError("MCP server names must be unique")
        return self

    @classmethod
    def load(cls, path: Path) -> RuntimeDocument:
        """Load a document while producing a clear schema-version error."""
        raw = json.loads(path.read_text(encoding="utf-8"))
        if raw.get("schema_version") != 1:
            raise ValueError(
                f"unsupported schema_version {raw.get('schema_version')}; "
                "expected schema_version 1",
            )
        return cls.model_validate(raw)


class ExternalChannelDocument(BaseModel):
    """Versioned, secret-reference-only HTTP channel configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[2] = 2
    provider: Literal[
        "discord",
        "telegram",
        "slack",
        "feishu",
        "whatsapp",
        "signal",
        "dingtalk",
    ]
    mode: Literal["native", "relay"] = "native"
    outbound_url: str = Field(pattern=r"^https?://")
    secret_envs: dict[str, str] = Field(default_factory=dict)
    options: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def migrate_legacy_relay(
        cls,
        value: Any,
    ) -> Any:
        if not isinstance(value, dict):
            return value
        if (
            "token_env" not in value
            and "webhook_secret_env" not in value
        ):
            return value
        migrated = dict(value)
        secrets = dict(migrated.pop("secret_envs", {}))
        token_env = migrated.pop("token_env", None)
        webhook_secret_env = migrated.pop(
            "webhook_secret_env",
            None,
        )
        if token_env is not None:
            secrets.setdefault("token", token_env)
        if webhook_secret_env is not None:
            secrets.setdefault("webhook_secret", webhook_secret_env)
        migrated["secret_envs"] = secrets
        migrated["schema_version"] = 2
        migrated["mode"] = "relay"
        provider = str(migrated.get("provider", "unknown"))
        warnings.warn(
            f"legacy {provider} channel configuration migrated to relay "
            "mode; move credentials to secret_envs for native mode",
            DeprecationWarning,
            stacklevel=2,
        )
        return migrated

    @model_validator(mode="after")
    def validate_environment_references(
        self,
    ) -> ExternalChannelDocument:
        pattern = re.compile(r"^env:[A-Z][A-Z0-9_]{2,127}$")
        for name, reference in self.secret_envs.items():
            if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", name):
                raise ValueError(
                    f"invalid channel secret name {name!r}",
                )
            if not pattern.fullmatch(reference):
                raise ValueError(
                    f"channel secret {name!r} must be an env reference",
                )
        if self.provider == "signal" and self.mode != "relay":
            raise ValueError("Signal supports relay mode only")
        required = (
            {"token", "webhook_secret"}
            if self.mode == "relay"
            else {
                "telegram": {"token", "webhook_secret"},
                "slack": {"token", "signing_secret"},
                "whatsapp": {
                    "token",
                    "app_secret",
                    "verify_token",
                },
                "feishu": {"token", "verification_token"},
                "dingtalk": {"token", "webhook_secret"},
                "discord": {"token", "public_key"},
                "signal": {"token", "webhook_secret"},
            }[self.provider]
        )
        missing = sorted(required - self.secret_envs.keys())
        if missing:
            raise ValueError(
                "channel secret_envs is missing required references: "
                + ", ".join(missing),
            )
        sensitive_markers = (
            "credential",
            "password",
            "private",
            "secret",
            "token",
        )
        unsafe_options = sorted(
            name
            for name in self.options
            if any(marker in name.casefold() for marker in sensitive_markers)
        )
        if unsafe_options:
            raise ValueError(
                "channel options must not contain credentials: "
                + ", ".join(unsafe_options),
            )
        return self


class ManagerConfig(BaseModel):
    """Environment-owned process configuration.

    Credentials use ``SecretStr`` so diagnostics never reveal their values.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    manager_name: str
    manager_user_id: str
    matrix_url: str
    matrix_domain: str
    matrix_access_token: SecretStr
    matrix_password: SecretStr | None = None
    matrix_registration_token: SecretStr | None = None
    controller_url: str
    controller_auth_token: SecretStr | None
    ai_gateway_url: str
    gateway_key: SecretStr
    fs_endpoint: str
    fs_bucket: str
    fs_access_key: str
    fs_secret_key: SecretStr
    storage_prefix: str
    default_model: str
    workspace: Path
    runtime_document_path: Path
    runtime_document_key: str
    session_database: Path
    health_port: int = 18799
    heartbeat_interval_seconds: int = 1_800
    worker_idle_timeout_seconds: int = 43_200
    yolo: bool = False
    admin_user_id: str = ""
    manager_admin_room_id: str = ""
    runtime_mode: str = "local"
    session_timezone: str = "Asia/Shanghai"
    higress_admin_url: str | None = None
    higress_admin_user: str | None = None
    higress_admin_password: SecretStr | None = None
    admin_api_token: SecretStr | None = None
    external_channels: tuple[ExternalChannelDocument, ...] = ()
    host_share_root: Path | None = None
    host_read_allowlist: tuple[str, ...] = ()
    host_write_allowlist: tuple[str, ...] = ()
    coding_cli_enabled: bool = False
    coding_cli_providers: tuple[CodingCLIProviderName, ...] = ()
    coding_cli_trusted_directory: Path = Path(
        "/opt/agentteams/coding-cli/bin",
    )
    coding_cli_timeout_seconds: int = Field(default=600, gt=0, le=3_600)
    coding_cli_max_output_bytes: int = Field(
        default=64 * 1024,
        ge=1_024,
        le=1024 * 1024,
    )

    @model_validator(mode="after")
    def validate_coding_cli(self) -> ManagerConfig:
        if len(self.coding_cli_providers) != len(
            set(self.coding_cli_providers),
        ):
            raise ValueError("coding CLI providers must be unique")
        if self.coding_cli_enabled and not self.coding_cli_providers:
            raise ValueError(
                "enabled coding CLI delegation requires a provider",
            )
        return self

    @classmethod
    def from_env(cls) -> ManagerConfig:
        """Build configuration from the Controller-provided environment."""
        env = os.environ
        workspace = Path(
            env.get("AGENTTEAMS_MANAGER_WORKSPACE", str(Path.home())),
        ).resolve()
        matrix_domain = env["AGENTTEAMS_MATRIX_DOMAIN"]
        raw_admin_user = env.get("AGENTTEAMS_ADMIN_USER", "admin")
        admin_user_id = (
            raw_admin_user
            if raw_admin_user.startswith("@")
            else f"@{raw_admin_user}:{matrix_domain}"
        )
        return cls(
            manager_name=env["AGENTTEAMS_MANAGER_NAME"],
            manager_user_id=env["AGENTTEAMS_MANAGER_MATRIX_USER_ID"],
            matrix_url=env["AGENTTEAMS_MATRIX_URL"].rstrip("/"),
            matrix_domain=matrix_domain,
            matrix_access_token=SecretStr(
                env["AGENTTEAMS_MANAGER_MATRIX_TOKEN"],
            ),
            matrix_password=(
                SecretStr(env["AGENTTEAMS_MANAGER_MATRIX_PASSWORD"])
                if env.get("AGENTTEAMS_MANAGER_MATRIX_PASSWORD")
                else None
            ),
            matrix_registration_token=(
                SecretStr(
                    env["AGENTTEAMS_MATRIX_REGISTRATION_TOKEN"],
                )
                if env.get(
                    "AGENTTEAMS_MATRIX_REGISTRATION_TOKEN",
                )
                else None
            ),
            controller_url=env["AGENTTEAMS_CONTROLLER_URL"].rstrip("/"),
            controller_auth_token=(
                SecretStr(env["AGENTTEAMS_AUTH_TOKEN"])
                if env.get("AGENTTEAMS_AUTH_TOKEN")
                else None
            ),
            ai_gateway_url=env["AGENTTEAMS_AI_GATEWAY_URL"].rstrip("/"),
            gateway_key=SecretStr(
                env["AGENTTEAMS_MANAGER_GATEWAY_KEY"],
            ),
            fs_endpoint=env["AGENTTEAMS_FS_ENDPOINT"].rstrip("/"),
            fs_bucket=env["AGENTTEAMS_FS_BUCKET"],
            fs_access_key=env["AGENTTEAMS_FS_ACCESS_KEY"],
            fs_secret_key=SecretStr(env["AGENTTEAMS_FS_SECRET_KEY"]),
            storage_prefix=env.get(
                "AGENTTEAMS_STORAGE_PREFIX",
                "agentteams",
            ).strip("/"),
            default_model=env["AGENTTEAMS_DEFAULT_MODEL"],
            workspace=workspace,
            runtime_document_path=workspace / "agentscope-manager.json",
            runtime_document_key=env[
                "AGENTTEAMS_MANAGER_RUNTIME_DOCUMENT_KEY"
            ],
            session_database=workspace / "state" / "manager.db",
            health_port=int(
                env.get("AGENTTEAMS_MANAGER_HEALTH_PORT", "18799"),
            ),
            heartbeat_interval_seconds=int(
                env.get(
                    "AGENTTEAMS_MANAGER_HEARTBEAT_INTERVAL_SECONDS",
                    "1800",
                ),
            ),
            worker_idle_timeout_seconds=int(
                env.get(
                    "AGENTTEAMS_MANAGER_WORKER_IDLE_TIMEOUT_SECONDS",
                    "43200",
                ),
            ),
            yolo=env.get("AGENTTEAMS_YOLO") == "1",
            admin_user_id=admin_user_id,
            manager_admin_room_id=env[
                "AGENTTEAMS_MANAGER_ADMIN_ROOM_ID"
            ],
            runtime_mode=env.get("AGENTTEAMS_RUNTIME", "local"),
            session_timezone=env.get(
                "AGENTTEAMS_MANAGER_TIMEZONE",
                "Asia/Shanghai",
            ),
            higress_admin_url=(
                env["AGENTTEAMS_AI_GATEWAY_ADMIN_URL"].rstrip("/")
                if env.get("AGENTTEAMS_AI_GATEWAY_ADMIN_URL")
                else None
            ),
            higress_admin_user=(
                env.get("AGENTTEAMS_HIGRESS_ADMIN_USER")
                or env.get("AGENTTEAMS_ADMIN_USER")
            ),
            higress_admin_password=(
                SecretStr(
                    env.get("AGENTTEAMS_HIGRESS_ADMIN_PASSWORD")
                    or env["AGENTTEAMS_ADMIN_PASSWORD"],
                )
                if (
                    env.get("AGENTTEAMS_HIGRESS_ADMIN_PASSWORD")
                    or env.get("AGENTTEAMS_ADMIN_PASSWORD")
                )
                else None
            ),
            admin_api_token=(
                SecretStr(
                    env.get("AGENTTEAMS_MANAGER_ADMIN_TOKEN")
                    or env["AGENTTEAMS_ADMIN_PASSWORD"],
                )
                if (
                    env.get("AGENTTEAMS_MANAGER_ADMIN_TOKEN")
                    or env.get("AGENTTEAMS_ADMIN_PASSWORD")
                )
                else None
            ),
            external_channels=tuple(
                ExternalChannelDocument.model_validate(item)
                for item in json.loads(
                    env.get("AGENTTEAMS_EXTERNAL_CHANNELS", "[]"),
                )
            ),
            host_share_root=(
                Path(env["AGENTTEAMS_HOST_SHARE_ROOT"]).resolve()
                if env.get("AGENTTEAMS_HOST_SHARE_ROOT")
                else None
            ),
            host_read_allowlist=tuple(
                item.strip()
                for item in env.get(
                    "AGENTTEAMS_HOST_READ_ALLOWLIST",
                    "",
                ).split(",")
                if item.strip()
            ),
            host_write_allowlist=tuple(
                item.strip()
                for item in env.get(
                    "AGENTTEAMS_HOST_WRITE_ALLOWLIST",
                    "",
                ).split(",")
                if item.strip()
            ),
            coding_cli_enabled=env.get(
                "AGENTTEAMS_CODING_CLI_ENABLED",
                "",
            ).casefold()
            in {"1", "true", "yes"},
            coding_cli_providers=tuple(
                cast(CodingCLIProviderName, item.strip().casefold())
                for item in env.get(
                    "AGENTTEAMS_CODING_CLI_PROVIDERS",
                    "",
                ).split(",")
                if item.strip()
            ),
            coding_cli_trusted_directory=Path(
                env.get(
                    "AGENTTEAMS_CODING_CLI_TRUSTED_DIRECTORY",
                    "/opt/agentteams/coding-cli/bin",
                ),
            ).resolve(),
            coding_cli_timeout_seconds=int(
                env.get(
                    "AGENTTEAMS_CODING_CLI_TIMEOUT_SECONDS",
                    "600",
                ),
            ),
            coding_cli_max_output_bytes=int(
                env.get(
                    "AGENTTEAMS_CODING_CLI_MAX_OUTPUT_BYTES",
                    str(64 * 1024),
                ),
            ),
        )
