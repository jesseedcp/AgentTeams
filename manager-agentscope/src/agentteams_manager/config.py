"""Validated, secret-safe Manager configuration."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr


class MCPServerDocument(BaseModel):
    """A secret-free MCP endpoint descriptor."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1)
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

    @classmethod
    def from_env(cls) -> ManagerConfig:
        """Build configuration from the Controller-provided environment."""
        env = os.environ
        workspace = Path(
            env.get("AGENTTEAMS_MANAGER_WORKSPACE", str(Path.home())),
        ).resolve()
        return cls(
            manager_name=env["AGENTTEAMS_MANAGER_NAME"],
            manager_user_id=env["AGENTTEAMS_MANAGER_MATRIX_USER_ID"],
            matrix_url=env["AGENTTEAMS_MATRIX_URL"].rstrip("/"),
            matrix_domain=env["AGENTTEAMS_MATRIX_DOMAIN"],
            matrix_access_token=SecretStr(
                env["AGENTTEAMS_MANAGER_MATRIX_TOKEN"],
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
        )
