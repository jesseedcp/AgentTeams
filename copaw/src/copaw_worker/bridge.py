"""
Bridge: translate openclaw.json (AgentTeams Worker config) into CoPaw's
config.json + providers.json, then set COPAW_WORKING_DIR so CoPaw
picks up the right workspace.
"""

# 初学者导读：Controller 为所有 Worker runtime 生成统一的 ``openclaw.json``，
# 但 CoPaw 实际读取的是另一套 config/providers/agent 文件。本模块就是翻译层：
# 它不决定模型和身份，只把 Controller 已决定的值映射到 CoPaw 能理解的位置。
# “标准工作区”用于跨 runtime 持久化，“.copaw 工作区”是 CoPaw 的本地投影；
# 重启时可以从前者重新生成后者，不能让投影反过来覆盖 Controller 管理字段。
from __future__ import annotations

import json
import logging
import os
import shutil
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _port_remap(url: str, is_container: bool) -> str:
    """Remap container-internal :8080 to host-exposed gateway port when needed."""
    # 逻辑说明：`_port_remap` 接收 url、is_container，根据是否位于容器内，把内部网关端口转换为宿主机可达端口，返回 str；
    # 会读写本地文件。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    if not is_container and url and ":8080" in url:
        gateway_port = os.environ.get("AGENTTEAMS_PORT_GATEWAY", "18080")
        return url.replace(":8080", f":{gateway_port}")
    return url


def _is_in_container() -> bool:
    # 逻辑说明：检查 Docker 与 Podman 常用的两个容器标志文件，只要任一存在就判定为容器环境；仅查询文件元数据，不创建或修改文件。
    return Path("/.dockerenv").exists() or Path("/run/.containerenv").exists()


def _secret_dir(working_dir: Path) -> Path:
    """Return the secret dir path that copaw uses alongside working_dir."""
    # 逻辑说明：`_secret_dir` 接收 working_dir，根据 runtime 工作目录计算 CoPaw 敏感配置目录，返回 Path；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    return Path(str(working_dir) + ".secret")


def _patch_copaw_paths(working_dir: Path) -> None:
    """Patch copaw's module-level path constants to point at working_dir.

    copaw.constant captures WORKING_DIR / SECRET_DIR at import time from
    env vars, so setting COPAW_WORKING_DIR after import has no effect.
    We must update the live module objects directly.
    初学者注意：Python import 默认只执行模块顶层一次。上游 CoPaw 因而可能已经
    把旧环境变量保存成模块全局常量；这里只改 ``os.environ`` 已经太晚，必须同步
    修正已加载模块中的路径。漏掉任何一个副本，文件就可能被写进默认 Agent 目录。
    """
    # 逻辑说明：`_patch_copaw_paths` 接收 working_dir，创建敏感目录，并修正已导入 CoPaw 模块缓存的路径常量，返回 None；
    # 会读写本地文件。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    secret_dir = _secret_dir(working_dir)
    secret_dir.mkdir(parents=True, exist_ok=True)

    try:
        import copaw.constant as _const
        _const.WORKING_DIR = working_dir
        _const.SECRET_DIR = secret_dir
        _const.ACTIVE_SKILLS_DIR = (
            working_dir / "workspaces" / "default" / "skills"
        )
        _const.CUSTOMIZED_SKILLS_DIR = working_dir / "customized_skills"
        _const.MEMORY_DIR = working_dir / "memory"
        _const.CUSTOM_CHANNELS_DIR = working_dir / "custom_channels"
        _const.MODELS_DIR = working_dir / "models"
    except ImportError:
        pass

    try:
        import copaw.providers.store as _store
        _store._PROVIDERS_JSON = secret_dir / "providers.json"
        _store._LEGACY_PROVIDERS_JSON_CANDIDATES = (
            Path(__file__).resolve().parent / "providers.json",
            working_dir / "providers.json",
        )
    except ImportError:
        pass

    try:
        import copaw.envs.store as _envs
        _envs._BOOTSTRAP_WORKING_DIR = working_dir
        _envs._BOOTSTRAP_SECRET_DIR = secret_dir
        _envs._ENVS_JSON = secret_dir / "envs.json"
        _envs._LEGACY_ENVS_JSON_CANDIDATES = (working_dir / "envs.json",)
    except (ImportError, AttributeError):
        pass

    # copaw.app.channels.registry binds CUSTOM_CHANNELS_DIR via
    # `from ...constant import CUSTOM_CHANNELS_DIR` at import time, so it keeps
    # a STALE copy of the default path even after we patch copaw.constant above.
    # _discover_custom_channels() / register_custom_channel_routes() read this
    # module global at CALL time, so rebinding it here (before ChannelManager
    # starts) makes them see our working_dir/custom_channels regardless of
    # import order. Without this the patched matrix_channel.py is never
    # discovered and copaw falls back to its builtin (broken) Matrix channel.
    try:
        import copaw.app.channels.registry as _channels_registry
        _channels_registry.CUSTOM_CHANNELS_DIR = working_dir / "custom_channels"
        logger.info(
            "bridge: patched channels registry CUSTOM_CHANNELS_DIR -> %s",
            _channels_registry.CUSTOM_CHANNELS_DIR,
        )
    except ImportError:
        pass


def bridge_controller_to_copaw(
    openclaw_cfg: dict[str, Any],
    working_dir: Path,
    *,
    profile: str = "worker",
    agent: str = "default",
) -> None:
    """
    Read openclaw_cfg (parsed openclaw.json) and write:
      - <working_dir>/config.json          (global config)
      - <working_dir>/workspaces/default/agent.json (per-agent config)
      - <working_dir>/providers.json       (LLM credentials, for reference)
      - <working_dir>.secret/providers.json (where copaw actually reads from)

    Also sets COPAW_WORKING_DIR env var and patches copaw's module-level
    path constants so the running process uses the correct directory.

    """
    # 逻辑说明：`bridge_controller_to_copaw` 接收 openclaw_cfg、working_dir、profile、agent，验证 profile 和 agent 名称，
    # 把 Controller 配置写成 CoPaw 的 config、agent 与 provider 文件，返回 None；
    #
    # 会读写本地文件、会修改进程环境。失败策略：非法输入或状态会立即抛错，其他底层异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    if profile not in {"manager", "worker"}:
        raise ValueError(f"unknown bridge profile: {profile}")
    if not agent or "/" in agent or "\\" in agent or agent in {".", ".."}:
        raise ValueError(f"invalid CoPaw workspace agent: {agent!r}")

    working_dir.mkdir(parents=True, exist_ok=True)
    in_container = _is_in_container()

    _write_config_json(
        openclaw_cfg,
        working_dir,
        in_container,
        profile=profile,
    )
    _write_agent_json(
        openclaw_cfg,
        working_dir,
        in_container,
        profile=profile,
        agent=agent,
    )
    _write_providers_json(openclaw_cfg, working_dir, in_container)

    os.environ["COPAW_WORKING_DIR"] = str(working_dir)

    # Patch module-level constants (import-time values won't reflect env change)
    _patch_copaw_paths(working_dir)

    # Copy providers.json into secret_dir — that's where copaw actually reads it
    secret_dir = _secret_dir(working_dir)
    providers_src = working_dir / "providers.json"
    if providers_src.exists():
        shutil.copy2(providers_src, secret_dir / "providers.json")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_active_model(cfg: dict[str, Any]) -> dict[str, Any] | None:
    """Return the config dict of the active model from openclaw.json, or None.

    Prefers agents.defaults.model.primary ("provider_id/model_id");
    falls back to the first model of the first provider.
    """
    # 逻辑说明：`_resolve_active_model` 接收 cfg，按 primary 配置优先、首个模型兜底的顺序选择活动模型，返回 dict[str, Any] | None；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    providers_raw = cfg.get("models", {}).get("providers", {})
    if not providers_raw:
        return None

    primary = (
        cfg.get("agents", {})
        .get("defaults", {})
        .get("model", {})
        .get("primary", "")
    )

    if primary and "/" in primary:
        pid, mid = primary.split("/", 1)
        provider = providers_raw.get(pid, {})
        for m in provider.get("models", []):
            if m.get("id") == mid:
                return m

    # Fallback: first provider, first model
    for provider_cfg in providers_raw.values():
        models = provider_cfg.get("models", [])
        if models:
            return models[0]

    return None


def _resolve_context_window(cfg: dict[str, Any]) -> int | None:
    """Return the contextWindow of the active (or first) model, or None."""
    # 逻辑说明：`_resolve_context_window` 接收 cfg，从活动模型读取上下文窗口，返回 int | None；不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；
    # 本函数不额外重试，避免掩盖持续故障。
    m = _resolve_active_model(cfg)
    if m and "contextWindow" in m:
        return int(m["contextWindow"])
    return None


def _resolve_vision_enabled(cfg: dict[str, Any]) -> bool:
    """Return True if the active model declares image input support.

    The openclaw.json model's ``input`` field is a list of supported modalities
    (e.g. ["text", "image"]).  If the field is absent we assume text-only to
    avoid sending images to a model that cannot handle them.
    """
    # 逻辑说明：`_resolve_vision_enabled` 接收 cfg，从活动模型声明判断能否接收图片，返回 bool；不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    m = _resolve_active_model(cfg)
    if m is None:
        return False
    input_types = m.get("input", [])
    return "image" in input_types


def _resolve_matrix_user_id(
    matrix_raw: dict[str, Any],
    *,
    profile: str = "worker",
) -> str:
    """Resolve the Matrix MXID that CoPaw tools use for proactive sends."""
    # 逻辑说明：`_resolve_matrix_user_id` 接收 matrix_raw、profile，从显式 MXID、用户名与 homeserver 域名推导 Matrix 用户 ID，返回 str；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    explicit = matrix_raw.get("userId") or matrix_raw.get("user_id")
    if explicit:
        return str(explicit)

    env_user_id = (
        os.environ.get("AGENTTEAMS_MATRIX_USER_ID")
        or os.environ.get("COPAW_MATRIX_USER_ID")
    )
    if env_user_id:
        return env_user_id

    matrix_domain = os.environ.get("AGENTTEAMS_MATRIX_DOMAIN")
    localpart = (
        os.environ.get("AGENTTEAMS_WORKER_NAME")
        or ("manager" if profile == "manager" else "")
    )
    if matrix_domain and localpart:
        return f"@{localpart}:{matrix_domain}"

    return ""


# ---------------------------------------------------------------------------
# config.json
# ---------------------------------------------------------------------------

def _write_config_json(
    cfg: dict[str, Any],
    working_dir: Path,
    in_container: bool,
    *,
    profile: str = "worker",
) -> None:
    # 逻辑说明：`_write_config_json` 接收 cfg、working_dir、in_container、profile，合并并写入 CoPaw 全局 config.json，返回 None；
    # 会读写本地文件。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    config_path = working_dir / "config.json"
    if config_path.exists():
        try:
            global_cfg = json.loads(config_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"invalid CoPaw config JSON: {config_path}",
            ) from exc
        if not isinstance(global_cfg, dict):
            raise ValueError(f"CoPaw config must be an object: {config_path}")
    else:
        template_path = (
            Path(__file__).resolve().parent / "templates" / "config.json"
        )
        if template_path.exists():
            global_cfg = json.loads(
                template_path.read_text(encoding="utf-8"),
            )
        else:
            global_cfg = {
                "security": {
                    "tool_guard": {"enabled": True},
                    "file_guard": {
                        "enabled": True,
                        "sensitive_files": [],
                    },
                    "skill_scanner": {"mode": "off"},
                },
            }

    # Lite CoPaw 0.0.x reads channels from the root config.json, while
    # CoPaw 1.0.2+ reads them from workspaces/<agent>/agent.json. Keep the
    # Matrix controller fields in both schemas so either runtime can start
    # the same Worker image. Non-controller fields remain user-owned.
    channels = global_cfg.setdefault("channels", {})
    if not isinstance(channels, dict):
        raise ValueError(f"CoPaw channels must be an object: {config_path}")
    matrix_ch = channels.setdefault("matrix", {})
    if not isinstance(matrix_ch, dict):
        raise ValueError(
            f"CoPaw Matrix channel must be an object: {config_path}",
        )
    _overlay_matrix_channel(
        matrix_ch,
        cfg,
        in_container,
        profile=profile,
    )
    global_cfg = _drop_none_dict_values(global_cfg)
    config_path.write_text(
        json.dumps(global_cfg, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )




# ---------------------------------------------------------------------------
# agent.json — per-agent config (CoPaw 1.0.2+ reads this, not config.json)
# ---------------------------------------------------------------------------

def _merge_unique(existing: object, incoming: object) -> list[str]:
    # 逻辑说明：`_merge_unique` 接收 existing、incoming，保持原顺序合并并去重字符串列表，返回 list[str]；不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；
    # 本函数不额外重试，避免掩盖持续故障。
    values: list[str] = []
    for source in (existing, incoming):
        if not isinstance(source, list):
            continue
        for item in source:
            value = str(item)
            if value not in values:
                values.append(value)
    return values


def _deep_merge_local_wins(
    remote: dict[str, Any],
    local: dict[str, Any],
) -> dict[str, Any]:
    # 逻辑说明：`_deep_merge_local_wins` 接收 remote、local，递归合并字典，并在叶子冲突时保留本地值，返回 dict[str, Any]；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    result: dict[str, Any] = dict(remote)
    for key, local_value in local.items():
        remote_value = result.get(key)
        if isinstance(remote_value, dict) and isinstance(local_value, dict):
            result[key] = _deep_merge_local_wins(
                remote_value,
                local_value,
            )
        else:
            result[key] = local_value
    return result


def _drop_none_dict_values(value: Any) -> Any:
    """Remove persisted nulls so both CoPaw config generations can load.

    Standard CoPaw writes optional fields such as ``media_dir`` and
    ``agents.defaults`` as JSON null. Lite CoPaw models those same fields as
    non-null values with defaults, so a standard-to-lite switch otherwise
    fails validation before any channel starts.
    """
    # 逻辑说明：`_drop_none_dict_values` 接收 value，递归删除字典中的 None，保留列表结构，返回 Any；不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；
    # 本函数不额外重试，避免掩盖持续故障。
    if isinstance(value, dict):
        return {
            key: _drop_none_dict_values(item)
            for key, item in value.items()
            if item is not None
        }
    if isinstance(value, list):
        return [_drop_none_dict_values(item) for item in value]
    return value


def _overlay_matrix_channel(
    matrix_ch: dict[str, Any],
    cfg: dict[str, Any],
    in_container: bool,
    *,
    profile: str,
) -> None:
    """Refresh controller-owned Matrix fields without erasing user fields."""
    # 逻辑说明：`_overlay_matrix_channel` 接收 matrix_ch、cfg、in_container、profile，
    # 把 Controller 管理的 Matrix 字段覆盖到现有本地 channel 配置，返回 None；
    #
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    matrix_raw = cfg.get("channels", {}).get("matrix", {})
    matrix_raw = matrix_raw if isinstance(matrix_raw, dict) else {}
    homeserver = _port_remap(
        str(matrix_raw.get("homeserver") or ""),
        in_container,
    )
    access_token = str(matrix_raw.get("accessToken") or "")
    user_id = _resolve_matrix_user_id(matrix_raw, profile=profile)

    matrix_ch["enabled"] = matrix_raw.get("enabled", True)
    if homeserver:
        matrix_ch["homeserver"] = homeserver
    if access_token:
        matrix_ch["access_token"] = access_token
    if user_id:
        matrix_ch["user_id"] = user_id

    dm_cfg = matrix_raw.get("dm")
    dm_cfg = dm_cfg if isinstance(dm_cfg, dict) else {}
    matrix_ch["allow_from"] = _merge_unique(
        matrix_ch.get("allow_from"),
        dm_cfg.get("allowFrom"),
    )
    matrix_ch["group_allow_from"] = _merge_unique(
        matrix_ch.get("group_allow_from"),
        matrix_raw.get("groupAllowFrom"),
    )

    remote_groups = matrix_raw.get("groups")
    remote_groups = (
        remote_groups if isinstance(remote_groups, dict) else {}
    )
    local_groups = matrix_ch.get("groups")
    local_groups = (
        local_groups if isinstance(local_groups, dict) else {}
    )
    matrix_ch["groups"] = _deep_merge_local_wins(
        remote_groups,
        local_groups,
    )
    matrix_ch["filter_tool_messages"] = bool(
        matrix_raw.get("filterToolMessages", False),
    )
    matrix_ch["filter_thinking"] = bool(
        matrix_raw.get("filterThinking", True),
    )
    matrix_ch["vision_enabled"] = _resolve_vision_enabled(cfg)
    if dm_cfg.get("policy") is not None:
        matrix_ch["dm_policy"] = dm_cfg["policy"]
    if matrix_raw.get("groupPolicy") is not None:
        matrix_ch["group_policy"] = matrix_raw["groupPolicy"]

    history_limit = matrix_raw.get("historyLimit")
    if history_limit is None:
        history_limit = (
            cfg.get("messages", {})
            .get("groupChat", {})
            .get("historyLimit")
        )
    if history_limit is not None:
        matrix_ch["history_limit"] = int(history_limit)


def _resolve_embedding_config(
    cfg: dict[str, Any],
    *,
    in_container: bool,
) -> dict[str, Any] | None:
    # 逻辑说明：`_resolve_embedding_config` 接收 cfg、in_container，解析 memorySearch 并生成 CoPaw embedding 配置，
    # 返回 dict[str, Any] | None；
    #
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    memory_search = (
        cfg.get("agents", {})
        .get("defaults", {})
        .get("memorySearch")
    )
    if not isinstance(memory_search, dict):
        return None
    remote = memory_search.get("remote")
    remote = remote if isinstance(remote, dict) else {}
    base_url = _port_remap(
        str(remote.get("baseUrl") or ""),
        in_container,
    )
    result: dict[str, Any] = {
        "backend": str(memory_search.get("provider") or "openai"),
        "model_name": str(memory_search.get("model") or ""),
        "dimensions": int(
            memory_search.get("outputDimensionality") or 1024,
        ),
    }
    if base_url:
        result["base_url"] = base_url
    if remote.get("apiKey"):
        result["api_key"] = str(remote["apiKey"])
    return result


def _openclaw_heartbeat(cfg: dict[str, Any]) -> dict[str, Any] | None:
    # 逻辑说明：`_openclaw_heartbeat` 接收 cfg，读取并规范化 OpenClaw heartbeat 配置，返回 dict[str, Any] | None；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    source = (
        cfg.get("agents", {})
        .get("defaults", {})
        .get("heartbeat")
    )
    if not isinstance(source, dict) or not source:
        return None
    heartbeat: dict[str, Any] = {
        "enabled": bool(source.get("enabled", True)),
    }
    for source_key, target_key in (
        ("every", "every"),
        ("target", "target"),
        ("activeHours", "active_hours"),
    ):
        if source.get(source_key) is not None:
            heartbeat[target_key] = source[source_key]
    return heartbeat


def _write_agent_json(
    cfg: dict[str, Any],
    working_dir: Path,
    in_container: bool,
    *,
    profile: str = "worker",
    agent: str = "default",
) -> None:
    """Create agent.json from template, then overlay Matrix channel config.

    CoPaw 1.0.2+ reads workspace/agent.json for per-agent configuration.
    The template provides defaults; we overlay controller-owned fields
    (Matrix access_token, homeserver, allowlists, context window).
    """
    # 逻辑说明：`_write_agent_json` 接收 cfg、working_dir、in_container、profile、agent，生成指定 workspace 的 agent.json，
    # 同时保留允许的本地字段，返回 None；
    #
    # 会读写本地文件。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    workspace_dir = working_dir / "workspaces" / agent
    workspace_dir.mkdir(parents=True, exist_ok=True)
    agent_path = workspace_dir / "agent.json"

    # Install from template if missing
    if not agent_path.exists():
        template_name = f"agent.{profile}.json"
        tmpl_path = (
            Path(__file__).resolve().parent
            / "templates"
            / template_name
        )
        if tmpl_path.exists():
            shutil.copy2(tmpl_path, agent_path)
        else:
            minimal = {
                "id": agent,
                "name": (
                    "Manager"
                    if profile == "manager"
                    else "Default Agent"
                ),
                "language": "zh",
                "channels": {
                    "console": {"enabled": True},
                    "matrix": {
                        "enabled": True,
                        "filter_tool_messages": False,
                        "filter_thinking": True,
                        "allow_from": [],
                        "group_allow_from": [],
                        "groups": {},
                    },
                },
                "running": {"max_iters": 200},
            }
            agent_path.write_text(
                json.dumps(minimal, indent=2),
                encoding="utf-8",
            )

    # Load existing agent.json
    try:
        with agent_path.open(encoding="utf-8") as f:
            agent_cfg = json.load(f)
    except (OSError, json.JSONDecodeError):
        agent_cfg = {"id": agent, "channels": {}, "running": {}}

    matrix_ch = agent_cfg.setdefault("channels", {}).setdefault("matrix", {})
    if not isinstance(matrix_ch, dict):
        raise ValueError("CoPaw Matrix channel must be an object")
    _overlay_matrix_channel(
        matrix_ch,
        cfg,
        in_container,
        profile=profile,
    )

    # Bridge context window
    context_window = _resolve_context_window(cfg)
    if context_window is not None:
        agent_cfg.setdefault("running", {})["max_input_length"] = context_window
    running = agent_cfg.setdefault("running", {})
    embedding_config = _resolve_embedding_config(
        cfg,
        in_container=in_container,
    )
    if embedding_config is None:
        running.pop("embedding_config", None)
    else:
        running["embedding_config"] = embedding_config

    if "heartbeat" not in agent_cfg:
        heartbeat = _openclaw_heartbeat(cfg)
        if heartbeat is not None:
            agent_cfg["heartbeat"] = heartbeat

    # Set workspace_dir
    agent_cfg.setdefault("workspace_dir", str(workspace_dir))

    with agent_path.open("w", encoding="utf-8") as f:
        json.dump(agent_cfg, f, indent=2, ensure_ascii=False)

# ---------------------------------------------------------------------------
# providers.json
# ---------------------------------------------------------------------------

def _write_providers_json(
    cfg: dict[str, Any],
    working_dir: Path,
    in_container: bool,
) -> None:
    # 逻辑说明：`_write_providers_json` 接收 cfg、working_dir、in_container，把活动模型 Provider 与凭据写入 CoPaw provider 文件，返回 None；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    providers_raw = cfg.get("models", {}).get("providers", {})

    custom_providers: dict[str, Any] = {}
    active_provider_id = ""
    active_model = ""

    for provider_id, provider_cfg in providers_raw.items():
        base_url = _port_remap(
            provider_cfg.get("baseUrl", ""), in_container
        )
        api_key = provider_cfg.get("apiKey", "")

        models_raw = provider_cfg.get("models", [])
        models = [
            {"id": m["id"], "name": m.get("name", m["id"])}
            for m in models_raw
            if m.get("id")
        ]

        custom_providers[provider_id] = {
            "id": provider_id,
            "name": provider_id,
            "default_base_url": base_url,
            "api_key_prefix": "",
            "models": models,
            "base_url": base_url,
            "api_key": api_key,
            "chat_model": "OpenAIChatModel",
        }

        # Use first provider + first model as active LLM
        if not active_provider_id and models:
            active_provider_id = provider_id
            active_model = models[0]["id"]

    # Resolve active model from agents.defaults.model.primary
    # Format: "provider_id/model_id"
    primary = (
        cfg.get("agents", {})
        .get("defaults", {})
        .get("model", {})
        .get("primary", "")
    )
    if primary and "/" in primary:
        pid, mid = primary.split("/", 1)
        if pid in custom_providers:
            active_provider_id = pid
            active_model = mid

    providers_data: dict[str, Any] = {
        "providers": {},
        "custom_providers": custom_providers,
        "active_llm": {
            "provider_id": active_provider_id,
            "model": active_model,
        },
    }

    providers_path = working_dir / "providers.json"
    with open(providers_path, "w") as f:
        json.dump(providers_data, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Standard-to-runtime materialization
# ---------------------------------------------------------------------------

def _workspace_dir(runtime_dir: Path) -> Path:
    return runtime_dir / "workspaces" / "default"


def sync_outer_prompt_files_to_inner(
    standard_dir: Path,
    runtime_dir: Path,
) -> None:
    """Project canonical prompt files into CoPaw's default workspace."""
    # 逻辑说明：`sync_outer_prompt_files_to_inner` 接收 standard_dir、runtime_dir，
    # 把标准工作区的提示文件投影到 CoPaw default workspace，返回 None；
    #
    # 会读写本地文件。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    workspace_dir = _workspace_dir(runtime_dir)
    workspace_dir.mkdir(parents=True, exist_ok=True)

    for name in ("SOUL.md", "AGENTS.md", "PROFILE.md", "TOOLS.md"):
        source = standard_dir / name
        if source.exists():
            shutil.copy2(source, workspace_dir / name)

    heartbeat_source = standard_dir / "HEARTBEAT.md"
    heartbeat_target = workspace_dir / "HEARTBEAT.md"
    if heartbeat_source.exists() and not heartbeat_target.exists():
        shutil.copy2(heartbeat_source, heartbeat_target)


def sync_mcporter_config_to_runtime(
    standard_dir: Path,
    runtime_dir: Path,
) -> Path | None:
    """Project the current or legacy mcporter config into the workspace."""
    # 逻辑说明：`sync_mcporter_config_to_runtime` 接收 standard_dir、runtime_dir，从当前或旧路径选择 mcporter 配置并复制到 runtime，
    # 返回 Path | None；
    #
    # 会读写本地文件。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    candidates = (
        standard_dir / "config" / "mcporter.json",
        standard_dir / "mcporter-servers.json",
    )
    source = next((path for path in candidates if path.exists()), None)
    if source is None:
        return None
    target = _workspace_dir(runtime_dir) / "config" / "mcporter.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    return target


def _remove_path(path: Path) -> None:
    # 逻辑说明：`_remove_path` 接收 path，按文件、符号链接或目录类型安全删除指定路径，返回 None；会读写本地文件。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    if path.is_symlink() or path.is_file():
        path.unlink(missing_ok=True)
    elif path.exists():
        shutil.rmtree(path)


def _load_skill_manifest(path: Path) -> dict[str, Any]:
    # 逻辑说明：`_load_skill_manifest` 接收 path，读取 Skill manifest；文件不存在或内容无效时返回空配置，返回 dict[str, Any]；
    # 不修改外部状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    if path.exists():
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(value, dict):
                return value
        except (OSError, json.JSONDecodeError):
            logger.warning("Replacing invalid CoPaw skill manifest: %s", path)
    return {
        "schema_version": "workspace-skill-manifest.v1",
        "version": 1,
        "skills": {},
    }


def sync_skills_to_runtime(
    standard_dir: Path,
    runtime_dir: Path,
    skill_names: Iterable[str],
) -> list[str]:
    """Expose the exact Controller-owned skill set to CoPaw."""
    # 逻辑说明：`sync_skills_to_runtime` 接收 standard_dir、runtime_dir、skill_names，
    # 让 runtime 中的 Skill 集合精确匹配 Controller 指定列表，返回 list[str]；
    #
    # 会读写本地文件。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    requested = list(dict.fromkeys(str(name) for name in skill_names if name))
    requested_set = set(requested)
    standard_skills = standard_dir / "skills"
    standard_skills.mkdir(parents=True, exist_ok=True)

    for child in list(standard_skills.iterdir()):
        if child.is_dir() and child.name not in requested_set:
            shutil.rmtree(child)

    installed: list[str] = []
    for name in requested:
        skill_dir = standard_skills / name
        if not skill_dir.is_dir():
            continue
        installed.append(name)
        for script in skill_dir.rglob("*.sh"):
            try:
                script.chmod(script.stat().st_mode | 0o111)
            except OSError:
                logger.warning(
                    "Unable to restore executable bit on %s",
                    script,
                )

    workspace_dir = _workspace_dir(runtime_dir)
    workspace_dir.mkdir(parents=True, exist_ok=True)
    workspace_skills = workspace_dir / "skills"
    if not (
        workspace_skills.is_symlink()
        and workspace_skills.resolve() == standard_skills.resolve()
    ):
        _remove_path(workspace_skills)
        relative_target = os.path.relpath(
            standard_skills,
            workspace_skills.parent,
        )
        try:
            workspace_skills.symlink_to(
                relative_target,
                target_is_directory=True,
            )
        except OSError:
            # Native Windows normally runs CoPaw in Linux containers, but
            # developer-mode-free test hosts cannot create directory
            # symlinks. A refreshed copy preserves behavior for that fallback
            # environment; Linux runtime deployments still use the canonical
            # zero-copy projection above.
            shutil.copytree(
                standard_skills,
                workspace_skills,
                dirs_exist_ok=True,
            )

    manifest_path = workspace_dir / "skill.json"
    manifest = _load_skill_manifest(manifest_path)
    manifest["schema_version"] = "workspace-skill-manifest.v1"
    manifest["version"] = 1
    skills = manifest.get("skills")
    if not isinstance(skills, dict):
        skills = {}
        manifest["skills"] = skills
    for name in installed:
        item = skills.get(name)
        item = dict(item) if isinstance(item, dict) else {}
        item["enabled"] = True
        item.setdefault("channels", ["all"])
        item.setdefault("source", "agentteams")
        skills[name] = item
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return installed


def _seed_prompt_fallback(
    path: Path,
    loader: Callable[[], str] | None,
) -> None:
    # 逻辑说明：`_seed_prompt_fallback` 接收 path、loader，仅在提示文件缺失时用 loader 结果创建兜底文件，返回 None；
    # 会读写本地文件。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    if path.exists() or loader is None:
        return
    value = loader()
    if value:
        path.write_text(value, encoding="utf-8")


def bridge_standard_to_runtime(
    standard_dir: Path,
    runtime_dir: Path,
    openclaw_cfg: dict[str, Any],
    *,
    skill_names: Iterable[str] = (),
    profile: str = "worker",
) -> None:
    """Apply config and project canonical Worker files into CoPaw."""
    # 逻辑说明：`bridge_standard_to_runtime` 接收 standard_dir、runtime_dir、openclaw_cfg、skill_names、profile，
    # 依次翻译配置并投影提示、Skill 和 MCP 文件到 CoPaw runtime，返回 None；
    #
    # 会读写本地文件。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    standard_dir.mkdir(parents=True, exist_ok=True)
    bridge_controller_to_copaw(
        openclaw_cfg,
        runtime_dir,
        profile=profile,
    )
    sync_outer_prompt_files_to_inner(standard_dir, runtime_dir)
    sync_mcporter_config_to_runtime(standard_dir, runtime_dir)
    sync_skills_to_runtime(standard_dir, runtime_dir, skill_names)

    from copaw_worker.hooks.credential_guard import apply_credential_guard

    apply_credential_guard(standard_dir, runtime_dir)


def refresh_standard_to_runtime(
    standard_dir: Path,
    runtime_dir: Path,
    openclaw_cfg: dict[str, Any],
    *,
    skill_names: Iterable[str] | None = None,
    get_soul: Callable[[], str] | None = None,
    get_agents_md: Callable[[], str] | None = None,
    profile: str = "worker",
) -> None:
    """Refresh CoPaw after Controller-owned files change."""
    # 逻辑说明：`refresh_standard_to_runtime` 接收标准/runtime 目录、配置、Skill 和提示 loader，
    # 在 Controller 管理文件更新后刷新 runtime 投影，返回 None；
    #
    # 会读写本地文件。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    standard_dir.mkdir(parents=True, exist_ok=True)
    _seed_prompt_fallback(standard_dir / "SOUL.md", get_soul)
    _seed_prompt_fallback(standard_dir / "AGENTS.md", get_agents_md)
    if skill_names is None:
        skills_dir = standard_dir / "skills"
        skill_names = (
            sorted(
                child.name
                for child in skills_dir.iterdir()
                if child.is_dir()
            )
            if skills_dir.is_dir()
            else ()
        )
    bridge_standard_to_runtime(
        standard_dir,
        runtime_dir,
        openclaw_cfg,
        skill_names=skill_names,
        profile=profile,
    )



# ---------------------------------------------------------------------------
# Runtime-to-standard sync (worker uses this to push edits back to sync root)
# ---------------------------------------------------------------------------

def bridge_runtime_to_standard(standard_dir):
    """Materialize runtime-space edits back into the standard sync root."""
    # 逻辑说明：把 CoPaw runtime workspace 中由 Agent 修改且较新的 AGENTS.md、SOUL.md、HEARTBEAT.md 回写到标准同步根；具体比较、容错和文件写入由同步 helper 统一处理。
    sync_inner_prompt_files_to_outer(standard_dir)


def sync_inner_prompt_files_to_outer(local_dir):
    """Copy agent-edited prompt files from CoPaw workspace back to sync root."""
    # 逻辑说明：`sync_inner_prompt_files_to_outer` 接收 local_dir，把 CoPaw 内部可持久提示文件同步回标准工作区，返回 约定结果；
    # 会读写本地文件。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    inner_outer_files = ("AGENTS.md", "SOUL.md", "HEARTBEAT.md")
    copaw_ws_dir = Path(local_dir) / ".copaw" / "workspaces" / "default"
    for name in inner_outer_files:
        inner = copaw_ws_dir / name
        outer = Path(local_dir) / name
        if not inner.exists():
            continue
        try:
            inner_mtime = inner.stat().st_mtime
        except OSError:
            continue
        outer_mtime = outer.stat().st_mtime if outer.exists() else 0
        if inner_mtime > outer_mtime:
            inner_content = inner.read_text(errors="replace")
            outer_content = outer.read_text(errors="replace") if outer.exists() else ""
            if inner_content != outer_content:
                outer.write_text(inner_content)
                logger.debug(
                    "Inner->Outer sync: .copaw/workspaces/default/%s -> %s",
                    name,
                    name,
                )

# ---------------------------------------------------------------------------
# CLI entry point for Worker bridge diagnostics and compatibility
# ---------------------------------------------------------------------------

def _main_cli(argv=None):
    # 逻辑说明：`_main_cli` 接收 argv，解析 bridge 子命令参数并执行对应同步方向，返回 约定结果；会读写本地文件。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m copaw_worker.bridge",
        description="Bridge Controller config into CoPaw runtime files.",
    )
    parser.add_argument("--openclaw-json", required=True,
                        help="Path to openclaw.json")
    parser.add_argument("--working-dir", required=True,
                        help="CoPaw working dir (e.g. ~/.copaw)")
    parser.add_argument("--profile", default="manager",
                        choices=["worker", "manager"],
                        help="Template profile (default: manager)")
    args = parser.parse_args(argv)

    import json as _json
    from pathlib import Path as _Path

    openclaw_path = _Path(args.openclaw_json)
    if not openclaw_path.exists():
        print(f"ERROR: {openclaw_path} not found", flush=True)
        return 1

    working_dir = _Path(args.working_dir)
    working_dir.mkdir(parents=True, exist_ok=True)

    with open(openclaw_path) as f:
        controller_config = _json.load(f)

    bridge_controller_to_copaw(
        controller_config,
        working_dir,
        profile=args.profile,
    )
    return 0


if __name__ == "__main__":
    import sys as _sys
    _sys.exit(_main_cli())
