"""Runtime desired-state update support for qwenpaw-worker."""

# 初学者导读：这是 QwenPaw Worker 的“期望状态应用器”。Controller 把 Worker
# 应该加入的 Team/Matrix 房间、模型、MCP、身份与 Agent 包写入 runtime config；
# 本模块读取其中的 generation，只在版本真正变化时把差异应用到本地 QwenPaw。
# Manager 负责规划和派工，这里的 Worker 只接受属于自己的配置与任务，不能反过来
# 修改 Controller 的权威状态。

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlencode, urlparse

from qwenpaw_worker.api import QwenPawApiClient

import yaml

from qwenpaw_worker.config import WorkerConfig

logger = logging.getLogger(__name__)
DEFAULT_AGENT_ID = "default"
TEAMS_PROMPT_FILE = "TEAMS.md"
PACKAGE_PROMPT_FILES = ("AGENTS.md", "SOUL.md")
PACKAGE_RUNTIME_OWNED_CONFIG_FILES = {Path(TEAMS_PROMPT_FILE)}
TEAMS_INTERNAL_CONTROL_MARKER = (
    "<!-- AGENTTEAMS_INTERNAL_CONTROL_FILE: TEAMS.md is managed by "
    "TeamHarness/QwenPaw runtime; agent packages must not overwrite or delete it. -->"
)
TEAMS_CONTEXT_START = "<!-- BEGIN AGENTTEAMS RUNTIME TEAM CONTEXT -->"
TEAMS_CONTEXT_END = "<!-- END AGENTTEAMS RUNTIME TEAM CONTEXT -->"
AGENT_IDENTITY_DATA_ENDPOINT_FORMAT = "agentidentitydata.{region_id}.aliyuncs.com"
REGION_ID_ENV_NAMES = ("AGENTTEAMS_REGION", "ALIBABA_CLOUD_REGION_ID", "REGION_ID")


def _section(data: Dict[str, Any], name: str) -> Dict[str, Any]:
    # 逻辑说明：从配置中取一个字典分区；缺失或类型错误统一为空字典，避免调用方重复判型。
    value = data.get(name) or {}
    return value if isinstance(value, dict) else {}


def _string(value: Any) -> str:
    # 逻辑说明：把可选配置值规范为去空白字符串，None 表示未配置而不是文本 "None"。
    return str(value).strip() if value is not None else ""


_ENV_NAME_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _instance_id_from_controller_url(value: str) -> str:
    # 逻辑说明：从 controller.<instance> 主机名提取实例 ID；格式不匹配时返回空值。
    host = urlparse(value.strip()).hostname or ""
    parts = host.split(".")
    if len(parts) >= 2 and parts[0] == "controller":
        return parts[1]
    return ""


def _worker_instance_id() -> str:
    # 逻辑说明：优先读显式实例 ID，否则从 Controller URL 推断，供凭据名称去实例前缀使用。
    explicit = _string(os.getenv("AGENTTEAMS_INSTANCE_ID"))
    if explicit:
        return explicit
    return _instance_id_from_controller_url(
        _string(os.getenv("AGENTTEAMS_CONTROLLER_URL"))
    )


def _worker_region_id() -> str:
    # 逻辑说明：按兼容环境变量顺序查找地域 ID；全部缺失时返回空值，由上层决定是否启用云能力。
    for name in REGION_ID_ENV_NAMES:
        value = _string(os.getenv(name))
        if value:
            return value
    return ""


def credential_provider_env_name(provider_name: str, instance_id: str = "") -> str:
    # 逻辑说明：校验 provider 是否能安全作为环境变量名，并兼容去掉当前实例前缀后的名称。
    text = _string(provider_name)
    if _ENV_NAME_PATTERN.fullmatch(text):
        return text
    prefix = f"{_string(instance_id)}-"
    if prefix != "-" and text.startswith(prefix):
        suffix = text[len(prefix) :]
        if _ENV_NAME_PATTERN.fullmatch(suffix):
            return suffix
    return ""


def _credential_provider_env_name(provider_name: str) -> str:
    # 逻辑说明：使用当前 Worker 实例 ID 调用公共名称规范器，避免调用处各自解析环境。
    return credential_provider_env_name(provider_name, _worker_instance_id())


def _download_path_part(value: str, fallback: str) -> str:
    # 逻辑说明：把远端名称清洗为安全的本地下载目录片段，清洗为空时采用 fallback。
    text = value.strip() or fallback
    return re.sub(r"[^A-Za-z0-9._=-]+", "_", text).strip("._") or fallback


def _string_list(value: Any) -> List[str]:
    # 逻辑说明：只接受列表并提取其中非空文本，忽略无效项以形成稳定的配置列表。
    if not isinstance(value, list):
        return []
    result = []
    for item in value:
        text = _string(item)
        if text:
            result.append(text)
    return result


def _stable_json(value: Any) -> str:
    return json.dumps(
        value if value is not None else {},
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _count_collection(value: Any) -> int:
    # 逻辑说明：为日志统计字典或列表长度；其他类型视为零，避免诊断代码影响主流程。
    if isinstance(value, dict):
        return len(value)
    if isinstance(value, list):
        return len(value)
    return 0


def _named_keys(value: Any) -> str:
    # 逻辑说明：将字典的非空键排序拼成日志字段，不是字典或无键时使用占位符。
    if not isinstance(value, dict):
        return "-"
    names = sorted(str(name).strip() for name in value.keys() if str(name).strip())
    return ",".join(names) if names else "-"


def _duration_ms(started_at: float) -> int:
    # 逻辑说明：用单调时钟计算非负毫秒耗时，避免系统时间调整产生负数。
    return max(0, int((time.monotonic() - started_at) * 1000))


def _strip_json_line_comments(text: str) -> str:
    # 逻辑说明：逐字符移除 JSON 字符串之外的 // 行注释，同时保留字符串内的斜杠和转义字符。
    result: List[str] = []
    in_string = False
    escaped = False
    index = 0
    while index < len(text):
        char = text[index]
        if in_string:
            result.append(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            index += 1
            continue

        if char == '"':
            in_string = True
            result.append(char)
            index += 1
            continue
        if char == "/" and index + 1 < len(text) and text[index + 1] == "/":
            index += 2
            while index < len(text) and text[index] not in "\r\n":
                index += 1
            continue
        result.append(char)
        index += 1
    return "".join(result)


def _string_fields(value: Any, keys: Iterable[str]) -> Dict[str, str]:
    # 逻辑说明：从字典白名单字段中抽取非空文本，隔离未知配置并统一空值行为。
    if not isinstance(value, dict):
        return {}
    result: Dict[str, str] = {}
    for key in keys:
        text = _string(value.get(key))
        if text:
            result[key] = text
    return result


def _env_bool(name: str) -> bool:
    # 逻辑说明：把环境变量常见真值文本转成布尔值，未设置或其他文本均为 False。
    value = _string(os.getenv(name)).lower()
    return value in {"1", "true", "yes", "on"}


def _bool(value: Any) -> bool:
    # 逻辑说明：兼容布尔、数字和文本真值，将松散 YAML/JSON 配置规范为 bool。
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return _string(value).lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class MemberRuntimeConfig:
    """对 Controller 生成的 runtime config 提供只读、带类型的访问入口。

    原始文件是嵌套 JSON/YAML 数据。把字段解析集中在这里，可以统一处理旧版本缺失
    字段与默认值；如果业务代码到处直接索引字典，一次配置格式升级就会造成不同
    模块得到不同答案。

    Normalized runtime.yaml snapshot used by QwenPaw worker and adapter.
    """

    path: Path
    raw: Dict[str, Any]

    @classmethod
    def load(cls, path: Path) -> "MemberRuntimeConfig":
        # 逻辑说明：读取 YAML、验证根对象和 runtime 类型后构造不可变快照；缺失或格式错误直接失败。
        if not path.exists():
            raise FileNotFoundError(f"runtime config missing: {path}")
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(data, dict):
            raise ValueError("runtime config must be a YAML object")
        member = _section(data, "member")
        runtime = _string(member.get("runtime"))
        if runtime and runtime != "qwenpaw":
            raise ValueError(f"runtime must be qwenpaw, got {runtime}")
        return cls(path=path, raw=data)

    @property
    def generation(self) -> str:
        # 逻辑说明：从 metadata 读取并规范化 generation，供更新器判断是否出现新配置。
        return _string(_section(self.raw, "metadata").get("generation"))

    @property
    def team(self) -> Dict[str, Any]:
        return _section(self.raw, "team")

    @property
    def team_members(self) -> List[Dict[str, str]]:
        # 逻辑说明：筛选成员列表中允许的文本字段，跳过非字典或全空成员，返回规范化名册。
        raw = self.team.get("members")
        if not isinstance(raw, list):
            return []
        members: List[Dict[str, str]] = []
        for item in raw:
            entry = _string_fields(
                item, ("name", "runtimeName", "role", "matrixUserId", "personalRoomId")
            )
            if entry:
                members.append(entry)
        return members

    @property
    def member(self) -> Dict[str, Any]:
        return _section(self.raw, "member")

    @property
    def desired(self) -> Dict[str, Any]:
        return _section(self.raw, "desired")

    @property
    def storage(self) -> Dict[str, Any]:
        return _section(self.raw, "storage")

    @property
    def credentials(self) -> Dict[str, Any]:
        return _section(self.raw, "credentials")

    @property
    def agent_identity_data(self) -> Dict[str, str]:
        # 逻辑说明：只暴露身份服务 endpoint/regionId 两个文本字段，避免下游依赖未知结构。
        return _string_fields(
            _section(self.raw, "agentIdentityData"), ("endpoint", "regionId")
        )

    @property
    def agent_identity_data_region_id(self) -> str:
        # 逻辑说明：优先采用 runtime config 地域，缺失时回落 Worker 环境中的地域配置。
        return _string(self.agent_identity_data.get("regionId")) or _worker_region_id()

    @property
    def agent_identity_data_endpoint(self) -> str:
        # 逻辑说明：优先使用显式 endpoint，否则由地域拼出标准地址；无地域时表示功能不可用。
        endpoint = _string(self.agent_identity_data.get("endpoint"))
        if endpoint:
            return endpoint
        region_id = self.agent_identity_data_region_id
        if region_id:
            return AGENT_IDENTITY_DATA_ENDPOINT_FORMAT.format(region_id=region_id)
        return ""

    @property
    def agent_identity(self) -> Dict[str, str]:
        # 逻辑说明：从 desired 配置提取允许的工作负载身份名称，作为凭据运行时输入。
        return _string_fields(
            _section(self.desired, "agentIdentity"), ("workloadIdentityName",)
        )

    @property
    def workload_identity_name(self) -> str:
        # 逻辑说明：从规范化身份字典读取名称并再次清理空白，缺失返回空字符串。
        return _string(self.agent_identity.get("workloadIdentityName"))

    @property
    def credential_bindings(self) -> List[Dict[str, Any]]:
        # 逻辑说明：验证每条凭据绑定、保留引用与可选工具白名单，忽略不完整条目。
        raw = self.desired.get("credentialBindings")
        if not isinstance(raw, list):
            return []
        bindings: List[Dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            credential_ref = _string_fields(
                _section(item, "credentialRef"),
                ("tokenVaultName", "apiKeyCredentialProviderName"),
            )
            if credential_ref:
                binding: Dict[str, Any] = {"credentialRef": credential_ref}
                tool_whitelist = _string_list(item.get("toolWhitelist"))
                if tool_whitelist:
                    binding["toolWhitelist"] = tool_whitelist
                bindings.append(binding)
        return bindings

    @property
    def credential_binding_env_names(self) -> List[str]:
        # 逻辑说明：把绑定 provider 转成不重复的安全环境变量名，供敏感输出清洗使用。
        names: List[str] = []
        for binding in self.credential_bindings:
            name = _credential_provider_env_name(
                _string(
                    binding.get("credentialRef", {}).get("apiKeyCredentialProviderName")
                )
            )
            if name and name not in names:
                names.append(name)
        return names

    @property
    def credential_binding_env_provider_names(self) -> Dict[str, str]:
        # 逻辑说明：建立环境变量名到原 provider 名的首个映射，供运行时注入凭据时查找。
        providers: Dict[str, str] = {}
        for binding in self.credential_bindings:
            provider_name = _string(
                binding.get("credentialRef", {}).get("apiKeyCredentialProviderName")
            )
            env_name = _credential_provider_env_name(provider_name)
            if env_name and env_name not in providers:
                providers[env_name] = provider_name
        return providers

    @property
    def credential_runtime_identity(self) -> str:
        # 逻辑说明：仅选取会改变运行期凭据身份的 Agent 身份、内联/远端身份数据和 credential bindings，做稳定 JSON 序列化；调用方可据此精确判断是否需要重建凭据运行时。
        return _stable_json(
            {
                "agentIdentity": self.agent_identity,
                "agentIdentityData": self.agent_identity_data,
                "agentIdentityDataEndpoint": self.agent_identity_data_endpoint,
                "credentialBindings": self.credential_bindings,
            }
        )

    @property
    def team_name(self) -> str:
        # 逻辑说明：读取并规范化 Team 名称；空值表示当前 Worker 尚未加入团队。
        return _string(self.team.get("name"))

    @property
    def member_name(self) -> str:
        # 逻辑说明：优先成员展示名、回落 runtimeName，得到当前 Worker 的稳定成员名称。
        return _string(self.member.get("name") or self.member.get("runtimeName"))

    @property
    def member_role(self) -> str:
        # 逻辑说明：读取成员角色，未配置时默认为普通 worker，避免误获得 Leader 权限。
        return _string(self.member.get("role") or "worker")

    @property
    def agent_package(self) -> Dict[str, Any]:
        return _section(self.desired, "agentPackage")

    @property
    def agent_package_identity(self) -> Tuple[str, str, str, str]:
        # 逻辑说明：将包引用、名称、版本和摘要组成稳定元组，用于判断包内容是否变化。
        package = self.agent_package
        return (
            _string(package.get("ref")),
            _string(package.get("name")),
            _string(package.get("version")),
            _string(package.get("digest")),
        )

    @property
    def inline_config(self) -> Dict[str, str]:
        # 逻辑说明：只提取 Controller 允许内联覆盖的身份、SOUL 和 AGENTS 文本。
        return _string_fields(
            _section(self.desired, "inlineConfig"), ("identity", "soul", "agents")
        )

    @property
    def model(self) -> Dict[str, Any]:
        return _section(self.desired, "model")

    @property
    def mcp_servers(self) -> Any:
        # 逻辑说明：保留 MCP 配置原始容器形状以兼容字典/列表版本；完全缺失时返回空字典。
        value = self.desired.get("mcpServers")
        return value if value is not None else {}

    @property
    def channels(self) -> Dict[str, Any]:
        return _section(self.desired, "channels")

    @property
    def dingtalk_channel(self) -> Optional[Dict[str, Any]]:
        # 逻辑说明：仅当钉钉 channel 为字典时返回配置，其他类型视为未启用。
        value = self.channels.get("dingtalk")
        return value if isinstance(value, dict) else None

    @property
    def channel_policy(self) -> Dict[str, Any]:
        return _section(self.desired, "channelPolicy")

    @property
    def desired_identity(self) -> Tuple[str, str, str, str, str, str, str, str, str]:
        # 逻辑说明：将影响运行行为的各分区稳定序列化为元组，供 changed_from 做深层变化判断。
        return (
            *self.agent_package_identity,
            _stable_json(self.inline_config),
            _stable_json(self.model),
            _stable_json(self.mcp_servers),
            _stable_json(self.channels),
            _stable_json(self.channel_policy),
        )

    @property
    def team_context_facts(self) -> Dict[str, Any]:
        # 逻辑说明：从 Team、成员、模型和协调者字段构建最小上下文事实；只输出存在的信息。
        team = _string_fields(
            self.team,
            ("name", "teamRoomId", "leaderName", "leaderRuntimeName", "leaderDmRoomId"),
        )
        admin = _string_fields(_section(self.team, "admin"), ("name", "matrixUserId"))
        if admin:
            team["admin"] = admin  # type: ignore[assignment]
        if self.team_members:
            team["members"] = self.team_members  # type: ignore[assignment]

        facts: Dict[str, Any] = {}
        if self.generation:
            facts["metadata"] = {"generation": self.generation}
        if team:
            facts["team"] = team
        member = _string_fields(
            self.member,
            (
                "name",
                "runtimeName",
                "role",
                "runtime",
                "matrixUserId",
                "personalRoomId",
            ),
        )
        if member:
            facts["member"] = member
        runtime: Dict[str, Any] = {}
        model = _string_fields(
            self.model,
            ("providerId", "provider_id", "provider", "model", "name"),
        )
        provider_id = _string(
            model.get("providerId")
            or model.get("provider_id")
            or model.get("provider"),
        )
        model_name = _string(model.get("model") or model.get("name"))
        if provider_id or model_name:
            runtime["model"] = {
                key: value
                for key, value in (
                    ("providerId", provider_id),
                    ("name", model_name),
                )
                if value
            }
        coordinator = self.coordinator_matrix_user_id
        if coordinator:
            runtime["coordinator"] = {"matrixUserId": coordinator}
        if runtime:
            facts["runtime"] = runtime
        return facts

    @property
    def coordinator_matrix_user_id(self) -> str:
        # 逻辑说明：普通成员优先找到 Leader Matrix ID，Leader 或无 Team 时回落 Manager；无法推断则为空。
        role = self.member_role.casefold()
        member_id = _string(self.member.get("matrixUserId"))
        domain = member_id.split(":", 1)[1] if ":" in member_id else ""
        if self.team_name and role not in {
            "leader",
            "team_leader",
            "team-leader",
        }:
            leader_name = _string(
                self.team.get("leaderRuntimeName") or self.team.get("leaderName"),
            )
            for item in self.team_members:
                if (
                    _string(item.get("runtimeName")) == leader_name
                    or _string(item.get("name")) == leader_name
                    or _string(item.get("role")).casefold()
                    in {"leader", "team_leader", "team-leader"}
                ):
                    leader_id = _string(item.get("matrixUserId"))
                    if leader_id:
                        return leader_id
            if leader_name and domain:
                return f"@{leader_name}:{domain}"
        if domain:
            return f"@manager:{domain}"
        return ""

    @property
    def team_context_identity(self) -> str:
        # 逻辑说明：把已规范化的团队事实稳定序列化为身份指纹，供更新循环判断 Team 上下文是否变化；这里只计算字符串，不写配置或触发运行时重建。
        return _stable_json(self.team_context_facts)

    @property
    def output_sanitize_policy(self) -> Dict[str, Any]:
        return _section(self.desired, "outputSanitize")

    @property
    def output_sanitize_keywords(self) -> List[str]:
        # 逻辑说明：将输出清洗关键字规范为非空文本列表，供日志/消息发送前匹配。
        return _string_list(self.output_sanitize_policy.get("keywords"))

    @property
    def output_sanitize_env_refs(self) -> List[str]:
        # 逻辑说明：合并显式 envRefs 与内置秘密环境变量名并去重，避免凭据出现在 Agent 输出。
        refs = _string_list(self.output_sanitize_policy.get("envRefs"))
        for key in (
            "matrixTokenEnv",
            "gatewayKeyEnv",
            "storageAccessKeyEnv",
            "storageSecretKeyEnv",
        ):
            value = _string(self.credentials.get(key))
            if value and value not in refs:
                refs.append(value)
        return refs

    def changed_from(self, previous: "MemberRuntimeConfig") -> bool:
        # 逻辑说明：比较 generation 以及所有会影响运行、Team、凭据和存储的稳定身份，返回是否需重应用。
        return (
            self.generation != previous.generation
            or self.desired_identity != previous.desired_identity
            or self.team_context_identity != previous.team_context_identity
            or self.credential_runtime_identity != previous.credential_runtime_identity
            or _stable_json(self.storage) != _stable_json(previous.storage)
        )


class AgentPackageManager:
    """下载并原子应用 Controller 指定的 Agent 包。

    Agent 包包含身份提示、技能和 MCP 配置。应用前会校验归档路径，防止恶意的
    ``../`` 条目写出工作区；应用时先做快照，只有全部文件成功替换后才提交当前
    identity。这样进程在复制到一半时退出，重启仍能恢复旧的完整版本。

    Download and extract desired AgentSpec packages without restarting.
    """

    def __init__(self, root_dir: Path, workspace_dir: Optional[Path] = None) -> None:
        # 逻辑说明：建立包缓存、当前版本和 identity 标记路径；实际下载和文件替换延迟到 apply。
        self.root_dir = root_dir
        self.workspace_dir = workspace_dir
        self.current_dir = root_dir / "current"
        self.marker_path = root_dir / "current.identity"
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def apply(self, config: MemberRuntimeConfig) -> Optional[Path]:
        # 逻辑说明：比较期望包 identity，必要时下载、校验、原子应用并提交标记；未变化时幂等返回。
        identity = config.agent_package_identity
        if not any(identity):
            return None
        if self._current_identity() == identity and self.current_dir.exists():
            self._apply_to_workspace_atomic(self.current_dir)
            return self.current_dir

        package_path = self._fetch(identity[0])
        staging = Path(
            tempfile.mkdtemp(prefix="qwenpaw-agent-package-", dir=str(self.root_dir))
        )
        previous = self.current_dir if self.current_dir.exists() else None
        workspace_snapshot = None
        try:
            self._extract(package_path, staging)
            workspace_snapshot = self._snapshot_workspace(staging, previous)
            self._cleanup_stale_workspace_targets(previous, staging)
            self._apply_to_workspace(staging, previous)
            self._commit_current(staging, identity)
            self._cleanup_workspace_snapshot(workspace_snapshot)
            return self.current_dir
        except Exception:
            self._restore_workspace_snapshot(workspace_snapshot)
            shutil.rmtree(staging, ignore_errors=True)
            raise

    def _current_identity(self) -> Tuple[str, str, str, str]:
        # 逻辑说明：读取当前 identity JSON 标记并规范为四元组；文件缺失或损坏视为尚未安装。
        if not self.marker_path.exists():
            return ("", "", "", "")
        parts = self.marker_path.read_text(encoding="utf-8").splitlines()
        return tuple((parts + ["", "", "", ""])[:4])  # type: ignore[return-value]

    def _fetch(self, ref: str) -> Path:
        # 逻辑说明：按 ref 协议分派本地、HTTP、OSS 或 Nacos 下载，验证可选摘要并返回可解包路径。
        if not ref:
            raise RuntimeError("desired.agentPackage.ref is required")
        parsed = urlparse(ref)
        if parsed.scheme in ("", "file"):
            if parsed.scheme == "file":
                raw_path = unquote(parsed.path)
                if parsed.netloc:
                    # Accept both RFC file:///C:/... URLs and the historical
                    # file://C:\... form emitted by AgentTeams on Windows.
                    if re.match(r"^[A-Za-z]:[\\/]", parsed.netloc):
                        raw_path = parsed.netloc + raw_path
                    else:
                        raw_path = f"//{parsed.netloc}{raw_path}"
                path = Path(urllib.request.url2pathname(raw_path))
            else:
                path = Path(ref)
            if not path.exists():
                raise RuntimeError(f"agent package not found: {ref}")
            return path
        if parsed.scheme in ("http", "https"):
            target = self.root_dir / "downloads" / Path(parsed.path).name
            target.parent.mkdir(parents=True, exist_ok=True)
            urllib.request.urlretrieve(ref, target)
            return target
        if parsed.scheme == "oss":
            return self._fetch_oss(parsed)
        if parsed.scheme == "nacos":
            return self._fetch_nacos(parsed)
        raise RuntimeError(f"unsupported agent package ref scheme: {parsed.scheme}")

    def _fetch_oss(self, parsed) -> Path:
        # 逻辑说明：解析 OSS 桶/键并通过 ossutil 下载到隔离目录；工具缺失或命令失败会中止更新。
        oss_path = f"{parsed.netloc}{parsed.path}".strip("/")
        if not oss_path:
            raise RuntimeError("oss agent package path is required")
        target = self.root_dir / "downloads" / Path(oss_path).name
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            return target

        storage_prefix = (
            os.getenv("AGENTTEAMS_STORAGE_PREFIX", "").strip().rstrip("/")
            or "agentteams/agentteams-storage"
        )
        if os.name == "nt" and Path(storage_prefix).is_absolute():
            remote = str(Path(storage_prefix) / Path(oss_path))
        else:
            remote = f"{storage_prefix}/{oss_path}"
        mc_bin = shutil.which("mc")
        if mc_bin is None:
            raise RuntimeError("mc binary not found for oss agent package fetch")
        command = [mc_bin, "cp", remote, str(target)]
        if os.name == "nt" and Path(mc_bin).suffix.casefold() in {".bat", ".cmd"}:
            command = [
                os.environ.get("COMSPEC", "cmd.exe"),
                "/d",
                "/c",
                "call",
                mc_bin,
                "cp",
                remote,
                str(target),
            ]
        try:
            subprocess.run(command, check=True, capture_output=True, text=True)
        except FileNotFoundError:
            raise RuntimeError(
                "mc binary not found for oss agent package fetch"
            ) from None
        except subprocess.CalledProcessError as exc:
            detail = (exc.stderr or exc.stdout or "").strip()
            message = f": {detail}" if detail else ""
            raise RuntimeError(
                f"fetch oss agent package failed: {remote}{message}"
            ) from None
        return target

    def _fetch_nacos(self, parsed) -> Path:
        # 逻辑说明：解析 Nacos AgentSpec 引用，优先 HTTP 资源 API，必要时回落 CLI 下载。
        parts = [part for part in parsed.path.strip("/").split("/") if part]
        if len(parts) < 2:
            raise RuntimeError(
                f"invalid nacos agent package ref: expected nacos://[user:pass@]host:port/"
                f"{{namespace}}/{{agentspec-name}}[/{{version}}], got {parsed.geturl()}"
            )

        namespace, spec_name = parts[0], parts[1]
        version = parts[2] if len(parts) >= 3 else ""
        label = ""
        if version.startswith("label:"):
            label = version.removeprefix("label:")
            version = ""
        query = parse_qs(parsed.query)
        query_version = _string((query.get("version") or [""])[0])
        query_label = _string((query.get("label") or [""])[0])
        if query_version:
            version = query_version
            label = ""
        if query_label:
            label = query_label
            version = ""

        auth_type = (query.get("authType") or [""])[0].strip()
        if auth_type == "sts-agentteams":
            return self._fetch_nacos_cli(
                parsed, namespace, spec_name, version, label, auth_type
            )

        output_dir = self._nacos_download_output_dir(
            namespace, spec_name, version, label
        )
        target = output_dir / spec_name
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True, exist_ok=True)

        spec = self._get_nacos_agentspec(parsed, namespace, spec_name, version, label)
        resources = spec.get("resource") or {}
        if isinstance(resources, dict):
            for resource in resources.values():
                if isinstance(resource, dict):
                    self._write_nacos_resource(target, resource)

        content = _string(spec.get("content"))
        try:
            content = json.dumps(json.loads(content), ensure_ascii=False, indent=2)
        except Exception:
            pass
        (target / "manifest.json").write_text(content, encoding="utf-8")
        return target

    def _fetch_nacos_cli(
        self,
        parsed,
        namespace: str,
        spec_name: str,
        version: str,
        label: str,
        auth_type: str,
    ) -> Path:
        # 逻辑说明：组装鉴权和版本参数运行 Nacos CLI，再定位输出目录；缺少 CLI 或产物时失败。
        host = parsed.hostname
        if not host:
            raise RuntimeError(
                f"invalid nacos agent package ref: missing host in {parsed.geturl()}"
            )
        port = parsed.port or 8848

        output_dir = self._nacos_download_output_dir(
            namespace, spec_name, version, label
        )
        target = output_dir / spec_name
        if target.exists():
            shutil.rmtree(target)
        output_dir.mkdir(parents=True, exist_ok=True)

        command = [
            "nacos-cli",
            "--host",
            host,
            "--port",
            str(port),
            "--namespace",
            namespace,
        ]
        if auth_type:
            command.extend(["--auth-type", auth_type])
        if auth_type == "sts-agentteams":
            access_key, secret_key, security_token = self._nacos_sts_credentials()
            command.extend(["--access-key", access_key, "--secret-key", secret_key])
            if security_token:
                command.extend(["--security-token", security_token])
        command.extend(["agentspec-get", spec_name, "-o", str(output_dir)])
        # 逻辑说明：版本和标签是可选筛选条件，只在非空时加入 argv；凭据保持独立参数，日志不会展开 secret 或 security token。
        if version:
            command.extend(["--version", version])
        if label:
            command.extend(["--label", label])

        logger.info(
            "fetching nacos agentspec package component=update step=fetch_agentspec host=%s port=%s namespace=%s "
            "spec=%s version=%s label=%s",
            host,
            port,
            namespace,
            spec_name,
            version or "-",
            label or "-",
        )
        try:
            subprocess.run(command, check=True, capture_output=True, text=True)
        except FileNotFoundError:
            raise RuntimeError(
                "nacos-cli binary not found for nacos agent package fetch"
            ) from None
        except subprocess.CalledProcessError as exc:
            detail = (exc.stderr or exc.stdout or "").strip()
            message = f": {detail}" if detail else ""
            raise RuntimeError(
                f"fetch agentspec {spec_name} from nacos with nacos-cli failed{message}"
            ) from None

        if not target.exists():
            raise RuntimeError(
                f"nacos-cli agentspec download finished but {target} was not created"
            )
        if not target.is_dir():
            raise RuntimeError(
                f"nacos-cli agentspec output {target} is not a directory"
            )
        return target

    def _nacos_download_output_dir(
        self,
        namespace: str,
        spec_name: str,
        version: str,
        label: str,
    ) -> Path:
        # 逻辑说明：按 namespace、AgentSpec 名和版本/标签生成稳定隔离的 Nacos 下载目录。
        if version:
            selector = f"version-{version}"
        elif label:
            selector = f"label-{label}"
        else:
            selector = "latest"
        return (
            self.root_dir
            / "downloads"
            / "nacos"
            / _download_path_part(namespace, "default")
            / _download_path_part(spec_name, "agentspec")
            / _download_path_part(selector, "latest")
        )

    def _get_nacos_agentspec(
        self, parsed, namespace: str, spec_name: str, version: str, label: str
    ) -> Dict[str, Any]:
        # 逻辑说明：请求 Nacos AgentSpec 元数据并验证返回对象；鉴权或 JSON 错误向上报告。
        host = parsed.hostname
        if not host:
            raise RuntimeError(
                f"invalid nacos agent package ref: missing host in {parsed.geturl()}"
            )
        port = parsed.port or 8848
        params = {"namespaceId": namespace, "name": spec_name}
        if version:
            params["version"] = version
        if label:
            params["label"] = label
        url = f"http://{host}:{port}/nacos/v3/client/ai/agentspecs?{urlencode(params)}"
        request = urllib.request.Request(
            url, headers=self._nacos_auth_headers(parsed, namespace)
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"fetch agentspec {spec_name} from nacos failed: HTTP {exc.code}: {body}"
            ) from None
        except urllib.error.URLError as exc:
            raise RuntimeError(
                f"fetch agentspec {spec_name} from nacos failed: {exc.reason}"
            ) from None
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"fetch agentspec {spec_name} from nacos failed: invalid JSON response"
            ) from exc

        if int(payload.get("code", 0)) != 0:
            raise RuntimeError(
                f"fetch agentspec {spec_name} from nacos failed: code={payload.get('code')}, "
                f"message={payload.get('message')}"
            )
        data = payload.get("data") or {}
        if not isinstance(data, dict):
            raise RuntimeError(
                f"fetch agentspec {spec_name} from nacos failed: response data must be an object"
            )
        return data

    def _nacos_auth_headers(self, parsed, namespace: str) -> Dict[str, str]:
        # 逻辑说明：按 query/env/STS 的优先级构造 Nacos 鉴权头，缺少凭据时保持匿名请求。
        query = parse_qs(parsed.query)
        auth_type = (query.get("authType") or [""])[0].strip()
        token = os.getenv("AGENTTEAMS_NACOS_TOKEN", "").strip()
        username = parsed.username or os.getenv("AGENTTEAMS_NACOS_USERNAME", "")
        password = parsed.password or os.getenv("AGENTTEAMS_NACOS_PASSWORD", "")

        if auth_type == "sts-agentteams":
            return self._nacos_sts_auth_headers(namespace)
        if auth_type == "":
            if token:
                return {"Authorization": f"Bearer {token}"}
            auth_type = "nacos" if username or password else "none"
        if auth_type == "none":
            return {}
        if auth_type != "nacos":
            raise RuntimeError(f"unsupported nacos auth type: {auth_type}")
        if not username or not password:
            raise RuntimeError("nacos auth requires username and password")

        access_token = self._nacos_login(parsed, username, password)
        return {"Authorization": f"Bearer {access_token}"}

    def _nacos_sts_auth_headers(self, namespace: str) -> Dict[str, str]:
        # 逻辑说明：获取临时 AK/SK/token 并生成签名请求头；无临时凭据时返回空头。
        access_key, secret_key, security_token = self._nacos_sts_credentials()
        timestamp = str(int(time.time() * 1000))
        sign_data = f"{namespace}+DEFAULT_GROUP+{timestamp}" if namespace else timestamp
        signature = base64.b64encode(
            hmac.new(
                secret_key.encode("utf-8"), sign_data.encode("utf-8"), hashlib.sha1
            ).digest()
        ).decode("utf-8")
        headers = {
            "Spas-AccessKey": access_key,
            "Timestamp": timestamp,
            "Spas-Signature": signature,
        }
        if security_token:
            headers["Spas-SecurityToken"] = security_token
        return headers

    def _nacos_sts_credentials(self) -> Tuple[str, str, str]:
        # 逻辑说明：从 Controller STS 响应兼容提取三项临时凭据，不完整时返回全空元组。
        sts = self._fetch_controller_sts()
        access_key = _string(
            sts.get("access_key_id")
            or sts.get("accessKeyId")
            or sts.get("accessKeyID")
            or sts.get("AccessKeyID")
        )
        secret_key = _string(
            sts.get("access_key_secret")
            or sts.get("accessKeySecret")
            or sts.get("AccessKeySecret")
        )
        security_token = _string(
            sts.get("security_token")
            or sts.get("securityToken")
            or sts.get("SecurityToken")
        )
        if not access_key or not secret_key:
            raise RuntimeError("controller STS response missing access key fields")
        return access_key, secret_key, security_token

    def _fetch_controller_sts(self) -> Dict[str, Any]:
        # 逻辑说明：带 Worker Bearer token 请求 Controller STS；未配置、网络或格式失败返回空对象。
        controller_url = os.getenv("AGENTTEAMS_CONTROLLER_URL", "").strip().rstrip("/")
        if not controller_url:
            raise RuntimeError(
                "nacos authType=sts-agentteams requires AGENTTEAMS_CONTROLLER_URL"
            )
        bearer = self._controller_bearer_token()
        headers = {"Authorization": f"Bearer {bearer}"}
        request = urllib.request.Request(
            f"{controller_url}/api/v1/credentials/sts",
            data=b"",
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"controller STS request failed: HTTP {exc.code}: {body}"
            ) from None
        except urllib.error.URLError as exc:
            raise RuntimeError(f"controller STS request failed: {exc.reason}") from None
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                "controller STS request failed: invalid JSON response"
            ) from exc
        if not isinstance(payload, dict):
            raise RuntimeError("controller STS response must be an object")
        return payload

    def _controller_bearer_token(self) -> str:
        # 逻辑说明：优先直接环境 token，其次读取挂载文件；读失败返回空值且不记录秘密。
        token = os.getenv("AGENTTEAMS_AUTH_TOKEN", "").strip()
        if token:
            return token
        token_file = os.getenv("AGENTTEAMS_AUTH_TOKEN_FILE", "").strip()
        if token_file:
            path = Path(token_file)
            if not path.exists():
                raise RuntimeError(
                    f"AGENTTEAMS_AUTH_TOKEN_FILE does not exist: {token_file}"
                )
            token = path.read_text(encoding="utf-8").strip()
            if token:
                return token
        raise RuntimeError(
            "nacos authType=sts-agentteams requires AGENTTEAMS_AUTH_TOKEN or AGENTTEAMS_AUTH_TOKEN_FILE"
        )

    def _nacos_login(self, parsed, username: str, password: str) -> str:
        # 逻辑说明：用静态用户名密码请求 Nacos 登录 token；主机缺失、网络或响应无 token 时失败。
        host = parsed.hostname
        if not host:
            raise RuntimeError(
                f"invalid nacos agent package ref: missing host in {parsed.geturl()}"
            )
        port = parsed.port or 8848
        body = urlencode({"username": username, "password": password}).encode("utf-8")
        for path in ("/nacos/v3/auth/user/login", "/nacos/v1/auth/login"):
            request = urllib.request.Request(
                f"http://{host}:{port}{path}",
                data=body,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(request, timeout=60) as response:
                    payload = json.loads(response.read().decode("utf-8"))
            except Exception:
                continue
            data = (
                payload.get("data")
                if isinstance(payload.get("data"), dict)
                else payload
            )
            token = data.get("accessToken") if isinstance(data, dict) else ""
            if token:
                return _string(token)
        raise RuntimeError("nacos login failed")

    def _write_nacos_resource(self, target: Path, resource: Dict[str, Any]) -> None:
        # 逻辑说明：校验资源路径位于目标目录内，按内容编码写入文件，拒绝路径穿越和无效资源。
        content = resource.get("content")
        if content in (None, ""):
            return
        rel = self._nacos_resource_path(resource)
        if not rel:
            return
        self._ensure_inside_target(target, [rel])
        data = str(content).encode("utf-8")
        metadata = (
            resource.get("metadata")
            if isinstance(resource.get("metadata"), dict)
            else {}
        )
        if metadata.get("encoding") == "base64":
            data = base64.b64decode(str(content))
        destination = target / rel
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)

    def _nacos_resource_path(self, resource: Dict[str, Any]) -> str:
        # 逻辑说明：把 Nacos 资源类型/名称映射为包内相对路径，并经过越界校验返回目标文件。
        resource_type = _string(resource.get("type"))
        resource_name = _string(resource.get("name")).strip("/")
        if not resource_type:
            return resource_name
        prefix = f"{resource_type}/"
        return (
            resource_name
            if resource_name.startswith(prefix)
            else prefix + resource_name
        )

    def _extract(self, package_path: Path, target_dir: Path) -> None:
        # 逻辑说明：清空目标后按目录、tar 或 zip 类型安全展开；未知归档格式直接拒绝。
        if package_path.is_dir():
            shutil.copytree(package_path, target_dir, dirs_exist_ok=True)
            return
        if tarfile.is_tarfile(package_path):
            with tarfile.open(package_path) as archive:
                self._safe_extract_tar(archive, target_dir)
            return
        if zipfile.is_zipfile(package_path):
            with zipfile.ZipFile(package_path) as archive:
                self._safe_extract_zip(archive, target_dir)
            return
        raise RuntimeError(f"unsupported agent package format: {package_path}")

    def _ensure_inside_target(self, target_dir: Path, names: Iterable[str]) -> None:
        # 逻辑说明：解析真实路径并确认候选位于目标根内，阻断归档中的 ../ 或绝对路径穿越。
        target_root = target_dir.resolve()
        for name in names:
            resolved = (target_dir / name).resolve()
            try:
                resolved.relative_to(target_root)
            except ValueError:
                raise RuntimeError(f"unsafe agent package path: {name}")

    def _safe_extract_tar(self, archive: tarfile.TarFile, target_dir: Path) -> None:
        # 逻辑说明：先检查所有 tar 成员路径，全部安全后才整体解包，避免部分恶意内容落盘。
        members = archive.getmembers()
        for member in members:
            if member.issym() or member.islnk():
                raise RuntimeError(f"unsafe agent package link: {member.name}")
        self._ensure_inside_target(target_dir, (member.name for member in members))
        archive.extractall(target_dir, members=members)

    def _safe_extract_zip(self, archive: zipfile.ZipFile, target_dir: Path) -> None:
        # 逻辑说明：先检查所有 zip 成员路径，全部安全后才解包，失败时不留下越界文件。
        names = archive.namelist()
        self._ensure_inside_target(target_dir, names)
        archive.extractall(target_dir)

    def _apply_to_workspace_atomic(self, package_dir: Path) -> None:
        # 逻辑说明：应用前快照工作区，失败时完整恢复，成功后清理快照，实现跨文件近似原子更新。
        snapshot = self._snapshot_workspace(package_dir)
        try:
            self._apply_to_workspace(package_dir)
            self._cleanup_workspace_snapshot(snapshot)
        except Exception:
            self._restore_workspace_snapshot(snapshot)
            raise

    def _apply_to_workspace(
        self, package_dir: Path, previous_package_dir: Optional[Path] = None
    ) -> None:
        # 逻辑说明：确定包内容根，将提示词、配置和技能复制到实时工作区，并清理旧包遗留目标。
        if self.workspace_dir is None:
            return
        workspace_dir = self.workspace_dir
        package_root = self._package_content_root(package_dir)
        workspace_dir.mkdir(parents=True, exist_ok=True)

        config_dir = package_root / "config"
        if config_dir.is_dir():
            self._copy_config_to_workspace(config_dir)
        self._clear_missing_package_prompt_files(package_root)

        self._package_mcp_clients(package_root)

        skills_dir = package_root / "skills"
        if skills_dir.is_dir():
            self._copy_skills_to_workspace(skills_dir)

    def _package_content_root(self, package_dir: Path) -> Path:
        # 逻辑说明：识别包本身或唯一子目录作为内容根；多层或模糊结构保持原目录供后续验证。
        if self._looks_like_agent_package(package_dir):
            return package_dir
        children = [path for path in package_dir.iterdir() if path.is_dir()]
        if len(children) == 1 and self._looks_like_agent_package(children[0]):
            return children[0]
        return package_dir

    def _looks_like_agent_package(self, path: Path) -> bool:
        # 逻辑说明：检查提示词、配置或技能等标记，判断目录是否已经是 Agent 包内容根。
        markers = (
            "manifest.json",
            "template.json",
            "config",
            "skills",
            "AGENTS.md",
            "SOUL.md",
            "MEMORY.md",
            "BOOTSTRAP.md",
            "crons",
            "mcp.json",
        )
        return any((path / marker).exists() for marker in markers)

    def _config_files(self, config_dir: Path) -> List[Tuple[Path, Path]]:
        # 逻辑说明：递归收集允许同步的配置源/目标对，排除运行时自有和单独管理的文件。
        if self.workspace_dir is None:
            return []
        files: List[Tuple[Path, Path]] = []
        for child in sorted(config_dir.rglob("*")):
            if child.is_file():
                rel = child.relative_to(config_dir)
                if rel == Path("config/mcporter.json"):
                    continue
                if rel in PACKAGE_RUNTIME_OWNED_CONFIG_FILES:
                    continue
                files.append((child, self.workspace_dir / rel))
        return files

    def _workspace_targets(self, package_dir: Path) -> List[Path]:
        # 逻辑说明：列出本次包会修改的提示、配置和技能目标，作为原子快照边界。
        if self.workspace_dir is None:
            return []
        package_root = self._package_content_root(package_dir)
        workspace_dir = self.workspace_dir
        targets: List[Path] = []

        config_dir = package_root / "config"
        if config_dir.is_dir():
            targets.extend(target for _source, target in self._config_files(config_dir))
        for file_name in PACKAGE_PROMPT_FILES:
            targets.append(workspace_dir / file_name)

        skills_dir = package_root / "skills"
        if skills_dir.is_dir():
            targets.extend(
                workspace_dir / "skills" / source.name
                for source in skills_dir.iterdir()
                if source.is_dir()
            )

        return self._dedupe_paths(targets)

    def _workspace_state_targets(self, package_dir: Path) -> List[Path]:
        # 逻辑说明：合并本次与当前包会影响的工作区路径，使新旧状态都处于快照保护范围。
        if self.workspace_dir is None:
            return []
        return []

    def _snapshot_workspace(
        self,
        package_dir: Path,
        previous_package_dir: Optional[Path] = None,
    ) -> Optional[Tuple[Path, List[Tuple[Path, Path, bool]]]]:
        # 逻辑说明：把去重后的现有目标复制到临时目录并记录原存在性，返回恢复所需清单。
        targets = self._dedupe_paths(
            self._workspace_targets(package_dir)
            + self._workspace_state_targets(package_dir)
        )
        if previous_package_dir is not None:
            targets = self._dedupe_paths(
                targets
                + self._workspace_targets(previous_package_dir)
                + self._workspace_state_targets(previous_package_dir)
            )
        if not targets:
            return None
        backup_root = Path(
            tempfile.mkdtemp(
                prefix="qwenpaw-workspace-rollback-", dir=str(self.root_dir)
            )
        )
        entries: List[Tuple[Path, Path, bool]] = []
        try:
            for index, target in enumerate(targets):
                backup = backup_root / str(index)
                if target.exists():
                    if target.is_dir():
                        shutil.copytree(target, backup)
                    else:
                        backup.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(target, backup)
                    entries.append((target, backup, True))
                else:
                    entries.append((target, backup, False))
            return backup_root, entries
        except Exception:
            shutil.rmtree(backup_root, ignore_errors=True)
            raise

    def _cleanup_stale_workspace_targets(
        self, previous_package_dir: Optional[Path], package_dir: Path
    ) -> None:
        # 逻辑说明：比较新旧包目标，仅删除旧包曾管理而新包不再提供的路径，并清理空父目录。
        if previous_package_dir is None or self.workspace_dir is None:
            return
        current_targets = {str(path) for path in self._workspace_targets(package_dir)}
        for target in self._workspace_targets(previous_package_dir):
            if str(target) in current_targets or not target.exists():
                continue
            if (
                target.parent == self.workspace_dir
                and target.name in PACKAGE_PROMPT_FILES
            ):
                target.write_text("", encoding="utf-8")
                continue
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
            self._cleanup_empty_parent_dirs(target.parent)

    def _cleanup_empty_parent_dirs(self, path: Path) -> None:
        # 逻辑说明：从被删除目标向上移除空目录，但绝不越过工作区根目录。
        if self.workspace_dir is None:
            return
        workspace_root = self.workspace_dir.resolve()
        current = path
        while current != self.workspace_dir and str(current.resolve()).startswith(
            str(workspace_root)
        ):
            try:
                current.rmdir()
            except OSError:
                return
            current = current.parent

    def _dedupe_paths(self, paths: Iterable[Path]) -> List[Path]:
        # 逻辑说明：按路径文本保序去重，避免同一目标被重复快照或恢复。
        result = []
        seen = set()
        for path in paths:
            key = str(path)
            if key not in seen:
                result.append(path)
                seen.add(key)
        return result

    def _restore_workspace_snapshot(
        self, snapshot: Optional[Tuple[Path, List[Tuple[Path, Path, bool]]]]
    ) -> None:
        # 逻辑说明：先移除本次修改目标，再从快照复制原内容；原本缺失的目标保持删除。
        if snapshot is None:
            return
        backup_root, entries = snapshot
        try:
            for target, backup, existed in reversed(entries):
                if target.exists():
                    if target.is_dir():
                        shutil.rmtree(target)
                    else:
                        target.unlink()
                if existed:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    if backup.is_dir():
                        shutil.copytree(backup, target)
                    else:
                        shutil.copy2(backup, target)
        finally:
            shutil.rmtree(backup_root, ignore_errors=True)

    def _cleanup_workspace_snapshot(
        self, snapshot: Optional[Tuple[Path, List[Tuple[Path, Path, bool]]]]
    ) -> None:
        # 逻辑说明：递归删除临时快照目录；无快照时为空操作，文件系统错误向上暴露。
        if snapshot is not None:
            shutil.rmtree(snapshot[0], ignore_errors=True)

    def _commit_current(
        self, staging: Path, identity: Tuple[str, str, str, str]
    ) -> None:
        # 逻辑说明：以备份交换方式替换 current 目录并写 identity；中途失败则恢复旧包。
        backup = Path(
            tempfile.mkdtemp(
                prefix="qwenpaw-agent-package-current-", dir=str(self.root_dir)
            )
        )
        shutil.rmtree(backup, ignore_errors=True)
        moved_current = False
        moved_staging = False
        try:
            if self.current_dir.exists():
                self.current_dir.rename(backup)
                moved_current = True
            staging.rename(self.current_dir)
            moved_staging = True
            self._write_current_identity(identity)
            if moved_current:
                shutil.rmtree(backup, ignore_errors=True)
        except Exception:
            if moved_staging and self.current_dir.exists():
                shutil.rmtree(self.current_dir, ignore_errors=True)
            if moved_current and backup.exists():
                backup.rename(self.current_dir)
            raise

    def _write_current_identity(self, identity: Tuple[str, str, str, str]) -> None:
        # 逻辑说明：先写同目录临时 JSON 再原子替换标记，避免断电留下半个 identity 文件。
        tmp = self.marker_path.with_name(f".{self.marker_path.name}.tmp")
        tmp.write_text("\n".join(identity), encoding="utf-8")
        tmp.replace(self.marker_path)

    def _copy_config_to_workspace(self, config_dir: Path) -> None:
        # 逻辑说明：把筛选后的配置源逐个替换到实时工作区，父目录和覆盖规则由 helper 统一处理。
        if self.workspace_dir is None:
            return
        for source, target in self._config_files(config_dir):
            self._replace_path(source, target)

    def _clear_missing_package_prompt_files(self, package_root: Path) -> None:
        # 逻辑说明：删除新包明确不再提供的受管提示词，同时保护 runtime 自有 TEAMS.md。
        if self.workspace_dir is None:
            return
        config_dir = package_root / "config"
        for file_name in PACKAGE_PROMPT_FILES:
            if (config_dir / file_name).is_file():
                continue
            target = self.workspace_dir / file_name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("", encoding="utf-8")

    def _package_mcp_clients(
        self, package_root: Optional[Path]
    ) -> Dict[str, Dict[str, Any]]:
        # 逻辑说明：读取包内 MCP 配置、兼容不同结构并转换为 QwenPaw API client payload 映射。
        if package_root is None:
            return {}
        mcp_path = package_root / "mcp.json"
        if not mcp_path.is_file():
            return {}
        try:
            raw = json.loads(
                _strip_json_line_comments(mcp_path.read_text(encoding="utf-8"))
            )
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"agent package mcp.json is invalid JSON: {mcp_path}"
            ) from exc
        if not isinstance(raw, dict):
            raise RuntimeError("agent package mcp.json must be a JSON object")

        clients_raw: Any = raw
        if isinstance(raw.get("mcpServers"), (dict, list)):
            clients_raw = raw["mcpServers"]
        elif isinstance(raw.get("clients"), (dict, list)):
            clients_raw = raw["clients"]
        elif isinstance(raw.get("mcp"), dict) and isinstance(
            raw["mcp"].get("clients"), (dict, list)
        ):
            clients_raw = raw["mcp"]["clients"]

        clients: Dict[str, Dict[str, Any]] = {}
        if isinstance(clients_raw, list):
            for item in clients_raw:
                if not isinstance(item, dict):
                    continue
                name = _string(item.get("name") or item.get("id"))
                if not name:
                    continue
                clients[name] = self._qwenpaw_mcp_client_payload(name, item)
            return clients

        if isinstance(clients_raw, dict):
            for name, item in clients_raw.items():
                if not isinstance(item, dict):
                    continue
                client_name = _string(item.get("name") or name)
                if not client_name:
                    continue
                clients[client_name] = self._qwenpaw_mcp_client_payload(
                    client_name, item
                )
        return clients

    def package_mcp_clients(
        self, package_dir: Optional[Path]
    ) -> Dict[str, Dict[str, Any]]:
        # 逻辑说明：将可选包目录规范到内容根后解析 MCP；无包时返回空映射。
        if package_dir is None:
            return {}
        return self._package_mcp_clients(self._package_content_root(package_dir))

    def package_skill_names(self, package_dir: Optional[Path]) -> List[str]:
        # 逻辑说明：扫描包内容根的 skills 子目录，返回含 SKILL.md 的排序技能名。
        if package_dir is None:
            return []
        skills_dir = self._package_content_root(package_dir) / "skills"
        if not skills_dir.is_dir():
            return []
        return sorted(path.name for path in skills_dir.iterdir() if path.is_dir())

    def _qwenpaw_mcp_client_payload(
        self, name: str, item: Dict[str, Any]
    ) -> Dict[str, Any]:
        # 逻辑说明：校验单条 MCP 配置并翻译 transport/命令/环境字段；无有效名称时跳过。
        payload = dict(item)
        payload.pop("id", None)
        if "name" not in payload:
            payload["name"] = name
        if "enabled" not in payload and "isActive" in payload:
            payload["enabled"] = bool(payload.pop("isActive"))
        else:
            payload.pop("isActive", None)
        if "url" not in payload and "baseUrl" in payload:
            payload["url"] = payload.pop("baseUrl")
        else:
            payload.pop("baseUrl", None)
        if "transport" not in payload and "type" in payload:
            payload["transport"] = payload.pop("type")
        else:
            payload.pop("type", None)
        if _string(payload.get("transport")).lower() == "http":
            payload["transport"] = "streamable_http"
        payload = self._expand_mcp_workspace_placeholders(payload)
        self._ensure_mcp_stdio_workspace_env(payload)
        return payload

    def _mcp_workspace_env_value(self) -> str:
        # 逻辑说明：返回实时工作区绝对路径，未配置工作区时使用当前目录，供 stdio MCP 注入。
        if self.workspace_dir is not None:
            return str(self.workspace_dir)
        return _string(os.getenv("AGENT_WORKSPACE"))

    def _expand_mcp_workspace_placeholders(self, value: Any) -> Any:
        # 逻辑说明：递归替换 MCP 配置中的工作区占位符，保留字典/列表结构和其他值。
        workspace = self._mcp_workspace_env_value()
        if not workspace:
            return value
        if isinstance(value, str):
            return value.replace("${AGENT_WORKSPACE}", workspace).replace(
                "{AGENT_WORKSPACE}", workspace
            )
        if isinstance(value, list):
            return [self._expand_mcp_workspace_placeholders(item) for item in value]
        if isinstance(value, dict):
            return {
                key: self._expand_mcp_workspace_placeholders(item)
                for key, item in value.items()
            }
        return value

    def _ensure_mcp_stdio_workspace_env(self, payload: Dict[str, Any]) -> None:
        # 逻辑说明：仅为 stdio MCP 补齐 cwd 与工作区环境变量，不覆盖包已显式指定的值。
        workspace = self._mcp_workspace_env_value()
        if not workspace:
            return
        transport = _string(payload.get("transport") or "stdio").lower()
        if transport != "stdio" or not _string(payload.get("command")):
            return
        env = payload.get("env")
        if not isinstance(env, dict):
            env = {}
        env["AGENT_WORKSPACE"] = workspace
        payload["env"] = env

    def _copy_skills_to_workspace(self, skills_dir: Path) -> List[str]:
        # 逻辑说明：把每个有效技能目录替换到实时 skills 目录，并返回实际复制的排序名称。
        if self.workspace_dir is None:
            return []
        target_root = self.workspace_dir / "skills"
        target_root.mkdir(parents=True, exist_ok=True)
        copied = []
        for source in skills_dir.iterdir():
            if source.is_dir():
                self._replace_path(source, target_root / source.name)
                copied.append(source.name)
        return copied

    def _replace_path(self, source: Path, target: Path) -> None:
        # 逻辑说明：先删除目标再按文件或目录复制 source；父目录自动创建，形成确定性替换语义。
        if target.exists():
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
        target.parent.mkdir(parents=True, exist_ok=True)
        if source.is_dir():
            shutil.copytree(source, target)
        elif source.is_file():
            shutil.copy2(source, target)


@dataclass(frozen=True)
class ApplyResult:
    runtime_config: MemberRuntimeConfig
    changed: bool
    agent_package_dir: Optional[Path]


class RuntimeUpdater:
    """协调一次期望状态更新，并记录上一次已应用的 generation。

    一次更新可能同时涉及 Agent 包、模型、MCP、Matrix channel 和团队上下文。
    ``apply_once`` 负责决定哪些部分变化，具体写入委托给 QwenPaw API 或文件管理器。
    generation 只有在关键步骤完成后才前进，避免重启后把半成品误认成已生效版本。

    Apply controller-projected runtime desired state inside one worker pod.
    """

    def __init__(
        self,
        config: WorkerConfig,
        adapter_apply: Optional[Callable[[], None]] = None,
        package_manager: Optional[AgentPackageManager] = None,
        runtime_config_pull: Optional[Callable[[], None]] = None,
        team_context_renderer: Optional[Callable[[MemberRuntimeConfig], str]] = None,
        api_client: Optional[QwenPawApiClient] = None,
        runtime_reconcile: Optional[Callable[[MemberRuntimeConfig], None]] = None,
    ) -> None:
        # 逻辑说明：保存运行依赖与当前快照槽位；构造时不读取配置，便于启动阶段按顺序恢复状态。
        self.config = config
        self.adapter_apply = adapter_apply
        self.runtime_config_pull = runtime_config_pull
        self.team_context_renderer = team_context_renderer
        self.api_client = api_client
        self.runtime_reconcile = runtime_reconcile
        self.package_manager = package_manager or AgentPackageManager(
            config.qwenpaw_working_dir / "agent-packages",
            workspace_dir=config.default_workspace_dir,
        )
        self.current_config: Optional[MemberRuntimeConfig] = None

    def load(self) -> MemberRuntimeConfig:
        # 逻辑说明：可选地先从对象存储拉取 runtime.yaml，再解析为经过验证的不可变配置快照。
        if self.runtime_config_pull is not None:
            self.runtime_config_pull()
        return MemberRuntimeConfig.load(self.config.runtime_config_path)

    def refresh_team_context(self, config: MemberRuntimeConfig) -> None:
        """Reconcile the runtime-owned TEAMS.md block after asset writers run."""
        # 逻辑说明：重新写入 runtime 自有 Team 上下文块，修复包或插件复制后对 TEAMS.md 的覆盖。
        self._apply_team_context_prompt(config)

    def apply_once(
        self,
        runtime_config: Optional[MemberRuntimeConfig] = None,
        force: bool = False,
        reapply_adapter: bool = True,
    ) -> ApplyResult:
        """按固定顺序应用一次配置快照，并只在全部关键步骤后保存 current_config。

        身份/存储先就位，随后模型、MCP、channel 和 Agent 包生效，最后刷新 Team
        上下文。若中途抛错，旧 generation 仍被视为当前版本，轮询或重启可再次执行。
        各写入操作必须幂等，才能安全处理“上次实际成功但响应丢失”的情况。
        """
        # 逻辑说明：按身份、存储、模型、MCP、channel、包与 Team 上下文顺序应用；全成功才更新快照。
        started_at = time.monotonic()
        config = runtime_config or self.load()
        previous = self.current_config
        changed = force or previous is None or config.changed_from(previous)
        if not changed:
            logger.info(
                "runtime config apply skipped component=update worker=%s generation=%s changed=%s "
                "mcp_server_count=%s channel_names=%s credential_binding_count=%s duration_ms=%s",
                self.config.worker_name,
                config.generation,
                False,
                _count_collection(config.mcp_servers),
                _named_keys(config.channels),
                len(config.credential_bindings),
                _duration_ms(started_at),
            )
            return ApplyResult(
                runtime_config=config, changed=False, agent_package_dir=None
            )

        adapter_should_apply = (
            reapply_adapter
            and self.adapter_apply is not None
            and not self._adapter_neutral_change(config)
        )
        logger.info(
            "runtime config apply begin component=update worker=%s generation=%s team=%s member=%s role=%s "
            "force=%s reapply_adapter=%s adapter_applied=%s mcp_server_count=%s channel_names=%s "
            "credential_binding_count=%s duration_ms=%s",
            self.config.worker_name,
            config.generation,
            config.team_name,
            config.member_name,
            config.member_role,
            force,
            reapply_adapter,
            adapter_should_apply,
            _count_collection(config.mcp_servers),
            _named_keys(config.channels),
            len(config.credential_bindings),
            _duration_ms(started_at),
        )
        self._apply_member_identity(config)
        if self.runtime_reconcile is not None:
            self.runtime_reconcile(config)
        self._apply_model(config)
        self._apply_mcp_servers(config)
        self._apply_matrix_channel(config)
        self._apply_dingtalk_channel(config)
        self._apply_channel_policy(config)

        applied_package = self.package_manager.apply(config)
        self._apply_package_mcp_servers(applied_package)
        self._apply_package_skills(applied_package)
        self._apply_inline_config(config)

        adapter_applied = False
        if adapter_should_apply:
            self.adapter_apply()
            adapter_applied = True

        self.refresh_team_context(config)
        self.current_config = config
        logger.info(
            "runtime config apply complete component=update worker=%s generation=%s changed=%s "
            "agent_package_dir=%s mcp_server_count=%s channel_names=%s credential_binding_count=%s "
            "adapter_applied=%s duration_ms=%s",
            self.config.worker_name,
            config.generation,
            True,
            applied_package,
            _count_collection(config.mcp_servers),
            _named_keys(config.channels),
            len(config.credential_bindings),
            adapter_applied,
            _duration_ms(started_at),
        )
        return ApplyResult(
            runtime_config=config, changed=True, agent_package_dir=applied_package
        )

    def _apply_inline_config(self, config: MemberRuntimeConfig) -> None:
        # 逻辑说明：将 Controller 内联提示字段写入对应工作区文件；未提供的字段不覆盖现状。
        prompt_files = {
            "IDENTITY.md": config.inline_config.get("identity", ""),
            "SOUL.md": config.inline_config.get("soul", ""),
            "AGENTS.md": config.inline_config.get("agents", ""),
        }
        for file_name, content in prompt_files.items():
            if not content:
                continue
            for base_dir in (
                self.config.default_workspace_dir,
                self.config.worker_home,
            ):
                path = base_dir / file_name
                path.parent.mkdir(parents=True, exist_ok=True)
                tmp = path.with_name(f".{path.name}.tmp")
                tmp.write_text(f"{content.rstrip()}\n", encoding="utf-8")
                tmp.replace(path)

    def _adapter_neutral_change(self, config: MemberRuntimeConfig) -> bool:
        # 逻辑说明：比较旧快照判断变化是否不影响适配器；首轮或关键字段变化返回 False。
        previous = self.current_config
        if previous is None:
            return False
        return (
            config.agent_package_identity == previous.agent_package_identity
            and _stable_json(config.model) == _stable_json(previous.model)
            and _stable_json(config.channel_policy)
            == _stable_json(previous.channel_policy)
            and config.credential_runtime_identity
            == previous.credential_runtime_identity
            and self._team_context_content_identity(config)
            == self._team_context_content_identity(previous)
            and (
                _stable_json(config.mcp_servers) != _stable_json(previous.mcp_servers)
                or _stable_json(config.channels) != _stable_json(previous.channels)
            )
        )

    def _team_context_content_identity(self, config: MemberRuntimeConfig) -> str:
        # 逻辑说明：移除 generation 后稳定序列化 Team 事实，避免仅版本号变化触发内容重写。
        facts = dict(config.team_context_facts)
        facts.pop("metadata", None)
        return _stable_json(facts)

    def _load_and_apply_once(self) -> None:
        # 逻辑说明：供监听器回调使用，重新加载权威配置并应用，同时保留现有适配器进程。
        self.apply_once(runtime_config=self.load(), reapply_adapter=False)

    def _apply_team_context_prompt(self, config: MemberRuntimeConfig) -> None:
        # 逻辑说明：生成受标记的 Team 上下文块并替换旧块；内容未变时避免磁盘写入。
        block = self._runtime_team_context_block(config)
        if not block:
            return
        path = self.config.default_workspace_dir / TEAMS_PROMPT_FILE
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            existing = path.read_text(encoding="utf-8")
        else:
            existing = self._render_full_team_context_prompt(config)
            if not existing:
                logger.warning(
                    "full TeamHarness TEAMS renderer unavailable component=update worker=%s action=fallback",
                    self.config.worker_name,
                )
                existing = "# TeamHarness Runtime Context\n"
        existing = self._ensure_teams_internal_marker(existing)
        if TEAMS_CONTEXT_START in existing and TEAMS_CONTEXT_END in existing:
            prefix, rest = existing.split(TEAMS_CONTEXT_START, 1)
            _old, suffix = rest.split(TEAMS_CONTEXT_END, 1)
            text = prefix.rstrip() + "\n\n" + block + suffix
        else:
            text = existing.rstrip() + "\n\n" + block + "\n"
        tmp = path.with_name(f".{path.name}.tmp")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(path)

    def _render_full_team_context_prompt(self, config: MemberRuntimeConfig) -> str:
        # 逻辑说明：有外部 renderer 时生成完整上下文并确保内部控制标记存在；否则返回空。
        if self.team_context_renderer is None:
            return ""
        try:
            text = self.team_context_renderer(config)
        except Exception as exc:
            logger.warning(
                "full TeamHarness TEAMS renderer failed component=update worker=%s error_type=%s",
                self.config.worker_name,
                type(exc).__name__,
            )
            return ""
        return text if isinstance(text, str) and text.strip() else ""

    def _ensure_teams_internal_marker(self, text: str) -> str:
        # 逻辑说明：幂等加入 runtime 管理标记，供包清理逻辑识别并保护 TEAMS.md。
        if TEAMS_INTERNAL_CONTROL_MARKER in text:
            return text
        body = text.lstrip("\n")
        return (
            f"{TEAMS_INTERNAL_CONTROL_MARKER}\n{body}"
            if body
            else f"{TEAMS_INTERNAL_CONTROL_MARKER}\n"
        )

    def _runtime_team_context_block(self, config: MemberRuntimeConfig) -> str:
        # 逻辑说明：把规范化 Team 事实渲染为 Agent 可读的 Markdown 受控块；无事实返回空。
        facts = config.team_context_facts
        if not facts:
            return ""
        team = _section(facts, "team")
        member = _section(facts, "member")
        runtime = _section(facts, "runtime")
        lines = [
            TEAMS_CONTEXT_START,
            "## Runtime Team Context",
            "",
        ]
        for key, value in (
            ("team.name", team.get("name")),
            ("team.teamRoomId", team.get("teamRoomId")),
            ("team.leaderName", team.get("leaderName")),
            ("team.leaderRuntimeName", team.get("leaderRuntimeName")),
            ("team.leaderDmRoomId", team.get("leaderDmRoomId")),
            ("team.admin.name", _section(team, "admin").get("name")),
            ("team.admin.matrixUserId", _section(team, "admin").get("matrixUserId")),
            ("member.name", member.get("name")),
            ("member.runtimeName", member.get("runtimeName")),
            ("member.role", member.get("role")),
            ("member.runtime", member.get("runtime")),
            ("member.matrixUserId", member.get("matrixUserId")),
            ("member.personalRoomId", member.get("personalRoomId")),
            (
                "runtime.model.providerId",
                _section(runtime, "model").get("providerId"),
            ),
            (
                "runtime.model.name",
                _section(runtime, "model").get("name"),
            ),
            (
                "runtime.coordinator.matrixUserId",
                _section(runtime, "coordinator").get("matrixUserId"),
            ),
        ):
            text = _string(value)
            if text:
                lines.append(f"- {key}: {text}")
        members = team.get("members")
        if isinstance(members, list) and members:
            lines.extend(["", "### Team Members"])
            for item in members:
                entry = _string_fields(
                    item,
                    ("name", "runtimeName", "role", "matrixUserId", "personalRoomId"),
                )
                if entry:
                    lines.append(
                        "- "
                        + ", ".join(f"{key}: {value}" for key, value in entry.items())
                    )
        lines.extend(
            [
                "",
                "Do not write secrets, credentials, or live task status into this file.",
                TEAMS_CONTEXT_END,
            ]
        )
        return "\n".join(lines)

    def _apply_member_identity(self, config: MemberRuntimeConfig) -> None:
        # 逻辑说明：把成员名和角色映射到默认 Agent 身份配置，并通过 API 写入验证。
        role = config.member_role
        if role:
            self.config.agent_role = role
            os.environ["AGENTTEAMS_AGENT_ROLE"] = role
            os.environ["AGENTTEAMS_WORKER_ROLE"] = role

    def _apply_model(self, config: MemberRuntimeConfig) -> None:
        # 逻辑说明：解析期望 provider/model 与网关地址，调用 QwenPaw API 建模并切换活动模型。
        model = config.model
        if not model:
            return
        provider_id = _string(
            model.get("providerId") or model.get("provider_id") or model.get("provider")
        )
        model_name = _string(model.get("model") or model.get("name"))
        if not provider_id or not model_name:
            return
        if self.api_client is None:
            raise RuntimeError("QwenPaw API client is required for model configuration")
        base_url = _string(
            model.get("baseUrl")
            or model.get("base_url")
            or model.get("gatewayUrl")
            or model.get("gateway_url")
            or model.get("endpoint")
            or os.getenv("AGENTTEAMS_AI_GATEWAY_URL")
        )
        api_key = _string(model.get("apiKey") or model.get("api_key"))
        api_key_env = _string(
            model.get("apiKeyEnv")
            or model.get("api_key_env")
            or config.credentials.get("gatewayKeyEnv")
            or "AGENTTEAMS_WORKER_GATEWAY_KEY"
        )
        if not api_key and api_key_env:
            api_key = _string(os.getenv(api_key_env))
        self.api_client.configure_active_model(
            provider_id,
            model_name,
            base_url=self._openai_compatible_base_url(base_url) if base_url else "",
            api_key=api_key,
            provider_name=_string(
                model.get("providerName") or model.get("provider_name") or provider_id
            ),
            chat_model=_string(
                model.get("chatModel") or model.get("chat_model") or "OpenAIChatModel"
            ),
        )

    def _openai_compatible_base_url(self, base_url: str) -> str:
        # 逻辑说明：规范化 OpenAI 兼容网关 URL，缺少 /v1 时补上，空值保持为空。
        value = base_url.rstrip("/")
        if value.endswith("/v1"):
            return value
        return f"{value}/v1"

    def _apply_mcp_servers(self, config: MemberRuntimeConfig) -> None:
        # 逻辑说明：将 runtime MCP 与现有受管客户端做增删改并校验工具，不触碰其他客户端。
        servers = self._mcporter_servers(config)
        if self.api_client is None:
            if servers:
                raise RuntimeError(
                    "QwenPaw API client is required for MCP configuration"
                )
            return
        existing = {str(item.get("key")): item for item in self.api_client.list_mcp()}
        ownership_path = (
            self.config.qwenpaw_working_dir / ".agentteams-managed-mcp.json"
        )
        try:
            managed = set(json.loads(ownership_path.read_text(encoding="utf-8")))
        except (FileNotFoundError, json.JSONDecodeError, TypeError):
            managed = set()
        for key in sorted((managed & existing.keys()) - servers.keys()):
            self.api_client.delete_mcp(key)
        for key, server in servers.items():
            transport = _string(server.get("transport") or "http")
            payload = {
                "name": key,
                "enabled": True,
                "transport": "streamable_http"
                if transport in {"http", "streamable_http"}
                else transport,
                "url": _string(server.get("url")),
                "headers": dict(server.get("headers") or {}),
                "command": _string(server.get("command")),
                "args": list(server.get("args") or []),
                "env": dict(server.get("env") or {}),
                "cwd": _string(server.get("cwd")),
            }
            if key in existing:
                self.api_client.update_mcp(key, payload)
            else:
                self.api_client.create_mcp(key, payload)
        ownership_path.parent.mkdir(parents=True, exist_ok=True)
        ownership_path.write_text(
            json.dumps(sorted(servers), ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    def _apply_package_mcp_servers(self, package_dir: Optional[Path]) -> None:
        # 逻辑说明：解析包附带 MCP 并与 QwenPaw 对账，删除旧包已移除的受管项。
        package_clients = getattr(self.package_manager, "package_mcp_clients", None)
        servers = package_clients(package_dir) if callable(package_clients) else {}
        if self.api_client is None:
            if servers:
                raise RuntimeError(
                    "QwenPaw API client is required for agent package MCP configuration",
                )
            return
        existing = {str(item.get("key")): item for item in self.api_client.list_mcp()}
        ownership_path = (
            self.config.qwenpaw_working_dir / ".agentteams-managed-package-mcp.json"
        )
        try:
            managed = set(json.loads(ownership_path.read_text(encoding="utf-8")))
        except (FileNotFoundError, json.JSONDecodeError, TypeError):
            managed = set()
        for key in sorted((managed & existing.keys()) - servers.keys()):
            self.api_client.delete_mcp(key)
        for key, server in servers.items():
            payload = dict(server)
            payload.setdefault("name", key)
            if key in existing:
                self.api_client.update_mcp(key, payload)
            else:
                self.api_client.create_mcp(key, payload)
        ownership_path.parent.mkdir(parents=True, exist_ok=True)
        ownership_path.write_text(
            json.dumps(sorted(servers), ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    def _apply_package_skills(self, package_dir: Optional[Path]) -> None:
        # 逻辑说明：收集包内技能并调用 API 刷新启用；依赖或技能缺失时幂等跳过。
        if package_dir is None or self.api_client is None:
            return
        package_skills = getattr(self.package_manager, "package_skill_names", None)
        skill_names = package_skills(package_dir) if callable(package_skills) else []
        if skill_names:
            self.api_client.refresh_and_enable_skills(skill_names)

    def _apply_channel_policy(self, config: MemberRuntimeConfig) -> None:
        # 逻辑说明：计算 Matrix 群聊/私聊允许与拒绝集合，并写入 ACL 后读回验证。
        group_allow, dm_allow, group_deny, dm_deny = self._matrix_policy_ids(config)
        if not (group_allow or dm_allow or group_deny or dm_deny):
            return

        self_allow = _string(config.member.get("matrixUserId"))
        self_allowlist = [self_allow] if self_allow else []
        whitelist = self._dedupe(self_allowlist + group_allow + dm_allow)
        blacklist = self._dedupe(group_deny + dm_deny)
        deny_set = set(blacklist)
        whitelist = [value for value in whitelist if value not in deny_set]
        self._apply_matrix_channel_access_flags(
            group_enabled=bool(group_allow or group_deny),
            dm_enabled=bool(dm_allow or dm_deny),
        )
        self._write_matrix_access_control(whitelist, blacklist)

    def _apply_matrix_channel(self, config: MemberRuntimeConfig) -> None:
        # 逻辑说明：构造 Matrix 期望配置、写入秘密保留字段和访问标志，再停用遗留客户端。
        desired = self._matrix_channel_desired_state(config)
        if desired is None:
            return
        if self.api_client is None:
            raise RuntimeError(
                "QwenPaw API client is required for Matrix configuration"
            )
        self._disable_legacy_matrix_channel()
        groups: Dict[str, Any] = {}
        self._ensure_group_mention_policy(groups, "*", require_mention=True)
        personal_room_id = desired["personal_room_id"]
        if personal_room_id:
            self._ensure_group_mention_policy(
                groups,
                personal_room_id,
                require_mention=False,
            )
        team_room_id = desired["team_room_id"]
        if team_room_id:
            self._ensure_group_mention_policy(
                groups,
                team_room_id,
                require_mention=True,
            )
        self.api_client.put_channel(
            "agentteams_matrix",
            {
                "enabled": True,
                "homeserver": desired["homeserver"],
                "user_id": desired["user_id"],
                "access_token": desired["access_token"],
                "password": "",
                "encryption": _env_bool("AGENTTEAMS_MATRIX_E2EE"),
                "group_disabled": False,
                "dm_disabled": False,
                "show_tool_calls": True,
                "show_tool_results": True,
                "show_thinking": True,
                "groups": groups,
            },
            secret_fields={"access_token", "password"},
        )

    def _disable_legacy_matrix_channel(self) -> None:
        """Prevent the built-in Matrix client from consuming events twice.

        AgentTeams owns the ``agentteams_matrix`` plugin channel.  Workspaces
        upgraded from the pre-plugin runtime may still have QwenPaw's built-in
        ``matrix`` channel enabled with the same account.  Leaving both clients
        active duplicates replies and applies the stale ``matrix`` ACL instead
        of the current Team roster.
        """
        # 逻辑说明：将旧 matrix channel 设为禁用，防止两个消费者争抢同一 sync token。
        if self.api_client is None:
            raise RuntimeError(
                "QwenPaw API client is required for Matrix configuration"
            )
        legacy = self.api_client.get_channel("matrix")
        if legacy.get("enabled") is not True:
            return
        self.api_client.put_channel(
            "matrix",
            {**legacy, "enabled": False},
            secret_fields={"access_token", "password"},
        )

    def _apply_dingtalk_channel(self, config: MemberRuntimeConfig) -> None:
        # 逻辑说明：将可选钉钉配置写入 QwenPaw 并保留秘密字段；未配置时不创建 channel。
        desired = config.dingtalk_channel
        if desired is None:
            return
        if self.api_client is None:
            raise RuntimeError(
                "QwenPaw API client is required for DingTalk configuration"
            )
        current = self.api_client.get_channel("dingtalk")
        if not _bool(desired.get("enabled")):
            self.api_client.put_channel(
                "dingtalk",
                {**current, "enabled": False},
                secret_fields={"client_secret"},
            )
            return

        streaming_enabled = _bool(desired.get("streaming_enabled"))
        client_id = _string(desired.get("client_id"))
        client_secret = _string(desired.get("client_secret"))
        robot_code = _string(desired.get("robot_code"))
        desired_fields = {
            "enabled": True,
            "client_id": client_id,
            "client_secret": client_secret,
            "robot_code": robot_code,
            "show_thinking": not _bool(desired.get("filter_thinking")),
            "show_tool_calls": not _bool(desired.get("filter_tool_messages")),
            "show_tool_results": not _bool(desired.get("filter_tool_messages")),
            "streaming_enabled": streaming_enabled,
        }
        if streaming_enabled:
            missing = [
                name
                for name, value in (
                    ("client_id", client_id),
                    ("client_secret", client_secret),
                    ("robot_code", robot_code),
                    ("card_template_id", _string(desired.get("card_template_id"))),
                )
                if not value
            ]
            if missing:
                raise ValueError(
                    "DingTalk streaming requires client_id, client_secret, "
                    "robot_code, and card_template_id. Create and publish the "
                    "streaming card template in DingTalk Open Platform, select "
                    "card mode, then set card_template_id; missing "
                    f"{', '.join(missing)}"
                )
            card_template_id = _string(desired.get("card_template_id"))
            previous_message_type = _string(current.get("message_type"))
            previous_template_id = _string(current.get("card_template_id"))
            if (
                previous_message_type == "card"
                and previous_template_id
                and previous_template_id != card_template_id
            ):
                logger.warning(
                    "DingTalk streaming enabled; current runtime card configuration will switch "
                    "component=update step=apply_dingtalk_channel previous_template=%s next_template=%s "
                    "existing_template_deleted=False",
                    previous_template_id,
                    card_template_id,
                )
            desired_fields.update(
                {
                    "message_type": "card",
                    "card_template_id": card_template_id,
                    "card_template_key": _string(
                        desired.get("card_template_key") or "content"
                    ),
                    "card_auto_layout": False,
                }
            )
        else:
            if "message_type" in desired:
                desired_fields["message_type"] = _string(
                    desired.get("message_type") or "markdown"
                )
            if "card_template_id" in desired:
                desired_fields["card_template_id"] = _string(
                    desired.get("card_template_id")
                )
            if "card_template_key" in desired:
                desired_fields["card_template_key"] = _string(
                    desired.get("card_template_key") or "content"
                )
            if "card_auto_layout" in desired:
                desired_fields["card_auto_layout"] = _bool(
                    desired.get("card_auto_layout")
                )
        self.api_client.put_channel(
            "dingtalk",
            {**current, **desired_fields},
            secret_fields={"client_secret"},
        )

    def _matrix_channel_desired_state(
        self, config: MemberRuntimeConfig
    ) -> Optional[Dict[str, str]]:
        # 逻辑说明：汇总环境和成员配置形成 Matrix channel 期望状态；关键连接字段缺失则返回 None。
        homeserver = _string(
            os.getenv("AGENTTEAMS_MATRIX_URL")
            or os.getenv("AGENTTEAMS_MATRIX_SERVER")
            or os.getenv("AGENTTEAMS_MATRIX_HOMESERVER")
        ).rstrip("/")
        user_id = _string(
            config.member.get("matrixUserId") or os.getenv("AGENTTEAMS_MATRIX_USER_ID")
        )
        if not user_id:
            matrix_domain = _string(os.getenv("AGENTTEAMS_MATRIX_DOMAIN"))
            if config.member_name and matrix_domain:
                user_id = f"@{config.member_name}:{matrix_domain}"
        token_env = _string(
            config.credentials.get("matrixTokenEnv") or "AGENTTEAMS_WORKER_MATRIX_TOKEN"
        )
        access_token = _string(os.getenv(token_env)) if token_env else ""
        if not access_token:
            access_token = _string(os.getenv("AGENTTEAMS_MATRIX_TOKEN"))
        team_room_id = _string(config.team.get("teamRoomId"))
        personal_room_id = _string(config.member.get("personalRoomId"))
        if not (homeserver and user_id and access_token):
            return None
        return {
            "homeserver": homeserver,
            "user_id": user_id,
            "access_token": access_token,
            "team_room_id": team_room_id,
            "personal_room_id": personal_room_id,
        }

    def _ensure_group_mention_policy(
        self,
        groups: Dict[str, Any],
        room_id: str,
        *,
        require_mention: bool,
    ) -> bool:
        # 逻辑说明：复制房间配置、移除旧 autoReply 并设置 mention 策略；返回是否真的发生变化。
        room_cfg = dict(groups.get(room_id) or {})
        changed = False
        if room_cfg.pop("autoReply", None) is not None:
            changed = True
        if room_cfg.get("requireMention") is not require_mention:
            room_cfg["requireMention"] = require_mention
            changed = True
        if changed:
            groups[room_id] = room_cfg
        return changed

    def _matrix_policy_ids(
        self, config: MemberRuntimeConfig
    ) -> Tuple[List[str], List[str], List[str], List[str]]:
        # 逻辑说明：从默认名册和额外规则生成四类 ACL，补齐 Matrix 域并保序去重。
        policy = config.channel_policy
        domain = _string(os.getenv("AGENTTEAMS_MATRIX_DOMAIN"))
        group_allow = self._default_group_allow(config, domain)
        dm_allow = list(group_allow)
        group_allow.extend(
            self._matrix_ids(_string_list(policy.get("groupAllowExtra")), domain)
        )
        dm_allow.extend(
            self._matrix_ids(_string_list(policy.get("dmAllowExtra")), domain)
        )
        group_deny = self._matrix_ids(
            _string_list(policy.get("groupDenyExtra")), domain
        )
        dm_deny = self._matrix_ids(_string_list(policy.get("dmDenyExtra")), domain)
        return (
            self._dedupe(group_allow),
            self._dedupe(dm_allow),
            self._dedupe(group_deny),
            self._dedupe(dm_deny),
        )

    def _default_group_allow(
        self, config: MemberRuntimeConfig, domain: str
    ) -> List[str]:
        # 逻辑说明：优先按完整 Team 名册产生允许列表；名册不可用时安全回落管理员与协调者。
        team_admin = _string(_section(config.team, "admin").get("matrixUserId"))
        system_admin_user = _string(os.getenv("AGENTTEAMS_ADMIN_USER") or "admin")
        system_admin = self._matrix_id(system_admin_user, domain)
        admin = team_admin or system_admin
        roster_allow = self._team_roster_group_allow(config, domain, admin)
        if roster_allow:
            # Ensure system admin is always present even when team admin differs
            if system_admin and system_admin not in roster_allow:
                roster_allow.append(system_admin)
            return roster_allow
        if config.team and config.member_role not in {"team_leader", "leader"}:
            leader = _string(
                config.team.get("leaderRuntimeName") or config.team.get("leaderName")
            )
            manager = self._matrix_id("manager", domain)
            return [
                item
                for item in (
                    manager,
                    self._matrix_id(leader, domain),
                    admin,
                    system_admin,
                )
                if item
            ]
        manager = self._matrix_id("manager", domain)
        return [item for item in (manager, admin, system_admin) if item]

    def _team_roster_group_allow(
        self, config: MemberRuntimeConfig, domain: str, admin: str
    ) -> List[str]:
        # 逻辑说明：从 Team 名册提取管理员、当前成员及同队成员 Matrix ID，缺数据时拒绝猜测。
        members = config.team_members
        if not config.team or not members:
            return []

        current_names = {
            _string(config.member.get("name")),
            _string(config.member.get("runtimeName")),
        }
        current_names.discard("")
        current_mxid = _string(config.member.get("matrixUserId"))
        leader_roles = {"team_leader", "leader"}
        leader_ids: List[str] = []
        peer_ids: List[str] = []

        for member in members:
            mxid = self._member_matrix_id(member, domain)
            if not mxid:
                continue
            if (
                mxid == current_mxid
                or _string(member.get("runtimeName") or member.get("name"))
                in current_names
            ):
                continue
            if _string(member.get("role")) in leader_roles:
                leader_ids.append(mxid)
            else:
                peer_ids.append(mxid)

        if config.member_role in leader_roles:
            manager = self._matrix_id("manager", domain)
            return [item for item in (manager, admin, *peer_ids) if item]

        leader = _string(
            config.team.get("leaderRuntimeName") or config.team.get("leaderName")
        )
        if not leader_ids:
            leader_ids = [self._matrix_id(leader, domain)]
        manager = self._matrix_id("manager", domain)
        return [item for item in (manager, *leader_ids, admin, *peer_ids) if item]

    def _member_matrix_id(self, member: Dict[str, str], domain: str) -> str:
        # 逻辑说明：优先采用成员显式 mxid，否则用 runtimeName/name 和已知域构造 ID。
        mxid = _string(member.get("matrixUserId"))
        if mxid:
            return mxid
        return self._matrix_id(
            _string(member.get("runtimeName") or member.get("name")), domain
        )

    def _matrix_ids(self, values: List[str], domain: str) -> List[str]:
        return [
            mxid
            for mxid in (self._matrix_id(value, domain) for value in values)
            if mxid
        ]

    def _matrix_id(self, value: str, domain: str) -> str:
        # 逻辑说明：保留已有用户/房间 ID；普通名称仅在有域时转换，避免生成无效 mxid。
        text = _string(value)
        if not text:
            return ""
        if text.startswith("@") or text.startswith("!"):
            return text
        return f"@{text}:{domain}" if domain else ""

    def _mcporter_servers(self, config: MemberRuntimeConfig) -> Dict[str, Any]:
        # 逻辑说明：兼容 MCP 列表或映射形态，逐条转换为 mcporter 配置并注入可选网关认证。
        raw = config.mcp_servers
        gateway_key = self._gateway_key(config)
        if isinstance(raw, dict) and isinstance(raw.get("mcpServers"), dict):
            raw = raw["mcpServers"]

        servers: Dict[str, Any] = {}
        if isinstance(raw, list):
            for item in raw:
                if isinstance(item, dict):
                    name = _string(item.get("name"))
                    payload = self._mcporter_server_payload(item, gateway_key)
                    if name and payload:
                        servers[name] = payload
            return servers

        if isinstance(raw, dict):
            for name, item in raw.items():
                if isinstance(item, dict):
                    payload = self._mcporter_server_payload(item, gateway_key)
                    if _string(name) and payload:
                        servers[_string(name)] = payload
        return servers

    def _mcporter_server_payload(
        self, item: Dict[str, Any], gateway_key: str
    ) -> Dict[str, Any]:
        # 逻辑说明：验证远端 MCP URL、合并 headers 和网关 Bearer token，返回标准 HTTP client 配置。
        url = _string(item.get("url"))
        if not url:
            return {}
        headers = item.get("headers")
        headers = dict(headers) if isinstance(headers, dict) else {}
        if gateway_key and "Authorization" not in headers:
            headers["Authorization"] = f"Bearer {gateway_key}"
        return {
            "url": url,
            "transport": _string(item.get("transport") or "http"),
            "headers": headers,
        }

    def _gateway_key(self, config: MemberRuntimeConfig) -> str:
        # 逻辑说明：按 runtime 指定的环境变量名读取模型/MCP 网关 key；缺失名称或值时为空。
        env_name = _string(
            config.credentials.get("gatewayKeyEnv") or "AGENTTEAMS_WORKER_GATEWAY_KEY"
        )
        return os.getenv(env_name, "") if env_name else ""

    def _apply_matrix_channel_access_flags(
        self, group_enabled: bool, dm_enabled: bool
    ) -> None:
        # 逻辑说明：读取当前 Matrix channel、只更新群聊/私聊启用标志，并由 API 做读回校验。
        if self.api_client is None:
            raise RuntimeError(
                "QwenPaw API client is required for Matrix ACL configuration"
            )
        current = self.api_client.get_channel("agentteams_matrix")
        self.api_client.put_channel(
            "agentteams_matrix",
            {
                **current,
                "access_control_group": group_enabled,
                "access_control_dm": dm_enabled,
            },
            secret_fields={"access_token", "password"},
        )

    def _write_matrix_access_control(
        self, whitelist: List[str], blacklist: List[str]
    ) -> None:
        # 逻辑说明：把计算后的允许/拒绝列表交给 API 差异化对账；无 API 时明确失败。
        if self.api_client is None:
            raise RuntimeError(
                "QwenPaw API client is required for Matrix ACL configuration"
            )
        self.api_client.reconcile_acl("agentteams_matrix", whitelist, blacklist)

    def _dedupe(self, values: List[str]) -> List[str]:
        # 逻辑说明：按首次出现顺序去重 ID，保持策略可预测且避免重复 ACL 请求。
        result = []
        seen = set()
        for value in values:
            if value not in seen:
                result.append(value)
                seen.add(value)
        return result

    async def loop(self) -> None:
        # 逻辑说明：按轮询周期加载并应用新 runtime config；单轮失败记录后继续，取消时正常退出。
        logger.info(
            "runtime config update loop started component=update worker=%s interval_seconds=%s",
            self.config.worker_name,
            self.config.runtime_config_poll_interval,
        )
        try:
            while True:
                await asyncio.sleep(self.config.runtime_config_poll_interval)
                try:
                    started_at = time.monotonic()
                    await asyncio.to_thread(self._load_and_apply_once)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "runtime config update failed component=update worker=%s error_type=%s duration_ms=%s",
                        self.config.worker_name,
                        type(exc).__name__,
                        _duration_ms(started_at),
                    )
        except asyncio.CancelledError:
            logger.info(
                "runtime config update loop stopped component=update worker=%s",
                self.config.worker_name,
            )
            raise
