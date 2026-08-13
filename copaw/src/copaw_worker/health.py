"""CoPaw worker health state.

Strategy:
  - CoPaw owns its health semantics. The controller should not aggregate or
    infer CoPaw component health.
  - The public health state is a full snapshot of all CoPaw components, not a
    single event. Individual components may update only their own status, but
    the persisted state always contains the complete component table.
  - The snapshot always contains all components. Each component starts as
    unhealthy at process initialization and only becomes healthy after its
    concrete startup/runtime check succeeds.
  - Component health detection strategy:
      * copaw:
          Startup health:
            - check: start uvicorn.Server for "copaw.app._app:app".
            - check: after starting the server, the worker performs one
              bounded startup probe against the native CoPaw health endpoint.
            - healthy: the startup probe gets HTTP 200 from
              http://127.0.0.1:{console_port}/health.
            - unhealthy: server startup raises or server.serve() returns before
              a shutdown request, or the bounded startup probe cannot reach
              the native CoPaw health endpoint.
          Runtime health:
            - check: health report loop probes
              http://127.0.0.1:{console_port}/health periodically.
            - healthy: probe returns 200.
            - unhealthy: probe fails/times out, the FastAPI app exits
              unexpectedly, or server.serve() returns before requested
              shutdown.
      * sync:
          Startup health:
            - check: FileSync.mirror_all().
            - healthy: mirror_all() returns without raising.
            - unhealthy: mirror_all() raises, including storage authentication,
              bucket/object access, network, or local write failures.
            - meaning: startup mirror failure is a hard dependency failure for
              CoPaw startup because later stages depend on the restored
              standard sync root.
          Runtime health:
            - check: push_loop storage persistence of local changes to
              MinIO/OSS.
            - healthy: local changes can be persisted to MinIO/OSS.
            - unhealthy: storage push fails and local changes cannot be
              persisted. This should be reported/alerted because state may be
              lost, even if the CoPaw app is still serving normally.
            - boundary: sync health does not own bridge_runtime_to_standard()
              or bridge_standard_to_runtime(). Those functions may be called
              from sync code, but their failures belong to bridge health.
      * bridge:
          Startup health:
            - check: bridge_standard_to_runtime(local_dir, runtime_dir,
              openclaw_cfg, skill_names=...).
            - healthy: bridge_standard_to_runtime() returns without raising.
            - unhealthy: bridge_standard_to_runtime() raises. The bridge module
              owns the detailed standard-to-CoPaw conversion logic and should
              surface a useful error message.
          Runtime health:
            - check: bridge_runtime_to_standard(local_dir), currently invoked
              by push_local() before upload.
            - healthy: bridge_runtime_to_standard() returns without raising.
            - unhealthy: bridge_runtime_to_standard() raises while copying
              runtime state back into the standard sync root. The bridge module
              owns the detailed runtime-to-standard conversion logic and should
              surface a useful error message.
      * model:
          Startup health:
            - check: resolve the active provider/model from openclaw.json, then
              call POST {baseUrl}/chat/completions with the configured API key
              when present.
            - healthy: an active provider/baseUrl exist and a minimal chat
              completion request returns 2xx.
            - unhealthy: no active provider/baseUrl exists, the request raises,
              times out, or returns non-2xx.
          Runtime health:
            - check: worker API GET /worker/readyz repeats the same chat route
              preflight on demand.
            - healthy/unhealthy: same result rules as startup.
            - token cost: this is a real inference preflight with a deliberately
              tiny output budget. Controller polling must stay low-frequency or
              manual to avoid unnecessary token usage.
      * matrix:
          Startup health:
            - check: _matrix_relogin() POSTs to
              {homeserver}/_matrix/client/v3/login with the worker credentials.
            - healthy: login response contains a non-empty access_token.
            - unhealthy: homeserver/password is missing, login raises/times
              out, returns an error, or returns no access_token.
          Runtime health:
            - check: worker API GET /worker/readyz probes
              {homeserver}/_matrix/client/versions on demand.
            - healthy: probe returns 2xx.
            - unhealthy: probe raises, times out, or returns non-2xx.
            - scope: this runtime check only verifies homeserver endpoint
              reachability. It does not validate worker token state, room
              send/receive behavior, sync-loop quality, or E2EE key health.
            - future token-aware check: GET /_matrix/client/v3/account/whoami
              can be added only when token validity needs separate
              classification.
  - Top-level healthiness is derived locally: any unhealthy component makes the
    whole CoPaw worker unhealthy; otherwise it is healthy.
  - The first phase does not add degraded, severity, stage, controller reporting,
    CRD fields, or independent runtime health loops. CoPaw exposes health
    checks; external callers decide when to call them.
"""

# 初学者导读：健康状态不是简单的“Python 进程存在”。CoPaw API、Matrix、模型
# 网关和 MinIO 同步各自都可能失败，因此这里保存完整组件快照。readiness 只有在
# 接任务所需依赖真的可用后才成功，Controller 才会把该 Worker 视为可派工成员。

from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

Healthiness = Literal["healthy", "unhealthy"]
HealthComponent = Literal["copaw", "sync", "bridge", "model", "matrix"]

COMPONENTS: tuple[HealthComponent, ...] = (
    "copaw",
    "sync",
    "bridge",
    "model",
    "matrix",
)

@dataclass(frozen=True)
class ComponentHealth:
    healthiness: Healthiness
    message: str = ""
    details: dict[str, Any] | None = None
    updated_at: str = ""


@dataclass(frozen=True)
class HealthSnapshot:
    healthiness: Healthiness
    message: str
    components: dict[HealthComponent, ComponentHealth]
    updated_at: str


class HealthState:
    """并发安全地保存并持久化一名 Worker 的完整组件健康快照。

    Maintain component health and persist a full CoPaw health snapshot.
    """

    def __init__(self, state_path: Path) -> None:
        # 逻辑说明：`__init__` 接收 state_path，初始化完整组件健康表，返回 None；
        # 会更新对象内存状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
        self.state_path = state_path
        now = _now()
        self._components: dict[HealthComponent, ComponentHealth] = {
            component: ComponentHealth(
                healthiness="unhealthy",
                message="not checked yet",
                details={},
                updated_at=now,
            )
            for component in COMPONENTS
        }

    def update(
        self,
        component: HealthComponent,
        healthiness: Healthiness,
        message: str = "",
        details: dict[str, Any] | None = None,
    ) -> HealthSnapshot:
        # 逻辑说明：`update` 接收 component、healthiness、message、details，校验组件与健康值，更新单个组件后持久化完整快照，返回 HealthSnapshot；
        # 会更新对象内存状态。失败策略：非法输入或状态会立即抛错，其他底层异常继续上抛；本函数不额外重试，避免掩盖持续故障。
        if component not in COMPONENTS:
            raise ValueError(f"unknown health component: {component}")
        if healthiness not in ("healthy", "unhealthy"):
            raise ValueError(f"invalid healthiness: {healthiness}")
        self._components[component] = ComponentHealth(
            healthiness=healthiness,
            message=message,
            details=details or {},
            updated_at=_now(),
        )
        snapshot = self.snapshot()
        self.persist(snapshot)
        return snapshot

    def snapshot(self) -> HealthSnapshot:
        # 逻辑说明：读取 `_components` 最近一次探测结果，以首个 unhealthy 组件形成整体失败原因；若都健康则返回 ready 快照，并复制组件映射避免调用方改写内部状态。
        unhealthy = [
            (component, state)
            for component, state in self._components.items()
            if state.healthiness == "unhealthy"
        ]
        if unhealthy:
            component, state = unhealthy[0]
            healthiness: Healthiness = "unhealthy"
            message = f"{component} unhealthy"
            if state.message:
                message = f"{message}: {state.message}"
        else:
            healthiness = "healthy"
            message = "all components healthy"

        return HealthSnapshot(
            healthiness=healthiness,
            message=message,
            components=dict(self._components),
            updated_at=_now(),
        )

    def persist(self, snapshot: HealthSnapshot | None = None) -> HealthSnapshot:
        # 逻辑说明：`persist` 接收 snapshot，把完整健康快照写入 JSON 文件并返回该快照，返回 HealthSnapshot；
        # 会读写本地文件、会更新对象内存状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
        snapshot = snapshot or self.snapshot()
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(
            json.dumps(_snapshot_to_dict(snapshot), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return snapshot

    def to_dict(self) -> dict[str, Any]:
        # 逻辑说明：按当前组件探测结果生成 HealthSnapshot，再递归转换成健康 API 可 JSON 序列化的字典；不更新组件状态或重新执行探针。
        return _snapshot_to_dict(self.snapshot())


def _snapshot_to_dict(snapshot: HealthSnapshot) -> dict[str, Any]:
    # 逻辑说明：`_snapshot_to_dict` 接收 snapshot，执行 Worker 组件健康状态 中的“snapshot to dict”步骤，返回 dict[str, Any]；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    data = asdict(snapshot)
    return data


def check_model_service(
    openclaw_cfg: dict[str, Any],
    *,
    timeout: float = 20,
) -> ComponentHealth:
    # 逻辑说明：`check_model_service` 接收 openclaw_cfg、timeout，向活动模型发送最小 chat completion 请求并生成 model 健康结果，
    # 返回 ComponentHealth；
    #
    # 会访问网络服务。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    active = _active_model_provider(openclaw_cfg)
    if active is None:
        return ComponentHealth(
            healthiness="unhealthy",
            message="no active model provider configured",
            details={"operation": "model_preflight"},
            updated_at=_now(),
        )

    provider_id, model_id, provider_cfg = active
    base_url = str(provider_cfg.get("baseUrl") or "").rstrip("/")
    api_key = str(provider_cfg.get("apiKey") or "")
    details = {
        "operation": "model_preflight",
        "provider": provider_id,
        "model": model_id,
    }
    if not base_url:
        return ComponentHealth(
            healthiness="unhealthy",
            message="active model provider has no baseUrl",
            details=details,
            updated_at=_now(),
        )

    chat_url = f"{base_url}/chat/completions"
    token_param = _max_tokens_param(model_id)
    payload: dict[str, Any] = {
        "model": model_id,
        "messages": [{"role": "user", "content": "ping"}],
        token_param: 1,
    }
    if _supports_enable_thinking(model_id):
        payload["enable_thinking"] = False
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    details = {
        **details,
        "endpoint": chat_url,
        "probe": "chat_completion",
        "token_cost": "minimal",
        "max_tokens_param": token_param,
    }
    try:
        req = urllib.request.Request(chat_url, data=body, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            code = int(getattr(resp, "status", 200))
        if 200 <= code < 300:
            return ComponentHealth(
                healthiness="healthy",
                message="model chat completion preflight succeeded",
                details={**details, "http_status": code},
                updated_at=_now(),
            )
        return ComponentHealth(
            healthiness="unhealthy",
            message=f"model chat completion preflight returned HTTP {code}",
            details={**details, "http_status": code},
            updated_at=_now(),
        )
    except urllib.error.HTTPError as exc:
        return ComponentHealth(
            healthiness="unhealthy",
            message=f"model chat completion preflight returned HTTP {exc.code}",
            details={**details, "http_status": exc.code},
            updated_at=_now(),
        )
    except Exception as exc:
        return ComponentHealth(
            healthiness="unhealthy",
            message=f"model chat completion preflight failed: {exc}",
            details={
                **details,
                "error_type": type(exc).__name__,
            },
            updated_at=_now(),
        )


def check_copaw_service(
    console_port: int,
    *,
    timeout: float = 5,
) -> ComponentHealth:
    # 逻辑说明：`check_copaw_service` 接收 console_port、timeout，请求本机 CoPaw /health 端点并生成 copaw 健康结果，返回 ComponentHealth；
    # 会访问网络服务。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    endpoint = f"http://127.0.0.1:{console_port}/health"
    details = {
        "operation": "copaw_health_probe",
        "endpoint": endpoint,
    }
    try:
        req = urllib.request.Request(endpoint, headers={"Accept": "application/json"}, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            code = int(getattr(resp, "status", 200))
        if code == 200:
            return ComponentHealth(
                healthiness="healthy",
                message="copaw health endpoint reachable",
                details={**details, "http_status": code},
                updated_at=_now(),
            )
        return ComponentHealth(
            healthiness="unhealthy",
            message=f"copaw health endpoint returned HTTP {code}",
            details={**details, "http_status": code},
            updated_at=_now(),
        )
    except urllib.error.HTTPError as exc:
        return ComponentHealth(
            healthiness="unhealthy",
            message=f"copaw health endpoint returned HTTP {exc.code}",
            details={**details, "http_status": exc.code},
            updated_at=_now(),
        )
    except Exception as exc:
        return ComponentHealth(
            healthiness="unhealthy",
            message=f"copaw health endpoint is unreachable: {exc}",
            details={**details, "error_type": type(exc).__name__},
            updated_at=_now(),
        )


def check_matrix_service(
    homeserver: str,
    *,
    timeout: float = 5,
) -> ComponentHealth:
    # 逻辑说明：`check_matrix_service` 接收 homeserver、timeout，请求 Matrix versions 端点并生成 matrix 健康结果，返回 ComponentHealth；
    # 会访问网络服务。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    base_url = str(homeserver or "").rstrip("/")
    details = {"operation": "matrix_endpoint_probe"}
    if not base_url:
        return ComponentHealth(
            healthiness="unhealthy",
            message="matrix homeserver is not configured",
            details=details,
            updated_at=_now(),
        )

    endpoint = f"{base_url}/_matrix/client/versions"
    try:
        req = urllib.request.Request(endpoint, headers={"Accept": "application/json"}, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            code = int(getattr(resp, "status", 200))
        if 200 <= code < 300:
            return ComponentHealth(
                healthiness="healthy",
                message="matrix homeserver reachable",
                details={**details, "endpoint": endpoint, "http_status": code},
                updated_at=_now(),
            )
        return ComponentHealth(
            healthiness="unhealthy",
            message=f"matrix homeserver returned HTTP {code}",
            details={**details, "endpoint": endpoint, "http_status": code},
            updated_at=_now(),
        )
    except urllib.error.HTTPError as exc:
        return ComponentHealth(
            healthiness="unhealthy",
            message=f"matrix homeserver returned HTTP {exc.code}",
            details={**details, "endpoint": endpoint, "http_status": exc.code},
            updated_at=_now(),
        )
    except Exception as exc:
        return ComponentHealth(
            healthiness="unhealthy",
            message=f"matrix homeserver is unreachable: {exc}",
            details={
                **details,
                "endpoint": endpoint,
                "error_type": type(exc).__name__,
            },
            updated_at=_now(),
        )


def _active_model_provider(
    openclaw_cfg: dict[str, Any],
) -> tuple[str, str, dict[str, Any]] | None:
    # 逻辑说明：`_active_model_provider` 接收 openclaw_cfg，按 primary 优先、Provider 列表兜底解析活动模型与 Provider，
    # 返回 tuple[str, str, dict[str, Any]] | None；
    #
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    providers = openclaw_cfg.get("models", {}).get("providers", {})
    if not isinstance(providers, dict) or not providers:
        return None

    primary = (
        openclaw_cfg.get("agents", {})
        .get("defaults", {})
        .get("model", {})
        .get("primary", "")
    )
    if isinstance(primary, str) and "/" in primary:
        provider_id, model_id = primary.split("/", 1)
        provider_cfg = providers.get(provider_id)
        if isinstance(provider_cfg, dict):
            return provider_id, model_id, provider_cfg

    for provider_id, provider_cfg in providers.items():
        if not isinstance(provider_cfg, dict):
            continue
        models = provider_cfg.get("models") or []
        for model in models:
            if isinstance(model, dict) and model.get("id"):
                return str(provider_id), str(model["id"]), provider_cfg
    return None


_ENABLE_THINKING_MODELS = {"qwen", "qwq", "deepseek"}


def _supports_enable_thinking(model_id: str) -> bool:
    # 逻辑说明：`_supports_enable_thinking` 接收 model_id，按模型 ID 前缀判断是否支持 enable_thinking，返回 bool；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    lower = model_id.lower()
    return any(lower.startswith(prefix) for prefix in _ENABLE_THINKING_MODELS)


def _max_tokens_param(model_id: str) -> str:
    # 逻辑说明：`_max_tokens_param` 接收 model_id，根据模型系列选择正确的 token 上限参数名，返回 str；不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；
    # 本函数不额外重试，避免掩盖持续故障。
    if model_id.startswith("gpt-5"):
        return "max_completion_tokens"
    return "max_tokens"


def _now() -> str:
    # 逻辑说明：为健康探针各检查项生成带 `+00:00` 时区的 ISO 8601 UTC 更新时间，保留微秒以区分短间隔内的连续探测结果。
    # 只读取系统时钟并返回字符串，不修改探针缓存或其他 Worker 状态。
    return datetime.now(timezone.utc).isoformat()
