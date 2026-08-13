"""TeamHarness integration implemented with QwenPaw 2 public plugin APIs."""

# 初学者导读：适配器只使用 QwenPaw 公共 plugin API 注册技能、MCP 与中间件，
# 避免修改上游内部模块。TeamHarness 让正式 Team 的 Leader/Worker 按 Matrix 房间
# 协作；它与 WorkerFlow 创建的临时本地 Agent 是两套边界，不能混用身份或状态。

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import re
from typing import Any, AsyncGenerator, Callable


PLUGIN_DIR = Path(__file__).resolve().parent
ASSET_DIR = PLUGIN_DIR / "teamharness"
if not (ASSET_DIR / "plugin.yaml").exists():
    ASSET_DIR = PLUGIN_DIR.parent.parent


def _team_prompt(_agent: Any) -> str:
    # 逻辑说明：`_team_prompt` 从受包管理的 TEAMS.md 读取团队上下文；缺失时返回空 Prompt。
    path = ASSET_DIR / "prompts" / "team" / "TEAMS.md"
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def _sanitizer_rules() -> list[str]:
    # 逻辑说明：`_sanitizer_rules` 从环境变量解析非空脱敏词，不持久化或输出原始配置。
    raw = os.getenv("AGENTTEAMS_OUTPUT_SANITIZE_KEYWORDS", "")
    return [value.strip() for value in raw.split(",") if value.strip()]


def _sanitize(value: Any) -> None:
    # 逻辑说明：`_sanitize` 递归遍历 dict/list/对象文本字段并原位替换敏感词；非文本保持原值。
    rules = _sanitizer_rules()
    if not rules:
        return
    if isinstance(value, dict):
        for key, item in list(value.items()):
            if isinstance(item, str):
                for rule in rules:
                    item = re.sub(
                        re.escape(rule),
                        "[REDACTED]",
                        item,
                        flags=re.IGNORECASE,
                    )
                value[key] = item
            else:
                _sanitize(item)
        return
    if isinstance(value, list):
        for item in value:
            _sanitize(item)
        return
    for attr in ("content", "output", "text"):
        if hasattr(value, attr):
            item = getattr(value, attr)
            if isinstance(item, str):
                for rule in rules:
                    item = re.sub(
                        re.escape(rule),
                        "[REDACTED]",
                        item,
                        flags=re.IGNORECASE,
                    )
                setattr(value, attr, item)
            else:
                _sanitize(item)


def _sanitizer_factory(_ctx: Any, _agent_config: Any):
    # 逻辑说明：`_sanitizer_factory` 在 AgentScope middleware 可用时构造脱敏器，否则安全跳过集成。
    try:
        from agentscope.middleware import MiddlewareBase
    except ImportError:
        return None

    class TeamHarnessSanitizer(MiddlewareBase):
        async def on_acting(
            self,
            agent: Any,
            input_kwargs: dict[str, Any],
            next_handler: Callable[..., AsyncGenerator[Any, None]],
        ) -> AsyncGenerator[Any, None]:
            # 逻辑说明：`on_acting` 转发 Agent 事件前原位脱敏可见内容；不改变事件顺序或流式语义。
            del agent
            async for item in next_handler(**input_kwargs):
                _sanitize(item)
                yield item

    return TeamHarnessSanitizer()


def _load_trace_module() -> Any | None:
    # 逻辑说明：`_load_trace_module` 从同目录显式加载 trace helper；缺文件或加载失败返回 None。
    path = PLUGIN_DIR / "task_trace.py"
    if not path.is_file():
        return None
    spec = importlib.util.spec_from_file_location(
        "agentteams_teamharness_task_trace",
        path,
    )
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _room_id(request: Any) -> str:
    # 逻辑说明：`_room_id` 从 request/session/channel metadata 的兼容字段提取当前 Matrix room ID。
    if hasattr(request, "model_dump"):
        data = request.model_dump()
    elif hasattr(request, "__dict__"):
        data = vars(request)
    else:
        data = {}
    meta = data.get("channel_meta") or data.get("meta") or {}
    room = str(meta.get("room_id") or meta.get("roomId") or "") if isinstance(meta, dict) else ""
    if room:
        return room
    session = str(data.get("session_id") or "")
    for prefix in ("agentteams_matrix:", "matrix:"):
        if session.startswith(prefix):
            return session[len(prefix):]
    return ""


def _register_trace_hooks(api: Any) -> None:
    # 逻辑说明：`_register_trace_hooks` 注册 turn 前后 hook，把 room ContextVar 与 trace processor 生命周期对齐。
    try:
        from qwenpaw.runtime.hooks import HookBase, HookResult
        from qwenpaw.runtime.phases import Phase
    except ImportError:
        return
    trace = _load_trace_module()
    if trace is None:
        return
    workspace = Path(os.getenv("QWENPAW_WORKING_DIR", ".")) / "workspaces" / "default"
    trace.register_task_trace_processor(workspace)

    class TraceEnter(HookBase):
        phase = Phase.PRE_DISPATCH
        name = "teamharness_trace_enter"
        priority = 20

        async def run(self, ctx: Any) -> Any:
            # 逻辑说明：`run` 在进入 Agent turn 前保存 ContextVar token，并尝试给待处理根 span 添加任务标签。
            room = _room_id(ctx.request)
            if room:
                ctx.extras["teamharness_trace_token"] = trace.set_current_room(room)
            return HookResult()

    class TraceExit(HookBase):
        phase = Phase.FINALLY
        name = "teamharness_trace_exit"
        priority = 200

        async def run(self, ctx: Any) -> Any:
            # 逻辑说明：`run` 在 turn 完成后恢复先前 ContextVar；finally 语义由宿主 middleware 链保障。
            token = ctx.extras.pop("teamharness_trace_token", None)
            if token is not None:
                trace.reset_current_room(token)
            return HookResult()

    api.register_runtime_hook(TraceEnter())
    api.register_runtime_hook(TraceExit())


class TeamHarnessPlugin:
    """把 TeamHarness 资源注册到一个正在启动的 QwenPaw Worker。"""
    def register(self, api: Any) -> None:
        # 逻辑说明：`register` 按顺序注册 Prompt、skill、MCP、sanitizer、trace hook 与 HTTP 路由。
        api.register_prompt_section(
            "teamharness_context",
            after="workspace",
            provider=_team_prompt,
            priority=40,
        )
        api.register_skill_provider(
            ASSET_DIR / "qwenpaw-skills",
            enabled_by_default=True,
            channels=["all"],
        )
        api.register_middleware(_sanitizer_factory, priority=30)
        _register_trace_hooks(api)
        self._register_http(api)

    def _register_http(self, api: Any) -> None:
        # 逻辑说明：`_register_http` 在 FastAPI 可用时挂载只读诊断端点；缺少可选依赖则跳过。
        try:
            from fastapi import APIRouter
        except ImportError:
            return
        router = APIRouter()

        @router.get("/health")
        def health() -> dict[str, Any]:
            # 逻辑说明：`health` 返回 TeamHarness adapter 的静态健康信息，不访问外部系统。
            return {"ok": True, "plugin": "teamharness", "adapter": "qwenpaw-2"}

        @router.post("/sync")
        def sync_endpoint() -> dict[str, Any]:
            # 逻辑说明：`sync_endpoint` 声明配置由 QwenPaw plugin API 管理，不自行修改 runtime 状态。
            return {
                "ok": True,
                "plugin": "teamharness",
                "managedBy": "qwenpaw-plugin-api",
            }

        api.register_http_router(
            router,
            prefix="/teamharness",
            tags=["teamharness"],
        )


plugin = TeamHarnessPlugin()
