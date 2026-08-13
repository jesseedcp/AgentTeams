"""WorkerFlow integration implemented with QwenPaw 2 public plugin APIs."""

# 初学者导读：这个薄适配器注册 WorkerFlow 技能和 reload/health 端点；实际临时
# Agent/DAG 状态机位于 MCP server。使用公共 API 可避免把 QwenPaw 上游内部实现
# 复制进项目，也明确 WorkerFlow 不会创建正式 TeamHarness Worker。

from __future__ import annotations

from pathlib import Path
from typing import Any


PLUGIN_DIR = Path(__file__).resolve().parent
ASSET_DIR = PLUGIN_DIR / "workerflow"
if not (ASSET_DIR / "plugin.yaml").exists():
    ASSET_DIR = PLUGIN_DIR.parent.parent


class WorkerFlowPlugin:
    """把 WorkerFlow 能力注册到当前 QwenPaw Worker，而非 AgentScope Manager。"""
    def register(self, api: Any) -> None:
        # 逻辑说明：`register` 注册 WorkerFlow skill provider 后再挂载 HTTP 端点；失败由宿主感知。
        api.register_skill_provider(
            ASSET_DIR / "skills" / "agent",
            enabled_by_default=True,
            channels=["all"],
        )
        self._register_http(api)

    def _register_http(self, api: Any) -> None:
        # 逻辑说明：`_register_http` 在 FastAPI 可用时注册 health/sync；缺少可选依赖时安全跳过。
        try:
            from fastapi import APIRouter
        except ImportError:
            return
        router = APIRouter()

        @router.get("/health")
        def health() -> dict[str, Any]:
            # 逻辑说明：`health` 返回 adapter 静态健康标识，不读取或修改 WorkerFlow 状态。
            return {"ok": True, "plugin": "workerflow", "adapter": "qwenpaw-2"}

        @router.post("/sync")
        def sync_endpoint() -> dict[str, Any]:
            # 逻辑说明：`sync_endpoint` 确认配置由 plugin API 管理；不自行同步外部资源。
            return {
                "ok": True,
                "plugin": "workerflow",
                "managedBy": "qwenpaw-plugin-api",
            }

        api.register_http_router(
            router,
            prefix="/workerflow",
            tags=["workerflow"],
        )


plugin = WorkerFlowPlugin()
