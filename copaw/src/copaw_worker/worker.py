"""
Worker main entry point.

Bootstrap flow:
1. Pull openclaw.json + SOUL.md + AGENTS.md from MinIO
2. Bridge openclaw.json -> CoPaw config.json + providers.json
3. Install MatrixChannel into CoPaw's custom_channels dir
4. Start CoPaw AgentRunner + ChannelManager (Matrix channel)
"""

# 初学者导读：这个类负责把一名 CoPaw Worker 从“只有容器”带到“可以在 Matrix
# 房间接任务”。启动顺序保证先恢复持久文件、再翻译 Controller 配置、再注册房间
# channel，最后才运行 Agent；否则 Worker 可能带着默认身份在错误房间回复。运行后
# 文件同步、健康报告与 HTTP 探针作为后台协程并行工作，Pod 重启可重新走同一路径。
from __future__ import annotations

import asyncio
import logging
import os
import platform
import shutil
import stat
import time
import uuid
from pathlib import Path
from typing import Any, Optional

from rich.console import Console
from rich.panel import Panel

from copaw_worker.bridge import (
    bridge_standard_to_runtime,
    refresh_standard_to_runtime,
    sync_mcporter_config_to_runtime,
    sync_skills_to_runtime,
)
from copaw_worker.config import WorkerConfig
from copaw_worker.health import (
    ComponentHealth,
    HealthState,
    check_copaw_service,
    check_matrix_service,
    check_model_service,
)
from copaw_worker.sync import FileSync, push_loop, sync_loop
from copaw_worker.worker_api import WorkerAPIServer

console = Console()
logger = logging.getLogger(__name__)


class Worker:
    """管理 CoPaw Worker 的本地生命周期，不拥有 Controller 中的期望状态。

    ``start`` 完成恢复、翻译、channel 与健康服务准备；``run`` 托管上游 Agent；
    ``stop`` 统一取消后台任务。集中持有这些资源可以保证关闭时不会遗留仍在上传
    文件或使用旧 Matrix 身份的协程。
    """
    def __init__(self, config: WorkerConfig) -> None:
        # 逻辑说明：`__init__` 接收 config，执行 CoPaw Worker 生命周期 中的“init”步骤，返回 None；
        # 会访问网络服务、会更新对象内存状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
        self.config = config
        self.worker_name = config.worker_name
        self.sync: Optional[FileSync] = None
        self._copaw_working_dir: Optional[Path] = None
        self._runner = None
        self._channel_manager = None
        self._worker_api: WorkerAPIServer | None = None
        self._health: HealthState | None = None
        self._openclaw_cfg: dict[str, Any] = {}
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._copaw_server = None
        self._stopping = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run(self) -> bool:
        # 逻辑说明：`run` 接收 当前对象/进程状态，完成启动后托管 CoPaw，退出或取消时统一停止所有资源，返回 bool；
        # 不修改外部状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
        if not await self.start():
            return False
        try:
            await self._run_copaw()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()
        return True

    async def stop(self) -> None:
        # 逻辑说明：把 Worker 标记为停机并通知 CoPaw server 退出，随后尽力停止所有 channel 与 runner、取消并汇总后台任务，最后关闭健康 API。
        # channel/runner 的局部异常被隔离以继续释放其余资源；后台任务异常通过 return_exceptions 收集，健康 API 无论成功与否都会在 finally 中清空引用。
        self._stopping = True
        console.print("[yellow]Stopping worker...[/yellow]")
        if self._copaw_server is not None:
            self._copaw_server.should_exit = True
        if self._channel_manager is not None:
            try:
                await self._channel_manager.stop_all()
            except Exception:
                pass
        if self._runner is not None:
            try:
                await self._runner.stop()
            except Exception:
                pass
        tasks = list(self._background_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._background_tasks.clear()
        if self._worker_api is not None:
            try:
                await self._worker_api.stop()
            finally:
                self._worker_api = None
        console.print("[green]Worker stopped.[/green]")

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    async def start(self) -> bool:
        """依次恢复远端状态并启动依赖，全部就绪后才允许 Worker 接收任务。"""
        # 逻辑说明：`start` 接收 当前对象/进程状态，按存储恢复、配置转换、健康检查、channel 与后台任务顺序启动 Worker，返回 bool；
        # 会读写本地文件、会访问网络服务、会更新对象内存状态、会修改进程环境。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；循环/重试受现有次数、超时或间隔限制。
        self._stopping = False
        console.print(
            Panel.fit(
                f"[bold green]CoPaw Worker[/bold green]\n"
                f"Worker: [cyan]{self.worker_name}[/cyan]",
                title="Starting",
            )
        )

        # 1. Ensure mc (MinIO Client) is available
        self._ensure_mc()

        # 2. Init file sync
        self.sync = FileSync(
            endpoint=self.config.minio_endpoint,
            access_key=self.config.minio_access_key,
            secret_key=self.config.minio_secret_key,
            bucket=self.config.minio_bucket,
            worker_name=self.worker_name,
            secure=self.config.minio_secure,
            local_dir=self.config.install_dir / self.worker_name,
        )
        self._copaw_working_dir = self.sync.local_dir / ".copaw"
        self._copaw_working_dir.mkdir(parents=True, exist_ok=True)
        self._health = HealthState(self._copaw_working_dir / "health.json")
        self._health.persist()

        # 2. Full mirror from MinIO (restore all state: config, sessions, sync token, etc.)
        #    Mirrors the OpenClaw worker's startup approach: pull everything first,
        #    then use selective sync during runtime. Controller writes and worker
        #    container start can be close together, so tolerate a short initial
        #    storage visibility race before giving up.
        openclaw_cfg = None
        max_attempts = max(
            1,
            int(os.environ.get("COPAW_STARTUP_MIRROR_ATTEMPTS", "1")),
        )
        retry_delay = max(
            0.0,
            float(os.environ.get("COPAW_STARTUP_MIRROR_RETRY_SECONDS", "5")),
        )
        for attempt in range(1, max_attempts + 1):
            console.print("[yellow]Pulling all files from MinIO...[/yellow]")
            try:
                self.sync.mirror_all()
                openclaw_cfg = self.sync.get_config()
                self._health.update(
                    "sync",
                    "healthy",
                    "startup mirror restored",
                    {"operation": "mirror_all"},
                )
                break
            except Exception as exc:
                if attempt >= max_attempts:
                    self._health.update(
                        "sync",
                        "unhealthy",
                        f"startup mirror failed: {exc}",
                        {
                            "operation": "mirror_all",
                            "error_type": type(exc).__name__,
                        },
                    )
                    console.print(f"[red]Failed to read worker config from MinIO: {exc}[/red]")
                    return False
                logger.warning(
                    "Worker config not ready yet (attempt %s/%s): %s",
                    attempt,
                    max_attempts,
                    exc,
                )
                await asyncio.sleep(retry_delay)

        self._openclaw_cfg = openclaw_cfg or {}

        # 3b. Re-login to Matrix to get fresh access token + device ID
        #     Under E2EE, reusing the old access token (same device_id) with a
        #     regenerated identity key causes other clients to reject key
        #     distribution. Re-login creates a new device_id, matching the
        #     Manager's behavior.
        openclaw_cfg = self._matrix_relogin(self._openclaw_cfg)
        self._openclaw_cfg = openclaw_cfg
        self._join_pending_matrix_invites(openclaw_cfg)

        # 4. Set up CoPaw working directory
        # 5. Bridge openclaw.json -> CoPaw config.json + providers.json
        #    Infer gateway port from FS endpoint so bridge's _port_remap uses
        #    the correct host port instead of the hardcoded default.
        if not os.environ.get("AGENTTEAMS_PORT_GATEWAY"):
            from urllib.parse import urlparse
            _parsed = urlparse(self.config.minio_endpoint)
            if _parsed.port:
                os.environ["AGENTTEAMS_PORT_GATEWAY"] = str(_parsed.port)

        console.print("[yellow]Bridging configuration to CoPaw...[/yellow]")
        try:
            bridge_standard_to_runtime(
                self.sync.local_dir,
                self._copaw_working_dir,
                openclaw_cfg,
                skill_names=self.sync.list_skills(),
                profile="worker",
            )
            self._health.update(
                "bridge",
                "healthy",
                "standard-to-copaw bridge completed",
                {"operation": "bridge_standard_to_runtime"},
            )
        except Exception as exc:
            self._health.update(
                "bridge",
                "unhealthy",
                f"standard-to-copaw bridge failed: {exc}",
                {
                    "operation": "bridge_standard_to_runtime",
                    "error_type": type(exc).__name__,
                },
            )
            console.print(f"[red]Config bridge failed: {exc}[/red]")
            return False

        # 6. Install MatrixChannel into CoPaw's custom_channels dir
        self._install_matrix_channel()

        # 7. Verify the configured model without blocking startup. A failed
        #    preflight remains visible through readiness and is reported to
        #    Matrix so the operator sees it before the first task times out.
        model_health = check_model_service(openclaw_cfg)
        self._health.update(
            "model",
            model_health.healthiness,
            model_health.message,
            model_health.details,
        )
        if model_health.healthiness == "unhealthy":
            details = model_health.details or {}
            provider = details.get("provider", "unknown")
            model = details.get("model", "unknown")
            self._notify_matrix(
                "Model service check failed "
                f"(provider={provider}, model={model}): "
                f"{model_health.message}",
                openclaw_cfg,
            )

        # 8. Start the adapter API for Kubernetes liveness/readiness probes.
        self._worker_api = WorkerAPIServer(
            host="0.0.0.0",
            port=self.config.worker_port,
            liveness_handler=self.build_worker_liveness,
            readiness_handler=self.build_worker_readiness,
        )
        try:
            await self._worker_api.start()
        except Exception as exc:
            logger.exception("Worker API failed to start: %s", exc)
            self._worker_api = None
            return False

        # 9. Start background MinIO sync.
        self._track_background_task(
            sync_loop(
                self.sync,
                interval=self.config.sync_interval,
                on_pull=self._on_files_pulled,
                health=self._health,
            )
        )
        self._track_background_task(
            push_loop(
                self.sync,
                check_interval=5,
                health=self._health,
            )
        )

        console.print("[bold green]Worker initialized.[/bold green]")
        if self.config.console_port:
            console.print(
                f"[dim]Note: web console enabled on port {self.config.console_port} "
                f"(~500MB extra RAM). Remove --console-port to save memory.[/dim]"
            )
        else:
            console.print(
                "[dim]Tip: add --console-port 8088 to enable the web console "
                "(costs ~500MB extra RAM).[/dim]"
            )
        return True

    def _track_background_task(self, awaitable: Any) -> asyncio.Task[Any]:
        # 逻辑说明：`_track_background_task` 接收 awaitable，创建后台 Task，登记到集合并在完成回调中自动移除，返回 asyncio.Task[Any]；
        # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
        task = asyncio.create_task(awaitable)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    def _ensure_health(self) -> HealthState:
        # 逻辑说明：`_ensure_health` 接收 当前对象/进程状态，返回现有 HealthState，缺失时按 runtime 路径创建并持久化初始状态，返回 HealthState；
        # 会更新对象内存状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
        if self._health is None:
            runtime_dir = (
                self._copaw_working_dir
                or self.config.install_dir / self.worker_name / ".copaw"
            )
            self._health = HealthState(runtime_dir / "health.json")
            self._health.persist()
        return self._health

    @staticmethod
    def _health_details(result: ComponentHealth) -> dict[str, Any] | None:
        return result.details

    async def build_worker_liveness(self) -> dict[str, Any]:
        """Return a process-local probe without touching dependencies."""
        return {
            "liveness": "alive",
            "message": "worker api alive",
            "details": {"worker_port": self.config.worker_port},
        }

    async def build_worker_readiness(self) -> dict[str, Any]:
        """Probe the live dependencies and return the complete health snapshot."""
        # 逻辑说明：`build_worker_readiness` 接收 当前对象/进程状态，实时探测 Matrix、模型和 CoPaw，并返回完整 readiness 快照，返回 dict[str, Any]；
        # 会更新对象内存状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
        health = self._ensure_health()

        matrix_cfg = (
            self._openclaw_cfg.get("channels", {}).get("matrix", {})
            if isinstance(self._openclaw_cfg, dict)
            else {}
        )
        model_probe = asyncio.to_thread(
            check_model_service,
            self._openclaw_cfg,
        )
        matrix_probe = asyncio.to_thread(
            check_matrix_service,
            matrix_cfg.get("homeserver", ""),
        )
        if self.config.console_port is not None:
            # The Worker API and the CoPaw console share one asyncio event
            # loop. A synchronous self-probe would block that loop while
            # waiting for /health, making the endpoint time out on itself.
            copaw_health, model_health, matrix_health = await asyncio.gather(
                asyncio.to_thread(
                    check_copaw_service,
                    self.config.console_port,
                ),
                model_probe,
                matrix_probe,
            )
            health.update(
                "copaw",
                copaw_health.healthiness,
                copaw_health.message,
                self._health_details(copaw_health),
            )
        else:
            model_health, matrix_health = await asyncio.gather(
                model_probe,
                matrix_probe,
            )
        health.update(
            "model",
            model_health.healthiness,
            model_health.message,
            self._health_details(model_health),
        )

        health.update(
            "matrix",
            matrix_health.healthiness,
            matrix_health.message,
            self._health_details(matrix_health),
        )

        snapshot = health.to_dict()
        return {
            "readiness": (
                "ready"
                if snapshot["healthiness"] == "healthy"
                else "not_ready"
            ),
            **snapshot,
        }

    async def _mark_copaw_startup_health(
        self,
        *,
        timeout: float = 30,
        interval: float = 1,
    ) -> None:
        """Wait boundedly for CoPaw's native health endpoint."""
        # 逻辑说明：`_mark_copaw_startup_health` 接收 timeout、interval，在限定时间内轮询 CoPaw 健康端点并更新 startup health，返回 None；
        # 会更新对象内存状态。失败策略：底层异常继续上抛，由调用链统一处理；循环/重试受现有次数、超时或间隔限制。
        if self.config.console_port is None:
            return
        health = self._ensure_health()
        deadline = asyncio.get_running_loop().time() + max(timeout, 0)
        result: ComponentHealth | None = None
        while True:
            result = await asyncio.to_thread(
                check_copaw_service,
                self.config.console_port,
            )
            if result.healthiness == "healthy":
                health.update(
                    "copaw",
                    result.healthiness,
                    result.message,
                    self._health_details(result),
                )
                return
            if asyncio.get_running_loop().time() >= deadline:
                health.update(
                    "copaw",
                    result.healthiness,
                    result.message,
                    self._health_details(result),
                )
                return
            await asyncio.sleep(max(interval, 0.05))

    # ------------------------------------------------------------------
    # CoPaw runner
    # ------------------------------------------------------------------

    async def _run_copaw(self) -> None:
        """Start CoPaw. If console_port is set, run the full FastAPI app via
        uvicorn (gives access to the web console). Otherwise start the runner
        and channel manager directly (lightweight, no HTTP server)."""
        # 逻辑说明：`_run_copaw` 接收 当前对象/进程状态，安装工具 hook，并按 console 开关选择完整或 headless runtime，返回 None；
        # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
        from copaw_worker.hooks import install_tool_hooks

        install_tool_hooks()
        if self.config.console_port:
            await self._run_copaw_with_console(self.config.console_port)
        else:
            await self._run_copaw_headless()

    async def _run_copaw_with_console(self, port: int) -> None:
        """Run CoPaw's full FastAPI app (runner + channels + web console)."""
        # 逻辑说明：`_run_copaw_with_console` 接收 port，启动带 FastAPI/uvicorn Console 的 CoPaw runtime，返回 None；
        # 会更新对象内存状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
        import uvicorn
        from copaw.app.channels.registry import clear_builtin_channel_cache

        clear_builtin_channel_cache()

        uv_config = uvicorn.Config(
            "copaw.app._app:app",
            host="0.0.0.0",
            port=port,
            log_level="info",
        )
        self._copaw_server = uvicorn.Server(uv_config)
        console.print(
            f"[bold green]CoPaw console available at "
            f"http://127.0.0.1:{port}/[/bold green]"
        )
        startup_probe = self._track_background_task(
            self._mark_copaw_startup_health(),
        )
        try:
            await self._copaw_server.serve()
            if not self._stopping and self._health is not None:
                self._health.update(
                    "copaw",
                    "unhealthy",
                    "CoPaw app exited unexpectedly",
                    {"operation": "run_copaw"},
                )
        except asyncio.CancelledError:
            self._copaw_server.should_exit = True
            raise
        except Exception as exc:
            if self._health is not None:
                self._health.update(
                    "copaw",
                    "unhealthy",
                    f"CoPaw app failed: {exc}",
                    {
                        "operation": "run_copaw",
                        "error_type": type(exc).__name__,
                    },
                )
            raise
        finally:
            startup_probe.cancel()
            await asyncio.gather(startup_probe, return_exceptions=True)
            self._copaw_server = None

    async def _run_copaw_headless(self) -> None:
        """Start CoPaw's AgentRunner + ChannelManager (no HTTP server)."""
        # 逻辑说明：`_run_copaw_headless` 接收 当前对象/进程状态，直接启动 AgentRunner 与 ChannelManager，不暴露 Web Console，返回 None；
        # 会更新对象内存状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；循环/重试受现有次数、超时或间隔限制。
        from copaw.app.channels.manager import ChannelManager
        from copaw.app.channels.registry import clear_builtin_channel_cache
        from copaw.app.channels.utils import make_process_from_runner
        from copaw.app.runner.runner import AgentRunner
        from copaw.config.utils import load_config

        # Force registry reload so newly installed matrix_channel.py is picked up
        clear_builtin_channel_cache()

        self._runner = AgentRunner()
        await self._runner.start()

        # load_config reads COPAW_WORKING_DIR/config.json (set by bridge.py)
        config = load_config()
        self._channel_manager = ChannelManager.from_config(
            process=make_process_from_runner(self._runner),
            config=config,
            on_last_dispatch=None,
        )
        await self._channel_manager.start_all()
        if self._health is not None:
            self._health.update(
                "copaw",
                "healthy",
                "CoPaw headless runner started",
                {"operation": "run_copaw_headless"},
            )

        console.print("[bold green]CoPaw channels started. Worker is running.[/bold green]")

        try:
            while True:
                await asyncio.sleep(60)
        except asyncio.CancelledError:
            pass
        finally:
            await self._channel_manager.stop_all()
            await self._runner.stop()
            # Clear refs so stop() doesn't double-call
            self._channel_manager = None
            self._runner = None

    # ------------------------------------------------------------------
    # Matrix re-login (E2EE device_id refresh)
    # ------------------------------------------------------------------

    def _matrix_relogin(self, openclaw_cfg: dict) -> dict:
        """Re-login to Matrix to get a fresh access token and device ID.

        Under E2EE, crypto state is not persisted across restarts. Reusing
        the old access token keeps the same device_id but with a new identity
        key, which causes other Matrix clients to reject key distribution.
        A fresh login creates a new device_id, matching the
        Manager's restart behavior.

        The password is read directly from MinIO (never written to disk).
        """
        # 逻辑说明：`_matrix_relogin` 接收 openclaw_cfg，用 Worker 密码重新登录 Matrix，并把新 token 合并回内存配置，返回 dict；
        # 会访问网络服务、会更新对象内存状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
        import json
        import urllib.request

        matrix_cfg = openclaw_cfg.get("channels", {}).get("matrix", {})
        from .bridge import _is_in_container, _port_remap
        homeserver = _port_remap(
            matrix_cfg.get("homeserver", ""), _is_in_container()
        )
        password_key = f"{self.sync._prefix}/credentials/matrix/password"
        matrix_password = (self.sync._cat(password_key) or "").strip()

        if not homeserver or not matrix_password:
            self._ensure_health().update(
                "matrix",
                "unhealthy",
                "matrix re-login skipped: missing homeserver or password",
                {
                    "operation": "matrix_relogin",
                    "has_homeserver": bool(homeserver),
                    "has_password": bool(matrix_password),
                },
            )
            console.print(
                "[dim]Matrix re-login skipped because the homeserver or "
                "password is missing.[/dim]"
            )
            return openclaw_cfg

        login_url = f"{homeserver}/_matrix/client/v3/login"
        login_body = json.dumps({
            "type": "m.login.password",
            "identifier": {"type": "m.id.user", "user": self.worker_name},
            "password": matrix_password,
        }).encode()

        try:
            req = urllib.request.Request(
                login_url,
                data=login_body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                login_resp = json.loads(resp.read())

            new_token = login_resp.get("access_token", "")
            new_device = login_resp.get("device_id", "")

            if new_token:
                openclaw_cfg["channels"]["matrix"]["accessToken"] = new_token
                # Write updated config back to disk so bridge reads the new token
                config_path = self.sync.local_dir / "openclaw.json"
                with open(config_path, "w", encoding="utf-8") as f:
                    json.dump(openclaw_cfg, f, indent=2, ensure_ascii=False)
                self._ensure_health().update(
                    "matrix",
                    "healthy",
                    "matrix re-login succeeded",
                    {
                        "operation": "matrix_relogin",
                        "device_id": new_device,
                    },
                )
                console.print(
                    f"[green]Matrix re-login OK[/green] "
                    f"(device: {new_device})"
                )
            else:
                self._ensure_health().update(
                    "matrix",
                    "unhealthy",
                    "matrix re-login failed: response contained no access token",
                    {"operation": "matrix_relogin"},
                )
                console.print(
                    "[yellow]Matrix re-login returned no token, "
                    "using existing access token[/yellow]"
                )
        except Exception as exc:
            self._ensure_health().update(
                "matrix",
                "unhealthy",
                f"matrix re-login failed: {exc}",
                {
                    "operation": "matrix_relogin",
                    "error_type": type(exc).__name__,
                },
            )
            console.print(
                f"[yellow]Matrix re-login failed: {exc} — "
                f"using existing access token (E2EE may not work)[/yellow]"
            )

        return openclaw_cfg

    def _wait_for_matrix_rooms(
        self,
        homeserver: str,
        headers: dict[str, str],
        *,
        timeout: float = 15,
        poll_interval: float = 1,
    ) -> list[str]:
        """Accept pending invites and wait boundedly for joined rooms."""
        # 逻辑说明：`_wait_for_matrix_rooms` 接收 homeserver、headers、timeout、poll_interval，接受待处理邀请并在截止时间前等待房间加入完成，
        # 返回 list[str]；
        #
        # 会访问网络服务。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；循环/重试受现有次数、超时或间隔限制。
        import json
        import urllib.parse
        import urllib.request

        deadline = time.monotonic() + max(timeout, 0)
        while True:
            try:
                sync_url = (
                    f"{homeserver}/_matrix/client/v3/sync?"
                    "timeout=0&full_state=true"
                )
                request = urllib.request.Request(
                    sync_url,
                    headers=headers,
                    method="GET",
                )
                with urllib.request.urlopen(request, timeout=30) as response:
                    sync_data = json.loads(response.read())

                invites = (
                    sync_data.get("rooms", {}).get("invite") or {}
                ).keys()
                for room_id in invites:
                    encoded = urllib.parse.quote(room_id, safe="")
                    join_url = (
                        f"{homeserver}/_matrix/client/v3/join/{encoded}"
                    )
                    request = urllib.request.Request(
                        join_url,
                        data=b"{}",
                        headers={
                            **headers,
                            "Content-Type": "application/json",
                        },
                        method="POST",
                    )
                    with urllib.request.urlopen(request, timeout=30):
                        pass

                joined_url = (
                    f"{homeserver}/_matrix/client/v3/joined_rooms"
                )
                request = urllib.request.Request(
                    joined_url,
                    headers=headers,
                    method="GET",
                )
                with urllib.request.urlopen(request, timeout=30) as response:
                    joined_data = json.loads(response.read())
                rooms = list(joined_data.get("joined_rooms") or [])
                if rooms:
                    return rooms
            except Exception as exc:
                logger.warning("Matrix room discovery failed: %s", exc)

            if time.monotonic() >= deadline:
                return []
            time.sleep(max(poll_interval, 0))

    def _notify_matrix(
        self,
        message: str,
        openclaw_cfg: dict[str, Any],
    ) -> None:
        """Send an operational warning to every joined Worker room."""
        # 逻辑说明：`_notify_matrix` 接收 message、openclaw_cfg，向已加入的 Worker 房间发送运维告警，返回 None；
        # 会访问网络服务、会更新对象内存状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
        import json
        import urllib.parse
        import urllib.request

        matrix_cfg = openclaw_cfg.get("channels", {}).get("matrix", {})
        access_token = matrix_cfg.get("accessToken", "")
        from .bridge import _is_in_container, _port_remap

        homeserver = _port_remap(
            matrix_cfg.get("homeserver", ""),
            _is_in_container(),
        )
        if not homeserver or not access_token:
            return

        headers = {"Authorization": f"Bearer {access_token}"}
        rooms = self._wait_for_matrix_rooms(homeserver, headers)
        for room_id in rooms:
            encoded = urllib.parse.quote(room_id, safe="")
            transaction_id = uuid.uuid4().hex
            url = (
                f"{homeserver}/_matrix/client/v3/rooms/{encoded}/send/"
                f"m.room.message/{transaction_id}"
            )
            request = urllib.request.Request(
                url,
                data=json.dumps(
                    {"msgtype": "m.text", "body": message},
                    ensure_ascii=False,
                ).encode("utf-8"),
                headers={
                    **headers,
                    "Content-Type": "application/json",
                },
                method="PUT",
            )
            try:
                with urllib.request.urlopen(request, timeout=30):
                    pass
            except Exception as exc:
                logger.warning(
                    "Matrix operational notification failed for %s: %s",
                    room_id,
                    exc,
                )

    def _join_pending_matrix_invites(self, openclaw_cfg: dict) -> None:
        """Accept pending Matrix invites before CoPaw's channel loop starts."""
        # 逻辑说明：`_join_pending_matrix_invites` 接收 openclaw_cfg，启动 channel 前登录 Matrix 并接受所有待处理邀请，返回 None；
        # 会访问网络服务。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
        import json
        import urllib.parse
        import urllib.request

        matrix_cfg = openclaw_cfg.get("channels", {}).get("matrix", {})
        access_token = matrix_cfg.get("accessToken", "")
        from .bridge import _is_in_container, _port_remap
        homeserver = _port_remap(
            matrix_cfg.get("homeserver", ""), _is_in_container()
        )
        if not homeserver or not access_token:
            return

        headers = {"Authorization": f"Bearer {access_token}"}
        sync_url = (
            f"{homeserver}/_matrix/client/v3/sync?"
            "timeout=0&full_state=true"
        )
        try:
            req = urllib.request.Request(sync_url, headers=headers, method="GET")
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
        except Exception as exc:
            logger.warning("Matrix pending invite sync failed: %s", exc)
            return

        invites = (data.get("rooms", {}).get("invite") or {}).keys()
        for room_id in invites:
            encoded = urllib.parse.quote(room_id, safe="")
            join_url = f"{homeserver}/_matrix/client/v3/join/{encoded}"
            try:
                req = urllib.request.Request(
                    join_url,
                    data=b"{}",
                    headers={**headers, "Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=30):
                    pass
                logger.info("Joined pending Matrix invite: %s", room_id)
            except Exception as exc:
                logger.warning("Matrix invite join failed for %s: %s", room_id, exc)

    # ------------------------------------------------------------------
    # mc (MinIO Client) auto-install
    # ------------------------------------------------------------------

    def _ensure_mc(self) -> None:
        """Ensure mc (MinIO Client) binary is available on PATH.

        If not found, downloads the latest release from dl.min.io and installs
        it to ~/.local/bin/mc (created if needed, added to PATH for this process).
        """
        # 逻辑说明：`_ensure_mc` 接收 当前对象/进程状态，确认 mc 可执行文件存在；缺失时按平台下载到用户目录，返回 None；
        # 会读写本地文件、会修改进程环境。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
        if shutil.which("mc"):
            logger.debug("mc already available")
            return

        system = platform.system().lower()   # linux / darwin
        machine = platform.machine().lower() # x86_64 / aarch64 / arm64

        arch_map = {"x86_64": "amd64", "aarch64": "arm64", "arm64": "arm64"}
        arch = arch_map.get(machine, machine)

        if system == "windows":
            url = "https://dl.min.io/client/mc/release/windows-amd64/mc.exe"
            install_dir = Path.home() / ".local" / "bin"
            install_dir.mkdir(parents=True, exist_ok=True)
            dest = install_dir / "mc.exe"
        elif system in ("linux", "darwin"):
            url = f"https://dl.min.io/client/mc/release/{system}-{arch}/mc"
            install_dir = Path.home() / ".local" / "bin"
            install_dir.mkdir(parents=True, exist_ok=True)
            dest = install_dir / "mc"
        else:
            console.print(f"[yellow]mc auto-install not supported on {system}, please install mc manually[/yellow]")
            return

        console.print(f"[yellow]mc not found, downloading from {url}...[/yellow]")
        try:
            import httpx
            with httpx.stream("GET", url, follow_redirects=True, timeout=60) as resp:
                resp.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in resp.iter_bytes(chunk_size=65536):
                        f.write(chunk)
            if system != "windows":
                dest.chmod(dest.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
            os.environ["PATH"] = str(install_dir) + os.pathsep + os.environ.get("PATH", "")
            console.print(f"[green]mc installed to {dest}[/green]")
        except Exception as exc:
            console.print(f"[yellow]mc auto-install failed: {exc}. Please install mc manually.[/yellow]")

    # ------------------------------------------------------------------
    # Skills sync
    # ------------------------------------------------------------------

    def _sync_skills(self) -> None:
        """Project the exact Controller-owned skill set into CoPaw."""
        # 逻辑说明：`_sync_skills` 接收 当前对象/进程状态，读取 Controller 指定 Skill 列表并刷新 CoPaw runtime，返回 None；
        # 会访问网络服务、会更新对象内存状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
        skill_names = self.sync.list_skills()
        sync_skills_to_runtime(
            self.sync.local_dir,
            self._copaw_working_dir,
            skill_names,
        )

    # ------------------------------------------------------------------
    # MatrixChannel installation
    # ------------------------------------------------------------------

    def _install_matrix_channel(self) -> None:
        """Copy matrix_channel.py into COPAW_WORKING_DIR/custom_channels/.

        CoPaw's CUSTOM_CHANNELS_DIR = WORKING_DIR / "custom_channels", and
        WORKING_DIR is read from COPAW_WORKING_DIR env var at import time.
        We set COPAW_WORKING_DIR in bridge.py before this runs, so the
        directory is already correct.
        """
        # 逻辑说明：`_install_matrix_channel` 接收 当前对象/进程状态，把 AgentTeams Matrix channel 实现复制到 CoPaw custom_channels，返回 None；
        # 会读写本地文件、会更新对象内存状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
        custom_channels_dir = self._copaw_working_dir / "custom_channels"
        custom_channels_dir.mkdir(parents=True, exist_ok=True)
        src = Path(__file__).parent / "matrix_channel.py"
        dst = custom_channels_dir / "matrix_channel.py"
        shutil.copy2(src, dst)
        logger.debug("MatrixChannel installed to %s", dst)

    # ------------------------------------------------------------------
    # mcporter config
    # ------------------------------------------------------------------

    def _copy_mcporter_config(self) -> None:
        """Project mcporter configuration into CoPaw's default workspace."""
        sync_mcporter_config_to_runtime(
            self.sync.local_dir,
            self._copaw_working_dir,
        )

    # ------------------------------------------------------------------
    # File sync callback
    # ------------------------------------------------------------------

    async def _on_files_pulled(self, pulled_files: list[str]) -> None:
        """Refresh the CoPaw runtime after Controller-owned files change."""
        # 逻辑说明：`_on_files_pulled` 接收 pulled_files，识别本次拉取是否涉及 Controller 管理文件，并按需刷新 runtime，返回 None；
        # 会访问网络服务、会更新对象内存状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
        refresh_prefixes = (
            "openclaw.json",
            "SOUL.md",
            "AGENTS.md",
            "PROFILE.md",
            "TOOLS.md",
            "HEARTBEAT.md",
            "config/mcporter.json",
            "mcporter-servers.json",
            "skills/",
        )
        if not any(
            file_name == prefix or file_name.startswith(prefix)
            for file_name in pulled_files
            for prefix in refresh_prefixes
        ):
            return

        console.print("[yellow]Config changed, re-bridging...[/yellow]")
        try:
            openclaw_cfg = self.sync.get_config()
            self._openclaw_cfg = openclaw_cfg
            refresh_standard_to_runtime(
                self.sync.local_dir,
                self._copaw_working_dir,
                openclaw_cfg,
                skill_names=self.sync.list_skills(),
                get_soul=self.sync.get_soul,
                get_agents_md=self.sync.get_agents_md,
                profile="worker",
            )
            self._ensure_health().update(
                "bridge",
                "healthy",
                "standard-to-copaw bridge completed",
                {"operation": "refresh_standard_to_runtime"},
            )
            console.print("[green]Config re-bridged.[/green]")
        except Exception as exc:
            self._ensure_health().update(
                "bridge",
                "unhealthy",
                f"standard-to-copaw bridge failed: {exc}",
                {
                    "operation": "refresh_standard_to_runtime",
                    "error_type": type(exc).__name__,
                },
            )
            console.print(f"[red]Re-bridge failed: {exc}[/red]")
