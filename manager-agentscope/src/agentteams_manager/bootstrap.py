"""Production dependency wiring for the single AgentScope Manager process."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from pydantic import SecretStr

from .admin.service import AdminSnapshotService
from .application import ManagerApplication
from .channels.http_providers import HttpChannelAdapter
from .channels.matrix import MatrixChannelEscalation
from .channels.service import ChannelService, ExternalContactRepository
from .clients.agt import AgtClient
from .clients.git import GitClient
from .clients.higress import HigressClient
from .clients.minio import (
    MinioClient,
    ObjectNotFound,
    ObjectVersionConflict,
)
from .clients.model_gateway import (
    ModelCapabilities,
    ModelGatewayClient,
)
from .clients.nacos import NacosClient
from .clients.process import ProcessRunner
from .config import (
    ManagerConfig,
    PromptSources,
    RuntimeDocument,
)
from .domain.ids import matrix_transaction_id
from .domain.models import (
    InboundEvent,
    OperationKind,
    RoomKind,
    RoomPolicy,
)
from .health import HealthServer, ReadinessState
from .matrix.client import MatrixClient, MatrixClientConfig
from .matrix.policy import (
    ALL_MANAGER_TOOLS,
    CONFIRM_TOOLS,
    RoomPolicyResolver,
)
from .matrix.router import EventRouter
from .matrix.session_runner import MatrixSessionRunner
from .observability.metrics import MetricsRegistry
from .runtime.agent_factory import AgentFactory
from .runtime.config_watcher import ConfigWatcher, RuntimeRegistry
from .runtime.mcp import MCPRegistry
from .runtime.prompts import PromptBuilder
from .runtime.session_manager import RoomSessionManager
from .runtime.skills import (
    EXPECTED_MANAGER_SKILLS,
    CompositeToolProvider,
    SkillRegistry,
    SkillToolkitFactory,
)
from .state.confirmations import (
    ConfirmationRepository,
    ConfirmationService,
)
from .state.database import Database
from .state.journal import S3Journal
from .state.leases import LeaseRepository
from .state.memory import MemoryRepository
from .state.notifications import NotificationRepository
from .state.operations import OperationRepository
from .state.projects import ProjectRepository
from .state.recovery import RecoveryCoordinator
from .state.sessions import SessionRepository
from .state.tasks import ProjectGraphRepository, TaskRepository
from .state.topology import TopologyRepository
from .tools.channels import ChannelToolkitFactory
from .tools.configuration import ConfigurationToolkitFactory
from .tools.host_files import HostFileAccess, HostFileToolkitFactory
from .tools.integrations import IntegrationToolkitFactory
from .tools.resources import ResourceToolkitFactory
from .tools.storage import FileSyncService
from .tools.tasks import TaskToolkitFactory
from .workflows.git_delegation import (
    GitDelegationService,
    ProcessingLeaseService,
)
from .workflows.heartbeat import (
    Heartbeat,
    IntegrationRecovery,
    NotificationRecovery,
    SemanticSupervisor,
    TaskCompletionRecovery,
    TaskHeartbeat,
    TaskRecovery,
)
from .workflows.integrations import IntegrationService
from .workflows.matrix_resources import (
    ChannelResolver,
    MatrixResourceService,
)
from .workflows.notifications import DailyMemory, NotificationService
from .workflows.projects import ProjectService
from .workflows.resources import (
    ResourceHeartbeat,
    ResourceService,
    TopologyResolver,
)
from .workflows.supervisor import OperationSupervisor
from .workflows.tasks import TaskService

logger = logging.getLogger(__name__)

_ASSET_ROOT = Path("/opt/agentteams/manager")
_KNOWN_MODELS = Path("/opt/agentteams/config/known-models.json")
_SECRET_REFERENCE = re.compile(r"^env:([A-Z][A-Z0-9_]*)$")


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


class MinioJournalStore:
    """Adapt bucket-relative MinIO operations to the journal protocol."""

    def __init__(self, minio: MinioClient | Any) -> None:
        self._minio = minio

    async def put(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str,
        if_none_match: bool,
    ) -> str:
        if if_none_match:
            try:
                receipt = await self._minio.put_bytes_if_version(
                    key,
                    data,
                    expected_etag=None,
                    content_type=content_type,
                )
            except ObjectVersionConflict:
                existing = await self._minio.get_bytes(key)
                if existing != data:
                    raise
                receipt = await self._minio.head(key)
                if receipt is None:
                    raise RuntimeError(
                        f"journal object disappeared: {key}",
                    )
        else:
            receipt = await self._minio.put_bytes(
                key,
                data,
                content_type=content_type,
            )
        return str(receipt.etag)

    async def get(self, key: str) -> bytes:
        try:
            return await self._minio.get_bytes(key)
        except ObjectNotFound as exc:
            raise KeyError(key) from exc

    async def list(self, prefix: str) -> tuple[str, ...]:
        return tuple(
            receipt.key
            for receipt in await self._minio.list_prefix(prefix)
        )


class MatrixRuntime:
    """Own routing and the single Matrix sync loop as one lifecycle unit."""

    def __init__(
        self,
        *,
        matrix: MatrixClient,
        router: EventRouter,
        metrics: MetricsRegistry,
        tracer: Any | None,
    ) -> None:
        self._matrix = matrix
        self._router = router
        self._metrics = metrics
        self._tracer = tracer

    @property
    def ready(self) -> bool:
        return self._matrix.ready.is_set()

    async def start(self) -> None:
        await self._router.start()
        try:
            await self._matrix.start(self._submit)
            await self._matrix.wait_until_ready()
        except Exception:
            await self._router.stop()
            raise

    async def stop(self) -> None:
        await self._matrix.stop()
        await self._router.stop()

    async def _submit(self, event: InboundEvent) -> None:
        self._metrics.increment(
            "agentteams_manager_matrix_events_total",
        )
        with _span(self._tracer, "manager.matrix.event"):
            await self._router.submit(event)


class MatrixMedia:
    def __init__(self, matrix: MatrixClient) -> None:
        self._matrix = matrix

    async def download(self, event: InboundEvent) -> tuple[Any, ...]:
        blocks: list[Any] = []
        for reference in event.media:
            blocks.extend(await self._matrix.download_media(reference))
        return tuple(blocks)


class WorkerNotifier:
    def __init__(self, *, agt: AgtClient, matrix: MatrixClient) -> None:
        self._agt = agt
        self._matrix = matrix

    async def notify_worker(
        self,
        worker: str,
        text: str,
        *,
        source_operation_id: str,
    ) -> None:
        resource = await self._agt.get_worker(worker)
        if resource is None or not resource.room_id:
            raise RuntimeError(
                f"worker/{worker} has no Matrix room",
            )
        await self._matrix.send_text(
            resource.room_id,
            text,
            txn_id=matrix_transaction_id(source_operation_id, 0),
        )


class HeartbeatRuntime:
    def __init__(
        self,
        *,
        heartbeat: Heartbeat,
        interval: Any,
        metrics: MetricsRegistry,
        tracer: Any | None,
    ) -> None:
        self._heartbeat = heartbeat
        self._interval = interval
        self._metrics = metrics
        self._tracer = tracer
        self._task: asyncio.Task[None] | None = None
        self.ready = False

    async def start(self) -> None:
        if self._task is not None:
            return
        await self._run_once()
        self.ready = True
        self._task = asyncio.create_task(
            self._run(),
            name="manager-heartbeat",
        )

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self.ready = False

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(max(1, float(self._interval())))
            try:
                await self._run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                self._metrics.increment(
                    "agentteams_manager_errors_total",
                )
                logger.exception("Manager heartbeat failed")

    async def _run_once(self) -> None:
        with _span(self._tracer, "manager.heartbeat"):
            report = await self._heartbeat.run_once()
        self._metrics.increment("agentteams_manager_heartbeats_total")
        self._metrics.increment(
            "agentteams_manager_recovery_reconciled_total",
            (
                report.reconciled
                + report.task_reconciled
                + report.integration_reconciled
                + report.completions_reconciled
                + report.notification_reconciled
            ),
        )
        self._metrics.increment(
            "agentteams_manager_recovery_errors_total",
            (
                report.failed
                + report.task_failed
                + report.integration_failed
                + report.completions_failed
                + report.notification_failed
            ),
        )


class SnapshotScheduler:
    def __init__(
        self,
        *,
        database: Database,
        operations: OperationRepository,
        journal: S3Journal,
        temporary_path: Path,
    ) -> None:
        self._database = database
        self._operations = operations
        self._journal = journal
        self._temporary_path = temporary_path
        self._last_sequence: int | None = None

    async def snapshot_if_due(self) -> bool:
        if self._last_sequence is None:
            latest = await self._journal.download_latest_snapshot()
            self._last_sequence = latest[0].sequence if latest else 0
        sequence = await self._operations.current_applied_sequence()
        if sequence <= self._last_sequence:
            return False
        self._temporary_path.parent.mkdir(parents=True, exist_ok=True)
        await self._database.backup_to(self._temporary_path)
        try:
            await self._journal.upload_snapshot(
                self._temporary_path,
                sequence=sequence,
            )
        finally:
            self._temporary_path.unlink(missing_ok=True)
        self._last_sequence = sequence
        return True


async def create_application(
    config: ManagerConfig,
    *,
    tracer: Any | None = None,
) -> ManagerApplication:
    """Connect external resources and assemble the complete Manager."""
    storage = await MinioClient.connect(
        endpoint_url=config.fs_endpoint,
        bucket=config.fs_bucket,
        access_key=config.fs_access_key,
        secret_key=config.fs_secret_key.get_secret_value(),
    )
    try:
        return build_application(
            config,
            storage=storage,
            tracer=tracer,
        )
    except Exception:
        await storage.close()
        raise


def build_application(
    config: ManagerConfig,
    *,
    storage: MinioClient | Any,
    tracer: Any | None = None,
) -> ManagerApplication:
    """Pure construction half of the production composition root."""
    if not config.admin_user_id:
        raise ValueError("Manager admin Matrix user ID is required")
    if not config.manager_admin_room_id:
        raise ValueError("Manager admin Matrix room ID is required")

    metrics = MetricsRegistry()
    _initialize_metrics(metrics)
    clock = SystemClock()
    database = Database(config.session_database)
    operations = OperationRepository(database)
    tasks = TaskRepository(database)
    project_graph = ProjectGraphRepository(database)
    projects = ProjectRepository(database)
    topology = TopologyRepository(
        database,
        admin_user_id=config.admin_user_id,
    )
    notifications = NotificationRepository(database)
    leases = LeaseRepository(database)
    session_repository = SessionRepository(database)
    memory_repository = MemoryRepository(database)
    confirmations = ConfirmationService(
        ConfirmationRepository(database),
        now=clock.now,
    )

    async def migrate_legacy_confirmations() -> None:
        await confirmations.migrate_legacy_sessions(
            admin_room_id=config.manager_admin_room_id,
            admin_user_id=config.admin_user_id,
            admin_policy=RoomPolicy(
                room_id=config.manager_admin_room_id,
                kind=RoomKind.ADMIN_DM,
                revision=await topology.revision(),
                allowed_tools=ALL_MANAGER_TOOLS,
                confirm_tools=CONFIRM_TOOLS,
                allowed_senders=frozenset({config.admin_user_id}),
            ),
            ttl=timedelta(minutes=15),
        )

    journal = S3Journal(MinioJournalStore(storage), prefix="")
    recovery = RecoveryCoordinator(
        database=database,
        journal=journal,
        replay_event=operations.replay_event,
        temp_directory=config.workspace / "tmp",
        prefer_local_database=True,
    )

    agt = AgtClient(ProcessRunner(allowed_executables=("agt",)))
    git = GitClient(ProcessRunner(allowed_executables=("git",)))
    known_models = _load_known_models()
    model_gateway = ModelGatewayClient(
        base_url=config.ai_gateway_url,
        api_key=config.gateway_key,
        known_models=known_models,
    )
    nacos = NacosClient.from_environment()
    higress = _higress_client(config)

    matrix = MatrixClient(
        MatrixClientConfig.from_manager_config(config),
        operations,
    )
    channel_service = ChannelService(
        contacts=ExternalContactRepository(database),
        adapters=tuple(
            HttpChannelAdapter(
                provider=document.provider,
                outbound_url=document.outbound_url,
                token=_environment_secret(
                    document.token_env,
                ).get_secret_value(),
                webhook_secret=_environment_secret(
                    document.webhook_secret_env,
                ).get_secret_value(),
            )
            for document in config.external_channels
        ),
        escalation=MatrixChannelEscalation(
            matrix=matrix,
            admin_room_id=config.manager_admin_room_id,
        ),
    )
    topology_resolver = TopologyResolver(
        controller=agt,
        matrix=matrix,
        topology=topology,
        manager_user_id=config.manager_user_id,
        admin_user_id=config.admin_user_id,
        admin_room_id=config.manager_admin_room_id,
    )

    resumers: dict[OperationKind, Any] = {}

    async def resume(operation: Any) -> None:
        handler = resumers.get(operation.kind)
        if handler is None:
            raise RuntimeError(
                f"no recovery handler for {operation.kind.value}",
            )
        await handler(operation)

    supervisor = OperationSupervisor(
        operations=operations,
        journal=journal,
        clock=clock,
        reconcilers={kind: resume for kind in OperationKind},
    )

    matrix_resources = MatrixResourceService(
        supervisor=supervisor,
        matrix=matrix,
        channels=topology,
        manager_admin_room=config.manager_admin_room_id,
    )
    channel_resolver = ChannelResolver(
        channels=topology,
        matrix=matrix,
        manager_admin_room=config.manager_admin_room_id,
    )
    notification_service = NotificationService(
        notifications=notifications,
        resolver=channel_resolver,
        matrix=matrix,
        supervisor=supervisor,
        memory=DailyMemory(storage=storage, clock=clock),
        clock=clock,
        admin_user_id=config.admin_user_id,
    )
    resource_service = ResourceService(
        controller=agt,
        supervisor=supervisor,
        topology=topology_resolver,
        matrix=matrix,
        nacos=nacos,
    )
    lease_service = ProcessingLeaseService(
        leases=leases,
        storage=storage,
        clock=clock,
    )
    file_sync = FileSyncService(
        storage=storage,
        leases=lease_service,
        tasks=tasks,
        cache_root=config.workspace,
        supervisor=supervisor,
        matrix=matrix,
    )
    task_service = TaskService(
        tasks=tasks,
        storage=storage,
        controller=agt,
        matrix=matrix,
        supervisor=supervisor,
        clock=clock,
        cache_root=config.workspace,
        matrix_domain=config.matrix_domain,
        notifications=notification_service,
        project_graph=project_graph,
    )
    project_service = ProjectService(
        projects=projects,
        tasks=tasks,
        task_service=task_service,
        storage=storage,
        controller=agt,
        matrix=matrix,
        topology=topology,
        graph=project_graph,
        supervisor=supervisor,
        clock=clock,
        admin_user_id=config.admin_user_id,
        manager_user_id=config.manager_user_id,
    )
    git_service = GitDelegationService(
        storage=storage,
        leases=lease_service,
        git=git,
        tasks=tasks,
        matrix=matrix,
        supervisor=supervisor,
        cache_root=config.workspace,
        events=operations,
    )

    initial_runtime = _initial_runtime(config)
    runtime_registry = RuntimeRegistry(initial_runtime)
    mcp_registry = MCPRegistry(
        gateway_key=config.gateway_key,
        reserved_tool_names=ALL_MANAGER_TOOLS,
    )

    async def prepare_runtime(document: RuntimeDocument) -> None:
        await mcp_registry.prepare(document)
        metrics.increment("agentteams_manager_runtime_reloads_total")
        metrics.set(
            "agentteams_manager_runtime_revision",
            document.revision,
        )

    config_watcher = ConfigWatcher(
        storage=storage,
        key=config.runtime_document_key,
        cache_path=config.runtime_document_path,
        registry=runtime_registry,
        prepare=prepare_runtime,
    )
    integration_service = IntegrationService(
        agt=agt,
        gateway=model_gateway,
        supervisor=supervisor,
        clock=clock,
        manager_name=config.manager_name,
        registry=runtime_registry,
        watcher=config_watcher,
        sleep=asyncio.sleep,
        higress=higress,
        mcp_verifier=mcp_registry,
        worker_notifications=WorkerNotifier(agt=agt, matrix=matrix),
        runtime_mode=config.runtime_mode,
    )

    resource_tools = ResourceToolkitFactory(
        resources=resource_service,
        matrix=matrix,
        matrix_workflows=matrix_resources,
        channels=topology,
        manager_admin_room=config.manager_admin_room_id,
        yolo=config.yolo,
    )
    task_tools = TaskToolkitFactory(
        tasks=tasks,
        projects=projects,
        task_service=task_service,
        project_service=project_service,
        file_sync=file_sync,
        git=git_service,
        yolo=config.yolo,
    )
    tool_provider = CompositeToolProvider(
        resource_tools,
        task_tools,
        ChannelToolkitFactory(
            service=channel_service,
            yolo=config.yolo,
        ),
        HostFileToolkitFactory(
            access=HostFileAccess(
                root=config.host_share_root,
                read_allowlist=config.host_read_allowlist,
                write_allowlist=config.host_write_allowlist,
            ),
            yolo=config.yolo,
        ),
        ConfigurationToolkitFactory(
            service=integration_service,
            yolo=config.yolo,
        ),
        IntegrationToolkitFactory(
            service=integration_service,
            secret_resolver=_environment_secret,
            yolo=config.yolo,
        ),
    )
    asset_root = Path(
        os.environ.get(
            "AGENTTEAMS_MANAGER_ASSET_ROOT",
            str(_ASSET_ROOT),
        ),
    )
    toolkit_factory = SkillToolkitFactory(
        SkillRegistry(asset_root / "skills"),
        tools=tool_provider,
        metrics=metrics,
    )
    agent_factory = AgentFactory(
        config=config,
        runtime=runtime_registry,
        prompt_builder=PromptBuilder(asset_root),
        toolkit_factory=toolkit_factory,
        mcp_registry=mcp_registry,
    )
    sessions = RoomSessionManager(
        factory=agent_factory,
        sessions=session_repository,
        session_timezone=config.session_timezone,
    )
    runner = MatrixSessionRunner(
        sessions=sessions,
        matrix=matrix,
        admin_user_id=config.admin_user_id,
        admin_room_id=config.manager_admin_room_id,
        confirmations=confirmations,
        confirmation_notifications=notification_service,
        history=matrix.history,
        media=MatrixMedia(matrix),
        memory=memory_repository,
        metrics=metrics,
        known_models={
            name: capabilities.reasoning
            for name, capabilities in known_models.items()
        },
    )
    policy = RoomPolicyResolver(
        topology=topology,
        admin_user_id=config.admin_user_id,
        manager_user_id=config.manager_user_id,
        revision=topology.revision,
    )
    router = EventRouter(
        claims=operations,
        resolver=policy,
        handler=runner.handle,
        control_handler=runner.handle_control,
        queue_settings=sessions.queue_settings,
        interrupt_handler=sessions.cancel,
    )
    matrix_runtime = MatrixRuntime(
        matrix=matrix,
        router=router,
        metrics=metrics,
        tracer=tracer,
    )

    resource_recovery = ResourceHeartbeat(
        operations=operations,
        resources=resource_service,
        matrix_resources=matrix_resources,
    )
    heartbeat = Heartbeat(
        recovery=resource_recovery,
        topology=topology_resolver,
        notifications=notification_service,
        task_recovery=TaskRecovery(
            operations=operations,
            tasks=task_service,
            projects=project_service,
            git=git_service,
            files=file_sync,
        ),
        leases=lease_service,
        task_scheduler=TaskHeartbeat(
            tasks=tasks,
            service=task_service,
        ),
        completions=TaskCompletionRecovery(
            operations=operations,
            tasks=task_service,
        ),
        snapshotter=SnapshotScheduler(
            database=database,
            operations=operations,
            journal=journal,
            temporary_path=(
                config.workspace / "tmp" / "manager-snapshot.db"
            ),
        ),
        integration_recovery=IntegrationRecovery(
            operations=operations,
            integrations=integration_service,
        ),
        notification_recovery=NotificationRecovery(
            operations=operations,
            notifications=notification_service,
        ),
        semantic_supervision=SemanticSupervisor(
            tasks=tasks,
            workers=resource_service,
            notifications=notification_service,
        ),
    )
    heartbeat_runtime = HeartbeatRuntime(
        heartbeat=heartbeat,
        interval=lambda: (
            runtime_registry.current.document
            .heartbeat_interval_seconds
        ),
        metrics=metrics,
        tracer=tracer,
    )

    for kind in (
        OperationKind.CREATE_WORKER,
        OperationKind.IMPORT_WORKER,
        OperationKind.UPDATE_WORKER,
        OperationKind.DELETE_WORKER,
        OperationKind.CREATE_TEAM,
        OperationKind.UPDATE_TEAM,
        OperationKind.DELETE_TEAM,
        OperationKind.CREATE_HUMAN,
        OperationKind.UPDATE_HUMAN,
        OperationKind.DELETE_HUMAN,
    ):
        resumers[kind] = resource_service.resume_operation
    resumers[OperationKind.MATRIX_MUTATION] = matrix_resources.resume
    resumers[OperationKind.CHANNEL_MUTATION] = matrix_resources.resume
    resumers[OperationKind.DELEGATE_TASK] = task_service.resume_operation
    resumers[OperationKind.COMPLETE_TASK] = task_service.resume_operation
    for kind in (
        OperationKind.CREATE_PROJECT,
        OperationKind.UPDATE_PROJECT,
        OperationKind.CLOSE_PROJECT,
    ):
        resumers[kind] = project_service.resume_operation
    resumers[OperationKind.GIT_DELEGATION] = git_service.resume_operation
    for kind in (
        OperationKind.CONFIGURE_MCP,
        OperationKind.SWITCH_MODEL,
        OperationKind.UPDATE_MANAGER_IDENTITY,
        OperationKind.PUBLISH_SERVICE,
    ):
        resumers[kind] = integration_service.resume_operation

    readiness = ReadinessState()
    admin_service = AdminSnapshotService(
        database=database,
        readiness=readiness,
        controller=agt,
        runtime_registry=runtime_registry,
    )
    health = HealthServer(
        readiness=readiness,
        metrics=metrics,
        port=config.health_port,
        admin_token=config.admin_api_token,
        admin_snapshot=admin_service.snapshot,
        webhook_handler=(
            channel_service.ingest
            if channel_service.providers
            else None
        ),
    )
    closeables = tuple(
        dependency
        for dependency in (
            storage,
            nacos,
            model_gateway,
            higress,
            mcp_registry,
            channel_service,
        )
        if dependency is not None
    )
    return ManagerApplication(
        database=database,
        recovery=recovery,
        config_watcher=config_watcher,
        matrix=matrix_runtime,
        heartbeat=heartbeat_runtime,
        health=health,
        sessions=sessions,
        readiness=readiness,
        startup_hooks=(migrate_legacy_confirmations,),
        closeables=closeables,
    )


def _initial_runtime(config: ManagerConfig) -> RuntimeDocument:
    return RuntimeDocument(
        revision=0,
        manager_name=config.manager_name,
        model=config.default_model,
        skills=tuple(sorted(EXPECTED_MANAGER_SKILLS)),
        prompt_sources=PromptSources(
            soul="manager/SOUL.md",
            agents="manager/AGENTS.md",
            tools="manager/TOOLS.md",
            heartbeat="manager/HEARTBEAT.md",
        ),
        heartbeat_interval_seconds=config.heartbeat_interval_seconds,
        worker_idle_timeout_seconds=config.worker_idle_timeout_seconds,
    )


def _load_known_models() -> dict[str, ModelCapabilities]:
    path = Path(
        os.environ.get(
            "AGENTTEAMS_KNOWN_MODELS_PATH",
            str(_KNOWN_MODELS),
        ),
    )
    rows = json.loads(path.read_text(encoding="utf-8"))
    known: dict[str, ModelCapabilities] = {}
    for row in rows:
        capabilities = ModelCapabilities(
            model=str(row["id"]),
            context_window=int(row["contextWindow"]),
            max_tokens=int(row["maxTokens"]),
            reasoning=bool(row["reasoning"]),
            input_modalities=tuple(row.get("input", ("text",))),
        )
        known[capabilities.model] = capabilities
    return known


def _higress_client(config: ManagerConfig) -> HigressClient | None:
    if (
        config.higress_admin_url is None
        or config.higress_admin_user is None
        or config.higress_admin_password is None
    ):
        return None
    gateway_domain = urlsplit(config.ai_gateway_url).hostname
    if not gateway_domain:
        raise ValueError("AI gateway URL has no hostname")
    return HigressClient(
        console_url=config.higress_admin_url,
        gateway_domain=gateway_domain,
        admin_user=config.higress_admin_user,
        admin_password=config.higress_admin_password,
    )


def _environment_secret(reference: str) -> SecretStr:
    match = _SECRET_REFERENCE.fullmatch(reference)
    if match is None:
        raise ValueError(
            "secret references must use env:UPPER_CASE_NAME",
        )
    value = os.environ.get(match.group(1), "")
    if not value:
        raise ValueError("referenced secret is not configured")
    return SecretStr(value)


def _initialize_metrics(metrics: MetricsRegistry) -> None:
    for name in (
        "agentteams_manager_errors_total",
        "agentteams_manager_heartbeats_total",
        "agentteams_manager_matrix_events_total",
        "agentteams_manager_matrix_turns_total",
        "agentteams_manager_model_turns_total",
        "agentteams_manager_recovery_errors_total",
        "agentteams_manager_recovery_reconciled_total",
        "agentteams_manager_runtime_reloads_total",
        "agentteams_manager_tool_calls_total",
        "agentteams_manager_tool_errors_total",
    ):
        metrics.set(name, 0)
    metrics.set("agentteams_manager_runtime_revision", 0)
    metrics.set("agentteams_manager_up", 1)


def _span(tracer: Any | None, name: str):
    if tracer is None:
        return nullcontext()
    return tracer.start_as_current_span(name)
