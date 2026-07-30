"""Deterministic Manager workflows."""

from .git_delegation import (
    GitDelegationService,
    ProcessingLeaseService,
)
from .heartbeat import (
    Heartbeat,
    HeartbeatReport,
    IntegrationRecovery,
    IntegrationRecoveryReport,
)
from .integrations import (
    IntegrationService,
    ManagerIdentityReceipt,
    ManagerIdentityRequest,
    ModelSwitchReceipt,
    ModelSwitchRequest,
)
from .matrix_resources import MatrixResourceService
from .memory import ManagerMemoryService, MemoryRecall, MemoryWriteReceipt
from .notifications import DailyMemory, NotificationService
from .projects import ProjectReceipt, ProjectService
from .resources import (
    MutationContext,
    ResourceHeartbeat,
    ResourceRecoveryReport,
    ResourceReconciler,
    ResourceService,
    TeamSpec,
    TopologyResolver,
)
from .supervisor import OperationSupervisor
from .tasks import (
    RecurringDispatchReceipt,
    TaskMessageFormatter,
    TaskReceipt,
    TaskService,
)

__all__ = [
    "GitDelegationService",
    "Heartbeat",
    "HeartbeatReport",
    "IntegrationService",
    "IntegrationRecovery",
    "IntegrationRecoveryReport",
    "DailyMemory",
    "OperationSupervisor",
    "NotificationService",
    "ProcessingLeaseService",
    "ProjectReceipt",
    "ProjectService",
    "MutationContext",
    "ManagerIdentityReceipt",
    "ManagerIdentityRequest",
    "ModelSwitchReceipt",
    "ModelSwitchRequest",
    "MatrixResourceService",
    "ManagerMemoryService",
    "MemoryRecall",
    "MemoryWriteReceipt",
    "ResourceHeartbeat",
    "ResourceRecoveryReport",
    "ResourceReconciler",
    "ResourceService",
    "RecurringDispatchReceipt",
    "TeamSpec",
    "TaskMessageFormatter",
    "TaskReceipt",
    "TaskService",
    "TopologyResolver",
]
