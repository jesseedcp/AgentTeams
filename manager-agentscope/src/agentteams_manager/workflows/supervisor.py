"""Recoverable sequencing for every cross-system side effect.

为所有跨系统外部效果提供统一的 durable Operation 编排。

规则是“先写 intent/journal，再调用外部系统，最后写 outcome”。收到明确成功可进入
succeeded，明确业务失败可进入 failed；网络超时或进程中断属于 ambiguous，必须进入
reconciling，由对应 handler 查询外部真实状态。稳定 operation ID 与按 target 加锁共同
保证重试和并发请求不会重复修改同一资源。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, cast

from agentteams_manager.domain.errors import ConflictError
from agentteams_manager.domain.models import (
    ExternalEffect,
    JournalEvent,
    OperationKind,
    OperationRecord,
    OperationStatus,
    RecoveryReport,
)
from agentteams_manager.domain.ports import Clock
from agentteams_manager.state.journal import S3Journal
from agentteams_manager.state.operations import OperationRepository

Reconciler = Callable[[OperationRecord], Awaitable[None]]

_SENSITIVE_PARTS = (
    "token",
    "secret",
    "password",
    "authorization",
    "api_key",
)


def redact(value: object, *, key: str = "") -> object:
    """Recursively redact values whose key names are credential-like."""
    # 逻辑说明：`redact` 把 `value`、`key` 转成适合持久化或日志的 `object`，删除/隐藏敏感值并限制不安全结构；该过程不修改原对象。
    lowered = key.casefold()
    if any(part in lowered for part in _SENSITIVE_PARTS):
        return "[REDACTED]"
    if isinstance(value, Mapping):
        return {
            str(child_key): redact(child, key=str(child_key))
            for child_key, child in value.items()
        }
    if isinstance(value, tuple):
        return tuple(redact(child) for child in value)
    if isinstance(value, list):
        return [redact(child) for child in value]
    return value


class OperationSupervisor:
    """持久化意图、区分确定/歧义结果，并串行化同目标恢复。

    ``before_effect`` 必须先于任何外部 I/O；``effect_succeeded`` 只接受可证明的回执；
    ``effect_ambiguous`` 用于 timeout、断线等“不知道是否已发生”的情况。``recover_all``
    按 operation kind 选择 typed reconciler，并按 target 加锁，恢复过程本身也可安全重跑。
    """

    def __init__(
        self,
        *,
        operations: OperationRepository,
        journal: S3Journal,
        clock: Clock,
        reconcilers: Mapping[OperationKind, Reconciler],
    ) -> None:
        # 逻辑说明：`__init__` 校验并保存 `operations`、`journal`、`clock`、`reconcilers`，为operation journal建立进程内服务状态；配置不合法时立即抛错，且构造阶段不执行远端变更。
        self._operations = operations
        self._journal = journal
        self._clock = clock
        self._reconcilers = dict(reconcilers)
        self._target_locks: dict[str, asyncio.Lock] = {}
        self._locks_guard = asyncio.Lock()
        self._journal_guard = asyncio.Lock()

    async def get(self, operation_id: str) -> OperationRecord | None:
        # 逻辑说明：`get` 接收 `operation_id`，读取 operation journal，核心调用为 `get`，返回 `OperationRecord | None`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
        return await self._operations.get(operation_id)

    async def begin(
        self,
        *,
        operation_id: str,
        kind: OperationKind,
        target_key: str,
        request: dict[str, object],
    ) -> OperationRecord:
        """创建或复用同一稳定 ID 的 Operation，并验证请求身份没有碰撞。"""
        # 逻辑说明：`begin` 接收 `operation_id`、`kind`、`target_key`、`request`，创建 operation operation journal，核心调用为 `_lock_for`、`get`、`_validate_identity`，返回 `OperationRecord`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
        lock = await self._lock_for(f"operation/{operation_id}")
        async with lock:
            existing = await self._operations.get(operation_id)
            if existing is not None:
                self._validate_identity(
                    existing,
                    kind=kind,
                    target_key=target_key,
                    request=request,
                )
                await self._ensure_operation_started(existing)
                return existing
            record = OperationRecord.new(
                operation_id=operation_id,
                kind=kind,
                target_key=target_key,
                request=request,
            )
            try:
                record = await self._operations.create(record)
            except Exception:
                raced = await self._operations.get(operation_id)
                if raced is None:
                    raise
                self._validate_identity(
                    raced,
                    kind=kind,
                    target_key=target_key,
                    request=request,
                )
                record = raced
            await self._ensure_operation_started(record)
            return record

    @staticmethod
    def _validate_identity(
        operation: OperationRecord,
        *,
        kind: OperationKind,
        target_key: str,
        request: dict[str, object],
    ) -> None:
        # 逻辑说明：`_validate_identity` 确保复用同一 operation_id 时 kind、target_key 与已脱敏 request 都与原记录一致；不一致即冲突，防止幂等键被另一项操作误用。
        if (
            operation.kind is not kind
            or operation.target_key != target_key
            or operation.request != request
        ):
            raise ConflictError(
                f"operation ID collision for {operation.operation_id}",
            )

    async def _ensure_operation_started(
        self,
        operation: OperationRecord,
    ) -> None:
        # 逻辑说明：`_ensure_operation_started` 先读取 operation started 的现状，再通过 `events_for`、`next_sequence`、`JournalEvent` 只补齐缺失部分，返回 `None`；已存在但内容冲突时拒绝覆盖，保证恢复操作幂等。
        async with self._journal_guard:
            events = await self._operations.events_for(
                operation.operation_id,
            )
            if any(
                event.event_type == "operation_started"
                for event in events
            ):
                return
            sequence = await self._operations.next_sequence(
                operation.operation_id,
            )
            event = JournalEvent(
                operation_id=operation.operation_id,
                sequence=sequence,
                event_type="operation_started",
                payload={
                    "operation": operation.model_dump(mode="json"),
                },
                created_at=self._clock.now(),
            )
            await self._journal.append(event)
            await self._operations.append_event(event)
            await self._operations.mark_event_applied(event.sequence)

    async def before_effect(
        self,
        operation_id: str,
        effect: ExternalEffect,
        request: dict[str, object],
    ) -> JournalEvent:
        """在外部调用前把脱敏请求持久化为 ``effect_planned``。"""
        # 逻辑说明：`before_effect` 接收 `operation_id`、`effect`、`request`，登记效果意图 effect，核心调用为 `_before_effect`，返回 `JournalEvent`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
        async with self._journal_guard:
            return await self._before_effect(
                operation_id,
                effect,
                request,
            )

    async def _before_effect(
        self,
        operation_id: str,
        effect: ExternalEffect,
        request: dict[str, object],
    ) -> JournalEvent:
        # 逻辑说明：`_before_effect` 接收 `operation_id`、`effect`、`request`，登记效果意图 effect，核心调用为 `next_sequence`、`JournalEvent`、`redact`，返回 `JournalEvent`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
        sequence = await self._operations.next_sequence(operation_id)
        event = JournalEvent(
            operation_id=operation_id,
            sequence=sequence,
            event_type="effect_planned",
            payload={
                "effect": effect.value,
                "request": redact(request),
            },
            created_at=self._clock.now(),
        )
        await self._journal.append(event)
        await self._operations.append_event(event)

        current = await self._operations.get(operation_id)
        if current is None:
            raise KeyError(operation_id)
        if current.status is OperationStatus.PLANNED:
            changed = await self._operations.transition(
                operation_id,
                expected={OperationStatus.PLANNED},
                target=OperationStatus.PREPARED,
            )
            current = changed or await self._require(operation_id)
        if current.status is OperationStatus.PREPARED:
            await self._operations.transition(
                operation_id,
                expected={OperationStatus.PREPARED},
                target=OperationStatus.DISPATCHED,
            )
        await self._operations.mark_event_applied(event.sequence)
        return event

    async def effect_succeeded(
        self,
        operation_id: str,
        effect: ExternalEffect,
        receipt: dict[str, object],
    ) -> OperationRecord:
        # 逻辑说明：`effect_succeeded` 接收 `operation_id`、`effect`、`receipt`，记录外部效果 succeeded，核心调用为 `_effect_succeeded`，返回 `OperationRecord`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
        async with self._journal_guard:
            return await self._effect_succeeded(
                operation_id,
                effect,
                receipt,
            )

    async def _effect_succeeded(
        self,
        operation_id: str,
        effect: ExternalEffect,
        receipt: dict[str, object],
    ) -> OperationRecord:
        # 逻辑说明：`_effect_succeeded` 接收 `operation_id`、`effect`、`receipt`，记录外部效果 succeeded，核心调用为 `_record_outcome`、`_require`、`transition`，返回 `OperationRecord`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
        event = await self._record_outcome(
            operation_id,
            "effect_succeeded",
            effect,
            {"receipt": receipt},
        )
        current = await self._require(operation_id)
        if current.status is OperationStatus.PREPARED:
            current = (
                await self._operations.transition(
                    operation_id,
                    expected={OperationStatus.PREPARED},
                    target=OperationStatus.DISPATCHED,
                )
                or await self._require(operation_id)
            )
        if current.status is OperationStatus.DISPATCHED:
            current = (
                await self._operations.transition(
                    operation_id,
                    expected={OperationStatus.DISPATCHED},
                    target=OperationStatus.RUNNING,
                )
                or await self._require(operation_id)
            )
        if current.status is OperationStatus.ACKNOWLEDGED:
            target_expected = {OperationStatus.ACKNOWLEDGED}
        elif current.status in {
            OperationStatus.RUNNING,
            OperationStatus.RECONCILING,
        }:
            target_expected = {current.status}
        elif current.status is OperationStatus.SUCCEEDED:
            await self._operations.mark_event_applied(event.sequence)
            return current
        else:
            raise ConflictError(
                f"cannot succeed operation from {current.status}",
            )
        changed = await self._operations.transition(
            operation_id,
            expected=target_expected,
            target=OperationStatus.SUCCEEDED,
            result=receipt,
        )
        result = changed or await self._require(operation_id)
        await self._operations.mark_event_applied(event.sequence)
        return result

    async def effect_acknowledged(
        self,
        operation_id: str,
        effect: ExternalEffect,
        receipt: dict[str, object],
    ) -> OperationRecord:
        """Record one successful step without terminating the operation."""
        # 逻辑说明：`effect_acknowledged` 接收 `operation_id`、`effect`、`receipt`，记录外部效果 acknowledged，核心调用为 `_effect_acknowledged`，返回 `OperationRecord`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
        async with self._journal_guard:
            return await self._effect_acknowledged(
                operation_id,
                effect,
                receipt,
            )

    async def _effect_acknowledged(
        self,
        operation_id: str,
        effect: ExternalEffect,
        receipt: dict[str, object],
    ) -> OperationRecord:
        # 逻辑说明：`_effect_acknowledged` 接收 `operation_id`、`effect`、`receipt`，记录外部效果 acknowledged，核心调用为 `_record_outcome`、`_require`、`mark_event_applied`，返回 `OperationRecord`。 它可能读写仓库、Controller、Matrix 或对象存储；前置条件不满足时保留现有领域异常，防止错误状态继续传播。
        event = await self._record_outcome(
            operation_id,
            "effect_acknowledged",
            effect,
            {"receipt": receipt},
        )
        current = await self._require(operation_id)
        if current.status is OperationStatus.SUCCEEDED:
            await self._operations.mark_event_applied(event.sequence)
            return current
        if current.status is OperationStatus.PREPARED:
            current = (
                await self._operations.transition(
                    operation_id,
                    expected={OperationStatus.PREPARED},
                    target=OperationStatus.DISPATCHED,
                )
                or await self._require(operation_id)
            )
        if current.status in {
            OperationStatus.DISPATCHED,
            OperationStatus.ACKNOWLEDGED,
            OperationStatus.RECONCILING,
        }:
            changed = await self._operations.transition(
                operation_id,
                expected={current.status},
                target=OperationStatus.RUNNING,
                result=receipt,
            )
            result = changed or await self._require(operation_id)
            await self._operations.mark_event_applied(event.sequence)
            return result
        if current.status is OperationStatus.RUNNING:
            await self._operations.mark_event_applied(event.sequence)
            return current
        raise ConflictError(
            f"cannot acknowledge operation from {current.status}",
        )

    async def effect_failed(
        self,
        operation_id: str,
        effect: ExternalEffect,
        reason: str,
    ) -> OperationRecord:
        """Persist a definite external failure as a terminal operation."""
        # 逻辑说明：`effect_failed` 接收 `operation_id`、`effect`、`reason`，记录外部效果 failed，核心调用为 `_effect_failed`，返回 `OperationRecord`。 它可能读写仓库、Controller、Matrix 或对象存储；下游异常按原语义向上传递，不会伪造成功结果。
        async with self._journal_guard:
            return await self._effect_failed(
                operation_id,
                effect,
                reason,
            )

    async def _effect_failed(
        self,
        operation_id: str,
        effect: ExternalEffect,
        reason: str,
    ) -> OperationRecord:
        # 逻辑说明：`_effect_failed` 接收 `operation_id`、`effect`、`reason`，记录效果 `failed`，依次复用 `_record_outcome`、`_require`、`mark_event_applied`，返回 `OperationRecord`。 它会推进 operation journal 与外部效果状态机 的外部或持久状态；校验、并发或恢复证据不足时保留现有异常，防止把歧义状态当作成功。
        event = await self._record_outcome(
            operation_id,
            "effect_failed",
            effect,
            {"reason": reason},
        )
        current = await self._require(operation_id)
        if current.status is OperationStatus.FAILED:
            await self._operations.mark_event_applied(event.sequence)
            return current
        if current.status is OperationStatus.SUCCEEDED:
            raise ConflictError("cannot fail a succeeded operation")
        if current.status in {
            OperationStatus.PLANNED,
            OperationStatus.PREPARED,
            OperationStatus.RUNNING,
            OperationStatus.RECONCILING,
            OperationStatus.NEEDS_ATTENTION,
        }:
            changed = await self._operations.transition(
                operation_id,
                expected={current.status},
                target=OperationStatus.FAILED,
                result={"effect": effect.value, "reason": str(redact(reason))},
            )
            result = changed or await self._require(operation_id)
            await self._operations.mark_event_applied(event.sequence)
            return result
        if current.status in {
            OperationStatus.DISPATCHED,
            OperationStatus.ACKNOWLEDGED,
        }:
            current = (
                await self._operations.transition(
                    operation_id,
                    expected={current.status},
                    target=OperationStatus.RUNNING,
                )
                or await self._require(operation_id)
            )
        elif current.status is OperationStatus.RETRY_WAIT:
            current = (
                await self._operations.transition(
                    operation_id,
                    expected={OperationStatus.RETRY_WAIT},
                    target=OperationStatus.RECONCILING,
                )
                or await self._require(operation_id)
            )
        changed = await self._operations.transition(
            operation_id,
            expected={current.status},
            target=OperationStatus.FAILED,
            result={"effect": effect.value, "reason": str(redact(reason))},
        )
        result = changed or await self._require(operation_id)
        await self._operations.mark_event_applied(event.sequence)
        return result

    async def effect_ambiguous(
        self,
        operation_id: str,
        effect: ExternalEffect,
        reason: str,
    ) -> OperationRecord:
        """把无法证明成功或失败的调用转入 reconciliation，而非重试。"""
        # 逻辑说明：`effect_ambiguous` 接收 `operation_id`、`effect`、`reason`，记录效果 `ambiguous`，依次复用 `_effect_ambiguous`，返回 `OperationRecord`。 它会推进 operation journal 与外部效果状态机 的外部或持久状态；下游失败沿用现有错误语义，不会伪造成功回执。
        async with self._journal_guard:
            return await self._effect_ambiguous(
                operation_id,
                effect,
                reason,
            )

    async def _effect_ambiguous(
        self,
        operation_id: str,
        effect: ExternalEffect,
        reason: str,
    ) -> OperationRecord:
        # 逻辑说明：`_effect_ambiguous` 接收 `operation_id`、`effect`、`reason`，记录效果 `ambiguous`，依次复用 `_record_outcome`、`_require`、`mark_event_applied`，返回 `OperationRecord`。 它会推进 operation journal 与外部效果状态机 的外部或持久状态；校验、并发或恢复证据不足时保留现有异常，防止把歧义状态当作成功。
        event = await self._record_outcome(
            operation_id,
            "effect_ambiguous",
            effect,
            {"reason": reason},
        )
        current = await self._require(operation_id)
        if current.status is OperationStatus.RECONCILING:
            await self._operations.mark_event_applied(event.sequence)
            return current
        expected = {
            OperationStatus.PREPARED,
            OperationStatus.DISPATCHED,
            OperationStatus.RUNNING,
            OperationStatus.RETRY_WAIT,
        }
        if current.status not in expected:
            raise ConflictError(
                f"cannot reconcile operation from {current.status}",
            )
        changed = await self._operations.transition(
            operation_id,
            expected={current.status},
            target=OperationStatus.RECONCILING,
            result={
                "effect": effect.value,
                "ambiguous_reason": str(redact(reason)),
            },
        )
        result = changed or await self._require(operation_id)
        await self._operations.mark_event_applied(event.sequence)
        return result

    async def recover_all(self) -> RecoveryReport:
        """扫描所有非终态 Operation，并交给对应 typed reconciler 对账。"""
        # 逻辑说明：`recover_all` 接收 当前服务依赖，恢复 `all`，依次复用 `list_recoverable`、`get`、`append`，返回 `RecoveryReport`。 它会推进 operation journal 与外部效果状态机 的外部或持久状态；下游失败沿用现有错误语义，不会伪造成功回执。
        operations = await self._operations.list_recoverable()
        reconciled = 0
        needs_attention: list[str] = []
        for operation in operations:
            handler = self._reconcilers.get(operation.kind)
            if handler is None:
                needs_attention.append(operation.operation_id)
                continue
            lock = await self._lock_for(operation.target_key)
            async with lock:
                current = await self._require(operation.operation_id)
                if current.status not in {
                    OperationStatus.PREPARED,
                    OperationStatus.DISPATCHED,
                    OperationStatus.RUNNING,
                    OperationStatus.RETRY_WAIT,
                    OperationStatus.RECONCILING,
                }:
                    continue
                await handler(current)
                reconciled += 1
        return RecoveryReport(
            reconciled_operations=reconciled,
            needs_attention=tuple(needs_attention),
        )

    async def _record_outcome(
        self,
        operation_id: str,
        event_type: str,
        effect: ExternalEffect,
        details: dict[str, Any],
    ) -> JournalEvent:
        # 逻辑说明：`_record_outcome` 接收 `operation_id`、`event_type`、`effect`、`details`，记录 `outcome`，依次复用 `next_sequence`、`redact`、`JournalEvent`，返回 `JournalEvent`。 它会推进 operation journal 与外部效果状态机 的外部或持久状态；下游失败沿用现有错误语义，不会伪造成功回执。
        sequence = await self._operations.next_sequence(operation_id)
        redacted_details = cast(dict[str, Any], redact(details))
        event = JournalEvent(
            operation_id=operation_id,
            sequence=sequence,
            event_type=event_type,
            payload={
                "effect": effect.value,
                **redacted_details,
            },
            created_at=self._clock.now(),
        )
        await self._journal.append(event)
        await self._operations.append_event(event)
        return event

    async def _require(self, operation_id: str) -> OperationRecord:
        # 逻辑说明：`_require` 接收 `operation_id`，校验并取得 `operation journal 与外部效果状态机`，依次复用 `get`、`KeyError`，返回 `OperationRecord`。 它会推进 operation journal 与外部效果状态机 的外部或持久状态；校验、并发或恢复证据不足时保留现有异常，防止把歧义状态当作成功。
        operation = await self._operations.get(operation_id)
        if operation is None:
            raise KeyError(operation_id)
        return operation

    async def _lock_for(self, target_key: str) -> asyncio.Lock:
        # 逻辑说明：`_lock_for` 为每个 target_key 懒创建并复用同一 asyncio.Lock，使同一资源的 operation 串行、不同资源可并发；只修改进程内锁表，不触碰持久状态。
        async with self._locks_guard:
            return self._target_locks.setdefault(
                target_key,
                asyncio.Lock(),
            )
