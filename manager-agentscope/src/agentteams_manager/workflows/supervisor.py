"""Recoverable sequencing for every cross-system side effect."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

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
    """Persist intent, classify ambiguity, and serialize reconciliation."""

    def __init__(
        self,
        *,
        operations: OperationRepository,
        journal: S3Journal,
        clock: Clock,
        reconcilers: Mapping[OperationKind, Reconciler],
    ) -> None:
        self._operations = operations
        self._journal = journal
        self._clock = clock
        self._reconcilers = dict(reconcilers)
        self._target_locks: dict[str, asyncio.Lock] = {}
        self._locks_guard = asyncio.Lock()
        self._journal_guard = asyncio.Lock()

    async def begin(
        self,
        *,
        operation_id: str,
        kind: OperationKind,
        target_key: str,
        request: dict[str, object],
    ) -> OperationRecord:
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
        sequence = await self._operations.next_sequence(operation_id)
        event = JournalEvent(
            operation_id=operation_id,
            sequence=sequence,
            event_type=event_type,
            payload={
                "effect": effect.value,
                **redact(details),
            },
            created_at=self._clock.now(),
        )
        await self._journal.append(event)
        await self._operations.append_event(event)
        return event

    async def _require(self, operation_id: str) -> OperationRecord:
        operation = await self._operations.get(operation_id)
        if operation is None:
            raise KeyError(operation_id)
        return operation

    async def _lock_for(self, target_key: str) -> asyncio.Lock:
        async with self._locks_guard:
            return self._target_locks.setdefault(
                target_key,
                asyncio.Lock(),
            )
