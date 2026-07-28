"""Crash-safe workflows for Matrix administration and channel policy."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Literal, Protocol

from agentteams_manager.domain.errors import (
    AmbiguousEffectError,
    ConflictError,
)
from agentteams_manager.domain.ids import matrix_transaction_id
from agentteams_manager.domain.models import (
    ExternalEffect,
    OperationKind,
    OperationRecord,
    OperationStatus,
)
from agentteams_manager.domain.ports import (
    MatrixAdministrationPort,
    MatrixPort,
)

from .resources import MutationContext, ResourceSupervisor

ChannelUpdateAction = Literal[
    "set_primary",
    "clear_primary",
    "trust",
]
ChannelDeleteAction = Literal["clear_primary", "remove_trusted"]
MembershipAction = Literal["invite", "kick", "ban", "unban"]


class MatrixResourcePort(
    MatrixPort,
    MatrixAdministrationPort,
    Protocol,
):
    """Combined Matrix surface needed by administration workflows."""


class ChannelTopology(Protocol):
    async def primary_channel(self, user_id: str) -> str | None: ...

    async def trusted_channels(
        self,
        user_id: str,
    ) -> tuple[str, ...]: ...


class ChannelStore(ChannelTopology, Protocol):
    async def set_primary_channel(
        self,
        user_id: str,
        room_id: str,
    ) -> None: ...

    async def clear_primary_channel(self, user_id: str) -> None: ...

    async def set_trusted_channel(
        self,
        first_user_id: str,
        second_user_id: str,
        room_id: str,
    ) -> None: ...

    async def remove_trusted_channel(
        self,
        first_user_id: str,
        second_user_id: str,
    ) -> None: ...


class MatrixMutationWorkflow(Protocol):
    async def create_channel(
        self,
        *,
        name: str,
        topic: str,
        invite: tuple[str, ...],
        revision: int,
        context: MutationContext,
    ) -> str: ...

    async def update_channel(
        self,
        *,
        action: ChannelUpdateAction,
        user_id: str,
        room_id: str | None,
        peer_user_id: str | None,
        context: MutationContext,
    ) -> dict[str, object]: ...

    async def delete_channel(
        self,
        *,
        action: ChannelDeleteAction,
        user_id: str,
        peer_user_id: str | None,
        context: MutationContext,
    ) -> dict[str, object]: ...

    async def send_notification(
        self,
        *,
        recipient: str,
        text: str,
        context: MutationContext,
    ) -> dict[str, object]: ...

    async def upload_media(
        self,
        *,
        path: Path,
        context: MutationContext,
    ) -> str: ...

    async def change_membership(
        self,
        *,
        action: MembershipAction,
        room_id: str,
        user_id: str,
        reason: str,
        context: MutationContext,
    ) -> dict[str, object]: ...


class ChannelResolver:
    """Choose only explicit notification channels in stable order."""

    def __init__(
        self,
        *,
        channels: ChannelTopology,
        matrix: MatrixAdministrationPort,
        manager_admin_room: str,
    ) -> None:
        self._channels = channels
        self._matrix = matrix
        self._manager_admin_room = manager_admin_room

    async def notification_room(self, *, recipient: str) -> str:
        joined = frozenset(await self._matrix.joined_rooms())
        primary = await self._channels.primary_channel(recipient)
        if primary and await self._usable(
            primary,
            recipient=recipient,
            joined=joined,
        ):
            return primary
        for room_id in await self._channels.trusted_channels(recipient):
            if await self._usable(
                room_id,
                recipient=recipient,
                joined=joined,
            ):
                return room_id
        return self._manager_admin_room

    async def _usable(
        self,
        room_id: str,
        *,
        recipient: str,
        joined: frozenset[str],
    ) -> bool:
        if room_id not in joined:
            return False
        return recipient in await self._matrix.members(room_id)


class MatrixResourceService:
    """Journal Matrix and channel mutations before their first effect."""

    def __init__(
        self,
        *,
        supervisor: ResourceSupervisor,
        matrix: MatrixResourcePort,
        channels: ChannelStore,
        manager_admin_room: str,
    ) -> None:
        self._supervisor = supervisor
        self._matrix = matrix
        self._channels = channels
        self._resolver = ChannelResolver(
            channels=channels,
            matrix=matrix,
            manager_admin_room=manager_admin_room,
        )

    async def create_channel(
        self,
        *,
        name: str,
        topic: str,
        invite: tuple[str, ...],
        revision: int,
        context: MutationContext,
    ) -> str:
        request: dict[str, object] = {
            "action": "create_channel",
            "name": name,
            "topic": topic,
            "invite": list(invite),
            "revision": revision,
        }
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.MATRIX_MUTATION,
            target_key=f"matrix-room/{name}",
            request=request,
        )
        result = await self._resume_create_channel(operation)
        return str(result["room_id"])

    async def update_channel(
        self,
        *,
        action: ChannelUpdateAction,
        user_id: str,
        room_id: str | None,
        peer_user_id: str | None,
        context: MutationContext,
    ) -> dict[str, object]:
        request: dict[str, object] = {
            "action": action,
            "user_id": user_id,
            "room_id": room_id,
            "peer_user_id": peer_user_id,
        }
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.CHANNEL_MUTATION,
            target_key=f"channel/{user_id}",
            request=request,
        )
        return await self._resume_channel(operation)

    async def delete_channel(
        self,
        *,
        action: ChannelDeleteAction,
        user_id: str,
        peer_user_id: str | None,
        context: MutationContext,
    ) -> dict[str, object]:
        request: dict[str, object] = {
            "action": action,
            "user_id": user_id,
            "peer_user_id": peer_user_id,
        }
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.CHANNEL_MUTATION,
            target_key=f"channel/{user_id}",
            request=request,
        )
        return await self._resume_channel(operation)

    async def send_notification(
        self,
        *,
        recipient: str,
        text: str,
        context: MutationContext,
    ) -> dict[str, object]:
        room_id = await self._resolver.notification_room(
            recipient=recipient,
        )
        request: dict[str, object] = {
            "action": "send_notification",
            "recipient": recipient,
            "room_id": room_id,
            "text": text,
        }
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.MATRIX_MUTATION,
            target_key=f"matrix-notification/{room_id}/{recipient}",
            request=request,
        )
        return await self._resume_notification(operation)

    async def upload_media(
        self,
        *,
        path: Path,
        context: MutationContext,
    ) -> str:
        resolved = path.resolve(strict=True)
        request: dict[str, object] = {
            "action": "upload_media",
            "path": str(resolved),
        }
        target_digest = hashlib.sha256(
            str(resolved).encode("utf-8"),
        ).hexdigest()[:16]
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.MATRIX_MUTATION,
            target_key=f"matrix-media/{target_digest}",
            request=request,
        )
        result = await self._resume_upload(operation)
        return str(result["mxc_uri"])

    async def change_membership(
        self,
        *,
        action: MembershipAction,
        room_id: str,
        user_id: str,
        reason: str,
        context: MutationContext,
    ) -> dict[str, object]:
        request: dict[str, object] = {
            "action": action,
            "room_id": room_id,
            "user_id": user_id,
            "reason": reason,
        }
        operation = await self._supervisor.begin(
            operation_id=context.operation_id,
            kind=OperationKind.MATRIX_MUTATION,
            target_key=f"matrix-membership/{room_id}/{user_id}",
            request=request,
        )
        return await self._resume_membership(operation)

    async def resume(
        self,
        operation: OperationRecord,
    ) -> dict[str, object]:
        """Reconcile one previously journaled Matrix/channel mutation."""
        if operation.kind is OperationKind.CHANNEL_MUTATION:
            return await self._resume_channel(operation)
        if operation.kind is not OperationKind.MATRIX_MUTATION:
            raise ValueError("operation is not a Matrix/channel mutation")
        action = str(operation.request.get("action", ""))
        if action == "create_channel":
            return await self._resume_create_channel(operation)
        if action == "send_notification":
            return await self._resume_notification(operation)
        if action == "upload_media":
            return await self._resume_upload(operation)
        if action in {"invite", "kick", "ban", "unban"}:
            return await self._resume_membership(operation)
        raise ValueError(f"unknown Matrix mutation action {action!r}")

    async def _resume_create_channel(
        self,
        operation: OperationRecord | Any,
    ) -> dict[str, object]:
        terminal = self._terminal_result(operation)
        if terminal is not None:
            return terminal
        if operation.status is not OperationStatus.PLANNED:
            room_id = await self._find_created_room(
                operation.operation_id,
            )
            if room_id is None:
                raise AmbiguousEffectError(
                    "Matrix room creation has no recoverable proof",
                )
            return await self._succeed(
                operation,
                {"action": "create_channel", "room_id": room_id},
            )

        await self._before_matrix(operation)
        try:
            room_id = await self._matrix.create_private_room(
                name=str(operation.request["name"]),
                topic=str(operation.request["topic"]),
                invite=tuple(operation.request["invite"]),
                creation_marker={
                    "kind": "channel",
                    "operation_id": operation.operation_id,
                    "revision": int(operation.request["revision"]),
                },
            )
        except Exception as exc:
            await self._record_error(operation, exc)
            raise
        return await self._succeed(
            operation,
            {"action": "create_channel", "room_id": room_id},
        )

    async def _resume_notification(
        self,
        operation: OperationRecord | Any,
    ) -> dict[str, object]:
        terminal = self._terminal_result(operation)
        if terminal is not None:
            return terminal
        if operation.status is OperationStatus.PLANNED:
            await self._before_matrix(operation)
        try:
            event_id = await self._matrix.send_text(
                str(operation.request["room_id"]),
                str(operation.request["text"]),
                txn_id=matrix_transaction_id(
                    operation.operation_id,
                    0,
                ),
                mentions=(str(operation.request["recipient"]),),
            )
        except Exception as exc:
            await self._record_error(operation, exc)
            raise
        return await self._succeed(
            operation,
            {
                "action": "send_notification",
                "room_id": str(operation.request["room_id"]),
                "event_id": event_id,
            },
        )

    async def _resume_upload(
        self,
        operation: OperationRecord | Any,
    ) -> dict[str, object]:
        terminal = self._terminal_result(operation)
        if terminal is not None:
            return terminal
        if operation.status is not OperationStatus.PLANNED:
            raise AmbiguousEffectError(
                "Matrix media upload cannot be repeated without a receipt",
            )
        await self._before_matrix(operation)
        try:
            uri = await self._matrix.upload_media(
                Path(str(operation.request["path"])),
            )
        except Exception as exc:
            await self._record_error(operation, exc)
            raise
        return await self._succeed(
            operation,
            {"action": "upload_media", "mxc_uri": uri},
        )

    async def _resume_membership(
        self,
        operation: OperationRecord | Any,
    ) -> dict[str, object]:
        terminal = self._terminal_result(operation)
        if terminal is not None:
            return terminal
        action = str(operation.request["action"])
        room_id = str(operation.request["room_id"])
        user_id = str(operation.request["user_id"])
        receipt: dict[str, object] = {
            "action": action,
            "room_id": room_id,
            "user_id": user_id,
        }
        if operation.status is not OperationStatus.PLANNED:
            if await self._membership_matches(
                action=action,
                room_id=room_id,
                user_id=user_id,
            ):
                receipt["recovered"] = True
                return await self._succeed(operation, receipt)
            raise AmbiguousEffectError(
                f"Matrix {action} has no converged membership proof",
            )

        await self._before_matrix(operation)
        reason = str(operation.request.get("reason", ""))
        try:
            if action == "invite":
                await self._matrix.invite_user(room_id, user_id)
            elif action == "kick":
                await self._matrix.kick_user(
                    room_id,
                    user_id,
                    reason=reason,
                )
            elif action == "ban":
                await self._matrix.ban_user(
                    room_id,
                    user_id,
                    reason=reason,
                )
            elif action == "unban":
                await self._matrix.unban_user(room_id, user_id)
            else:
                raise ValueError(
                    f"unknown Matrix membership action {action!r}",
                )
        except Exception as exc:
            await self._record_error(operation, exc)
            raise
        return await self._succeed(operation, receipt)

    async def _resume_channel(
        self,
        operation: OperationRecord | Any,
    ) -> dict[str, object]:
        terminal = self._terminal_result(operation)
        if terminal is not None:
            return terminal
        if operation.status is OperationStatus.PLANNED:
            await self._supervisor.before_effect(
                operation.operation_id,
                ExternalEffect.STORAGE,
                dict(operation.request),
            )
        action = str(operation.request["action"])
        user_id = str(operation.request["user_id"])
        room_id = operation.request.get("room_id")
        peer_user_id = operation.request.get("peer_user_id")
        try:
            if action == "set_primary":
                await self._channels.set_primary_channel(
                    user_id,
                    str(room_id),
                )
            elif action == "clear_primary":
                await self._channels.clear_primary_channel(user_id)
            elif action == "trust":
                await self._channels.set_trusted_channel(
                    user_id,
                    str(peer_user_id),
                    str(room_id),
                )
            elif action == "remove_trusted":
                await self._channels.remove_trusted_channel(
                    user_id,
                    str(peer_user_id),
                )
            else:
                raise ValueError(
                    f"unknown channel mutation action {action!r}",
                )
        except Exception as exc:
            await self._record_error(
                operation,
                exc,
                effect=ExternalEffect.STORAGE,
            )
            raise
        return await self._succeed(
            operation,
            dict(operation.request),
            effect=ExternalEffect.STORAGE,
        )

    def _terminal_result(
        self,
        operation: OperationRecord | Any,
    ) -> dict[str, object] | None:
        if operation.status is OperationStatus.SUCCEEDED:
            return dict(operation.result)
        if operation.status is OperationStatus.FAILED:
            raise ConflictError(
                f"{operation.kind.value} previously failed",
            )
        return None

    async def _before_matrix(
        self,
        operation: OperationRecord | Any,
    ) -> None:
        await self._supervisor.before_effect(
            operation.operation_id,
            ExternalEffect.MATRIX,
            dict(operation.request),
        )

    async def _succeed(
        self,
        operation: OperationRecord | Any,
        receipt: dict[str, object],
        *,
        effect: ExternalEffect = ExternalEffect.MATRIX,
    ) -> dict[str, object]:
        await self._supervisor.effect_succeeded(
            operation.operation_id,
            effect,
            receipt,
        )
        return receipt

    async def _record_error(
        self,
        operation: OperationRecord | Any,
        exc: Exception,
        *,
        effect: ExternalEffect = ExternalEffect.MATRIX,
    ) -> None:
        if isinstance(
            exc,
            (ValueError, FileNotFoundError, PermissionError),
        ):
            await self._supervisor.effect_failed(
                operation.operation_id,
                effect,
                type(exc).__name__,
            )
            return
        await self._supervisor.effect_ambiguous(
            operation.operation_id,
            effect,
            type(exc).__name__,
        )

    async def _find_created_room(
        self,
        operation_id: str,
    ) -> str | None:
        for room_id in await self._matrix.joined_rooms():
            for event in await self._matrix.room_state(room_id):
                content = event.get("content")
                if (
                    event.get("type") == "io.agentteams.creation"
                    and event.get("state_key", "") == ""
                    and isinstance(content, dict)
                    and content.get("operation_id") == operation_id
                ):
                    return room_id
        return None

    async def _membership_matches(
        self,
        *,
        action: str,
        room_id: str,
        user_id: str,
    ) -> bool:
        desired = {
            "invite": {"invite", "join"},
            "kick": {"leave"},
            "ban": {"ban"},
            "unban": {"leave"},
        }[action]
        for event in await self._matrix.room_state(room_id):
            content = event.get("content")
            if (
                event.get("type") == "m.room.member"
                and event.get("state_key") == user_id
                and isinstance(content, dict)
            ):
                return content.get("membership") in desired
        return False
