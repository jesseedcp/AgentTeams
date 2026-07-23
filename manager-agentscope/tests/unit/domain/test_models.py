import pytest
from pydantic import ValidationError

from agentteams_manager.domain.models import (
    OperationRecord,
    OperationStatus,
    RoomKind,
    RoomPolicy,
)


def test_operation_record_rejects_illegal_transition() -> None:
    record = OperationRecord.new(
        operation_id="a" * 32,
        kind="create_worker",
        target_key="worker/alice",
        request={"name": "alice"},
    )

    assert record.can_transition_to(OperationStatus.PREPARED)
    assert not record.can_transition_to(OperationStatus.SUCCEEDED)


def test_room_policy_is_immutable_and_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        RoomPolicy.model_validate(
            {
                "room_id": "!admin:example",
                "kind": RoomKind.ADMIN_DM,
                "revision": 1,
                "allowed_tools": ["list_workers"],
                "confirm_tools": [],
                "unexpected": True,
            },
        )
