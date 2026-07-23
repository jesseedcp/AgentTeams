from agentteams_manager.domain.ids import (
    matrix_transaction_id,
    operation_id_for,
)


def test_operation_id_is_stable_and_tool_call_specific() -> None:
    first = operation_id_for("!room:a", "$event", "call-1")
    same = operation_id_for("!room:a", "$event", "call-1")
    other = operation_id_for("!room:a", "$event", "call-2")

    assert first == same
    assert first != other
    assert len(first) == 32


def test_matrix_transaction_id_is_stable_per_effect_sequence() -> None:
    operation_id = "a" * 32

    assert matrix_transaction_id(operation_id, 2) == (
        f"agentteams:{operation_id}:2"
    )

