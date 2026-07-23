from datetime import UTC, datetime

import pytest

from agentteams_manager.domain.cron import CronSchedule


def test_cron_supports_lists_ranges_and_steps() -> None:
    schedule = CronSchedule.parse("*/15 9-17 * * 1,3,5")

    assert schedule.minute == frozenset({0, 15, 30, 45})
    assert 9 in schedule.hour and 17 in schedule.hour
    assert schedule.weekday == frozenset({1, 3, 5})


def test_next_after_crosses_dst_with_zoneinfo() -> None:
    schedule = CronSchedule.parse("30 9 * * *")

    result = schedule.next_after(
        datetime(2026, 3, 7, 15, tzinfo=UTC),
        "America/New_York",
    )

    assert result is not None
    assert result.tzinfo is UTC
    assert result == datetime(2026, 3, 8, 13, 30, tzinfo=UTC)


def test_nonexistent_spring_forward_minute_is_skipped() -> None:
    schedule = CronSchedule.parse("30 2 * * *")

    result = schedule.next_after(
        datetime(2026, 3, 8, 6, 59, tzinfo=UTC),
        "America/New_York",
    )

    assert result == datetime(2026, 3, 9, 6, 30, tzinfo=UTC)


@pytest.mark.parametrize(
    "expression",
    (
        "@daily",
        "0 0 0 * * *",
        "*/0 * * * *",
        "60 * * * *",
        "* * * JAN *",
    ),
)
def test_invalid_cron_forms_are_rejected(expression: str) -> None:
    with pytest.raises(ValueError):
        CronSchedule.parse(expression)
