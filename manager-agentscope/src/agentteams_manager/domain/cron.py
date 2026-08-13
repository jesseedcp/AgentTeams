"""Bounded five-field cron parsing and timezone-aware evaluation.

解析五字段 cron，并在明确时区中计算下一次执行时间。

Recurring Task 保存的是计划而不是后台 sleep。heartbeat 每次读取 cron，结合任务时区
判断当前 occurrence 是否到期。解析器只支持有界语法并拒绝不合法数值，避免一条错误
表达式造成无限循环；夏令时等时间变化由时区感知的 datetime 处理。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

_MAX_EXPRESSION_LENGTH = 128
_MAX_SEARCH_MINUTES = 527_040


@dataclass(frozen=True, slots=True)
class CronSchedule:
    minute: frozenset[int]
    hour: frozenset[int]
    day_of_month: frozenset[int]
    month: frozenset[int]
    weekday: frozenset[int]

    @classmethod
    def parse(cls, expression: str) -> CronSchedule:
        if not expression or len(expression) > _MAX_EXPRESSION_LENGTH:
            raise ValueError("cron expression length is invalid")
        fields = expression.split()
        if len(fields) != 5:
            raise ValueError("cron expression must contain exactly five fields")
        minute = _parse_field(fields[0], 0, 59)
        hour = _parse_field(fields[1], 0, 23)
        day = _parse_field(fields[2], 1, 31)
        month = _parse_field(fields[3], 1, 12)
        weekday_raw = _parse_field(fields[4], 0, 7)
        weekday = frozenset(0 if value == 7 else value for value in weekday_raw)
        return cls(
            minute=minute,
            hour=hour,
            day_of_month=day,
            month=month,
            weekday=weekday,
        )

    def next_after(
        self,
        instant: datetime,
        timezone: str,
    ) -> datetime:
        """Return the next matching UTC minute within the bounded horizon."""

        if instant.tzinfo is None or instant.utcoffset() is None:
            raise ValueError("cron search instant must be timezone-aware")
        try:
            zone = ZoneInfo(timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown IANA timezone: {timezone}") from exc
        candidate = (
            instant.astimezone(UTC).replace(second=0, microsecond=0)
            + timedelta(minutes=1)
        )
        for _ in range(_MAX_SEARCH_MINUTES):
            local = candidate.astimezone(zone)
            if local.fold == 0 and self.matches(local):
                return candidate
            candidate += timedelta(minutes=1)
        raise ValueError(
            "cron expression has no occurrence within the bounded search",
        )

    def matches(self, local: datetime) -> bool:
        if (
            local.minute not in self.minute
            or local.hour not in self.hour
            or local.month not in self.month
        ):
            return False
        day_matches = local.day in self.day_of_month
        cron_weekday = (local.weekday() + 1) % 7
        weekday_matches = cron_weekday in self.weekday
        day_restricted = self.day_of_month != frozenset(range(1, 32))
        weekday_restricted = self.weekday != frozenset(range(0, 7))
        if day_restricted and weekday_restricted:
            return day_matches or weekday_matches
        if day_restricted:
            return day_matches
        if weekday_restricted:
            return weekday_matches
        return True


def _parse_field(value: str, minimum: int, maximum: int) -> frozenset[int]:
    if not re.fullmatch(r"[0-9*,/\-]+", value):
        raise ValueError(f"invalid cron field: {value!r}")
    selected: set[int] = set()
    for component in value.split(","):
        if not component:
            raise ValueError(f"empty cron field component: {value!r}")
        if component.count("/") > 1:
            raise ValueError(f"invalid cron step: {component!r}")
        base, separator, step_raw = component.partition("/")
        step = 1
        if separator:
            if not step_raw or not step_raw.isascii() or not step_raw.isdigit():
                raise ValueError(f"invalid cron step: {component!r}")
            step = int(step_raw)
            if step <= 0:
                raise ValueError("cron step must be positive")

        if base == "*":
            start, end = minimum, maximum
        elif "-" in base:
            if base.count("-") != 1:
                raise ValueError(f"invalid cron range: {base!r}")
            start_raw, end_raw = base.split("-", 1)
            start = _integer(start_raw, minimum, maximum)
            end = _integer(end_raw, minimum, maximum)
            if start > end:
                raise ValueError(f"descending cron range: {base!r}")
        else:
            start = _integer(base, minimum, maximum)
            end = maximum if separator else start
        selected.update(range(start, end + 1, step))
    if not selected:
        raise ValueError("cron field selects no values")
    return frozenset(selected)


def _integer(value: str, minimum: int, maximum: int) -> int:
    if not value or not value.isascii() or not value.isdigit():
        raise ValueError(f"invalid cron integer: {value!r}")
    number = int(value)
    if number < minimum or number > maximum:
        raise ValueError(
            f"cron value {number} is outside {minimum}..{maximum}",
        )
    return number
