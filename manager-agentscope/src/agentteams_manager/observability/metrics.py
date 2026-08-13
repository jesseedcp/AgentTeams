"""Dependency-free Prometheus text counters and gauges.

维护 Manager 的计数器与 gauge，并渲染 Prometheus 文本格式。

运行路径只更新内存中的有限标签指标，health server 在抓取时生成文本。这里刻意不把
用户输入、房间 ID 等高基数字段作为任意标签，否则一次聊天就可能创建大量时间序列并
耗尽监控系统内存；指标用于趋势判断，详细单次事件应查脱敏日志。
"""

from __future__ import annotations

import re
from collections import defaultdict
from threading import Lock

_METRIC_NAME = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")


class MetricsRegistry:
    def __init__(self) -> None:
        self._values: dict[str, float] = defaultdict(float)
        self._lock = Lock()

    def increment(self, name: str, amount: float = 1) -> None:
        self._validate(name)
        if amount < 0:
            raise ValueError("counter increments must not be negative")
        with self._lock:
            self._values[name] += amount

    def set(self, name: str, value: float) -> None:
        self._validate(name)
        with self._lock:
            self._values[name] = float(value)

    def value(self, name: str) -> float:
        with self._lock:
            return self._values.get(name, 0)

    def render(self) -> str:
        with self._lock:
            items = sorted(self._values.items())
        lines = [
            f"{name} {self._format(value)}"
            for name, value in items
        ]
        return "\n".join(lines) + ("\n" if lines else "")

    @staticmethod
    def _validate(name: str) -> None:
        if not _METRIC_NAME.fullmatch(name):
            raise ValueError(f"invalid Prometheus metric name: {name!r}")

    @staticmethod
    def _format(value: float) -> str:
        number = float(value)
        return str(int(number)) if number.is_integer() else str(number)
