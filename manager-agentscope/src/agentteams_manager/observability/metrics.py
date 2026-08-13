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
        # 逻辑说明：指标保存在进程内，并以同一把锁保护并发更新和抓取快照。
        self._values: dict[str, float] = defaultdict(float)
        self._lock = Lock()

    def increment(self, name: str, amount: float = 1) -> None:
        # 逻辑说明：验证名称和非负增量后在锁内累加，维持 counter 只能单调增长的语义。
        self._validate(name)
        if amount < 0:
            raise ValueError("counter increments must not be negative")
        with self._lock:
            self._values[name] += amount

    def set(self, name: str, value: float) -> None:
        # 逻辑说明：验证名称后覆盖 gauge 当前值，锁防止抓取线程读到并发写入的中间状态。
        self._validate(name)
        with self._lock:
            self._values[name] = float(value)

    def value(self, name: str) -> float:
        # 逻辑说明：在锁内读取单项指标；未创建的名称按 Prometheus 常用语义返回零。
        with self._lock:
            return self._values.get(name, 0)

    def render(self) -> str:
        # 逻辑说明：先在锁内取得按名称排序的稳定快照，再在锁外语义下生成 Prometheus 文本。
        with self._lock:
            items = sorted(self._values.items())
        lines = [
            f"{name} {self._format(value)}"
            for name, value in items
        ]
        return "\n".join(lines) + ("\n" if lines else "")

    @staticmethod
    def _validate(name: str) -> None:
        # 逻辑说明：拒绝不符合 Prometheus 标识符规则的名称，避免输出无法被采集器解析的整页指标。
        if not _METRIC_NAME.fullmatch(name):
            raise ValueError(f"invalid Prometheus metric name: {name!r}")

    @staticmethod
    def _format(value: float) -> str:
        # 逻辑说明：整数值去掉多余小数点，非整数保持浮点表示，输出兼容 Prometheus 数字语法。
        number = float(value)
        return str(int(number)) if number.is_integer() else str(number)
