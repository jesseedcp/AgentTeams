"""Output sanitizer: redact credentials from tool output before LLM sees it.

Registers as a Toolkit middleware so that every ToolResponse chunk is
sanitized before it reaches the agent's memory or the user's display.

Built-in rules cover mainstream cloud providers (Alibaba Cloud, AWS,
Tencent Cloud). User-defined rules can be added via credagent.json's
``output_sanitize`` field.
"""

# 初学者导读：工具可能意外返回 key/token。本层在结果进入模型记忆和用户显示前
# 做最后一道脱敏，因此规则既覆盖常见云凭据，也接受 Controller 配置的自定义
# 关键词。这里应替换敏感片段而保留错误上下文，方便排障又不暴露真实秘密。

from __future__ import annotations

import logging
import re
from typing import Any, AsyncGenerator, Callable

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Built-in patterns (always active)
# ---------------------------------------------------------------------------

_BUILTIN_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    # AccessKey ID prefixes
    (re.compile(r"\b(LTAI)[A-Za-z0-9]{12,}"), r"\1****"),
    (re.compile(r"\b(AKIA)[A-Za-z0-9]{12,}"), r"\1****"),
    (re.compile(r"\b(AKID)[A-Za-z0-9]{12,}"), r"\1****"),
    # AccessKey Secret — keyword + value (["']? before separator handles JSON-quoted keys)
    (
        re.compile(
            r"(?i)((?:access_?key_?secret|secret_?access_?key|accesskeysecret"
            r"|TENCENTCLOUD_SECRET_KEY|aws_secret_access_key"
            r"|credentials\.secret)"
            r"""["']?[\s]*[=:]\s*["']?)([A-Za-z0-9/+=]{16,})"""
        ),
        r"\1********",
    ),
    # STS / Security Token (300+ char base64)
    (
        re.compile(
            r"(?i)((?:security_?token|session_?token|sts_?token)"
            r"""["']?[\s]*[=:]\s*["']?)([A-Za-z0-9/+=]{100,})"""
        ),
        r"\1********",
    ),
    # Generic secret/password/token keyword context (30+ char value)
    (
        re.compile(
            r"(?i)((?:secret|token|password|passwd|key_secret)"
            r"""["']?[\s]*[=:]\s*["']?)([A-Za-z0-9/+=]{30,})"""
        ),
        r"\1********",
    ),
]


# ---------------------------------------------------------------------------
# Sanitizer class
# ---------------------------------------------------------------------------


class OutputSanitizer:
    """Manages built-in + user-defined sanitization patterns."""

    def __init__(self) -> None:
        # 逻辑说明：为每个脱敏器复制一份内置正则规则列表，使后续加载用户规则只改变本实例，避免不同 Worker 或测试实例互相污染。
        self._patterns: list[tuple[re.Pattern[str], str]] = list(_BUILTIN_PATTERNS)

    def load_user_rules(self, rules: list[dict[str, Any]]) -> None:
        """Replace user-defined rules from credagent.json output_sanitize."""
        # 逻辑说明：`load_user_rules` 接收 rules，加载用户 rules，返回 None；
        # 会更新对象内存状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
        self._patterns = list(_BUILTIN_PATTERNS)
        skipped = 0
        for rule in rules:
            try:
                self._add_rule(rule)
            except (re.error, KeyError, TypeError) as exc:
                skipped += 1
                logger.warning("output_sanitizer: invalid rule %r: %s", rule, exc)
        loaded = len(rules) - skipped
        if skipped:
            logger.warning("output_sanitizer: loaded %d rules, skipped %d invalid", loaded, skipped)

    def _add_rule(self, rule: dict[str, Any]) -> None:
        # 逻辑说明：`_add_rule` 接收 rule，执行 模型输出脱敏 中的“add 规则”步骤，返回 None；会访问网络服务。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
        rule_type = rule.get("type", "")
        if rule_type == "prefix":
            prefix = rule["prefix"]
            min_length = int(rule.get("min_length", 16))
            suffix_len = max(min_length - len(prefix), 1)
            escaped = re.escape(prefix)
            self._patterns.append((
                re.compile(rf"\b({escaped})[A-Za-z0-9]{{{suffix_len},}}"),
                r"\1****",
            ))
        elif rule_type == "keyword":
            keywords = rule.get("keywords", [])
            if not keywords:
                return
            kw_alt = "|".join(re.escape(k) for k in keywords)
            self._patterns.append((
                re.compile(
                    rf"""(?i)({kw_alt})(["']?[\s]*[=:]\s*["']?)([A-Za-z0-9/+=]{{16,}})"""
                ),
                r"\1\2********",
            ))
        elif rule_type == "regex":
            self._patterns.append((
                re.compile(rule["pattern"]),
                rule.get("replacement", "********"),
            ))

    def sanitize(self, text: str) -> str:
        """Apply all patterns to text, returning sanitized version."""
        # 逻辑说明：`sanitize` 接收 text，按内置与用户规则替换输出中的敏感值，返回 str；不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
        for pattern, replacement in self._patterns:
            text = pattern.sub(replacement, text)
        return text


# Module-level singleton
_sanitizer = OutputSanitizer()


def get_sanitizer() -> OutputSanitizer:
    """Return the global OutputSanitizer instance."""
    return _sanitizer


# ---------------------------------------------------------------------------
# Toolkit middleware
# ---------------------------------------------------------------------------


def create_sanitizer_middleware() -> Callable[..., Any]:
    """Create an agentscope Toolkit middleware that sanitizes tool output."""

    # 逻辑说明：构造 AgentScope toolkit 异步中间件；它消费下游工具响应流，逐个清洗 content 中的文本块后再原顺序 yield，防止凭据进入模型上下文。
    # 无文本内容保持不变；下游 handler 或 sanitizer 异常继续抛出，避免把未脱敏响应静默放行。
    async def _sanitizer_middleware(
        kwargs: dict[str, Any],
        next_handler: Callable[..., Any],
    ) -> "AsyncGenerator[Any, None]":
        # 逻辑说明：`_sanitizer_middleware` 接收 kwargs、next_handler，执行 模型输出脱敏 中的“sanitizer middleware”步骤，
        # 返回 'AsyncGenerator[Any, None]'；
        #
        # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
        async for response in await next_handler(**kwargs):
            content = getattr(response, "content", None)
            if content:
                sanitizer = get_sanitizer()
                for block in content:
                    text = getattr(block, "text", None)
                    if isinstance(text, str):
                        sanitized = sanitizer.sanitize(text)
                        if sanitized != text:
                            block.text = sanitized
                            logger.debug("output_sanitizer: redacted content in tool response")
            yield response

    return _sanitizer_middleware
