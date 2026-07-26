"""Build a deterministic system prompt from verified local sources."""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Mapping

from agentteams_manager.config import RuntimeDocument
from agentteams_manager.domain.models import RoomPolicy


class PromptBuilder:
    """Resolve prompt objects under one trusted image-owned root."""

    def __init__(
        self,
        root: Path,
        *,
        expected_sha256: Mapping[str, str] | None = None,
    ) -> None:
        self._root = root.resolve()
        self._expected_sha256 = dict(expected_sha256 or {})

    def build(
        self,
        policy: RoomPolicy,
        runtime: RuntimeDocument,
        *,
        registered_tools: tuple[str, ...] | None = None,
    ) -> str:
        sources = runtime.prompt_sources
        ordered = (
            ("soul", sources.soul),
            ("agents", sources.agents),
            ("tools", sources.tools),
            ("heartbeat", sources.heartbeat),
        )
        sections = [
            self._read_source(label, source)
            for label, source in ordered
        ]
        active_tools = (
            tuple(sorted(registered_tools))
            if registered_tools is not None
            else tuple(sorted(policy.allowed_tools))
        )
        allowed = ", ".join(sorted(policy.allowed_tools)) or "(read-only)"
        registered = ", ".join(active_tools) or "(read-only)"
        confirmations = (
            ", ".join(sorted(policy.confirm_tools)) or "(none)"
        )
        skills = ", ".join(runtime.skills) or "all retained skills"
        sections.append(
            "\n".join(
                (
                    "# Active AgentScope Runtime",
                    "Use only registered typed AgentScope tools.",
                    f"Room policy: {policy.kind.value}",
                    f"Room policy revision: {policy.revision}",
                    f"Registered tools: {registered}",
                    f"Allowed tools: {allowed}",
                    f"Confirmation tools: {confirmations}",
                    f"Enabled skills: {skills}",
                ),
            ),
        )
        return "\n\n".join(sections)

    def _read_source(self, label: str, source: str) -> str:
        relative = Path(source)
        if relative.parts and relative.parts[0] == "manager":
            relative = Path(*relative.parts[1:])
        if relative.is_absolute():
            candidate = relative.resolve()
        else:
            candidate = (self._root / relative).resolve()
        if not candidate.is_relative_to(self._root):
            raise ValueError(
                f"prompt source {source!r} escapes prompt root",
            )
        data = candidate.read_bytes()
        expected = self._expected_sha256.get(label)
        digest = sha256(data).hexdigest()
        if expected is not None and digest != expected:
            raise ValueError(
                f"prompt source checksum mismatch for {label}",
            )
        return data.decode("utf-8").strip()
