"""Typed parsing for Matrix session control commands."""

from __future__ import annotations

import re

from agentteams_manager.domain.models import (
    SessionCommand,
    SessionCommandAction,
)

_MODEL_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_ALIASES = {
    "/new": SessionCommandAction.NEW,
    "/reset": SessionCommandAction.RESET,
    "/compact": SessionCommandAction.COMPACT,
    "/status": SessionCommandAction.STATUS,
    "/model": SessionCommandAction.MODEL,
    "/models": SessionCommandAction.MODELS,
    "/help": SessionCommandAction.HELP,
    "/commands": SessionCommandAction.COMMANDS,
    "/stop": SessionCommandAction.STOP,
    "/think": SessionCommandAction.THINK,
    "/thinking": SessionCommandAction.THINK,
    "/t": SessionCommandAction.THINK,
    "/reasoning": SessionCommandAction.REASONING,
    "/reason": SessionCommandAction.REASONING,
    "/verbose": SessionCommandAction.VERBOSE,
    "/v": SessionCommandAction.VERBOSE,
    "/elevated": SessionCommandAction.ELEVATED,
    "/elev": SessionCommandAction.ELEVATED,
    "/queue": SessionCommandAction.QUEUE,
}
_NO_ARGUMENTS = {
    SessionCommandAction.RESET,
    SessionCommandAction.COMPACT,
    SessionCommandAction.STATUS,
    SessionCommandAction.MODELS,
    SessionCommandAction.HELP,
    SessionCommandAction.COMMANDS,
    SessionCommandAction.STOP,
}
_ONE_ARGUMENT = {
    SessionCommandAction.NEW,
    SessionCommandAction.MODEL,
    SessionCommandAction.THINK,
    SessionCommandAction.REASONING,
    SessionCommandAction.VERBOSE,
    SessionCommandAction.ELEVATED,
}


def parse_session_command(body: str) -> SessionCommand | None:
    parts = body.strip().split()
    if not parts or not parts[0].startswith("/"):
        return None
    source_name = parts[0].lower()
    action = _ALIASES.get(source_name, SessionCommandAction.UNKNOWN)
    arguments = tuple(parts[1:])
    if action in _NO_ARGUMENTS and arguments:
        raise ValueError(f"{source_name} does not accept arguments")
    if action in _ONE_ARGUMENT and len(arguments) > 1:
        raise ValueError(f"{source_name} accepts at most one argument")
    if action is SessionCommandAction.QUEUE and len(arguments) > 2:
        raise ValueError("/queue accepts a mode and optional limit")
    if (
        action in {SessionCommandAction.NEW, SessionCommandAction.MODEL}
        and arguments
        and arguments[0].lower() not in {"list", "status", "default"}
        and not arguments[0].isdigit()
        and _MODEL_NAME.fullmatch(arguments[0]) is None
    ):
        raise ValueError("invalid model name")
    return SessionCommand(
        action=action,
        arguments=arguments,
        source_name=source_name,
    )
