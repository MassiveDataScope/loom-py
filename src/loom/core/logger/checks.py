"""Deploy checks of ``observability.log``, read as written.

``loom.logger.E001``: ``format`` is not a log format.
``loom.logger.E002``: ``level`` or a ``named_levels`` value is not a log level.

Neither resolves a placeholder; a value that holds one is not judged.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from loom.core.checks import CheckSet, Finding
from loom.core.config.keys import INTERPOLATION
from loom.core.logger.settings import LogFormatName, is_log_level

if TYPE_CHECKING:
    from loom.core.config.context import ConfigContext

UNKNOWN_FORMAT_CHECK: Final = "loom.logger.E001"
"""Id of the check refusing a log format loom does not write."""

UNKNOWN_LEVEL_CHECK: Final = "loom.logger.E002"
"""Id of the check refusing a level the standard library does not know."""

CHECKS: Final = CheckSet()
"""Checks of ``observability.log``, run when the process uses that path."""

_PATH = "observability.log"


def _literal(value: object) -> str | None:
    """*value* when it is a string with no placeholder in it."""
    return value if isinstance(value, str) and INTERPOLATION not in value else None


def _written(config: ConfigContext) -> dict[str, object]:
    log = config.raw(_PATH, {})
    return log if isinstance(log, dict) else {}


@CHECKS.register(UNKNOWN_FORMAT_CHECK)
def _known_format(config: ConfigContext) -> Finding | None:
    name = _literal(_written(config).get("format"))
    if name is None or name in LogFormatName.__members__.values():
        return None
    return Finding(
        message=f"{_PATH}.format is {name!r}, which is not a log format.",
        hint=f"use one of {', '.join(LogFormatName)}.",
    )


@CHECKS.register(UNKNOWN_LEVEL_CHECK)
def _known_levels(config: ConfigContext) -> Finding | None:
    log = _written(config)
    named = log.get("named_levels")
    declared = {f"{_PATH}.level": log.get("level")}
    if isinstance(named, dict):
        declared |= {f"{_PATH}.named_levels.{key}": value for key, value in named.items()}
    for key, value in declared.items():
        level = _literal(value)
        if level is not None and not is_log_level(level):
            return Finding(
                message=f"{key} is {level!r}, which is not a log level.",
                hint="use DEBUG, INFO, WARNING, ERROR or CRITICAL.",
            )
    return None
