"""Deploy check of the logger: console lines where log collectors expect JSON.

A deployed environment renders JSON by default, so console lines there come
only from a ``renderer: console`` written in the configuration, top-level or
on a handler. The check reads the keys as written and resolves no
placeholder; a renderer given by a placeholder is not judged.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from loom.core.checks import Finding, deploy_check
from loom.core.logger.config import Renderer

if TYPE_CHECKING:
    from loom.core.config.context import ConfigContext

CONSOLE_RENDERER_CHECK: Final = "loom.logger.W001"
"""Id of the check warning about console log lines in a deployed environment."""

_LOGGER_CONFIG = "observability.log.config"


@deploy_check(CONSOLE_RENDERER_CHECK)
def _console_renderer(config: ConfigContext) -> Finding | None:
    log = config.raw("observability.log", {})
    enabled = isinstance(log, dict) and log.get("enabled") is not False
    logger = log.get("config") if enabled else None
    if not isinstance(logger, dict):
        return None
    handlers = [h for h in logger.get("handlers") or () if isinstance(h, dict)]
    declared = [logger.get("renderer"), *(handler.get("renderer") for handler in handlers)]
    if not any(_is_console(raw) for raw in declared):
        return None
    return Finding(
        message=f"{_LOGGER_CONFIG} renders console lines, which log collectors do not parse.",
        hint=(
            "drop 'renderer: console' from the deployed configuration (a deployed "
            "environment renders JSON by default) or set 'renderer: json'."
        ),
    )


def _is_console(raw: object) -> bool:
    return isinstance(raw, str) and raw.strip().lower() == Renderer.CONSOLE.value
