"""Deploy check of the logger: console lines where log collectors expect JSON.

A deployed environment renders JSON by default, so console lines there come
only from a ``renderer: console`` written in the configuration, top-level or
on a handler.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from loom.core.checks import Finding, deploy_check
from loom.core.logger.config import LoggerConfig, Renderer

if TYPE_CHECKING:
    from loom.core.config.context import ConfigContext

CONSOLE_RENDERER_CHECK: Final = "loom.logger.W001"
"""Id of the check warning about console log lines in a deployed environment."""

_LOGGER_CONFIG = "observability.log.config"


@deploy_check(CONSOLE_RENDERER_CHECK)
def _console_renderer(config: ConfigContext) -> Finding | None:
    if not config.section_or_default("observability.log.enabled", bool, True):
        return None
    logger = config.section_optional(_LOGGER_CONFIG, LoggerConfig)
    if logger is None:
        return None
    declared = [logger.renderer, *(handler.renderer for handler in logger.handlers)]
    if not any(raw is not None and Renderer.from_str(raw) is Renderer.CONSOLE for raw in declared):
        return None
    return Finding(
        message=f"{_LOGGER_CONFIG} renders console lines, which log collectors do not parse.",
        hint=(
            "drop 'renderer: console' from the deployed configuration (a deployed "
            "environment renders JSON by default) or set 'renderer: json'."
        ),
    )
