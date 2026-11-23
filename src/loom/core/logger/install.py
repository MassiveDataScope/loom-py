"""Put the chosen format and the context filter on the process's logging."""

from __future__ import annotations

import logging
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final

from loom.core.environment import Environment, lambda_writes_json_logs
from loom.core.logger.formats import JsonFormatter, LambdaFormatter, TextFormatter
from loom.core.logger.settings import LogFormatName, LogSettings
from loom.core.logger.stamp import ContextFilter, LogIdentity, LogRuntime

HANDLER_NAME: Final = "loom"
"""Name of the handler loom installs, so a second configuration replaces it only."""

_LAMBDA_RUNTIME_HANDLER: Final = "LambdaLoggerHandler"
"""Class-name prefix of the Lambda runtime's handlers (awslambdaric 4.2.3), which
cannot be imported."""


@dataclass(frozen=True, slots=True)
class LogInstallation:
    """The filters a configuration put on the handlers, to keep them current."""

    filters: tuple[ContextFilter, ...]

    def running_as(self, runtime: LogRuntime) -> None:
        """Stamp the following records as emitted by *runtime*."""
        for stamping in self.filters:
            stamping.use_runtime(runtime)


def configure_logging(
    settings: LogSettings,
    *,
    environment: Environment,
    service_name: str | None = None,
    service_version: str | None = None,
    extra_handlers: Sequence[logging.Handler] = (),
    filters: Sequence[logging.Filter] = (),
) -> LogInstallation:
    """Write records in the format *settings* choose and stamp every one on loom's handlers.

    ``json`` and ``text`` write to stdout through a handler named ``loom``, which
    replaces the one a previous call installed. ``lambda`` gives the runtime's
    handlers loom's JSON line, or writes JSON to stdout when there is none. The
    root level is always the settings'. Every handler loom owns, *extra_handlers*
    included, gets the context filter once, however often this runs.

    Args:
        settings: The ``observability.log`` settings.
        environment: The environment the process runs in.
        service_name: ``app.name``.
        service_version: ``app.version``.
        extra_handlers: Handlers loom owns beside the format's, such as the OTEL log export.
        filters: Further filters for every owned handler, beside the context filter.

    Returns:
        The installation, to name the runtime once it is known.
    """
    root = logging.getLogger()
    for extra in extra_handlers:
        if extra not in root.handlers:
            root.addHandler(extra)
    root.setLevel(settings.level.upper())
    match settings.format_for(environment):
        case LogFormatName.TEXT:
            written = [_stdout_handler(root, TextFormatter())]
        case LogFormatName.JSON:
            written = [_stdout_handler(root, JsonFormatter())]
        case LogFormatName.LAMBDA:
            written = [
                h for h in root.handlers if type(h).__name__.startswith(_LAMBDA_RUNTIME_HANDLER)
            ]
            formatter = LambdaFormatter(json_frames=lambda_writes_json_logs())
            for handler in written:
                handler.setFormatter(formatter)
            written = written or [_stdout_handler(root, JsonFormatter())]
    handlers = [*written, *extra_handlers]
    for name, level in settings.named_levels.items():
        logging.getLogger(name).setLevel(level.upper())
    identity = LogIdentity(
        environment=environment.value, service_name=service_name, service_version=service_version
    )
    stamping = tuple(ContextFilter(identity) for _ in handlers)
    for target, context_filter in zip(handlers, stamping, strict=True):
        target.filters = [f for f in target.filters if not isinstance(f, ContextFilter)]
        for each in (context_filter, *filters):
            target.addFilter(each)
    return LogInstallation(stamping)


def _stdout_handler(root: logging.Logger, formatter: logging.Formatter) -> logging.Handler:
    """Put loom's stdout handler with *formatter* on *root*, replacing a previous one."""
    for installed in [h for h in root.handlers if h.get_name() == HANDLER_NAME]:
        root.removeHandler(installed)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    handler.set_name(HANDLER_NAME)
    root.addHandler(handler)
    return handler
