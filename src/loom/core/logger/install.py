"""Put the chosen format and the context filter on the process's logging."""

from __future__ import annotations

import logging
import sys
from collections.abc import Sequence
from typing import Final

from loom.core.environment import Environment, lambda_writes_json_logs
from loom.core.logger.formats import JsonFormatter, LambdaFormatter, TextFormatter
from loom.core.logger.settings import LogFormatName, LogSettings
from loom.core.logger.stamp import ContextFilter, LogIdentity

HANDLER_NAME: Final = "loom"
"""Name of the handler loom installs, so a second configuration replaces it only."""

_LAMBDA_RUNTIME_HANDLER: Final = "LambdaLoggerHandler"
"""Class-name prefix of the Lambda runtime's handlers (awslambdaric 4.2.3), which
cannot be imported."""


def configure_logging(
    settings: LogSettings,
    *,
    environment: Environment,
    service_name: str | None = None,
    service_version: str | None = None,
    extra_handlers: Sequence[logging.Handler] = (),
) -> ContextFilter:
    """Write records in the format *settings* choose and stamp every one on loom's handlers.

    ``json`` and ``text`` write to stdout through a handler named ``loom``, which
    replaces the one a previous call installed. ``lambda`` gives the runtime's
    handlers loom's JSON line, or writes JSON to stdout when there is none. The
    root level is always the settings'. Each named logger of
    :meth:`~loom.core.logger.settings.LogSettings.logger_levels`, the quiet libraries
    included, gets its level set, whatever it was before. Every handler loom owns,
    *extra_handlers* included, carries one context filter, this call's: a previous
    call's is replaced, however often this runs.

    Args:
        settings: The ``observability.log`` settings.
        environment: The environment the process runs in.
        service_name: ``app.name``.
        service_version: ``app.version``.
        extra_handlers: Handlers loom owns beside the format's, such as the OTEL log export.

    Returns:
        The context filter, shared by every handler, to name the runtime once it is known.
    """
    root = logging.getLogger()
    for extra in extra_handlers:
        if extra not in root.handlers:
            root.addHandler(extra)
    for stale in [h for h in root.handlers if h.get_name() == HANDLER_NAME]:
        root.removeHandler(stale)
    root.setLevel(settings.level)
    match settings.format_for(environment):
        case LogFormatName.TEXT:
            written = [_stdout_handler(root, TextFormatter())]
        case LogFormatName.JSON:
            written = [_stdout_handler(root, JsonFormatter())]
        case LogFormatName.LAMBDA:
            runtime = [
                h for h in root.handlers if type(h).__name__.startswith(_LAMBDA_RUNTIME_HANDLER)
            ]
            formatter = LambdaFormatter(json_frames=lambda_writes_json_logs())
            for handler in runtime:
                handler.setFormatter(formatter)
            written = runtime or [_stdout_handler(root, JsonFormatter())]
    for name, level in settings.logger_levels().items():
        logging.getLogger(name).setLevel(level)
    identity = LogIdentity(
        environment=environment, service_name=service_name, service_version=service_version
    )
    stamp = ContextFilter(identity)
    for target in (*written, *extra_handlers):
        target.filters = [f for f in target.filters if not isinstance(f, ContextFilter)]
        target.addFilter(stamp)
    return stamp


def _stdout_handler(root: logging.Logger, formatter: logging.Formatter) -> logging.Handler:
    """Put loom's stdout handler with *formatter* on *root*."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    handler.set_name(HANDLER_NAME)
    root.addHandler(handler)
    return handler
