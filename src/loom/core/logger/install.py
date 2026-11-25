"""Put the chosen format on the process's logging."""

from __future__ import annotations

import logging
import sys
from collections.abc import Sequence
from typing import Final, assert_never

from loom.core.environment import lambda_writes_json_logs
from loom.core.logger.formats import JsonFormatter, LambdaFormatter, TextFormatter
from loom.core.logger.identity import LogIdentity
from loom.core.logger.settings import LogFormatName, LogSettings

HANDLER_NAME: Final = "loom"
"""Name of the handler loom installs, so a second configuration replaces it only."""

EXPORT_HANDLER_NAME: Final = "loom.export"
"""Name of the handler that exports the records, so a second configuration replaces it too."""

_OWNED_HANDLERS: Final = frozenset({HANDLER_NAME, EXPORT_HANDLER_NAME})

_LAMBDA_RUNTIME_HANDLER: Final = "LambdaLoggerHandler"
"""Class-name prefix of the Lambda runtime's handlers (awslambdaric 4.2.3), which
cannot be imported."""


def configure_logging(
    settings: LogSettings,
    identity: LogIdentity,
    *,
    extra_handlers: Sequence[logging.Handler] = (),
) -> None:
    """Write records in the format *settings* choose, with *identity* on every one.

    ``json`` and ``text`` write to stdout through a handler named ``loom``. It and an
    export handler named ``loom.export`` replace, and close, the ones a previous call
    installed. ``lambda`` gives the runtime's
    handlers loom's JSON line, or writes JSON to stdout when there is none. The
    root level is always the settings'. Each named logger of
    :meth:`~loom.core.logger.settings.LogSettings.logger_levels`, the quiet libraries
    included, gets its level set, whatever it was before. Each format derives the
    context of a record itself, when it writes it, and holds *identity*, so a
    handler you add without loom's formatter does not see it.

    Args:
        settings: The ``observability.log`` settings.
        identity: The service and process the records come from, shared by reference
            with the formats; :meth:`~loom.core.logger.identity.LogIdentity.run_as`
            names the runtime once it is known.
        extra_handlers: Handlers loom owns beside the format's, such as the OTEL log
            export, named :data:`EXPORT_HANDLER_NAME`; they render with *identity* themselves.
    """
    root = logging.getLogger()
    stale_handlers = [
        handler
        for handler in root.handlers
        if handler.get_name() in _OWNED_HANDLERS and handler not in extra_handlers
    ]
    for stale in stale_handlers:
        root.removeHandler(stale)
        stale.close()
    for extra in extra_handlers:
        if extra not in root.handlers:
            root.addHandler(extra)
    root.setLevel(settings.level)
    match settings.format_for(identity.environment):
        case LogFormatName.TEXT:
            _stdout_handler(root, TextFormatter(identity=identity))
        case LogFormatName.JSON:
            _stdout_handler(root, JsonFormatter(identity=identity))
        case LogFormatName.LAMBDA:
            runtime = [
                h for h in root.handlers if type(h).__name__.startswith(_LAMBDA_RUNTIME_HANDLER)
            ]
            formatter = LambdaFormatter(json_frames=lambda_writes_json_logs(), identity=identity)
            for handler in runtime:
                handler.setFormatter(formatter)
            if not runtime:
                _stdout_handler(root, JsonFormatter(identity=identity))
        case _ as unreachable:
            assert_never(unreachable)
    for name, level in settings.logger_levels().items():
        logging.getLogger(name).setLevel(level)


def _stdout_handler(root: logging.Logger, formatter: logging.Formatter) -> None:
    """Put loom's stdout handler with *formatter* on *root*."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    handler.set_name(HANDLER_NAME)
    root.addHandler(handler)
