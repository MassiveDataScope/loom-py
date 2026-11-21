"""Make the process's records safe, and put the chosen format on its logging."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Final, cast

from loom.core.config.errors import ConfigError
from loom.core.environment import Environment
from loom.core.logger.filter import MAX_LIST_ITEMS, LogFilter
from loom.core.logger.formats import json_format, lambda_format, text_format
from loom.core.logger.log_events import LoggerLogEvent, LoggerLogField
from loom.core.logger.record import LoomRecordFactory, TraceIds, loom_trace_ids
from loom.core.logger.settings import LogSettings
from loom.core.logger.stamp import LogIdentity, LogRuntime
from loom.core.logger.uncaught import install_uncaught_hooks
from loom.core.logger.vocabulary import LogField
from loom.core.plugins.entrypoints import (
    EntryPointNotFoundError,
    MissingDependency,
    load_entry_point,
    registered_names,
    select_entry_point,
)

_log = logging.getLogger(__name__)

LOG_FORMATS_GROUP: Final = "loom.logging.formats"
"""Entry-point group an extra or an application registers its own log formats under."""

HANDLER_NAME: Final = "loom"
"""Name of the handler loom installs, so a second configuration replaces it only."""

LogFormat = Callable[[LogSettings], logging.Handler | None]
"""A format: the settings in, the handler that writes the records out, or ``None``."""


BUILTIN_FORMATS: Final[Mapping[str, LogFormat]] = MappingProxyType(
    {"lambda": lambda_format, "json": json_format, "text": text_format}
)
"""The formats loom ships. They resolve without entry-point metadata, which a packaged
Lambda function or a zipapp may not carry."""


def resolve_format(name: str, *, path: str = "observability.log.format") -> LogFormat:
    """Load the format called *name*: one loom ships, else one registered under the group.

    Args:
        name: Name of the format.
        path: Configuration key that names it, for the message.

    Returns:
        The format.

    Raises:
        ConfigError: When no format is registered under *name* (the message lists
            the registered ones), or the format needs a module that is not installed.
    """
    builtin = BUILTIN_FORMATS.get(name)
    if builtin is not None:
        return builtin
    try:
        return cast("LogFormat", load_entry_point(LOG_FORMATS_GROUP, name, on_duplicate="error"))
    except EntryPointNotFoundError as exc:
        known = ", ".join(format_names())
        raise ConfigError(
            f"{path}: unknown log format {name!r}. Registered formats: {known}."
        ) from exc
    except ModuleNotFoundError as exc:
        entry = select_entry_point(LOG_FORMATS_GROUP, name, on_duplicate="error")
        missing = None if entry is None else MissingDependency.from_import_error(exc, entry)
        if missing is None:
            raise
        raise ConfigError(f"{path}: log format {name!r} {missing.describe()}") from exc


def format_names() -> tuple[str, ...]:
    """The names of the formats loom ships and the ones registered under the group."""
    return tuple(sorted({*BUILTIN_FORMATS, *registered_names(LOG_FORMATS_GROUP)}))


@dataclass(frozen=True, slots=True)
class LogInstallation:
    """The record factory a configuration installed, to keep its identity current."""

    factory: LoomRecordFactory

    def running_as(self, runtime: LogRuntime) -> None:
        """Stamp the following records as emitted by *runtime*."""
        self.factory.running_as(runtime)


def install_record_factory(
    identity: LogIdentity, trace_ids: TraceIds = loom_trace_ids
) -> LoomRecordFactory:
    """Chain a :class:`LoomRecordFactory` for *identity* in front of the process's factory.

    A second call replaces the first one's factory instead of stacking on it.
    """
    current = logging.getLogRecordFactory()
    previous = current.previous if isinstance(current, LoomRecordFactory) else current
    factory = LoomRecordFactory(previous, identity, trace_ids)
    logging.setLogRecordFactory(factory)
    return factory


def configure_logging(
    settings: LogSettings,
    *,
    environment: Environment,
    service_name: str | None = None,
    service_version: str | None = None,
    extra_handlers: Sequence[logging.Handler] = (),
    trace_ids: TraceIds = loom_trace_ids,
) -> LogInstallation:
    """Make every record of the process safe, and install the format *settings* choose.

    The record factory turns each record's context and exception into fields,
    and a :class:`~loom.core.logger.filter.LogFilter` on every root handler
    renders its values; both are installed whatever the format. The handler of
    a format is named ``loom`` and replaces the one a previous call installed; a
    handler somebody else added stays and gets the filter. A format that
    installs no handler, ``lambda``, keeps the runtime's, and its level too.
    *extra_handlers* are attached to the root logger when they are not yet. The
    third-party floors and ``named_levels`` are applied, and an uncaught
    exception of the interpreter or a thread is logged as one record. Calling
    it again leaves one filter on each handler.

    Args:
        settings: The ``observability.log`` settings.
        environment: The environment the process runs in.
        service_name: ``app.name``.
        service_version: ``app.version``.
        extra_handlers: Handlers loom owns beside the format's, such as the OTEL log export.
        trace_ids: Reads the trace and span ids every record carries.

    Returns:
        The installation, to name the runtime once it is known.

    Raises:
        ConfigError: When the format is not registered.
    """
    logging.raiseExceptions = False
    factory = install_record_factory(
        LogIdentity(
            environment=environment.value,
            service_name=service_name,
            service_version=service_version,
        ),
        trace_ids,
    )
    root = logging.getLogger()
    for extra in extra_handlers:
        if extra not in root.handlers:
            root.addHandler(extra)
    handler = resolve_format(settings.format_for(environment))(settings)
    if handler is not None:
        handler.set_name(HANDLER_NAME)
        for installed in [h for h in root.handlers if h.get_name() == HANDLER_NAME]:
            root.removeHandler(installed)
        root.addHandler(handler)
        root.setLevel(settings.level.upper())
    for name, level in settings.logger_levels().items():
        logging.getLogger(name).setLevel(level.upper())
    for target in root.handlers:
        target.filters = [f for f in target.filters if not isinstance(f, LogFilter)]
        target.addFilter(_LOG_FILTER)
    install_uncaught_hooks()
    return LogInstallation(factory)


_LOG_FILTER: Final = LogFilter()


def unfiltered_root_handlers() -> tuple[logging.Handler, ...]:
    """The root handlers that do not render values: added after loom configured logging."""
    return tuple(
        handler
        for handler in logging.getLogger().handlers
        if not any(isinstance(f, LogFilter) for f in handler.filters)
    )


def warn_unfiltered_handlers() -> None:
    """Warn, in one record, about every root handler without the filter: its values are raw."""
    unfiltered = unfiltered_root_handlers()
    if unfiltered:
        _log.warning(
            LoggerLogEvent.HANDLERS_UNFILTERED,
            extra={
                LogField.COUNT: len(unfiltered),
                LoggerLogField.HANDLER_TYPES: [type(h).__name__ for h in unfiltered][
                    :MAX_LIST_ITEMS
                ],
            },
        )
