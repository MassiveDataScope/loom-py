"""Put the chosen format and the context filter on the process's logging."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Final, cast

from loom.core.config.errors import ConfigError
from loom.core.environment import Environment
from loom.core.logger.formats import json_format, lambda_format, text_format
from loom.core.logger.settings import LogSettings
from loom.core.logger.stamp import ContextFilter, LogIdentity, LogRuntime
from loom.core.plugins.entrypoints import (
    EntryPointNotFoundError,
    MissingDependency,
    load_entry_point,
    registered_names,
    select_entry_point,
)

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
    filters: Sequence[logging.Filter] = (),
) -> LogInstallation:
    """Install the format *settings* choose and stamp every record that reaches its handler.

    The handler of a format is named ``loom`` and replaces the one a previous
    call installed; a handler somebody else added stays. A format that installs
    no handler, ``lambda``, leaves the runtime's, and the level too. The
    context filter goes on the handler, whoever installed it.

    Args:
        settings: The ``observability.log`` settings.
        environment: The environment the process runs in.
        service_name: ``app.name``.
        service_version: ``app.version``.
        filters: Further filters for the handler, beside the context filter.

    Returns:
        The installation, to name the runtime once it is known.

    Raises:
        ConfigError: When the format is not registered.
    """
    root = logging.getLogger()
    handler = resolve_format(settings.format_for(environment))(settings)
    if handler is None:
        handlers = list(root.handlers)
    else:
        handler.set_name(HANDLER_NAME)
        for installed in [h for h in root.handlers if h.get_name() == HANDLER_NAME]:
            root.removeHandler(installed)
        root.addHandler(handler)
        root.setLevel(settings.level.upper())
        handlers = [handler]
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
