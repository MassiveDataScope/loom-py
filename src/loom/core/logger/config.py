"""Logging configuration: the Renderer enum, handler configs, and configure_logging.

The renderer and colour defaults follow the central
:class:`~loom.core.environment.Environment`: a deployed environment
(production, staging) renders JSON without colours, a local one (development,
test) coloured console lines. An explicit ``renderer`` or ``colors`` wins.

The attached handlers pass every record and the stdlib loggers filter, so a
named level below the top-level one takes effect for stdlib loggers. structlog
loggers, loom's own included, still drop events below the top-level level.
"""

from __future__ import annotations

import logging
import logging.handlers
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal

import msgspec
import structlog

from loom.core.environment import Environment, resolve_environment
from loom.core.model import LoomFrozenStruct


class Renderer(StrEnum):
    """Supported structlog output renderers.

    Adding a new renderer requires only a new entry here and in ``_RENDERER_FACTORIES``.
    """

    CONSOLE = "console"
    JSON = "json"

    @classmethod
    def from_str(cls, value: str) -> Renderer:
        """Parse a string into a ``Renderer``.

        Args:
            value: Renderer name (case-insensitive).

        Returns:
            The matching ``Renderer``.

        Raises:
            ValueError: If ``value`` does not match any known renderer.
        """
        try:
            return cls(value.strip().lower())
        except ValueError:
            valid = [r.value for r in cls]
            raise ValueError(f"Unsupported renderer: {value!r}. Valid values: {valid}") from None


class StreamHandlerConfig(
    LoomFrozenStruct, frozen=True, tag="stream", tag_field="type", forbid_unknown_fields=True
):
    """Configuration for a stdlib ``StreamHandler``.

    Attributes:
        stream: Target stream — ``"stdout"`` or ``"stderr"``.
        renderer: Per-handler output renderer (``"console"`` / ``"json"``).
            When ``None`` the handler inherits the top-level
            :attr:`LogConfig.renderer`.
    """

    stream: Literal["stdout", "stderr"] = "stdout"
    renderer: str | None = None


class FileHandlerConfig(
    LoomFrozenStruct, frozen=True, tag="file", tag_field="type", forbid_unknown_fields=True
):
    """Configuration for a stdlib ``FileHandler``.

    Attributes:
        filename: Absolute or relative path to the log file.
        encoding: File encoding. Defaults to ``"utf-8"``.
        renderer: Per-handler output renderer. See :class:`StreamHandlerConfig`.
    """

    filename: str
    encoding: str = "utf-8"
    renderer: str | None = None


class RotatingFileHandlerConfig(
    LoomFrozenStruct,
    frozen=True,
    tag="rotating_file",
    tag_field="type",
    forbid_unknown_fields=True,
):
    """Configuration for a stdlib ``RotatingFileHandler``.

    Attributes:
        filename: Absolute or relative path to the log file.
        max_bytes: Maximum file size in bytes before rotation. Defaults to 10 MB.
        backup_count: Number of rotated backup files to retain.
        encoding: File encoding. Defaults to ``"utf-8"``.
        renderer: Per-handler output renderer. See :class:`StreamHandlerConfig`.
    """

    filename: str
    max_bytes: int = 10 * 1024 * 1024
    backup_count: int = 5
    encoding: str = "utf-8"
    renderer: str | None = None


HandlerConfig = StreamHandlerConfig | FileHandlerConfig | RotatingFileHandlerConfig


class LoggerConfig(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """YAML configuration struct for the ``logger:`` section.

    An unknown key is refused, a removed one (``environment``) included, so
    a stale setting fails loudly instead of being ignored.

    Use with :func:`~loom.core.config.loader.section` to parse the
    ``logger:`` block from an OmegaConf config, then pass directly to
    :func:`configure_logging_from_values`.

    Attributes:
        name: Logger name.  Empty string targets the root logger.
        renderer: Output renderer (``"json"`` or ``"console"``).
            ``None`` follows the environment the application resolved.
        colors: Enable ANSI colours.  ``None`` follows the environment.
        level: Minimum log level (``"DEBUG"``, ``"INFO"``, etc.).
        named_levels: Per-logger minimum levels applied after the main logger
            setup.  Useful for silencing noisy third-party libraries like
            ``celery`` or ``redis`` from YAML.
        handlers: Additional stdlib handler configurations.
        fields: Static key-value pairs bound globally at startup via
            ``structlog.contextvars``.  Every log entry will carry these
            fields automatically.  Useful for per-deployment metadata such as
            ``service``, ``version``, or ``region``.

    Example::

        cfg = section(raw, "logger", LoggerConfig)
        environment = resolve_environment()  # once, at startup
        configure_logging_from_values(
            name=cfg.name,
            environment=environment,
            renderer=cfg.renderer,
            colors=cfg.colors,
            level=cfg.level,
            named_levels=cfg.named_levels,
            handlers=cfg.handlers,
            fields=cfg.fields,
        )
    """

    name: str = ""
    renderer: str | None = None
    colors: bool | None = None
    level: str = "INFO"
    named_levels: dict[str, str] = msgspec.field(default_factory=dict)
    handlers: list[HandlerConfig] = msgspec.field(default_factory=list)
    fields: dict[str, str] = msgspec.field(default_factory=dict)


@dataclass(frozen=True)
class LogConfig:
    """Immutable logging configuration for the framework.

    Controls structlog processor chain, stdlib handler attachment, and rendering.

    The renderer and colour defaults come from the environment passed to
    :func:`configure_logging`, not from this configuration.

    Attributes:
        renderer: Output renderer. ``None`` follows the environment.
        colors: Enable ANSI colors. ``None`` follows the environment.
        level: Minimum log level name (e.g. ``"INFO"``, ``"DEBUG"``).
        name: stdlib logger name to attach handlers to. ``""`` targets the root logger.
        named_levels: Per-logger level overrides for specific stdlib logger names.
        handlers: stdlib handler configurations. Empty tuple falls back to ``basicConfig``.
        fields: Static key-value pairs bound globally at startup via
            ``structlog.contextvars``.  Every log entry will carry these fields.
        extra_processors: Additional structlog processors inserted before the final renderer.

    Example:
        >>> cfg = LogConfig(level="WARNING")
        >>> configure_logging(cfg, environment=Environment.DEVELOPMENT)
    """

    renderer: Renderer | None = None
    colors: bool | None = None
    level: str = "INFO"
    name: str = ""
    named_levels: tuple[tuple[str, str], ...] = ()
    handlers: tuple[HandlerConfig, ...] = ()
    fields: tuple[tuple[str, str], ...] = ()
    extra_processors: tuple[Any, ...] = ()


_RENDERER_FACTORIES: dict[Renderer, Callable[[bool], Any]] = {
    Renderer.CONSOLE: lambda colors: structlog.dev.ConsoleRenderer(colors=colors),
    Renderer.JSON: lambda _: structlog.processors.JSONRenderer(),
}


def _parse_level(level: str) -> int:
    value = logging.getLevelName(level.upper())
    if not isinstance(value, int):
        raise ValueError(f"Unsupported log level: {level!r}")
    return value


def _default_renderer(config: LogConfig, environment: Environment) -> Renderer:
    return config.renderer or (Renderer.JSON if environment.is_deployed else Renderer.CONSOLE)


def _default_colors(config: LogConfig, environment: Environment) -> bool:
    if config.colors is not None:
        return config.colors
    return not environment.is_deployed


def _handler_renderer(
    handler_cfg: HandlerConfig, config: LogConfig, environment: Environment
) -> Any:
    """Build the structlog renderer instance for a single handler.

    Honours the per-handler ``renderer`` field when set; otherwise falls
    back to the top-level :attr:`LogConfig.renderer` / environment default.
    """
    raw = getattr(handler_cfg, "renderer", None)
    renderer = Renderer.from_str(raw) if raw is not None else _default_renderer(config, environment)
    return _RENDERER_FACTORIES[renderer](_default_colors(config, environment))


def _build_stdlib_handler(cfg: HandlerConfig) -> logging.Handler:
    match cfg:
        case StreamHandlerConfig(stream="stderr"):
            return logging.StreamHandler(sys.stderr)
        case StreamHandlerConfig():
            return logging.StreamHandler(sys.stdout)
        case FileHandlerConfig():
            return logging.FileHandler(cfg.filename, encoding=cfg.encoding)
        case RotatingFileHandlerConfig():
            return logging.handlers.RotatingFileHandler(
                cfg.filename,
                maxBytes=cfg.max_bytes,
                backupCount=cfg.backup_count,
                encoding=cfg.encoding,
            )
        case _:
            raise TypeError(f"Unhandled handler config type: {type(cfg).__name__!r}")


def _setup_stdlib(config: LogConfig, level: int, environment: Environment) -> None:
    # Every stdlib record (loom-internal or third-party) is routed through
    # a structlog ProcessorFormatter so the whole process shares one render.
    target = logging.getLogger(config.name) if config.name else logging.getLogger()
    target.setLevel(level)
    while target.handlers:
        target.removeHandler(target.handlers[-1])

    # Handlers stay at NOTSET: stdlib loggers filter, so their named levels apply
    # as set. structlog's bound loggers still filter at the top-level level.
    for handler_cfg in config.handlers or (StreamHandlerConfig(),):
        handler = _build_stdlib_handler(handler_cfg)
        handler.setFormatter(
            structlog.stdlib.ProcessorFormatter(
                processor=_handler_renderer(handler_cfg, config, environment),
                foreign_pre_chain=_foreign_pre_chain(),
            )
        )
        target.addHandler(handler)


def _foreign_pre_chain() -> list[Any]:
    """Pre-chain applied to records that did NOT originate from structlog.

    Lets stdlib log records (e.g. third-party libraries) flow through the
    same processors so they render consistently with structlog events.
    """
    return [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.format_exc_info,
    ]


def _resolve_level(level_name: str, logger_name: str) -> int:
    try:
        return _parse_level(level_name)
    except ValueError as exc:
        raise ValueError(f"Invalid level {level_name!r} for logger {logger_name!r}") from exc


def _apply_named_levels(config: LogConfig) -> None:
    for logger_name, level_name in config.named_levels:
        logging.getLogger(logger_name).setLevel(_resolve_level(level_name, logger_name))


_DEFAULT_LOG_CONFIG = LogConfig()


def configure_logging_from_values(
    *,
    name: str = "",
    environment: Environment | str | None = None,
    renderer: str | None = None,
    colors: bool | None = None,
    level: str = "INFO",
    named_levels: Mapping[str, str] | None = None,
    handlers: Sequence[HandlerConfig] = (),
    fields: Mapping[str, str] | None = None,
    extra_processors: Sequence[Any] = (),
) -> None:
    """Configure logging from plain scalar values.

    Intended for bootstrap layers that parse config structs and want to avoid
    duplicating ``Environment``/``Renderer`` conversion logic.

    Args:
        name: stdlib logger name. ``""`` targets the root logger.
        environment: Environment of the process; drives the renderer and
            colour defaults. Resolved like
            :func:`~loom.core.environment.resolve_environment` when ``None``.
        renderer: Output renderer name (``"json"`` or ``"console"``).
        colors: Enable ANSI colours. ``None`` follows ``environment``.
        level: Minimum log level name.
        named_levels: Per-logger level overrides.
        handlers: stdlib handler configurations.
        fields: Static key-value pairs bound globally at startup. Every log
            entry will carry these fields automatically via ``structlog.contextvars``.
        extra_processors: Additional structlog processors inserted before the final renderer.
    """
    configure_logging(
        LogConfig(
            name=name,
            renderer=Renderer.from_str(renderer) if renderer is not None else None,
            colors=colors,
            level=level,
            named_levels=tuple((named_levels or {}).items()),
            handlers=tuple(handlers),
            fields=tuple((fields or {}).items()),
            extra_processors=tuple(extra_processors),
        ),
        environment=environment,
    )


def configure_logging(
    config: LogConfig = _DEFAULT_LOG_CONFIG,
    *,
    environment: Environment | str | None = None,
) -> None:
    """Configure structlog and stdlib logging from a ``LogConfig``.

    Sets up the global structlog processor chain and attaches stdlib handlers.
    Call once at application startup before any logger is used.

    Args:
        config: Logging configuration.
        environment: Environment of the process; drives the renderer and
            colour defaults. Resolved like
            :func:`~loom.core.environment.resolve_environment` (``LOOM_ENV``,
            else production) when ``None``.

    Raises:
        ValueError: If ``config.level`` is not a recognised log level name.

    Example:
        >>> configure_logging(LogConfig(level="WARNING"), environment=Environment.STAGING)
    """
    level = _parse_level(config.level)
    _setup_stdlib(config, level, resolve_environment(environment))
    _apply_named_levels(config)

    if config.fields:
        structlog.contextvars.bind_contextvars(**dict(config.fields))

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.format_exc_info,
            *config.extra_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
