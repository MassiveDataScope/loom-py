"""The deployment environment: one selector, resolved once at startup.

``LOOM_ENV`` names the environment, or an explicit argument does and wins over
it. With neither, the environment is :attr:`Environment.PRODUCTION`: a
container started without the variable runs with the secure defaults, and a
developer who forgot it locally meets the deploy checks, whose failure says to
set ``LOOM_ENV=development``. An unknown value is a startup error, never a
silent fallback.

This is the only loom module that names or reads loom's process variables:
the selector, the ``LOOM__SECTION__KEY`` configuration overrides (see
:mod:`loom.core.config.overrides`) and every other ``LOOM_*`` variable, which
the rest of loom reads through :func:`loom_variable`. A containment test
enforces it. Every other module receives the resolved :class:`Environment`
through configuration or the container; the entry points resolve it once
and pass it on.

The environment also picks the configuration overlay of a config directory:
``base.yaml`` first, then ``<environment>.yaml`` when it exists.
"""

from __future__ import annotations

import os
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from enum import StrEnum
from pathlib import Path
from typing import Final

from loom.core.config.errors import ConfigError

ENVIRONMENT_VARIABLE: Final = "LOOM_ENV"
"""Name of the variable selecting the environment."""

OVERRIDE_PREFIX: Final = "LOOM__"
"""Prefix of the variables overriding configuration keys."""

CONFIG_VARIABLE: Final = "LOOM_CONFIG"
"""Configuration file or directory the migration locator loads."""

STORAGE_CONFIG_VARIABLE: Final = "LOOM_STORAGE_CONFIG_PATH"
"""Storage configuration file or directory a Prefect flow run loads."""

ETL_CONFIG_VARIABLE: Final = "LOOM_ETL_CONFIG"
"""ETL declaration file a deployed YAML-declared flow is rebuilt from."""

SPOOL_DIR_VARIABLE: Final = "LOOM_SPOOL_DIR"
"""Directory the Polars streaming writer spools to."""

SPARK_IVY_DIR_VARIABLE: Final = "LOOM_SPARK_IVY_DIR"
"""Ivy cache directory of the Spark test session."""

SPARK_OFFLINE_VARIABLE: Final = "LOOM_SPARK_OFFLINE"
"""``1`` makes the Spark test session use only cached jars."""

_BASE_CONFIG_FILE: Final = "base.yaml"


class Environment(StrEnum):
    """The environments a loom application runs in."""

    PRODUCTION = "production"
    STAGING = "staging"
    DEVELOPMENT = "development"
    TEST = "test"

    @property
    def is_deployed(self) -> bool:
        """Whether this environment serves real traffic and runs the deploy checks."""
        return self in (Environment.PRODUCTION, Environment.STAGING)


_VALID = ", ".join(member.value for member in Environment)


def resolve_environment(explicit: Environment | str | None = None) -> Environment:
    """Resolve the environment from *explicit*, else ``LOOM_ENV``, else production.

    Call it once, at startup, and hand the result on.

    Args:
        explicit: Environment chosen by the caller; overrides ``LOOM_ENV``.

    Returns:
        The selected :class:`Environment`.

    Raises:
        ConfigError: When the selected value is not an :class:`Environment`,
            including an empty ``LOOM_ENV``.
    """
    if isinstance(explicit, Environment):
        return explicit
    if explicit is not None:
        return parse_environment(explicit, source="the environment argument")
    raw = os.environ.get(ENVIRONMENT_VARIABLE)
    if raw is None:
        return Environment.PRODUCTION
    return parse_environment(raw, source=ENVIRONMENT_VARIABLE)


def parse_environment(raw: str, *, source: str) -> Environment:
    """Parse *raw*, read from *source*, into an :class:`Environment`.

    For values that name an environment without selecting the running one,
    such as the keys of a per-environment configuration block.

    Args:
        raw: Environment name; surrounding blanks and case are ignored.
        source: Where *raw* comes from, named in the error.

    Returns:
        The matching :class:`Environment`.

    Raises:
        ConfigError: When *raw* names no environment.
    """
    try:
        return Environment(raw.strip().lower())
    except ValueError:
        raise ConfigError(
            f"unknown environment {raw!r} from {source}. Valid values: {_VALID}."
        ) from None


def confirm_environment(declared: str | None, *, source: str) -> Environment:
    """Resolve the process environment and check that *declared* agrees with it.

    For a value that states which environment something was meant for, such
    as the ``env`` parameter of a Prefect run: it confirms the environment the
    process resolved (``LOOM_ENV``, else production) and never selects it.

    Args:
        declared: Environment *source* states, or ``None`` to state none.
        source: What declared it, named in the error.

    Returns:
        The process environment.

    Raises:
        ConfigError: When *declared* names no environment, or another one
            than the process environment.
    """
    environment = resolve_environment()
    if declared is None:
        return environment
    stated = parse_environment(declared, source=source)
    if stated is not environment:
        raise ConfigError(
            f"{source} is for the {str(stated)!r} environment, but this process runs in "
            f"{str(environment)!r} ({ENVIRONMENT_VARIABLE}, else production); run it "
            f"where {ENVIRONMENT_VARIABLE}={stated.value}."
        )
    return environment


def loom_variable(name: str) -> str | None:
    """Return the value of the ``LOOM_*`` variable *name*, or ``None`` when unset.

    Raises:
        ValueError: When *name* is not a ``LOOM_*`` variable.
    """
    _require_loom_variable(name)
    return os.environ.get(name)


@contextmanager
def scoped_loom_variable(name: str, value: str) -> Iterator[None]:
    """Set the ``LOOM_*`` variable *name* to *value* inside the block, then restore it.

    Not thread-safe: the process environment is shared.

    Raises:
        ValueError: When *name* is not a ``LOOM_*`` variable.
    """
    _require_loom_variable(name)
    previous = os.environ.get(name)
    os.environ[name] = value
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = previous


def _require_loom_variable(name: str) -> None:
    if not name.startswith("LOOM_"):
        raise ValueError(f"{name!r} is not a loom variable")


def override_variables() -> dict[str, str]:
    """Return the ``LOOM__*`` variables of the process, by name.

    Returns:
        Each variable whose name starts with ``LOOM__``, with its value.
    """
    return {name: value for name, value in os.environ.items() if name.startswith(OVERRIDE_PREFIX)}


def environment_config_paths(
    config_dir: str | os.PathLike[str], environment: Environment
) -> tuple[str, ...]:
    """Return ``base.yaml`` of *config_dir*, then the overlay of *environment* if present.

    Later files override earlier ones, so the overlay wins over the base.

    Args:
        config_dir: Local directory holding ``base.yaml`` and the overlays.
        environment: Environment whose ``<environment>.yaml`` is the overlay.

    Returns:
        The paths to load, in merge order.

    Raises:
        ConfigError: When *config_dir* has no ``base.yaml``.
    """
    directory = Path(config_dir)
    base = directory / _BASE_CONFIG_FILE
    if not base.is_file():
        raise ConfigError(f"config directory {str(directory)!r} has no {_BASE_CONFIG_FILE}")
    overlay = directory / f"{environment.value}.yaml"
    return (str(base), str(overlay)) if overlay.is_file() else (str(base),)


def config_source_paths(
    source: str | os.PathLike[str], environment: Environment
) -> tuple[str, ...]:
    """Return the files a configuration *source* stands for.

    A local directory loads through :func:`environment_config_paths` with
    *environment*; a file or a cloud URI loads alone.

    Raises:
        ConfigError: When a directory has no ``base.yaml``.
    """
    if Path(source).is_dir():
        return environment_config_paths(source, environment)
    return (str(source),)


def select_config_paths(
    config_paths: Sequence[str],
    config_dir: str | os.PathLike[str] | None,
    environment: Environment,
    *,
    caller: str,
) -> tuple[str, ...]:
    """Return the files an application factory loads: explicit files or a directory.

    Args:
        config_paths: Explicit files, merged left to right as given.
        config_dir: Directory loaded through :func:`environment_config_paths`.
        environment: Resolved environment picking the overlay of *config_dir*.
        caller: Name of the factory, used in the error message.

    Returns:
        The paths to load, in merge order.

    Raises:
        ConfigError: When both or neither of *config_paths* and *config_dir*
            are given, or *config_dir* has no ``base.yaml``.
    """
    if config_paths and config_dir is not None:
        raise ConfigError(f"{caller} takes config files or config_dir, not both.")
    if config_dir is not None:
        return environment_config_paths(config_dir, environment)
    if not config_paths:
        raise ConfigError(f"{caller} requires config files or a config_dir.")
    return tuple(config_paths)


__all__ = [
    "CONFIG_VARIABLE",
    "ENVIRONMENT_VARIABLE",
    "ETL_CONFIG_VARIABLE",
    "OVERRIDE_PREFIX",
    "SPARK_IVY_DIR_VARIABLE",
    "SPARK_OFFLINE_VARIABLE",
    "SPOOL_DIR_VARIABLE",
    "STORAGE_CONFIG_VARIABLE",
    "Environment",
    "config_source_paths",
    "confirm_environment",
    "environment_config_paths",
    "loom_variable",
    "override_variables",
    "parse_environment",
    "resolve_environment",
    "scoped_loom_variable",
    "select_config_paths",
]
