"""The deployment environment: one selector, resolved once at startup.

``LOOM_ENV`` names the environment, or an explicit argument does and wins over
it. With neither, the environment is :attr:`Environment.PRODUCTION`: a
container started without the variable runs with the secure defaults, and a
developer who forgot it locally meets the deploy checks, whose failure says to
set ``LOOM_ENV=development``. An unknown value is a startup error, never a
silent fallback.

This is the only loom module that reads the process environment for loom:
the selector and the ``LOOM__SECTION__KEY`` configuration overrides (see
:mod:`loom.core.config.overrides`). Every other module receives the resolved
:class:`Environment` through configuration or the container; the entry
points resolve it once and pass it on.

The environment also picks the configuration overlay of a config directory:
``base.yaml`` first, then ``<environment>.yaml`` when it exists.
"""

from __future__ import annotations

import os
from collections.abc import Sequence
from enum import StrEnum
from pathlib import Path
from typing import Final

from loom.core.config.errors import ConfigError

ENVIRONMENT_VARIABLE: Final = "LOOM_ENV"
"""Name of the variable selecting the environment."""

OVERRIDE_PREFIX: Final = "LOOM__"
"""Prefix of the variables overriding configuration keys."""

BASE_CONFIG_FILE: Final = "base.yaml"
"""File of a config directory every environment loads first."""


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
    base = directory / BASE_CONFIG_FILE
    if not base.is_file():
        raise ConfigError(f"config directory {str(directory)!r} has no {BASE_CONFIG_FILE}")
    overlay = directory / f"{environment.value}.yaml"
    return (str(base), str(overlay)) if overlay.is_file() else (str(base),)


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
