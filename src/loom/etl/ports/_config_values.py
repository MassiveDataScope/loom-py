"""Resolution of :class:`~loom.etl.FromConfig` values against a config context.

Shared by the compiler, which checks every declared key before a run, and the
executor, which resolves the values a step receives.  Every failure reports
the key and the expected type, never the value: a ``msgspec`` message may
quote the rejected value, so the error is raised outside the handler that
caught it and carries neither a cause nor a context.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

import msgspec

from loom.core.config import ConfigContext, ConfigError
from loom.etl.declarative.source._from_config import type_label


class ConfigValueFailure(StrEnum):
    """Why a config value could not be supplied."""

    MISSING = "is not set"
    UNRESOLVED = "cannot be resolved (check its interpolations and resolvers)"
    INVALID = "does not validate as"


class ConfigValueError(ConfigError):
    """A declared config value is missing, unresolvable or of the wrong type.

    The message names the key and the expected type only; neither the value
    nor the underlying exception is attached (``__cause__`` and
    ``__context__`` are ``None``).

    Attributes:
        key: Dot-separated config path.
        failure: Why the value could not be supplied.
    """

    def __init__(self, key: str, value_type: object, failure: ConfigValueFailure) -> None:
        reason = failure.value
        if failure is ConfigValueFailure.INVALID:
            reason = f"{reason} {type_label(value_type)!r}"
        super().__init__(f"Config key {key!r} {reason}")
        self.key = key
        self.failure = failure


def resolve_config_value(context: ConfigContext, key: str, value_type: object) -> Any:
    """Resolve *key* in *context* and convert it to *value_type*.

    Args:
        context: Config the runner was built from.
        key: Dot-separated config path.
        value_type: Target type for ``msgspec.convert``.

    Returns:
        The converted value.

    Raises:
        ConfigValueError: When the key is absent or null, its interpolation
            cannot be resolved, or the value does not validate.
    """
    value: Any = None
    failure = _presence_failure(context, key)
    if failure is None:
        value, failure = _read_and_convert(context, key, value_type)
    if failure is not None:
        raise ConfigValueError(key, value_type, failure)
    return value


def _read_and_convert(
    context: ConfigContext, key: str, value_type: object
) -> tuple[Any, ConfigValueFailure | None]:
    """Return ``(value, None)``, or ``(None, failure)`` without the exception.

    The caller raises outside any ``except`` block, so the exception that
    may quote the value is released here instead of becoming ``__context__``.
    """
    try:
        raw = context.section(key, object)
    except ConfigError:
        return None, ConfigValueFailure.UNRESOLVED
    try:
        return msgspec.convert(raw, value_type, strict=False), None
    except msgspec.ValidationError:
        return None, ConfigValueFailure.INVALID


def _presence_failure(context: ConfigContext, key: str) -> ConfigValueFailure | None:
    try:
        present = context.has(key)
    except ValueError:
        return ConfigValueFailure.UNRESOLVED
    return None if present else ConfigValueFailure.MISSING
