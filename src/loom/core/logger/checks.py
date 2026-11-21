"""Deploy checks of ``observability.log``, read as written.

``loom.logger.E001``: ``format`` names no registered format.
``loom.logger.E002``: ``level`` or a ``named_levels`` value is not a log level.
``loom.logger.E003``: ``user_identity`` is not ``hash``, ``id`` or ``none``.
``loom.logger.E004``: ``user_identity`` is ``hash`` and ``user_hash_key`` is missing.
``loom.logger.W001``: ``user_identity`` is ``none`` in production.

Neither resolves a placeholder; a value that holds one is not judged.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from loom.core.checks import CheckSet, Finding
from loom.core.config.keys import INTERPOLATION
from loom.core.environment import Environment, resolve_environment
from loom.core.logger.install import format_names
from loom.core.logger.settings import UserIdentityMode, is_log_level

if TYPE_CHECKING:
    from loom.core.config.context import ConfigContext

UNKNOWN_FORMAT_CHECK: Final = "loom.logger.E001"
"""Id of the check refusing a log format nobody registered."""

UNKNOWN_LEVEL_CHECK: Final = "loom.logger.E002"
"""Id of the check refusing a level the standard library does not know."""

UNKNOWN_USER_IDENTITY_CHECK: Final = "loom.logger.E003"
"""Id of the check refusing a ``user_identity`` that is not a mode."""

MISSING_USER_HASH_KEY_CHECK: Final = "loom.logger.E004"
"""Id of the check refusing the ``hash`` mode with no ``user_hash_key``."""

NO_USER_IDENTITY_CHECK: Final = "loom.logger.W001"
"""Id of the check warning that production writes no user on a security event."""

CHECKS: Final = CheckSet()
"""Checks of ``observability.log``, run when the process uses that path."""

_PATH = "observability.log"


def _literal(value: object) -> str | None:
    """*value* when it is a string with no placeholder in it."""
    return value if isinstance(value, str) and INTERPOLATION not in value else None


def _written(config: ConfigContext) -> dict[str, object]:
    log = config.raw(_PATH, {})
    return log if isinstance(log, dict) else {}


@CHECKS.register(UNKNOWN_FORMAT_CHECK)
def _registered_format(config: ConfigContext) -> Finding | None:
    name = _literal(_written(config).get("format"))
    known_names = format_names()
    if name is None or name in known_names:
        return None
    known = ", ".join(known_names)
    return Finding(
        message=f"{_PATH}.format is {name!r}, which no log format registers.",
        hint=f"use one of the registered formats ({known}) or install the package that adds it.",
    )


@CHECKS.register(UNKNOWN_LEVEL_CHECK)
def _known_levels(config: ConfigContext) -> Finding | None:
    log = _written(config)
    named = log.get("named_levels")
    declared = {f"{_PATH}.level": log.get("level")}
    if isinstance(named, dict):
        declared |= {f"{_PATH}.named_levels.{key}": value for key, value in named.items()}
    for key, value in declared.items():
        level = _literal(value)
        if level is not None and not is_log_level(level):
            return Finding(
                message=f"{key} is {level!r}, which is not a log level.",
                hint="use DEBUG, INFO, WARNING, ERROR or CRITICAL.",
            )
    return None


@CHECKS.register(UNKNOWN_USER_IDENTITY_CHECK)
def _known_user_identity(config: ConfigContext) -> Finding | None:
    mode = _literal(_written(config).get("user_identity"))
    if mode is None or mode in UserIdentityMode:
        return None
    modes = ", ".join(UserIdentityMode)
    return Finding(
        message=f"{_PATH}.user_identity is {mode!r}, which is not a user identity mode.",
        hint=f"use one of {modes}.",
    )


@CHECKS.register(MISSING_USER_HASH_KEY_CHECK)
def _hash_key_present(config: ConfigContext) -> Finding | None:
    log = _written(config)
    if log.get("user_identity") != UserIdentityMode.HASH or log.get("user_hash_key") is not None:
        return None
    return Finding(
        message=f"{_PATH}.user_identity is 'hash' but {_PATH}.user_hash_key is not set.",
        hint=f"set {_PATH}.user_hash_key from a secret, such as ${{oc.env:LOG_USER_HASH_KEY}}.",
    )


@CHECKS.register(NO_USER_IDENTITY_CHECK)
def _user_identity_in_production(config: ConfigContext) -> Finding | None:
    if resolve_environment() is not Environment.PRODUCTION:
        return None
    mode = _written(config).get("user_identity", UserIdentityMode.NONE)
    if mode != UserIdentityMode.NONE:
        return None
    return Finding(
        message=(
            f"{_PATH}.user_identity is 'none' in production: a security event, such as a "
            "denied access or a rejected token, says nothing about who it concerns. "
            "loom cannot tell here whether the application emits any."
        ),
        hint=f"set {_PATH}.user_identity to hash (with user_hash_key) or id, or silence the check.",
    )
