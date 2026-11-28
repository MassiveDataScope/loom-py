"""Use-case key helpers for name-based invocation."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any, TypeVar

from loom.core.errors import InvalidValue

_UC = TypeVar("_UC")

_USE_CASE_KEY_ATTR = "__loom_use_case_key__"
_VALID_KEY = re.compile(r"^[a-z0-9_.:-]+$")


def set_use_case_key(use_case_type: type[Any], key: str) -> None:
    """Attach a stable invocation key to a use-case class.

    Args:
        use_case_type: Class receiving the key metadata.
        key: Stable invocation key.

    Raises:
        InvalidValue: If key is empty or contains invalid characters.
    """
    normalized = key.strip()
    if not normalized:
        raise InvalidValue("Use-case key cannot be empty.")
    if _VALID_KEY.fullmatch(normalized) is None:
        raise InvalidValue("Use-case key must match ^[a-z0-9_.:-]+$.")
    setattr(use_case_type, _USE_CASE_KEY_ATTR, normalized)


def get_use_case_key(use_case_type: type[Any]) -> str | None:
    """Return the explicit key attached to a use-case class, if any."""
    value = getattr(use_case_type, _USE_CASE_KEY_ATTR, None)
    if isinstance(value, str) and value:
        return value
    return None


def use_case_key(key: str) -> Callable[[type[_UC]], type[_UC]]:
    """Decorator that binds an invocation key to a use-case class.

    Args:
        key: Stable invocation key.

    Returns:
        Class decorator preserving the input type.
    """

    def _decorator(use_case_type: type[_UC]) -> type[_UC]:
        set_use_case_key(use_case_type, key)
        return use_case_type

    return _decorator
