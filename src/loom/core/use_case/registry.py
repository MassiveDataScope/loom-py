"""Read-only registry for name-based use-case resolution."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from loom.core.engine.compilable import Compilable
from loom.core.errors import InvalidValue, UnknownKey
from loom.core.use_case.keys import get_use_case_key

_CAMEL_BOUNDARY = re.compile(r"(?<!^)(?=[A-Z])")
_ENTITY_KEY_CACHE: dict[type[Any], str] = {}


def model_entity_key(model: type[Any]) -> str:
    """Return canonical entity key from a model type.

    Result is cached — the mapping from class to key is deterministic and
    computed at most once per model type.

    Example:
        ``ProductReview -> "product_review"``
    """
    cached = _ENTITY_KEY_CACHE.get(model)
    if cached is not None:
        return cached
    key = _CAMEL_BOUNDARY.sub("_", model.__name__).lower()
    _ENTITY_KEY_CACHE[model] = key
    return key


@dataclass(frozen=True)
class UseCaseRegistry:
    """Read-only registry keyed by stable use-case names."""

    _by_name: dict[str, type[Compilable]]
    _by_type: dict[type[Compilable], str]

    @classmethod
    def build(cls, use_cases: list[type[Compilable]]) -> UseCaseRegistry:
        """Build a registry from compiled use cases.

        Raises:
            InvalidValue: If duplicate keys are detected.
        """
        by_name: dict[str, type[Compilable]] = {}
        by_type: dict[type[Compilable], str] = {}
        for use_case_type in use_cases:
            key = get_use_case_key(use_case_type)
            if key is None:
                continue
            existing = by_name.get(key)
            if existing is not None and existing is not use_case_type:
                raise InvalidValue(
                    "Duplicate use-case key: "
                    f"{existing.__qualname__} and {use_case_type.__qualname__}"
                )
            by_name[key] = use_case_type
            by_type[use_case_type] = key
        return cls(_by_name=by_name, _by_type=by_type)

    def resolve(self, key: str) -> type[Compilable]:
        """Return use-case type for ``key``.

        Raises:
            UnknownKey: If key is not registered.
        """
        use_case_type = self._by_name.get(key)
        if use_case_type is None:
            raise UnknownKey(
                "Use-case key is not registered. "
                "If a callback uses ApplicationInvoker with explicit jobs= bootstrap, "
                "pass the required UseCase classes via use_cases= in bootstrap_worker()."
            )
        return use_case_type

    def key_for(self, use_case_type: type[Compilable]) -> str | None:
        """Return key for class, if registered."""
        return self._by_type.get(use_case_type)

    def has(self, key: str) -> bool:
        """Return whether ``key`` exists in registry."""
        return key in self._by_name

    def keys(self) -> tuple[str, ...]:
        """Return registered keys in deterministic order."""
        return tuple(sorted(self._by_name))
