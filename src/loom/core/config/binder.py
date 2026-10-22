"""Strategy for constructor injection from a resolved config mapping."""

from __future__ import annotations

import inspect
import sys
from collections.abc import Mapping
from typing import Any, TypeVar, cast, get_type_hints

import msgspec

from loom.core.config.errors import ConfigError

T = TypeVar("T")

_SKIP_KINDS = frozenset({inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD})


class StructBinder:
    """Strategy: constructor injection from a resolved config mapping.

    Converts every annotated ``__init__`` parameter present in *raw* via
    ``convert_config``.  Supports primitives (``int``, ``str``, ``bool``),
    ``Literal`` constraints, and ``LoomFrozenStruct`` subclasses uniformly.
    All reflection runs once per ``bind`` call at compile time — never on
    the message-processing hot path.

    Args:
        strict: When ``True``, ``msgspec.convert`` uses strict mode
            (no implicit coercion from string to int, etc.).

    Example::

        binder = StructBinder()
        step = binder.bind(
            ReadOrdersStep,
            {"db": {"host": "localhost", "port": 5432}, "mode": "batch"},
        )
    """

    def __init__(self, strict: bool = False) -> None:
        self._strict = strict

    def bind(self, target: type[T], raw: Mapping[str, object]) -> T:
        """Instantiate *target* injecting and converting values from *raw*.

        For each annotated ``__init__`` parameter that appears in *raw*,
        the value is converted to the declared type via ``msgspec.convert``.
        Parameters absent from *raw* use their default; required parameters
        absent from *raw* raise ``ConfigError``.

        Args:
            target: Class to instantiate.
            raw: Flat mapping of parameter names to raw values.

        Returns:
            A fully constructed instance of *target*.

        Raises:
            ConfigError: If a required parameter is absent from *raw* or
                a value fails type conversion.
        """
        kwargs: dict[str, object] = dict(raw)
        hints = _resolve_hints(target)
        sig = inspect.signature(target.__init__)
        for name, param in sig.parameters.items():
            if name == "self" or param.kind in _SKIP_KINDS:
                continue
            annotation = hints.get(name)
            if annotation is None:
                continue
            if name not in kwargs:
                _check_required(target, name, param)
                continue
            kwargs[name] = _convert(kwargs[name], annotation, name, target, self._strict)
        return target(**kwargs)


def _check_required(target: type, name: str, param: inspect.Parameter) -> None:
    if param.default is inspect.Parameter.empty:
        raise ConfigError(f"{target.__name__} requires config field {name!r}")


def _convert(
    value: object,
    annotation: object,
    name: str,
    target: type,
    strict: bool,
) -> object:
    try:
        from loom.core.config.convert import convert_config

        return convert_config(value, cast("type[Any]", annotation), strict=strict)
    except msgspec.ValidationError as exc:
        raise ConfigError(f"{target.__name__}.{name}: {exc}") from exc


def _resolve_hints(target: type) -> dict[str, object]:
    module = sys.modules.get(target.__module__)
    globalns = vars(module) if module is not None else {}
    return get_type_hints(target.__init__, globalns=globalns)  # type: ignore[misc]
