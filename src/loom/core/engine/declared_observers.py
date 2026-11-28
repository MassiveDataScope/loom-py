"""The execution observers an application declares in ``observability.execution_observers``."""

from __future__ import annotations

from collections.abc import Sequence

from loom.core.config import ConfigContext
from loom.core.config.errors import ConfigError
from loom.core.config.keys import INTERPOLATION
from loom.core.config.loader import config_strings, convert_section
from loom.core.engine.execution_observer import ExecutionObserver
from loom.core.symbols import import_symbol

EXECUTION_OBSERVERS_KEY = "observability.execution_observers"


def resolve_execution_observer_class(ref: str) -> type[ExecutionObserver]:
    """Import the class a ``module:Class`` entry names and check it observes executions.

    Raises:
        ConfigError: When the entry is malformed, does not import, or names
            something that is not a class with an ``on_event`` method.
    """
    try:
        symbol = import_symbol(ref)
    except Exception as exc:
        raise ConfigError(
            f"{EXECUTION_OBSERVERS_KEY} entry {ref!r} cannot be imported: {exc}"
        ) from exc
    if not isinstance(symbol, type) or not callable(getattr(symbol, "on_event", None)):
        raise ConfigError(
            f"{EXECUTION_OBSERVERS_KEY} entry {ref!r} is not an execution observer: "
            "it must be a class with an on_event(event) method"
        )
    return symbol


def check_written_execution_observers(ctx: ConfigContext) -> None:
    """Resolve every declared entry as written, without building it, as ``loom check`` does.

    A list holding a placeholder is left to start-up, which resolves it.

    Raises:
        ConfigError: When the list is malformed, or names the first entry that
            does not resolve to an execution observer class.
    """
    written = ctx.raw(EXECUTION_OBSERVERS_KEY)
    if written is None or any(INTERPOLATION in text for _, text in config_strings(written)):
        return
    for ref in convert_section(written, EXECUTION_OBSERVERS_KEY, tuple[str, ...]):
        resolve_execution_observer_class(ref)


def build_execution_observers(refs: Sequence[str]) -> list[ExecutionObserver]:
    """Build each declared observer with no arguments, in declaration order.

    Raises:
        ConfigError: Naming the entry that does not resolve, or whose class
            cannot be built with no arguments.
    """
    observers: list[ExecutionObserver] = []
    for ref in refs:
        observer_class = resolve_execution_observer_class(ref)
        try:
            observers.append(observer_class())
        except Exception as exc:
            raise ConfigError(
                f"{EXECUTION_OBSERVERS_KEY} entry {ref!r} cannot be built with no arguments: {exc}"
            ) from exc
    return observers
