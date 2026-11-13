"""Recording of a container, a start-up refusal and warnings, as stable JSON snapshots.

A snapshot holds qualnames, scope names and messages: never an id or an
address. ``LOOM_UPDATE_SNAPSHOTS=1`` rewrites the recorded file from the
current code instead of comparing against it.
"""

from __future__ import annotations

import json
import os
import re
import warnings
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from loom.core.bootstrap import BootstrapError
from loom.core.di.container import BindingKey, LoomContainer
from loom.core.di.scope import Scope

UPDATE_VARIABLE = "LOOM_UPDATE_SNAPSHOTS"
SNAPSHOTS = Path(__file__).parent / "snapshots"
_ADDRESS = re.compile(r" at 0x[0-9a-fA-F]+")


def describe_key(key: object) -> str:
    """A binding key by qualname; a tuple key by its parts."""
    if isinstance(key, type):
        return key.__qualname__
    if isinstance(key, tuple):
        return "(" + ", ".join(describe_key(part) for part in key) + ")"
    return _ADDRESS.sub("", repr(key))


def application_singletons(container: LoomContainer) -> dict[BindingKey, object]:
    """Each ``APPLICATION`` key with the instance the container serves for it.

    Resolves each one, as ``validate()`` does: bindings registered after the
    kernel's ``validate()`` are instantiated here.
    """
    bindings = container._bindings  # noqa: SLF001 - the scope is not public yet
    return {
        key: container.resolve(key)
        for key, binding in bindings.items()
        if binding.scope is Scope.APPLICATION
    }


def describe_container(container: LoomContainer) -> dict[str, Any]:
    """Every bound key with its scope and provider; an ``APPLICATION`` one with its type."""
    singletons = application_singletons(container)
    bindings: dict[str, Any] = {}
    for key, binding in container._bindings.items():  # noqa: SLF001
        record: dict[str, Any] = {
            "scope": binding.scope.name,
            "provider": binding.provider.__qualname__,
        }
        if key in singletons:
            record["resolved"] = type(singletons[key]).__qualname__
        bindings[describe_key(key)] = record
    repositories = {
        describe_key(model): describe_key(key)
        for model, key in container._repo_bindings.items()  # noqa: SLF001
    }
    return {"bindings": bindings, "repo_bindings": repositories}


def describe_refusal(error: BaseException, *, tmp_path: Path) -> dict[str, Any]:
    """The type and message the caller sees, whether it is wrapped, and the wrapped cause."""
    cause = error.__cause__
    return {
        "type": _qualified(error),
        "message": _stable(str(error), tmp_path),
        "wrapped": type(error) is BootstrapError,
        "cause": None
        if cause is None
        else {"type": _qualified(cause), "message": _stable(str(cause), tmp_path)},
    }


def describe_warnings(caught: list[warnings.WarningMessage]) -> list[dict[str, str]]:
    """Each warning's category and message, in the order emitted."""
    return [
        {"category": warning.category.__qualname__, "message": str(warning.message)}
        for warning in caught
    ]


@contextmanager
def recorded_warnings() -> Iterator[list[warnings.WarningMessage]]:
    """Every warning emitted inside, none filtered out or deduplicated."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        yield caught


def matches_snapshot(name: str, recorded: object) -> None:
    """Assert *recorded* equals ``snapshots/<name>.json``; rewrite it when updating."""
    path = SNAPSHOTS / f"{name}.json"
    text = json.dumps(recorded, indent=2, sort_keys=True) + "\n"
    if os.environ.get(UPDATE_VARIABLE) == "1":
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return
    if not path.exists():
        pytest.fail(f"no snapshot {path.name}; record it with {UPDATE_VARIABLE}=1")
    assert json.loads(text) == json.loads(path.read_text(encoding="utf-8"))


def _qualified(error: BaseException) -> str:
    return f"{type(error).__module__}.{type(error).__qualname__}"


def _stable(message: str, tmp_path: Path) -> str:
    return _ADDRESS.sub("", message.replace(str(tmp_path), "<tmp>"))


__all__ = [
    "SNAPSHOTS",
    "UPDATE_VARIABLE",
    "application_singletons",
    "describe_container",
    "describe_key",
    "describe_refusal",
    "describe_warnings",
    "matches_snapshot",
    "recorded_warnings",
]
