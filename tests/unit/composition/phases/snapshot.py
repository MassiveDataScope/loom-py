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
from collections import defaultdict
from collections.abc import Collection, Iterable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from types import MappingProxyType
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
    if os.environ.get(UPDATE_VARIABLE) == "1":
        _write(name, recorded)
        return
    assert _normalised(recorded) == _stored(name)


def matches_path_snapshots(
    name: str,
    recorded: Mapping[str, object],
    variants: Mapping[str, Collection[str]] = MappingProxyType({}),
) -> None:
    """Assert each path records its scenario's snapshot; rewrite them when updating.

    ``snapshots/<name>.json`` holds what every path records, but a path a
    variant names: it records ``snapshots/<name>.<variant>.json``. A mismatch
    names every path that differs from its snapshot.
    """
    by_snapshot: dict[str, dict[str, object]] = defaultdict(dict)
    for path, snapshot in _snapshots(name, recorded, variants).items():
        by_snapshot[snapshot][path] = _normalised(recorded[path])
    if os.environ.get(UPDATE_VARIABLE) == "1":
        for snapshot, values in by_snapshot.items():
            if len({json.dumps(value, sort_keys=True) for value in values.values()}) > 1:
                pytest.fail(f"{sorted(values)} record different results for {snapshot}")
            _write(snapshot, next(iter(values.values())))
        return
    mismatches = [
        f"{path} differs from {snapshot}.json: {value}"
        for snapshot, values in by_snapshot.items()
        for path, value in values.items()
        if value != _stored(snapshot)
    ]
    assert mismatches == [], "\n".join(mismatches)


def path_recordings(
    name: str,
    paths: Iterable[str],
    variants: Mapping[str, Collection[str]] = MappingProxyType({}),
) -> list[tuple[str, Any]]:
    """What each of *paths* records for the scenario *name*, read back from its snapshot."""
    return [
        (path, _stored(snapshot)) for path, snapshot in _snapshots(name, paths, variants).items()
    ]


def _snapshots(
    name: str, paths: Iterable[str], variants: Mapping[str, Collection[str]]
) -> dict[str, str]:
    """The snapshot each of *paths* records: its variant's, else the scenario's."""
    variant_of = {path: variant for variant, members in variants.items() for path in members}
    return {path: f"{name}.{variant_of[path]}" if path in variant_of else name for path in paths}


def _text(recorded: object) -> str:
    return json.dumps(recorded, indent=2, sort_keys=True) + "\n"


def _normalised(recorded: object) -> object:
    """*recorded* as it reads back from its snapshot file."""
    return json.loads(_text(recorded))


def _write(name: str, recorded: object) -> None:
    path = SNAPSHOTS / f"{name}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_text(recorded), encoding="utf-8")


def _stored(name: str) -> object:
    path = SNAPSHOTS / f"{name}.json"
    if not path.exists():
        pytest.fail(f"no snapshot {path.name}; record it with {UPDATE_VARIABLE}=1")
    return json.loads(path.read_text(encoding="utf-8"))


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
    "matches_path_snapshots",
    "matches_snapshot",
    "path_recordings",
    "recorded_warnings",
]
