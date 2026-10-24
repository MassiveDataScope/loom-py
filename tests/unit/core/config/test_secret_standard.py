"""The configuration standard: secret names and addresses use the redacted types.

The walk covers every decodable struct. A failure names the type and the field.
``brokers`` and a field that only stores a path or a variable name stay plain
text. ``headers_ref`` does not: the resolver has already filled it.
"""

from __future__ import annotations

import importlib
import pkgutil
import types
from collections.abc import Mapping
from typing import Any, Union, get_args, get_origin

import msgspec

import loom
from loom.core.model.redaction import Dsn, RedactedMap, Secret, is_secret_name

_ADDRESS = frozenset(
    {
        "url",
        "uri",
        "endpoint",
        "broker_url",
        "result_backend",
        "base_url",
        "pushgateway_url",
        "workspace",
        "root",
    }
)
_PLAIN_ADDRESS = frozenset({"brokers", "topic_arn"})
_FREE_DICT = frozenset(
    {
        "storage_options",
        "connect_args",
        "extra",
        "aiocache_config",
        "exporter_kwargs",
        "span_processor_kwargs",
        "watch_options",
        "headers",
        "auth",
        "env",
        "settings",
    }
)
_EXTRA_MODULES = (
    "loom.ai.inference",
    "loom.core.persistence.database",
    "loom.core.repository.abc.cursor",
    "loom.streaming.nodes._table.common",
)
_UNION = (Union, types.UnionType)


def _modules() -> list[str]:
    names = set(_EXTRA_MODULES)
    for module in pkgutil.walk_packages(loom.__path__, "loom."):
        parts = module.name.split(".")
        if "config" in parts or module.name.endswith("._config"):
            names.add(module.name)
    return sorted(names)


def _structs() -> list[type[msgspec.Struct]]:
    found: list[type[msgspec.Struct]] = []
    for name in _modules():
        try:
            module = importlib.import_module(name)
        except ImportError:
            continue
        for value in vars(module).values():
            if (
                isinstance(value, type)
                and issubclass(value, msgspec.Struct)
                and value.__module__ == module.__name__
            ):
                found.append(value)
    return found


def _options(annotation: Any) -> tuple[Any, ...]:
    if get_origin(annotation) in _UNION:
        return tuple(arg for arg in get_args(annotation) if arg is not type(None))
    return (annotation,)


def _holds(annotation: Any, kind: type[Any]) -> bool:
    for option in _options(annotation):
        origin = get_origin(option) or option
        if kind is Secret and origin is Secret:
            return True
        if option is kind:
            return True
    return False


def _is_mapping(annotation: Any) -> bool:
    for option in _options(annotation):
        origin = get_origin(option) or option
        if origin in (dict, Mapping):
            return True
    return False


def _plain_name(name: str, annotation: Any) -> bool:
    if name in _PLAIN_ADDRESS:
        return True
    if name.endswith("_path"):
        return True
    if name.endswith("_ref") and name != "headers_ref":
        return True
    return name == "env" and not _is_mapping(annotation)


def _problems() -> list[str]:
    problems: list[str] = []
    for struct in _structs():
        label = f"{struct.__module__}.{struct.__name__}"
        for field in msgspec.structs.fields(struct):
            name = field.name
            annotation = field.type
            where = f"{label}.{name}"
            if name == "headers_ref" and not _holds(annotation, Secret):
                problems.append(f"{where} must be Secret")
            if name in _ADDRESS and name not in _PLAIN_ADDRESS and not _holds(annotation, Dsn):
                problems.append(f"{where} must be Dsn")
            if _plain_name(name, annotation):
                continue
            if is_secret_name(name) and not _holds(annotation, Secret):
                if _is_mapping(annotation) and _holds(annotation, RedactedMap):
                    continue
                problems.append(f"{where} must be Secret")
            if (
                name in _FREE_DICT
                and _is_mapping(annotation)
                and not _holds(annotation, RedactedMap)
            ):
                problems.append(f"{where} must be RedactedMap")
    return problems


def test_decodable_config_uses_the_redacted_types() -> None:
    assert _problems() == []
