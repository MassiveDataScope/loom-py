"""Internal helpers for discovery engine implementations."""

from __future__ import annotations

import importlib
import inspect
import sys
import typing
from dataclasses import dataclass, field
from types import ModuleType
from typing import Any

from loom.core.job.job import Job
from loom.core.model import BaseModel
from loom.core.use_case.use_case import UseCase

if typing.TYPE_CHECKING:
    from loom.rest.model import RestInterface

ItemT = typing.TypeVar("ItemT")
_LocalClassResolver = typing.Callable[[type[Any]], type[Any] | None]
_REST_MODEL = "loom.rest.model"


@dataclass
class _DiscoveryCollector:
    models: list[type[BaseModel]] = field(default_factory=list)
    use_cases: list[type[UseCase[object, object]]] = field(default_factory=list)
    interfaces: list[type[RestInterface[object]]] = field(default_factory=list)
    jobs: list[type[Job[Any]]] = field(default_factory=list)

    _seen_models: set[type[BaseModel]] = field(default_factory=set)
    _seen_use_cases: set[type[UseCase[object, object]]] = field(default_factory=set)
    _seen_interfaces: set[type[RestInterface[object]]] = field(default_factory=set)
    _seen_jobs: set[type[Job[Any]]] = field(default_factory=set)

    def append_model(self, value: type[Any]) -> None:
        _append_unique(self.models, self._seen_models, typing.cast(type[BaseModel], value))

    def append_use_case(self, value: type[Any]) -> None:
        _append_unique(
            self.use_cases,
            self._seen_use_cases,
            typing.cast(type[UseCase[object, object]], value),
        )

    def append_interface(self, value: type[Any]) -> None:
        _append_unique(
            self.interfaces,
            self._seen_interfaces,
            typing.cast("type[RestInterface[object]]", value),
        )

    def append_job(self, value: type[Any]) -> None:
        _append_unique(self.jobs, self._seen_jobs, typing.cast(type[Job[Any]], value))


def import_modules(module_paths: list[str]) -> list[ModuleType]:
    return [importlib.import_module(path) for path in module_paths]


def _append_unique(items: list[ItemT], seen: set[ItemT], value: ItemT) -> None:
    if value in seen:
        return
    items.append(value)
    seen.add(value)


def _is_local_class(cls: type[Any], module_name: str) -> bool:
    return cls.__module__ == module_name


def _as_model(cls: type[Any]) -> type[BaseModel] | None:
    if issubclass(cls, BaseModel) and cls is not BaseModel:
        return cls
    return None


def _as_use_case(cls: type[Any]) -> type[UseCase[object, object]] | None:
    if issubclass(cls, UseCase) and cls is not UseCase:
        return typing.cast(type[UseCase[object, object]], cls)
    return None


def _loaded_rest_interface() -> type[Any] | None:
    rest_model = sys.modules.get(_REST_MODEL)
    if rest_model is None:
        return None
    return typing.cast(type[Any], rest_model.RestInterface)


def _as_interface(cls: type[Any]) -> type[RestInterface[object]] | None:
    interface = _loaded_rest_interface()
    if interface is not None and issubclass(cls, interface) and cls is not interface:
        return typing.cast("type[RestInterface[object]]", cls)
    return None


def _as_job(cls: type[Any]) -> type[Job[Any]] | None:
    if issubclass(cls, Job) and cls is not Job:
        return cls
    return None


def _append_discovered_class(
    cls: type[Any],
    collector: _DiscoveryCollector,
) -> None:
    appenders: tuple[
        tuple[_LocalClassResolver, typing.Callable[[type[Any]], None]],
        ...,
    ] = (
        (_as_model, collector.append_model),
        (_as_use_case, collector.append_use_case),
        (_as_interface, collector.append_interface),
        (_as_job, collector.append_job),
    )
    for resolver, append in appenders:
        discovered = resolver(cls)
        if discovered is None:
            continue
        append(discovered)
        return


def collect_from_modules(
    modules: list[ModuleType],
) -> tuple[
    list[type[BaseModel]],
    list[type[UseCase[object, object]]],
    list[type[RestInterface[object]]],
    list[type[Job[Any]]],
]:
    collector = _DiscoveryCollector()

    for module in modules:
        module_name = module.__name__
        for _, cls in inspect.getmembers(module, inspect.isclass):
            if not _is_local_class(cls, module_name):
                continue
            _append_discovered_class(cls, collector)

    return collector.models, collector.use_cases, collector.interfaces, collector.jobs


def collect_use_cases_from_interfaces(
    interfaces: list[type[RestInterface[object]]],
) -> list[type[UseCase[object, object]]]:
    """Extract unique UseCases referenced in interface routes.

    Required because auto-generated UseCase classes live in
    ``loom.rest.autocrud``, not in user modules, so
    :func:`collect_from_modules` cannot discover them via module scanning.

    Args:
        interfaces: List of concrete ``RestInterface`` subclasses to inspect.

    Returns:
        Ordered list of unique UseCase classes found across all routes,
        preserving first-seen order.
    """
    result: list[type[UseCase[object, object]]] = []
    seen: set[type[UseCase[object, object]]] = set()
    for iface in interfaces:
        for route in iface.routes:
            uc = typing.cast(type[UseCase[object, object]], route.use_case)
            _append_unique(result, seen, uc)
    return result


def infer_model_from_use_case(
    use_case_type: type[UseCase[object, object]],
) -> type[BaseModel] | None:
    for base in getattr(use_case_type, "__orig_bases__", ()):
        if typing.get_origin(base) is not UseCase:
            continue
        args = typing.get_args(base)
        if len(args) < 2:
            continue
        candidate = args[0]
        if candidate is Any:
            return None
        if isinstance(candidate, type) and issubclass(candidate, BaseModel):
            return candidate
        return None
    return None
