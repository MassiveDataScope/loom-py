from __future__ import annotations

import inspect
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from graphlib import TopologicalSorter
from typing import Any, Protocol

from loom.core.model.projection import (
    PROJECTION_DEFAULT_MISSING,
    Projection,
)

PROJECTION_DEP_PREFIX = "projection:"


class BackendProjectionLoader(Protocol):
    async def load_many(
        self,
        backend_context: Any,
        parent_ids: Sequence[object],
    ) -> Mapping[object, Any]: ...


class MemoryProjectionLoader(Protocol):
    def load_from_object(
        self,
        obj: Any,
        context: Mapping[str, Any] | None = None,
    ) -> Any: ...


@dataclass(frozen=True, slots=True)
class ProjectionStep:
    name: str
    projection: Projection
    prefer_memory: bool
    loader: Any


@dataclass(frozen=True, slots=True)
class ProjectionPlan:
    """Compiled projection execution plan grouped by dependency levels."""

    levels: tuple[tuple[ProjectionStep, ...], ...]


@dataclass(frozen=True, slots=True)
class _ExecutionBuckets:
    backend: tuple[ProjectionStep, ...]
    memory: tuple[ProjectionStep, ...]


def _projection_dependencies(
    name: str,
    projection: Projection,
    known_names: frozenset[str],
) -> frozenset[str]:
    deps: set[str] = set()
    for raw_dep in projection.depends_on:
        dep_name = raw_dep
        if raw_dep.startswith(PROJECTION_DEP_PREFIX):
            dep_name = raw_dep[len(PROJECTION_DEP_PREFIX) :]
        if dep_name in known_names and dep_name != name:
            deps.add(dep_name)
    return frozenset(deps)


def build_projection_plan(projections: Mapping[str, Projection]) -> ProjectionPlan:
    """Compile a deterministic projection plan from projection metadata.

    Routing (memory vs backend) is auto-detected from loader capabilities:
    loaders with ``load_from_object`` use the memory path; those with only
    ``load_many`` use the backend (SQL) path.

    Args:
        projections: Mapping of field name to :class:`~loom.core.model.projection.Projection`.

    Returns:
        :class:`ProjectionPlan` with topologically ordered levels.

    Raises:
        ValueError: If a dependency cycle is detected.
        TypeError: If a loader exposes neither ``load_from_object`` nor ``load_many``.
    """
    if not projections:
        return ProjectionPlan(levels=())

    names = frozenset(projections)
    sorter: TopologicalSorter[str] = TopologicalSorter()
    for name, projection in projections.items():
        sorter.add(name, *_projection_dependencies(name, projection, names))

    sorter.prepare()
    levels: list[tuple[ProjectionStep, ...]] = []
    while sorter.is_active():
        ready = tuple(sorter.get_ready())
        if not ready:
            raise ValueError("Projection dependency cycle detected")
        level = tuple(
            _build_projection_step(name=name, projection=projections[name]) for name in ready
        )
        levels.append(level)
        sorter.done(*ready)

    return ProjectionPlan(levels=tuple(levels))


def build_projection_plan_from_steps(steps: Mapping[str, ProjectionStep]) -> ProjectionPlan:
    """Build a projection plan from pre-resolved steps.

    Used by the SA compiler which has already determined loader strategy
    (memory vs SQL) for each projection at compile time.

    Args:
        steps: Pre-built projection steps keyed by field name.

    Returns:
        :class:`ProjectionPlan` with topologically ordered levels.

    Raises:
        ValueError: If a dependency cycle is detected.
    """
    if not steps:
        return ProjectionPlan(levels=())

    names = frozenset(steps)
    sorter: TopologicalSorter[str] = TopologicalSorter()
    for name, step in steps.items():
        sorter.add(name, *_projection_dependencies(name, step.projection, names))

    sorter.prepare()
    levels: list[tuple[ProjectionStep, ...]] = []
    while sorter.is_active():
        ready = tuple(sorter.get_ready())
        if not ready:
            raise ValueError("Projection dependency cycle detected")
        levels.append(tuple(steps[name] for name in ready))
        sorter.done(*ready)

    return ProjectionPlan(levels=tuple(levels))


async def execute_projection_plan(
    plan: ProjectionPlan,
    *,
    objs: Sequence[Any],
    id_attr: str,
    backend_context: Any | None,
) -> dict[int, dict[str, Any]]:
    """Execute a projection plan for the given objects.

    Steps with ``prefer_memory=True`` run synchronously via ``load_from_object``.
    Steps with ``prefer_memory=False`` are batched through ``load_many`` and
    require a non-``None`` ``backend_context``.

    Args:
        plan: Compiled :class:`ProjectionPlan`.
        objs: Domain objects to project over.
        id_attr: Attribute name of the primary key on each object.
        backend_context: SQLAlchemy session passed to SQL loaders, or ``None``.

    Returns:
        Mapping from object index to a dict of computed field values.
    """
    if not objs or not plan.levels:
        return {}

    parent_ids = [getattr(obj, id_attr) for obj in objs]
    values_by_index: dict[int, dict[str, Any]] = {index: {} for index in range(len(objs))}

    for level in plan.levels:
        buckets = _partition_execution_steps(level, backend_context=backend_context)
        await _execute_backend_steps(
            steps=buckets.backend,
            backend_context=backend_context,
            parent_ids=parent_ids,
            values_by_index=values_by_index,
        )
        _execute_memory_steps(
            steps=buckets.memory,
            objs=objs,
            values_by_index=values_by_index,
        )

    return values_by_index


def _merge_rows(
    *,
    values_by_index: dict[int, dict[str, Any]],
    parent_ids: Sequence[object],
    field_name: str,
    rows: Mapping[object, Any],
    default: Any,
) -> None:
    for index, parent_id in enumerate(parent_ids):
        loaded = rows.get(parent_id, PROJECTION_DEFAULT_MISSING)
        if loaded is PROJECTION_DEFAULT_MISSING:
            if default is PROJECTION_DEFAULT_MISSING:
                continue
            loaded = default
        values_by_index[index][field_name] = loaded


def _partition_execution_steps(
    level: tuple[ProjectionStep, ...],
    *,
    backend_context: Any | None,
) -> _ExecutionBuckets:
    backend_steps: list[ProjectionStep] = []
    memory_steps: list[ProjectionStep] = []
    for step in level:
        if step.prefer_memory:
            memory_steps.append(step)
        else:
            if backend_context is None:
                raise RuntimeError(f"Projection '{step.name}' requires backend context")
            backend_steps.append(step)
    return _ExecutionBuckets(
        backend=tuple(backend_steps),
        memory=tuple(memory_steps),
    )


async def _execute_backend_steps(
    *,
    steps: tuple[ProjectionStep, ...],
    backend_context: Any | None,
    parent_ids: Sequence[object],
    values_by_index: dict[int, dict[str, Any]],
) -> None:
    for step in steps:
        _merge_rows(
            values_by_index=values_by_index,
            parent_ids=parent_ids,
            field_name=step.name,
            rows=await step.loader.load_many(backend_context, parent_ids),
            default=step.projection.default,
        )


def _execute_memory_steps(
    *,
    steps: tuple[ProjectionStep, ...],
    objs: Sequence[Any],
    values_by_index: dict[int, dict[str, Any]],
) -> None:
    if not steps:
        return
    for index, obj in enumerate(objs):
        row_context = values_by_index[index]
        for step in steps:
            computed = step.loader.load_from_object(obj, row_context)
            row_context[step.name] = computed


def _build_projection_step(
    *,
    name: str,
    projection: Projection,
) -> ProjectionStep:
    loader = projection.loader
    prefer_memory = _has_declared_callable(loader, "load_from_object")
    if not prefer_memory and not _has_declared_callable(loader, "load_many"):
        raise TypeError(
            f"Projection '{name}': loader must implement 'load_from_object' or 'load_many'. "
            f"Got {type(loader).__name__}. Use CountLoader/ExistsLoader/JoinFieldsLoader "
            f"and call compile_all() before creating a repository."
        )
    return ProjectionStep(
        name=name,
        projection=projection,
        prefer_memory=prefer_memory,
        loader=loader,
    )


def _has_declared_callable(obj: Any, attr_name: str) -> bool:
    try:
        candidate = inspect.getattr_static(obj, attr_name)
    except AttributeError:
        return False
    return callable(candidate)
