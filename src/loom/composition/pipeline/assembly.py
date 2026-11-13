"""Assemble the application and its owned resources."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from loom.composition._ai import AiWiring
from loom.composition._sql import SqlWiring
from loom.composition.application import Application
from loom.composition.recipe import Recipe
from loom.composition.resources import Resource, ResourceKind, closing
from loom.composition.source import CompositionSource
from loom.core.bootstrap import KernelRuntime
from loom.core.cache.wiring import CacheGateways

if TYPE_CHECKING:
    from loom.composition.pipeline.persistence import Persisted
    from loom.composition.pipeline.registration import Registered


def assemble(
    source: CompositionSource,
    recipe: Recipe,
    persisted: Persisted,
    registered: Registered,
    ai: AiWiring,
) -> Application:
    kernel, sql = registered.kernel, registered.sql
    return Application(
        kernel=kernel,
        config=source.config,
        environment=source.environment,
        code_path=source.code_path,
        discovered=recipe.discovered,
        observability=source.observability,
        sql=sql,
        ai=ai,
        job_settings=persisted.job_settings,
        resources=_resources(
            kernel=kernel,
            stores=persisted.store_resources,
            sql=sql,
            ai=ai,
            job_resources=registered.job_resources,
        ),
    )


def _resources(
    *,
    kernel: KernelRuntime,
    stores: Sequence[Resource],
    sql: SqlWiring,
    ai: AiWiring,
    job_resources: Sequence[Resource],
) -> tuple[Resource, ...]:
    """Everything the application owns; :class:`Application` orders it by kind."""
    resources: list[Resource] = list(job_resources)
    if kernel.container.is_registered(CacheGateways):
        gateways = kernel.container.resolve(CacheGateways).distinct()
        for name, gateway in zip(("data", "counters"), gateways, strict=False):
            resources.append(
                Resource(kind=ResourceKind.CACHE, name=name, manager=closing(gateway.close))
            )
    registry, runtime = sql.registry, ai.runtime
    if registry is not None:
        resources.append(
            Resource(
                kind=ResourceKind.SQL,
                name="clickhouse",
                manager=lambda: registry,
                check=registry.check,
            )
        )
    if runtime is not None:
        resources.append(Resource(kind=ResourceKind.AGENTS, name="agents", manager=lambda: runtime))
    resources.extend(stores)
    return tuple(resources)
