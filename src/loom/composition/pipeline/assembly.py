"""Assemble the application and its owned resources."""

from __future__ import annotations

from collections.abc import Sequence

from loom.composition._ai import AiWiring
from loom.composition.application import Application
from loom.composition.pipeline.persistence import Persisted
from loom.composition.pipeline.registration import Registered
from loom.composition.recipe import Recipe
from loom.composition.resources import Resource, ResourceKind, closing
from loom.composition.source import CompositionSource
from loom.core.cache.wiring import CacheGateways
from loom.core.job.config import JobSettings


def assemble(
    source: CompositionSource,
    recipe: Recipe,
    persisted: Persisted,
    registered: Registered,
    ai: AiWiring,
    job_settings: JobSettings,
) -> Application:
    return Application(
        kernel=registered.kernel,
        config=source.config,
        environment=source.environment,
        code_path=source.code_path,
        discovered=recipe.discovered,
        observability=source.observability,
        sql=registered.sql,
        ai=ai,
        job_settings=job_settings,
        resources=_resources(registered, persisted.store_resources, ai),
    )


def _resources(
    registered: Registered, stores: Sequence[Resource], ai: AiWiring
) -> tuple[Resource, ...]:
    """Everything the application owns; :class:`Application` orders it by kind."""
    kernel = registered.kernel
    resources: list[Resource] = list(registered.job_resources)
    if kernel.container.is_registered(CacheGateways):
        gateways = kernel.container.resolve(CacheGateways).distinct()
        for name, gateway in zip(("data", "counters"), gateways, strict=False):
            resources.append(
                Resource(kind=ResourceKind.CACHE, name=name, manager=closing(gateway.close))
            )
    registry, runtime = registered.sql.registry, ai.runtime
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
