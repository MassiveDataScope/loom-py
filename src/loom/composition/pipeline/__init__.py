"""The ordered application composition phases.

Loading the source (:func:`~loom.composition.pipeline.load.load_source`) is the
step before them: the entry points load, then call :func:`compose`.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence

from loom.composition.application import Application
from loom.composition.pipeline.ai import wire_ai
from loom.composition.pipeline.assembly import assemble
from loom.composition.pipeline.persistence import persist
from loom.composition.pipeline.planning import plan
from loom.composition.pipeline.registration import register
from loom.composition.pipeline.seal import seal
from loom.composition.recipe import Adapter
from loom.composition.source import CompositionSource
from loom.core.di.container import LoomContainer
from loom.core.job.config import JobSettings
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.persistence.stores import StoreCatalog


def compose(
    source: CompositionSource,
    adapter: Adapter[object],
    *,
    modules: Sequence[Callable[[LoomContainer], None]] = (),
    observability_provider: Callable[[], ObservabilityRuntime],
    catalog_provider: Callable[[], StoreCatalog],
) -> Application:
    recipe = plan(source, adapter)
    job_settings = JobSettings.from_config(source.config, recipe.discovered.jobs)
    persisted = persist(source, recipe)
    registered = register(
        source,
        recipe,
        persisted,
        job_settings,
        modules=modules,
        observability_provider=observability_provider,
        catalog_provider=catalog_provider,
    )
    ai = wire_ai(source, recipe, registered)
    seal(source, recipe, persisted, registered)
    return assemble(source, recipe, persisted, registered, ai, job_settings)
