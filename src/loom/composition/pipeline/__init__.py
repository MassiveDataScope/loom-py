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


def compose(
    source: CompositionSource,
    adapter: Adapter[object],
    *,
    modules: Sequence[Callable[[LoomContainer], None]] = (),
) -> Application:
    """Compose the application *adapter* asks for from *source*, phase by phase."""
    registered = register(persist(plan(source, adapter)), modules=modules)
    ai = wire_ai(registered)
    seal(registered)
    return assemble(registered, ai)
