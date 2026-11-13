"""The ordered application composition phases.

Loading the source (:func:`~loom.composition.pipeline.load.load_source`) is the
step before them: the entry points load, then call :func:`compose`.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from typing import Any

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
from loom.core.logger import LoggerPort, get_logger
from loom.core.observability.span import elapsed_ms


def compose(
    source: CompositionSource,
    adapter: Adapter[object],
    *,
    modules: Sequence[Callable[[LoomContainer], None]] = (),
) -> Application:
    """Compose the application *adapter* asks for from *source*, phase by phase.

    Logs one ``loom.composition.<phase>`` info event per phase, with its
    ``duration_ms``, and ``loom.composition.composed`` with the total.
    """
    logger = get_logger(__name__)
    started = time.perf_counter()
    planned = plan(source, adapter)
    discovered = planned.recipe.discovered
    mark = _logged(
        logger,
        "planned",
        started,
        adapter=type(adapter).__qualname__,
        use_cases=len(discovered.use_cases),
        jobs=len(discovered.jobs),
    )
    persisted = persist(planned)
    mark = _logged(logger, "persisted", mark, backends=persisted.backends)
    registered = register(persisted, modules=modules, logger=logger)
    mark = _logged(
        logger,
        "registered",
        mark,
        keys=len(registered.kernel.container.bindings()),
        modules=len(modules),
        overrides=len(planned.recipe.overrides),
    )
    ai = wire_ai(registered)
    mark = _logged(
        logger,
        "ai_wired",
        mark,
        agents=len(ai.plans),
        mcp_servers=len(ai.config.mcp_servers) if ai.config is not None else 0,
    )
    seal(registered)
    _logged(logger, "sealed", mark)
    application = assemble(registered, ai)
    _logged(logger, "composed", started)
    return application


def _logged(logger: LoggerPort, phase: str, since: float, **fields: Any) -> float:
    """Log ``loom.composition.<phase>`` with *fields* and its duration since *since*; restart."""
    logger.info(f"loom.composition.{phase}", **fields, duration_ms=elapsed_ms(since))
    return time.perf_counter()
