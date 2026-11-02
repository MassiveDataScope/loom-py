"""Celery discovery with REST AutoCRUD compilation."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from loom.core.config import ConfigContext
from loom.core.discovery.worker import WorkerComponents, resolve_worker_components
from loom.core.discovery.worker import WorkerManifest as WorkerManifest
from loom.core.engine.compilable import Compilable
from loom.core.job.job import Job
from loom.core.logger import get_logger
from loom.rest.autocrud import build_auto_routes


def _autocrud_use_cases_from_models(
    models: Sequence[type[Any]],
) -> tuple[type[Compilable], ...]:
    """Compile AutoCRUD use cases for discovered models."""
    return tuple(
        dict.fromkeys(route.use_case for model in models for route in build_auto_routes(model, ()))
    )


def _resolve_compilables_and_jobs(
    ctx: ConfigContext,
    explicit_jobs: Sequence[type[Job[Any]]],
    explicit_use_cases: Sequence[type[Compilable]],
) -> WorkerComponents:
    """Resolve worker components, including REST generated use cases."""
    return resolve_worker_components(
        ctx, explicit_jobs, explicit_use_cases, _autocrud_use_cases_from_models
    )


def _emit_worker_init_graph(components: WorkerComponents) -> None:
    """Emit the Celery worker graph at startup."""
    get_logger("loom.celery.bootstrap").info(
        "[BOOT] Worker init graph",
        jobs=len(components.jobs),
        use_cases=len(components.compilables) - len(components.jobs),
        callbacks=len(components.callbacks),
        models=len(components.models),
    )
