"""Celery discovery with REST AutoCRUD compilation."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any

from loom.celery.event import CeleryEvent
from loom.core.discovery.base import DiscoveryResult
from loom.core.discovery.worker import WorkerComponents, resolve_worker_components
from loom.core.engine.compilable import Compilable
from loom.core.job.job import Job
from loom.core.log_names import LogField
from loom.jobs.event import JobsKey
from loom.rest.autocrud import build_auto_routes


def _autocrud_use_cases_from_models(
    models: Sequence[type[Any]],
) -> tuple[type[Compilable], ...]:
    """Compile AutoCRUD use cases for discovered models."""
    return tuple(
        dict.fromkeys(route.use_case for model in models for route in build_auto_routes(model, ()))
    )


def _resolve_compilables_and_jobs(
    discovered: DiscoveryResult,
    explicit_jobs: Sequence[type[Job[Any]]],
    explicit_use_cases: Sequence[type[Compilable]],
) -> WorkerComponents:
    """Resolve worker components, including REST generated use cases."""
    return resolve_worker_components(
        discovered, explicit_jobs, explicit_use_cases, _autocrud_use_cases_from_models
    )


def _emit_worker_init_graph(components: WorkerComponents) -> None:
    """Emit the Celery worker graph at startup."""
    logging.getLogger(__name__).info(
        CeleryEvent.WORKER_GRAPH_INITIALIZED,
        extra={
            JobsKey.COUNT: len(components.jobs),
            LogField.USE_CASE_COUNT: len(components.compilables) - len(components.jobs),
        },
    )
