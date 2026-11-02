"""Celery discovery with REST AutoCRUD compilation."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from loom.core.config import ConfigContext
from loom.core.engine.compilable import Compilable
from loom.core.job.discovery import (
    WorkerManifest as WorkerManifest,
)
from loom.core.job.discovery import (
    _apply_job_config_if_present as _apply_job_config_if_present,
)
from loom.core.job.discovery import (
    _emit_worker_init_graph as _emit_worker_init_graph,
)
from loom.core.job.discovery import (
    _merge_unique as _merge_unique,
)
from loom.core.job.discovery import _resolve_compilables_and_jobs as _resolve_shared
from loom.core.job.discovery import (
    _use_cases_from_interfaces as _use_cases_from_interfaces,
)
from loom.core.job.discovery import (
    _WorkerResolved,
)
from loom.core.job.job import Job
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
) -> _WorkerResolved:
    """Resolve worker components, including REST generated use cases."""
    return _resolve_shared(ctx, explicit_jobs, explicit_use_cases, _autocrud_use_cases_from_models)
