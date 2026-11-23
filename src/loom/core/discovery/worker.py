"""Worker components discovered from application packages."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from loom.core.config import ConfigError
from loom.core.discovery.base import DiscoveryResult
from loom.core.engine.compilable import Compilable
from loom.core.job.job import Job
from loom.core.log_names import LogEvent
from loom.core.model import BaseModel

_logger = logging.getLogger(__name__)


class DiscoveryEvent(LogEvent):
    """What worker discovery logs."""

    NO_JOBS_DISCOVERED = "discovery.no_jobs_discovered"


AutoCRUDUseCases = Callable[[Sequence[type[Any]]], tuple[type[Compilable], ...]]


class DuplicateJobName(ConfigError):
    """Two jobs have the same qualified name."""


@dataclass(frozen=True)
class WorkerComponents:
    """Classes served by a worker."""

    compilables: tuple[type[Compilable], ...]
    jobs: tuple[type[Job[Any]], ...]
    models: tuple[type[BaseModel], ...]
    agent_specs: tuple[str, ...] = ()


def resolve_worker_components(
    discovered: DiscoveryResult,
    explicit_jobs: Sequence[type[Job[Any]]],
    explicit_use_cases: Sequence[type[Compilable]],
    autocrud_use_cases: AutoCRUDUseCases | None = None,
) -> WorkerComponents:
    """Resolve jobs and compilables from explicit classes and what the scan discovered.

    Args:
        discovered: What the project's scan of ``app.packages`` found.
        explicit_jobs: Jobs the entry point names; they replace the discovered ones.
        explicit_use_cases: Use cases the entry point names.
        autocrud_use_cases: Compiles the use cases generated for the discovered models.

    Returns:
        The jobs, compilables and models the worker serves.

    Raises:
        DuplicateJobName: When two jobs share a qualified name.
    """
    jobs = tuple(explicit_jobs) if explicit_jobs else discovered.jobs
    names: set[str] = set()
    for job in jobs:
        if job.__qualname__ in names:
            raise DuplicateJobName(f"Duplicate job qualname {job.__qualname__!r}")
        names.add(job.__qualname__)
    generated = autocrud_use_cases(discovered.models) if autocrud_use_cases else ()
    compilables = tuple(
        dict.fromkeys((*explicit_use_cases, *discovered.use_cases, *generated, *jobs))
    )
    if not jobs:
        _logger.warning(DiscoveryEvent.NO_JOBS_DISCOVERED)
    return WorkerComponents(
        compilables=compilables,
        jobs=jobs,
        models=discovered.models,
        agent_specs=discovered.agent_specs,
    )
