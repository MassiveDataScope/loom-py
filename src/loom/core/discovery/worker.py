"""Worker components discovered from application packages."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, TypeVar

from loom.core.config import ConfigContext, ConfigError
from loom.core.discovery.scanner import PackageScanner
from loom.core.engine.compilable import Compilable
from loom.core.job.job import Job
from loom.core.model import BaseModel

_logger = logging.getLogger(__name__)
_T = TypeVar("_T")
AutoCRUDUseCases = Callable[[Sequence[type[Any]]], tuple[type[Compilable], ...]]


class DuplicateJobName(ConfigError):
    """Two jobs have the same qualified name."""


@dataclass(frozen=True)
class WorkerComponents:
    """Classes served by a worker."""

    compilables: tuple[type[Compilable], ...]
    jobs: tuple[type[Job[Any]], ...]
    models: tuple[type[BaseModel], ...]
    callbacks: tuple[type[Any], ...]
    agent_specs: tuple[str, ...] = ()


def merge_unique(first: Sequence[_T], second: Sequence[_T]) -> tuple[_T, ...]:
    """Merge two sequences, preserving order and uniqueness."""
    return tuple(dict.fromkeys((*first, *second)))


def use_cases_from_interfaces(interfaces: Sequence[type[Any]]) -> tuple[type[Compilable], ...]:
    """Return the use cases declared on interface routes."""
    return tuple(
        dict.fromkeys(
            route.use_case
            for interface in interfaces
            for route in interface.routes
            if route.use_case is not None
        )
    )


def discovered_jobs(packages: Sequence[str]) -> tuple[type[Job[Any]], ...]:
    """Return jobs declared in the configured packages."""
    return PackageScanner(list(packages)).discover().jobs


def resolve_worker_components(
    ctx: ConfigContext,
    explicit_jobs: Sequence[type[Job[Any]]],
    explicit_use_cases: Sequence[type[Compilable]],
    autocrud_use_cases: AutoCRUDUseCases | None = None,
) -> WorkerComponents:
    """Resolve jobs and compilables from explicit classes and application packages."""
    packages = ctx.raw("app.packages", [])
    agents = ctx.raw("app.agents", [])
    discovered = PackageScanner(packages).discover(agents=agents)
    jobs = tuple(explicit_jobs) if explicit_jobs else discovered.jobs
    names: set[str] = set()
    for job in jobs:
        if job.__qualname__ in names:
            raise DuplicateJobName(f"Duplicate job qualname {job.__qualname__!r}")
        names.add(job.__qualname__)
    generated = autocrud_use_cases(discovered.models) if autocrud_use_cases else ()
    compilables = merge_unique((*explicit_use_cases, *discovered.use_cases, *generated), jobs)
    if not jobs:
        _logger.warning("no Job classes discovered in app.packages")
    return WorkerComponents(
        compilables=compilables,
        jobs=jobs,
        models=discovered.models,
        callbacks=(),
        agent_specs=discovered.agent_specs,
    )
