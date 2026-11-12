"""A function running jobs: composed with Lambda pool defaults, each job run within the invocation.

Shared by the ``jobs`` function, the queued event triggers and the direct ones.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, NoReturn, cast

from loom.composition import LoopHost, Recipe
from loom.core.checks import REST_CHECKS
from loom.core.discovery.base import DiscoveryResult
from loom.core.discovery.worker import resolve_worker_components
from loom.core.job.envelope import JobEnvelope
from loom.core.job.run import run_job
from loom.core.job.service import InlineJobService, JobService
from loom.core.logger import get_logger
from loom.core.observability.event import Scope
from loom.core.persistence import LAMBDA_PROFILE

if TYPE_CHECKING:
    from loom.composition import Application, CompositionSource
    from loom.core.engine.compilable import Compilable
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.job.job import Job

_log = get_logger(__name__)
TIMEOUT_SAFETY_MARGIN_MS = 1000
"""Stop starting jobs when fewer than one second remains in the invocation."""

Read = Callable[[Any], Sequence[tuple[JobEnvelope, str | None]]]
"""Reads a message body or an event into the jobs it runs, in order, each with its span id."""


def jobs_recipe(
    source: CompositionSource,
    jobs: Sequence[type[Job[Any]]],
    use_cases: Sequence[type[Compilable]],
    metrics: MetricsAdapter | None,
) -> tuple[Recipe, dict[str, type[Job[Any]]]]:
    """The recipe of a function running jobs, with Lambda pool defaults, and the jobs by name.

    AutoCRUD use cases for discovered models are not compiled, and discovered
    callbacks are not registered. Agents are hosted for the use cases, without
    their HTTP endpoints.
    """
    resolved = resolve_worker_components(source.config, jobs, use_cases)
    if resolved.models:
        _log.warning(
            "Lambda ignores AutoCRUD use cases for discovered models",
            models=len(resolved.models),
        )
    if resolved.callbacks:
        _log.warning("Lambda ignores discovered job callbacks", callbacks=len(resolved.callbacks))
    recipe = Recipe(
        profile=LAMBDA_PROFILE,
        discovered=DiscoveryResult(
            models=tuple(dict.fromkeys(resolved.models)),
            use_cases=cast(Any, resolved.compilables),
            interfaces=(),
            jobs=resolved.jobs,
            agent_specs=resolved.agent_specs,
        ),
        skip_checks=frozenset({REST_CHECKS}),
        agents=True,
        agent_endpoints=False,
        metrics=metrics,
        connection_only=not resolved.models,
    )
    return recipe, {job_type.__qualname__: job_type for job_type in resolved.jobs}


def open_host(application: Application) -> LoopHost:
    """Open *application* at Init on a loop of its own, naming where dispatched jobs go.

    Raises:
        Exception: The failure to open the application, as itself.
    """
    service = application.kernel.container.resolve(JobService)
    host = LoopHost(application)
    host.start()
    if isinstance(service, InlineJobService):
        _log.info("Lambda dispatched jobs run inline in this invocation without a queue or retries")
    else:
        _log.warning(
            "Lambda dispatched jobs leave this invocation through the configured job service",
            job_service=type(service).__qualname__,
        )
    return host


def remaining_ms(context: Any) -> int | None:
    """The milliseconds left in the invocation; ``None`` without a Lambda context."""
    remaining = getattr(context, "get_remaining_time_in_millis", None)
    return cast(Callable[[], int], remaining)() if callable(remaining) else None


def run_timeout(job_timeout: int | None, context: Any) -> float | None:
    """The time a job may take: its own timeout, or what remains of the invocation."""
    limits: list[float] = []
    remaining = remaining_ms(context)
    if remaining is not None:
        limits.append(max(0.0, (remaining - TIMEOUT_SAFETY_MARGIN_MS) / 1000))
    if job_timeout is not None:
        limits.append(float(job_timeout))
    return min(limits) if limits else None


async def run_envelope(
    application: Application,
    envelope: JobEnvelope,
    job_type: type[Job[Any]],
    span_id: str | None,
    context: Any,
) -> None:
    """Run *envelope* as *job_type*, within its timeout and what remains of the invocation."""
    await asyncio.wait_for(
        run_job(
            envelope,
            job_type,
            factory=application.kernel.factory,
            executor=application.kernel.executor,
            observability=application.observability,
            message_id=span_id,
        ),
        timeout=run_timeout(application.job_settings.of(job_type).timeout, context),
    )


def fail_in_span(application: Application, error: Exception, name: str, **meta: Any) -> NoReturn:
    """Raise *error* inside a failed job span, as a message or event that never ran its job."""
    with application.observability.span(Scope.JOB, name, **meta):
        raise error
