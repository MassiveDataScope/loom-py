"""Job service wiring for the composition root."""

from __future__ import annotations

import asyncio
import importlib.util
import warnings
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, NoReturn, cast

from loom.composition.errors import JobBackendError
from loom.composition.resources import Resource, ResourceKind, closing
from loom.core.bootstrap import KernelRuntime
from loom.core.config import ConfigContext, ConfigKey
from loom.core.di.scope import Scope
from loom.core.job.router import JobRouter
from loom.core.job.service import InlineJobService, JobService
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.symbols import import_symbol

if TYPE_CHECKING:
    from loom.celery.service import CeleryJobService
    from loom.core.engine.executor import RuntimeExecutor
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.job.job import Job
    from loom.core.use_case.factory import UseCaseFactory


@dataclass(frozen=True, kw_only=True)
class JobBackendContext:
    """What a job backend is built with; a ``module:Class`` backend receives it.

    Args:
        config: Resolved configuration; a backend reads its own section.
        factory: Builds jobs and callbacks from the container.
        executor: Executes them.
        metrics: Use-case and job metrics, or ``None``.
        observability: Observability runtime the backend traces with, or ``None``.
    """

    config: ConfigContext
    factory: UseCaseFactory
    executor: RuntimeExecutor
    metrics: MetricsAdapter | None
    observability: ObservabilityRuntime | None


_Built = tuple[JobService, Resource | None]


def _celery_service(context: JobBackendContext) -> CeleryJobService:
    """Build the Celery producer; ``ImportError`` without the ``loom[celery]`` extra."""
    from loom.celery.config import CeleryConfig, create_celery_app
    from loom.celery.service import CeleryJobService

    celery_cfg = context.config.section(ConfigKey.CELERY, CeleryConfig)
    return CeleryJobService(
        create_celery_app(celery_cfg),
        metrics=context.metrics,
        factory=context.factory,
        executor=context.executor,
        observability_runtime=context.observability,
    )


def _celery_resource(service: CeleryJobService) -> Resource:
    return Resource(
        kind=ResourceKind.CELERY,
        name="jobs",
        manager=closing(lambda: asyncio.to_thread(service.close)),
    )


def _inline(context: JobBackendContext) -> _Built:
    return InlineJobService(context.factory, context.executor), None


def _celery(context: JobBackendContext) -> _Built:
    try:
        service = _celery_service(context)
    except ImportError as exc:
        raise JobBackendError(
            "app.job_service routes jobs to celery, but the loom[celery] extra is not installed"
        ) from exc
    return service, _celery_resource(service)


def _sqs(context: JobBackendContext) -> NoReturn:
    _ = context  # every builder takes the context; this one refuses before reading it
    raise JobBackendError("the sqs job backend arrives with T076; use inline or celery")


_BUILTIN_JOB_BACKENDS: Final[Mapping[str, Callable[[JobBackendContext], _Built]]] = {
    "inline": _inline,
    "celery": _celery,
    "sqs": _sqs,
}
"""Each built-in backend by name; its section is the one named alike."""


def _backend(name: str, context: JobBackendContext) -> _Built:
    builtin = _BUILTIN_JOB_BACKENDS.get(name)
    if builtin is not None:
        return builtin(context)
    if ":" not in name:
        raise JobBackendError(
            f"app.job_service {name!r} names no job backend: use "
            f"{', '.join(_BUILTIN_JOB_BACKENDS)} or a module:Class reference"
        )
    backend_type = cast(Callable[[JobBackendContext], JobService], import_symbol(name))
    return backend_type(context), None


def _declared_queues(ctx: ConfigContext, name: str) -> tuple[str, ...]:
    """The queues the section of built-in backend *name* declares, read without building it."""
    if name not in _BUILTIN_JOB_BACKENDS:
        return ()
    return tuple(ctx.section_or_default(f"{name}.queues", list[str], []))


def _routed(
    context: JobBackendContext, default: str, jobs: Sequence[type[Job[Any]]]
) -> tuple[JobService, tuple[Resource, ...]]:
    """The *default* backend, routed with every backend whose section declares queues."""
    ctx = context.config
    declaring = (n for n in _BUILTIN_JOB_BACKENDS if n != default and _declared_queues(ctx, n))
    names = (default, *declaring)
    built = [(*_backend(name, context), _declared_queues(ctx, name)) for name in names]
    resources = tuple(resource for _, resource, _ in built if resource is not None)
    default_service = built[0][0]
    if len(built) == 1:
        return default_service, resources
    router = JobRouter(
        [(service, queues) for service, _, queues in built],
        default=default_service,
        factory=context.factory,
        executor=context.executor,
    )
    router.require_served(jobs)
    return router, resources


def _implicit(context: JobBackendContext) -> _Built:
    """Today's rule without ``app.job_service``: Celery when configured and installed."""
    ctx = context.config
    configured = ctx.has(ConfigKey.CELERY)
    # find_spec locates without importing: without a celery section the
    # application must not pay for loading Celery.
    installed = configured or importlib.util.find_spec("celery") is not None
    if installed:
        warnings.warn(
            "app.job_service is not set: the job service follows the celery section. "
            "Set app.job_service to inline or celery; the implicit choice is deprecated.",
            DeprecationWarning,
            stacklevel=3,
        )
    if not configured:
        if installed:
            warnings.warn(
                "Celery config section is missing — falling back to InlineJobService. "
                "Add a 'celery' section to your config to suppress this.",
                stacklevel=3,
            )
        return _inline(context)
    try:
        service = _celery_service(context)
    except ImportError:
        return _inline(context)
    return service, _celery_resource(service)


def configure_job_service(
    ctx: ConfigContext,
    result: KernelRuntime,
    observability_runtime: ObservabilityRuntime | None,
    *,
    backend: str | None = None,
    jobs: Sequence[type[Job[Any]]] = (),
) -> tuple[Resource, ...]:
    """Register the ``JobService`` in the container, ``APPLICATION`` scoped.

    With *backend* (``app.job_service``) set, that backend serves every queue
    no other backend declares, and each built-in backend whose section
    declares ``queues`` serves those; with more than one backend, a
    :class:`~loom.core.job.router.JobRouter` routes each job by its queue.
    Without it, :class:`~loom.celery.service.CeleryJobService` is registered
    when a ``celery`` section is present and the ``loom[celery]`` extra is
    installed, :class:`~loom.core.job.service.InlineJobService` otherwise. A
    ``JobService`` a caller's DI module already registered is kept.

    Args:
        ctx: Resolved configuration context.
        result: Kernel runtime carrying container, factory and executor.
        observability_runtime: Observability runtime the backends trace
            with, or ``None``.
        backend: ``app.job_service``: a built-in name or ``module:Class``.
        jobs: Discovered jobs, whose queues must have a backend.

    Returns:
        One resource per built backend that owns something to close.

    Raises:
        JobBackendError: When *backend* names no backend, ``sqs``, or
            ``celery`` without the extra installed.
        DuplicateJobQueue: When two backends declare the same queue.
        UnservedJobQueue: When a job's queue has no backend.
    """
    if result.container.is_registered(JobService):
        return ()
    context = JobBackendContext(
        config=ctx,
        factory=result.factory,
        executor=result.executor,
        metrics=result.metrics,
        observability=observability_runtime,
    )
    if backend is None:
        service, resource = _implicit(context)
        resources: tuple[Resource, ...] = () if resource is None else (resource,)
    else:
        service, resources = _routed(context, backend, jobs)
    result.container.register(JobService, lambda: service, scope=Scope.APPLICATION)
    return resources
