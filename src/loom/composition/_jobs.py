"""Job service wiring for the composition root."""

from __future__ import annotations

import asyncio
import importlib.util
import warnings
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Final, cast

from loom.composition.errors import JobBackendError
from loom.composition.resources import Resource, ResourceKind, closing
from loom.core.bootstrap import KernelRuntime
from loom.core.config import ConfigContext, ConfigKey
from loom.core.di.scope import Scope
from loom.core.job.config import JobSettings
from loom.core.job.job import Job
from loom.core.job.router import JobRouter
from loom.core.job.service import InlineJobService, JobService
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.plugins.optional import MissingExtraError
from loom.core.symbols import import_symbol

if TYPE_CHECKING:
    from loom.celery.config import CeleryConfig
    from loom.core.engine.executor import RuntimeExecutor
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.use_case.factory import UseCaseFactory


@dataclass(frozen=True, kw_only=True)
class JobBackendContext:
    """What a job backend is built with; a ``module:Class`` backend receives it.

    ``jobs`` are the discovered jobs this backend serves: those on
    ``celery.queues`` go to Celery instead. ``settings`` hold each job's
    queue, retries, countdown, timeout and priority in this application.
    """

    config: ConfigContext
    factory: UseCaseFactory
    executor: RuntimeExecutor
    metrics: MetricsAdapter | None
    observability: ObservabilityRuntime | None
    jobs: tuple[type[Job[Any]], ...] = ()
    settings: JobSettings = field(default_factory=JobSettings)


_Built = tuple[JobService, tuple[Resource, ...]]


def _celery_config(ctx: ConfigContext) -> CeleryConfig:
    try:
        from loom.celery.config import CeleryConfig
    except ImportError as exc:
        raise JobBackendError(
            "jobs are routed to celery, but the loom[celery] extra is not installed"
        ) from exc
    return ctx.section(ConfigKey.CELERY, CeleryConfig)


def _celery_backend(context: JobBackendContext, celery_cfg: CeleryConfig) -> _Built:
    from loom.celery.config import create_celery_app
    from loom.celery.service import CeleryJobService

    try:
        app = create_celery_app(celery_cfg)
    except ValueError as exc:
        raise JobBackendError(f"the celery section is invalid: {exc}") from exc
    service = CeleryJobService(
        app,
        metrics=context.metrics,
        factory=context.factory,
        executor=context.executor,
        observability_runtime=context.observability,
        settings=context.settings,
    )
    closer = Resource(
        kind=ResourceKind.CELERY,
        name="jobs",
        manager=closing(lambda: asyncio.to_thread(service.close)),
    )
    return service, (closer,)


def _inline(context: JobBackendContext) -> _Built:
    return InlineJobService(context.factory, context.executor, settings=context.settings), ()


def _celery(context: JobBackendContext) -> _Built:
    return _celery_backend(context, _celery_config(context.config))


def _sqs(context: JobBackendContext) -> _Built:
    from loom.jobs.aws.sqs import build_backend

    try:
        return build_backend(context), ()
    except MissingExtraError as exc:
        raise JobBackendError(
            "app.job_service is sqs, but the loom[sqs] extra is not installed"
        ) from exc


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
    return backend_type(context), ()


def _routed(context: JobBackendContext, default: str) -> _Built:
    """The *default* backend; ``celery.queues`` but Celery's default queue go to Celery."""
    # Celery is imported only when it declares queues.
    if default == "celery" or not context.config.has("celery.queues"):
        return _backend(default, context)
    celery_cfg = _celery_config(context.config)
    # Celery's own default queue stays with the default backend: it is every job's queue.
    queues = [queue for queue in celery_cfg.queues if queue != celery_cfg.task_default_queue]
    served = tuple(job for job in context.jobs if context.settings.queue(job) not in queues)
    service, resources = _backend(default, replace(context, jobs=served))  # NOSONAR(S5655)
    if not queues:
        return service, resources
    celery, closers = _celery_backend(context, celery_cfg)
    return JobRouter(dict.fromkeys(queues, celery), default=service, settings=context.settings), (
        *resources,
        *closers,
    )


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
        celery_cfg = _celery_config(ctx)
    except JobBackendError:
        return _inline(context)
    return _celery_backend(context, celery_cfg)


def configure_job_service(
    ctx: ConfigContext,
    result: KernelRuntime,
    observability_runtime: ObservabilityRuntime | None,
    *,
    backend: str | None = None,
    jobs: tuple[type[Job[Any]], ...] = (),
    settings: JobSettings | None = None,
) -> tuple[Resource, ...]:
    """Register the ``JobService`` in the container, ``APPLICATION`` scoped.

    With *backend* (``app.job_service``) set, that backend serves every queue
    but those ``celery.queues`` declares, which a
    :class:`~loom.core.job.router.JobRouter` sends to Celery.
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
        jobs: The discovered jobs.
        settings: Each job's settings in this application; the ClassVars
            when omitted.

    Returns:
        One resource per built backend that owns something to close.

    Raises:
        JobBackendError: When *backend* names no backend, or Celery or
            SQS is needed without its extra installed.
        SQSQueueConfigError: When SQS serves a job whose queue has no URL,
            or whose FIFO queue it names no ``__group_by__`` for.
    """
    if result.container.is_registered(JobService):
        return ()
    context = JobBackendContext(
        config=ctx,
        factory=result.factory,
        executor=result.executor,
        metrics=result.metrics,
        observability=observability_runtime,
        jobs=jobs,
        settings=settings or JobSettings(),
    )
    service, resources = _implicit(context) if backend is None else _routed(context, backend)
    result.container.register(JobService, lambda: service, scope=Scope.APPLICATION)
    return resources
