"""Job service wiring for the composition root."""

from __future__ import annotations

import asyncio
import importlib.util
import warnings
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Final, cast

from loom.composition.errors import JobTransportError
from loom.composition.resources import Resource, ResourceKind, closing
from loom.core.bootstrap import KernelRuntime
from loom.core.config import ConfigContext, ConfigKey
from loom.core.connections.aws import AwsClients
from loom.core.di.scope import Scope
from loom.core.job.callback import DispatchableCallbacks, JobCallback
from loom.core.job.config import JobSettings
from loom.core.job.inline import InlineJobTransport
from loom.core.job.job import Job
from loom.core.job.service import JobService
from loom.core.job.timer import AsyncioJobTimer, JobTimer
from loom.core.job.transport import JobTransport
from loom.core.logger import get_logger
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.plugins.optional import MissingExtraError
from loom.core.symbols import import_symbol

if TYPE_CHECKING:
    from loom.celery.config import CeleryConfig
    from loom.core.engine.executor import RuntimeExecutor
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.use_case.factory import UseCaseFactory

_log = get_logger(__name__)


@dataclass(frozen=True, kw_only=True)
class JobTransportContext:
    """What a job transport is built with; a ``module:Class`` transport receives it.

    ``timer`` is the :class:`~loom.core.job.timer.JobTimer` a test bound in the
    container; ``can_wait_in_process`` says whether the entry point may hold
    delayed jobs in memory (not a Lambda). ``jobs`` are the discovered jobs this
    transport serves: those on ``celery.queues`` go to Celery instead. ``callbacks``
    holds the discovered job callbacks a dispatch may name. ``settings`` hold each job's
    queue, retries, countdown, timeout and priority in this application. ``aws``
    holds the application's AWS clients, closed with it.
    """

    config: ConfigContext
    aws: AwsClients
    factory: UseCaseFactory
    executor: RuntimeExecutor
    metrics: MetricsAdapter | None
    observability: ObservabilityRuntime | None
    jobs: tuple[type[Job[Any]], ...] = ()
    callbacks: DispatchableCallbacks = field(default_factory=DispatchableCallbacks.any)
    settings: JobSettings = field(default_factory=JobSettings)
    timer: JobTimer | None = None
    can_wait_in_process: bool = True


_Built = tuple[JobTransport, tuple[Resource, ...]]


def _celery_config(ctx: ConfigContext) -> CeleryConfig:
    try:
        from loom.celery.config import CeleryConfig
    except ImportError as exc:
        raise JobTransportError(
            "jobs are routed to celery, but the loom[celery] extra is not installed"
        ) from exc
    return ctx.section(ConfigKey.CELERY, CeleryConfig)


def _celery_transport(context: JobTransportContext, celery_cfg: CeleryConfig) -> _Built:
    from loom.celery.config import create_celery_app
    from loom.celery.service import CeleryJobTransport

    try:
        app = create_celery_app(celery_cfg)
    except ValueError as exc:
        raise JobTransportError(f"the celery section is invalid: {exc}") from exc
    service = CeleryJobTransport(
        app,
        metrics=context.metrics,
        observability_runtime=context.observability,
    )
    closer = Resource(
        kind=ResourceKind.CELERY,
        name="jobs",
        manager=closing(lambda: asyncio.to_thread(service.close)),
    )
    return service, (closer,)


def _inline(context: JobTransportContext) -> _Built:
    timer = context.timer
    resources: tuple[Resource, ...] = ()
    if timer is None and context.can_wait_in_process:
        timer = AsyncioJobTimer()
        resources = (Resource(kind=ResourceKind.TIMER, name="jobs", manager=closing(timer.close)),)
        _log.warning(
            "Inline jobs wait in process memory: a delayed job is lost when the process "
            "stops; use Celery or SQS in production"
        )
    service = InlineJobTransport(
        context.factory,
        context.executor,
        timer=timer,
        metrics=context.metrics,
    )
    if context.jobs:
        _log.info(
            "Job retry policies",
            transport="inline",
            jobs=context.settings.describe_retries(service.redelivery, context.jobs),
        )
    return service, resources


def _celery(context: JobTransportContext) -> _Built:
    return _celery_transport(context, _celery_config(context.config))


def _sqs(context: JobTransportContext) -> _Built:
    from loom.jobs.aws.sqs import build_transport

    try:
        return build_transport(context), ()
    except MissingExtraError as exc:
        raise JobTransportError(
            "app.job_service is sqs, but the loom[aws] extra is not installed"
        ) from exc


_BUILTIN_JOB_TRANSPORTS: Final[Mapping[str, Callable[[JobTransportContext], _Built]]] = {
    "inline": _inline,
    "celery": _celery,
    "sqs": _sqs,
}
"""Each built-in transport by name; its section is the one named alike."""


def _transport(name: str, context: JobTransportContext) -> _Built:
    builtin = _BUILTIN_JOB_TRANSPORTS.get(name)
    if builtin is not None:
        return builtin(context)
    if ":" not in name:
        raise JobTransportError(
            f"app.job_service {name!r} names no job transport: use "
            f"{', '.join(_BUILTIN_JOB_TRANSPORTS)} or a module:Class reference"
        )
    transport_type = cast(Callable[[JobTransportContext], JobTransport], import_symbol(name))
    return transport_type(context), ()


def _routed(
    context: JobTransportContext, default: str
) -> tuple[JobTransport, Mapping[str, JobTransport], tuple[Resource, ...]]:
    """Build the default transport and routes for the configured Celery queues."""
    if default == "celery" or not context.config.has("celery.queues"):
        transport, resources = _transport(default, context)
        return transport, {}, resources
    celery_cfg = _celery_config(context.config)
    queues = [queue for queue in celery_cfg.queues if queue != celery_cfg.task_default_queue]
    served = tuple(job for job in context.jobs if context.settings.of(job).queue not in queues)
    transport, resources = _transport(default, replace(context, jobs=served))
    if not queues:
        return transport, {}, resources
    celery, closers = _celery_transport(context, celery_cfg)
    return transport, dict.fromkeys(queues, celery), (*resources, *closers)


def _implicit(context: JobTransportContext) -> _Built:
    """Today's rule without ``app.job_service``: Celery when configured and installed."""
    ctx = context.config
    configured = ctx.has(ConfigKey.CELERY)
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
                "Celery config section is missing — falling back to InlineJobTransport. "
                "Add a 'celery' section to your config to suppress this.",
                stacklevel=3,
            )
        return _inline(context)
    try:
        celery_cfg = _celery_config(ctx)
    except JobTransportError:
        return _inline(context)
    return _celery_transport(context, celery_cfg)


def configure_job_service(
    ctx: ConfigContext,
    result: KernelRuntime,
    observability_runtime: ObservabilityRuntime | None,
    *,
    transport: str | None = None,
    jobs: tuple[type[Job[Any]], ...] = (),
    callbacks: tuple[type[JobCallback], ...] = (),
    settings: JobSettings,
    aws: AwsClients,
    can_wait_in_process: bool = True,
) -> tuple[Resource, ...]:
    """Register the ``JobService`` in the container, ``APPLICATION`` scoped.

    With *transport* (``app.job_service``) set, that transport serves every queue
    but those ``celery.queues`` declares, which a
    :class:`~loom.core.job.service.JobService` sends to Celery.
    Without it, :class:`~loom.celery.service.CeleryJobTransport` is registered
    when a ``celery`` section is present and the ``loom[celery]`` extra is
    installed, :class:`~loom.core.job.service.InlineJobTransport` otherwise. A
    ``JobService`` a caller's DI module already registered is kept.

    Args:
        ctx: Resolved configuration context.
        result: Kernel runtime carrying container, factory and executor.
        observability_runtime: Observability runtime the transports trace
            with, or ``None``.
        transport: ``app.job_service``: a built-in name or ``module:Class``.
        jobs: The discovered jobs.
        callbacks: The discovered job callbacks.
        settings: Each job's settings in this application.
        aws: The application's AWS clients, which the SQS transport takes its client from.
        can_wait_in_process: Whether the inline service may hold delayed jobs in memory;
            without it the inline service refuses every delay. A
            :class:`~loom.core.job.timer.JobTimer` already bound in the container
            is used either way.

    Returns:
        One resource per built transport that owns something to close.

    Raises:
        JobTransportError: When *transport* names no transport, or Celery or
            SQS is needed without its extra installed.
        SQSQueueConfigError: When SQS serves a job whose queue has no URL,
            or whose FIFO queue it names no ``__group_by__`` for.
    """
    if result.container.is_registered(JobService):
        return ()
    context = JobTransportContext(
        config=ctx,
        aws=aws,
        factory=result.factory,
        executor=result.executor,
        metrics=result.metrics,
        observability=observability_runtime,
        jobs=jobs,
        callbacks=DispatchableCallbacks(callbacks),
        settings=settings,
        timer=result.container.resolve(JobTimer)
        if result.container.is_registered(JobTimer)
        else None,
        can_wait_in_process=can_wait_in_process,
    )
    if transport is None:
        default, resources = _implicit(context)
        routes: Mapping[str, JobTransport] = {}
    else:
        default, routes, resources = _routed(context, transport)
    service = JobService(
        default,
        factory=context.factory,
        executor=context.executor,
        routes=routes,
        settings=context.settings,
        callbacks=context.callbacks,
    )
    result.container.register(JobService, lambda: service, scope=Scope.APPLICATION)
    return resources
