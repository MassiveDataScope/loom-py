"""A function running jobs: composed with Lambda pool defaults, each job run within the invocation.

Shared by the ``jobs`` function, the queued event triggers and the direct ones.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from typing import TYPE_CHECKING, Any, NoReturn, cast

from loom.composition import LoopHost, Recipe
from loom.core.checks import CheckScope
from loom.core.config import ConfigError
from loom.core.connections.aws import AwsClients
from loom.core.discovery.worker import resolve_worker_components
from loom.core.job.config import JobSettings
from loom.core.job.delivery import JobDelivery
from loom.core.job.envelope import JobEnvelope
from loom.core.job.outcome import JobOutcome
from loom.core.job.run import EnvelopeInvocation
from loom.core.job.runner import JobRunner
from loom.core.job.service import InlineJobService, JobService
from loom.core.logger import get_logger
from loom.core.observability.event import Scope
from loom.core.stores import LAMBDA_PROFILE
from loom.jobs.aws.queue import SqsQueues
from loom.jobs.aws.settlement import SqsSettlement
from loom.jobs.aws.sqs import SQSConfig

if TYPE_CHECKING:
    from loom.composition import Application, Project
    from loom.core.engine.compilable import Compilable
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.job.job import Job

_log = get_logger(__name__)
TIMEOUT_SAFETY_MARGIN_MS = 1000
"""Stop starting jobs when fewer than one second remains in the invocation."""

Read = Callable[[Any], Sequence[tuple[JobEnvelope, str | None]]]
"""Reads a message body or an event into the jobs it runs, in order, each with its span id."""


def jobs_recipe(
    project: Project,
    jobs: Sequence[type[Job[Any]]],
    use_cases: Sequence[type[Compilable]],
    metrics: MetricsAdapter | None,
) -> tuple[Recipe, dict[str, type[Job[Any]]]]:
    """The recipe of a function running jobs, with Lambda pool defaults, and the jobs by name.

    AutoCRUD use cases for discovered models are not compiled. Agents are hosted for the
    use cases, without their HTTP endpoints.
    """
    resolved = resolve_worker_components(project.discovered, jobs, use_cases)
    if resolved.models:
        _log.warning(
            "Lambda ignores AutoCRUD use cases for discovered models",
            models=len(resolved.models),
        )
    recipe = Recipe(
        profile=LAMBDA_PROFILE,
        discovered=project.discovered.with_changes(
            models=tuple(dict.fromkeys(resolved.models)),
            use_cases=cast(Any, resolved.compilables),
            interfaces=(),
            jobs=resolved.jobs,
        ),
        check_scope=CheckScope.without_rest(),
        agents=True,
        agent_endpoints=False,
        metrics=metrics,
        connection_only=not resolved.models,
        states_attempt_limits=False,
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


def consumed_settings(
    application: Application, queue_name: str, jobs: Iterable[type[Job[Any]]]
) -> JobSettings:
    """Check the queue *queue_name* at Init and return the settings *jobs* run under.

    Only a consumer asks SQS about its queue. The settings are the application's
    with each of *jobs* holding the retry policy the queue implies; the
    application's own are untouched. One line logs the consumer's policies.

    Raises:
        ConfigError: When ``sqs.queues`` maps no URL for *queue_name*, or a job on a
            ``redrive`` queue has ``max_attempts`` in its own ``jobs.<Job>.retry``.
        SQSQueueConfigError: When a ``redrive`` queue has no ``RedrivePolicy``.
    """
    config = SQSConfig.load(application.config)
    if queue_name not in config.queues:
        raise ConfigError(f"sqs.queues.{queue_name} is missing: a function consumes that queue")
    client = application.kernel.container.resolve(AwsClients).client("sqs", config)
    queue = SqsQueues(client, config.queues).named(queue_name)
    base = application.job_settings
    retries = queue.consumed_retries(jobs, lambda job: base.of(job).retry)
    settings = base.with_retries(retries)
    _log.info(
        "Job retry policies of the consumed queue",
        queue=queue_name,
        jobs=JobSettings({job: settings.of(job) for job in retries}).describe_retries(),
    )
    return settings


def sqs_settlement(application: Application) -> SqsSettlement:
    """The settlement of the messages a function consumes.

    Raises:
        ConfigError: When the ``sqs`` section is invalid.
    """
    return SqsSettlement(
        application.kernel.container.resolve(AwsClients), SQSConfig.load(application.config)
    )


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
    task_id: str | None,
    context: Any,
    *,
    settings: JobSettings,
    attempt: int,
    deferrals: int,
) -> JobOutcome:
    """Run *envelope* as *job_type*, within its timeout and what remains of the invocation.

    Args:
        application: Whose kernel runs the job.
        envelope: The message to run.
        job_type: The job it runs as.
        task_id: The transport's id of the message.
        context: The Lambda context, for the time that remains.
        settings: The options and retry policy the function runs its jobs under.
        attempt: The attempt number the transport counted.
        deferrals: How many times the job asked to run again.

    Returns:
        What the run came to.
    """
    options = settings.of(job_type)
    runner = JobRunner(
        EnvelopeInvocation(
            application.kernel.factory, application.kernel.executor, application.observability
        )
    )
    return await runner.run(
        JobDelivery(
            job_type=job_type,
            envelope=envelope,
            task_id=task_id,
            attempt=attempt,
            deferrals=deferrals,
            policy=options.retry.policy,
            timeout_s=run_timeout(options.timeout, context),
        )
    )


def fail_in_span(application: Application, error: Exception, name: str, **meta: Any) -> NoReturn:
    """Raise *error* inside a failed job span, as a message or event that never ran its job."""
    with application.observability.span(Scope.JOB, name, **meta):
        raise error
