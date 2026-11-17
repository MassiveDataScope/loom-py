"""A function running jobs: composed with Lambda pool defaults, each job run within the invocation.

Shared by the ``jobs`` function, the queued event triggers and the direct ones.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NoReturn, cast

from loom.composition import LoopHost, Recipe
from loom.core.checks import CheckScope
from loom.core.config import ConfigError
from loom.core.connections.aws import AwsClients, aws_island
from loom.core.discovery.worker import resolve_worker_components
from loom.core.job.config import JobSettings
from loom.core.job.delivery import JobDelivery
from loom.core.job.envelope import JobEnvelope
from loom.core.job.outcome import JobOutcome
from loom.core.job.policy import Redelivery, RetryPolicy
from loom.core.job.run import EnvelopeInvocation
from loom.core.job.runner import JobRunner
from loom.core.job.service import JobService
from loom.core.job.settlement import JobSettlement
from loom.core.logger import get_logger
from loom.core.observability.event import Scope
from loom.core.stores import LAMBDA_PROFILE
from loom.jobs.aws.queue import DeadLetterPolicy, SqsRecord
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
        logs_job_retries=False,
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
    _log.info(
        "Lambda dispatched jobs go through the configured job service",
        job_service=type(service).__qualname__,
    )
    return host


@dataclass(frozen=True)
class QueueConsumer:
    """What a function consuming SQS needs to run and settle a batch.

    Attributes:
        settings: The options and retry policies its jobs run under.
        unserved: The retry policy of a message that names no job it serves.
        settlement: Settles each message by what its job came to.
        runner: Runs and decides each message, knowing what its queue redelivers.
    """

    settings: JobSettings
    unserved: RetryPolicy
    settlement: JobSettlement[SqsRecord]
    runner: JobRunner

    @classmethod
    def bind(
        cls, application: Application, queue_name: str, jobs: Iterable[type[Job[Any]]]
    ) -> QueueConsumer:
        """Check the queue *queue_name* at Init, and fix the policies *jobs* run under.

        The queue's dead-letter policy is read here, with the application's own client
        and its retries: ``GetQueueAttributes`` and ``GetQueueUrl`` once per cold
        start. An invocation never reads queue metadata. The application's own
        settings are untouched; one line logs the consumer's policies. A FIFO
        queue brings a message back unchanged and without a delay (``Redelivery.SAME``);
        any other waits the delay it is sent with (``Redelivery.DELAYED``).

        Raises:
            ConfigError: When ``sqs.queues`` maps no URL for *queue_name*, a job on a
                ``redrive`` queue has ``max_attempts`` in its own ``jobs.<Job>.retry``,
                or the SDK fails to read the queue, naming it.
            SQSQueueConfigError: When a ``redrive`` queue has no readable ``RedrivePolicy``.
        """
        config = SQSConfig.load(application.config)
        if queue_name not in config.queues:
            raise ConfigError(f"sqs.queues.{queue_name} is missing: a function consumes that queue")
        clients = application.kernel.container.resolve(AwsClients)
        settlement = SqsSettlement(clients, config, queue_name)
        queue = settlement.queue
        if queue.dead_letter_policy is DeadLetterPolicy.REDRIVE:
            try:
                queue.read_dead_letter(clients.client("sqs", config))
            except aws_island().sdk_errors() as exc:
                raise ConfigError(
                    f"cannot read queue {queue_name!r} of sqs.queues at Init: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
        served = list(jobs)
        base = application.job_settings
        settings = base.with_retries(queue.consumed_retries(served, lambda job: base.of(job).retry))
        redelivery = Redelivery.SAME if queue.is_fifo else Redelivery.DELAYED
        if served:
            _log.info(
                "Job retry policies",
                queue=queue_name,
                jobs=JobSettings({job: settings.of(job) for job in served}).describe_retries(
                    redelivery
                ),
            )
        return cls(
            settings,
            queue.unserved_policy(),
            settlement,
            job_runner(application, redelivery),
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


def job_runner(application: Application, redelivery: Redelivery) -> JobRunner:
    """The runner of *application*'s jobs on a transport that redelivers as *redelivery*."""
    kernel = application.kernel
    return JobRunner(
        EnvelopeInvocation(kernel.factory, kernel.executor, application.observability),
        redelivery=redelivery,
        metrics=kernel.metrics,
    )


async def run_envelope(
    runner: JobRunner,
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
        runner: Runs the job and decides what its run came to.
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
