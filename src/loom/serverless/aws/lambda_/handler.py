"""The Lambda entry point: serve the trigger the deployed function declares.

An event trigger's job and identity come from the configuration only. A
direct trigger fails the invocation at the first job that fails, so Lambda
retries the event, then sends it to the on-failure destination; via SQS each
message is settled by what its job came to: a final failure is dead-lettered.
"""

from __future__ import annotations

import functools
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final, Protocol

from loom.composition import CompositionErrorCode, CompositionStateError, LoopHost, Recipe
from loom.core.config import ConfigError
from loom.core.errors import MalformedJobMessage
from loom.core.identity import ON_BEHALF_OF_ATTRIBUTE, Identity
from loom.core.identity.wire import encode_identity
from loom.core.job.envelope import JobEnvelope
from loom.core.job.outcome import Abandoned, Failed, failure_log_fields
from loom.core.job.policy import Redelivery
from loom.core.job.runner import JobRunner
from loom.core.logger import get_logger
from loom.serverless.aws.lambda_._jobs import (
    JobConsumer,
    QueueConsumer,
    Read,
    fail_in_span,
    job_runner,
    jobs_recipe,
    open_host,
)
from loom.serverless.aws.lambda_._refusals import (
    COMPONENT,
    check_resolved_ai,
    refuse_unserved_config,
)
from loom.serverless.aws.lambda_.functions import (
    EventTrigger,
    HttpTrigger,
    JobsTrigger,
    served_trigger,
    source_of,
)
from loom.serverless.aws.lambda_.trigger_jobs import trigger_job
from loom.serverless.aws.lambda_.triggers import (
    Decoded,
    Decoder,
    eventbridge,
    parsed,
    s3,
    schedule,
    sns,
)
from loom.serverless.aws.lambda_.triggers.sqs import QueueSource, SQSBatch

if TYPE_CHECKING:
    from loom.composition import Application, Project
    from loom.core.engine.compilable import Compilable
    from loom.core.engine.metrics import MetricsRecorder
    from loom.core.job.job import Job
    from loom.rest.auth import Authenticator

_log = get_logger(__name__)
_DIRECT: Final[Mapping[str, Decoder]] = {
    "eventbridge": eventbridge.decode,
    "s3": s3.decode,
    "sns": sns.decode,
    "schedule": schedule.decode,
}
_VIA_SQS: Final[Mapping[str, Decoder]] = {**_DIRECT, "sns": sns.decode_message}


class LambdaHandler(Protocol):
    """The Lambda function handler :meth:`LambdaRuntime.mount` returns, owning its application."""

    def __call__(self, event: dict[str, Any], context: Any) -> Any:
        """Answer one invocation."""
        ...

    def close(self) -> None:
        """Close the application and its loop, from a shutdown hook or a test; idempotent."""
        ...


_Bind = Callable[["Application"], LambdaHandler]


class LambdaRuntime:
    """Serve a composed application from AWS Lambda, as the deployed function's trigger asks.

    Swapping ``FastAPIRuntime()`` for ``LambdaRuntime()`` in ``create_app``
    serves the same routes, authentication and error bodies to API Gateway
    HTTP API (payload 2.0); without a declared function, every interface.

    Unlike other adapters, :meth:`mount` opens the application: Lambda calls
    ``create_app`` at Init, and the handler returned keeps it open across warm
    invocations. A failure to open propagates, so Init fails.

    One instance composes one application.

    Args:
        function: The function of ``lambda.functions`` to serve; the one
            ``app.entry`` names when omitted. Pin it in code for a function
            whose trigger declares an identity.
        authenticator: Custom authentication of an ``http`` function;
            requires ``custom: true`` in the authentication in effect.
        jobs: Explicit job classes of a function running jobs.
        use_cases: Explicit use case classes of a function running jobs.
        metrics: Receives the executor events of a function running jobs.
    """

    def __init__(
        self,
        *,
        function: str | None = None,
        authenticator: Authenticator | None = None,
        jobs: Sequence[type[Job[Any]]] = (),
        use_cases: Sequence[type[Compilable]] = (),
        metrics: MetricsRecorder | None = None,
    ) -> None:
        self._function = function
        self._authenticator = authenticator
        self._jobs = tuple(jobs)
        self._use_cases = tuple(use_cases)
        self._metrics = metrics
        self._bind: _Bind | None = None
        self._handler: LambdaHandler | None = None

    def recipe(self, project: Project) -> Recipe:
        """Compose what the function's trigger serves, with the Lambda pool profile.

        Raises:
            LambdaUnservedConfigError: When the configuration or the manifest
                declares what a Lambda function does not serve.
            CompositionStateError: When this instance already composed an
                application.
            ConfigError: When ``lambda.functions`` is misdeclared, an argument
                does not apply to the trigger, or its job or REST composition
                is refused.
        """
        if self._bind is not None:
            raise CompositionStateError(
                CompositionErrorCode.RUNTIME_ALREADY_COMPOSED.issue(component=COMPONENT)
            )
        refuse_unserved_config(project)
        name, trigger = served_trigger(project.config, self._function, project.app.entry)
        if trigger is None or isinstance(trigger, HttpTrigger):
            recipe, self._bind = self._compose_http(project, name)
        else:
            assert name is not None
            recipe, self._bind = self._compose_jobs(project, name, trigger)
        return recipe

    def mount(self, application: Application) -> LambdaHandler:
        """Open *application* once, at Init, and return the warm invocation handler.

        Raises:
            CompositionStateError: When :meth:`recipe` has not run on this
                instance, or it is already bound.
            ConfigError: When an authentication exclusion reaches a templated route.
            LambdaUnservedConfigError: When an MCP server resolves to ``transport: stdio``.
            Exception: The failure to open the application, as itself.
        """
        if self._bind is None:
            raise CompositionStateError(
                CompositionErrorCode.RUNTIME_RECIPE_MISSING.issue(component=COMPONENT)
            )
        if self._handler is not None:
            raise CompositionStateError(
                CompositionErrorCode.RUNTIME_ALREADY_COMPOSED.issue(component=COMPONENT)
            )
        check_resolved_ai(application.ai.config)
        self._handler = self._bind(application)
        return self._handler

    def close(self) -> None:
        """Close the bound handler, as :meth:`LambdaHandler.close` does."""
        if self._handler is not None:
            self._handler.close()

    def _compose_http(self, project: Project, name: str | None) -> tuple[Recipe, _Bind]:
        if self._jobs or self._use_cases or self._metrics is not None:
            raise ConfigError(f"{COMPONENT} jobs=, use_cases= and metrics= run jobs, not http")
        from loom.serverless.aws.lambda_.triggers.http import compose_http

        return compose_http(project, name, self._authenticator)

    def _compose_jobs(
        self, project: Project, name: str, trigger: JobsTrigger | EventTrigger
    ) -> tuple[Recipe, _Bind]:
        if self._authenticator is not None:
            raise ConfigError(f"{COMPONENT} authenticator= applies to an http function only")
        recipe, declared = jobs_recipe(project, self._jobs, self._use_cases, self._metrics)
        if not isinstance(trigger, EventTrigger):
            assert trigger.queue is not None
            return recipe, functools.partial(_bind_jobs, declared, trigger.queue.name)
        job_type = trigger_job(name, trigger, declared)
        if trigger.is_queued:
            return recipe, functools.partial(_bind_queued_events, name, trigger, job_type)
        return recipe, functools.partial(_bind_direct_events, name, trigger, job_type)


def _bind_jobs(
    declared: Mapping[str, type[Job[Any]]],
    queue_name: str,
    application: Application,
) -> LambdaHandler:
    """The SQS batch handler of a ``jobs`` function: a final failure is abandoned."""
    served = [
        job for job in declared.values() if application.job_settings.of(job).queue == queue_name
    ]
    consumer = QueueConsumer.bind(application, queue_name, served)
    return SQSBatch(application, declared, open_host(application), consumer)


def _bind_queued_events(
    name: str,
    trigger: EventTrigger,
    job_type: type[Job[Any]],
    application: Application,
) -> LambdaHandler:
    """The SQS batch handler of a queued event trigger: a final failure is abandoned."""
    assert trigger.queue is not None
    consumer = QueueConsumer.bind(application, trigger.queue.name, [job_type])
    reader = _event_reader(name, trigger, _VIA_SQS, trusts_requester=False)
    source = QueueSource(
        read=lambda body: reader(parsed(body, "the SQS message body")),
        label=trigger.job,
        job=job_type,
    )
    host = open_host(application)
    return SQSBatch(application, {}, host, consumer, source)


def _bind_direct_events(
    name: str, trigger: EventTrigger, job_type: type[Job[Any]], application: Application
) -> LambdaHandler:
    """The handler of a direct trigger: any failure fails the event, retried, then sent on."""
    read = _event_reader(name, trigger, _DIRECT, trusts_requester=True)
    host = open_host(application)
    runner = job_runner(application, Redelivery.SAME)
    return _DirectEvents(application, runner, host, job_type, read, trigger.job, name)


def _event_reader(
    name: str, trigger: EventTrigger, decoders: Mapping[str, Decoder], *, trusts_requester: bool
) -> Read:
    """Reads an event of *trigger*'s source into the envelopes of its job and identity.

    *trusts_requester* is true only when AWS delivers the event to the function
    itself. A queued event is an SQS message body: whoever may send to the queue
    writes it, the caller fields included, so it never names a requester.
    """
    source = source_of(trigger)
    decode = decoders[source]
    system = _identity(name, source, trigger, None)

    def identity_of(item: Decoded) -> dict[str, Any] | None:
        if not trusts_requester or item.requester is None:
            return system
        return _identity(name, source, trigger, item.requester)

    def read(document: Any) -> tuple[tuple[JobEnvelope, str | None], ...]:
        return tuple(
            (
                JobEnvelope(job=trigger.job, payload=item.payload, identity=identity_of(item)),
                item.event_id,
            )
            for item in decode(document)
        )

    return read


def _identity(
    name: str, source: str, trigger: EventTrigger, requester: str | None
) -> dict[str, Any] | None:
    """The declared system identity of *trigger*, encoded for its envelopes.

    It acts on behalf of *requester*, the caller AWS recorded for the event,
    when there is one: the only writer of ``loom:on_behalf_of``.
    """
    declared = trigger.identity
    attributes = dict(declared.attributes)
    if requester is not None:
        attributes[ON_BEHALF_OF_ATTRIBUTE] = requester
    return encode_identity(
        Identity(
            subject=declared.subject,
            roles=declared.roles,
            attributes=attributes,
            mechanism=f"lambda:{name}:{source}",
        )
    )


class _DirectEvents:
    """The warm invocation handler of a direct trigger: fails the event at its first failed job.

    Redelivery is ``Redelivery.SAME``: a ``JobPostponement`` is abandoned as one that cannot
    wait, and any other failure fails the invocation. Every run is attempt 1, so
    ``JOB_FAILED`` and ``JOB_ABANDONED`` are reported per invocation.
    """

    def __init__(
        self,
        application: Application,
        runner: JobRunner,
        host: LoopHost,
        job_type: type[Job[Any]],
        read: Read,
        label: str,
        function: str,
    ) -> None:
        self._application = application
        self._consumer = JobConsumer(application.job_settings, runner)
        self._host = host
        self._job_type = job_type
        self._read = read
        self._label = label
        self._function = function

    def __call__(self, event: dict[str, Any], context: Any) -> None:
        """Run the jobs of *event* in order; any failure fails the invocation.

        Raises:
            Exception: The first job's failure, or the event's malformation,
                as itself: Lambda retries the event, then sends it to the
                on-failure destination.
        """
        try:
            for envelope, span_id in self._envelopes(event):
                outcome = self._host.run(
                    self._consumer.run_envelope(
                        envelope,
                        self._job_type,
                        span_id,
                        context,
                        attempt=1,
                        postponements=0,
                    )
                )
                if isinstance(outcome, Failed | Abandoned):
                    raise outcome.as_exception()
        except Exception as exc:
            _log.exception(
                "Lambda event job failed; the event is retried, then sent to its destination",
                function=self._function,
                job=self._label,
                error_class=type(exc).__name__,
                **failure_log_fields(exc),
            )
            raise

    def close(self) -> None:
        """Close the application and its loop; a second call does nothing."""
        self._host.close()

    def _envelopes(self, event: Any) -> Sequence[tuple[JobEnvelope, str | None]]:
        try:
            return self._read(event)
        except MalformedJobMessage as exc:
            fail_in_span(self._application, exc, self._label)
