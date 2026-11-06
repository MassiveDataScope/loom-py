"""The Lambda entry point: serve the trigger the deployed function declares.

An event trigger's job and identity come from the configuration only. A
direct trigger fails the invocation at the first job that fails, so Lambda
retries the event, then sends it to the on-failure destination; via SQS every
failed message is reported, so it is retried, then dead-lettered.
"""

from __future__ import annotations

import functools
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final, Protocol

from loom.composition import CompositionErrorCode, CompositionStateError, LoopHost, Recipe
from loom.core.config import ConfigError
from loom.core.errors import MalformedJobMessage
from loom.core.identity import Identity
from loom.core.identity.wire import encode_identity
from loom.core.job.envelope import JobEnvelope
from loom.core.job.outcome import failure_log_fields
from loom.core.logger import get_logger
from loom.serverless.aws.lambda_._jobs import (
    Read,
    fail_in_span,
    jobs_recipe,
    open_host,
    run_envelope,
)
from loom.serverless.aws.lambda_._refusals import COMPONENT, refuse_unserved_config
from loom.serverless.aws.lambda_.checks import trigger_job
from loom.serverless.aws.lambda_.functions import (
    EventTrigger,
    HttpTrigger,
    JobsTrigger,
    served_trigger,
    source_of,
)
from loom.serverless.aws.lambda_.triggers import Decoder, eventbridge, parsed, s3, schedule, sns
from loom.serverless.aws.lambda_.triggers.sqs import QueueSource, SQSBatch

if TYPE_CHECKING:
    from loom.composition import Application, CompositionSource
    from loom.core.engine.compilable import Compilable
    from loom.core.engine.metrics import MetricsAdapter
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
    """The Lambda function handler :meth:`LambdaAdapter.bind` returns, owning its application."""

    def __call__(self, event: dict[str, Any], context: Any) -> Any:
        """Answer one invocation."""
        ...

    def close(self) -> None:
        """Close the application and its loop, from a shutdown hook or a test; idempotent."""
        ...


_Bind = Callable[["Application"], LambdaHandler]


class LambdaAdapter:
    """Serve a composed application from AWS Lambda, as the deployed function's trigger asks.

    Swapping ``FastAPIAdapter()`` for ``LambdaAdapter()`` in ``create_app``
    serves the same routes, authentication and error bodies to API Gateway
    HTTP API (payload 2.0); without a declared function, every interface.

    Unlike other adapters, :meth:`bind` opens the application: Lambda calls
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
        metrics: MetricsAdapter | None = None,
    ) -> None:
        self._function = function
        self._authenticator = authenticator
        self._jobs = tuple(jobs)
        self._use_cases = tuple(use_cases)
        self._metrics = metrics
        self._bind: _Bind | None = None
        self._handler: LambdaHandler | None = None

    def recipe(self, source: CompositionSource) -> Recipe:
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
                CompositionErrorCode.ADAPTER_ALREADY_COMPOSED.issue(component=COMPONENT)
            )
        refuse_unserved_config(source)
        name, trigger = served_trigger(source.config, self._function, source.app.entry)
        if trigger is None or isinstance(trigger, HttpTrigger):
            recipe, self._bind = self._compose_http(source, name)
        else:
            assert name is not None  # only a declared function has a trigger
            recipe, self._bind = self._compose_jobs(source, name, trigger)
        return recipe

    def bind(self, application: Application) -> LambdaHandler:
        """Open *application* once, at Init, and return the warm invocation handler.

        Raises:
            CompositionStateError: When :meth:`recipe` has not run on this
                instance, or it is already bound.
            ConfigError: When an authentication exclusion reaches a templated route.
            Exception: The failure to open the application, as itself.
        """
        if self._bind is None:
            raise CompositionStateError(
                CompositionErrorCode.ADAPTER_RECIPE_MISSING.issue(component=COMPONENT)
            )
        if self._handler is not None:
            raise CompositionStateError(
                CompositionErrorCode.ADAPTER_ALREADY_COMPOSED.issue(component=COMPONENT)
            )
        self._handler = self._bind(application)
        return self._handler

    def close(self) -> None:
        """Close the bound handler, as :meth:`LambdaHandler.close` does."""
        if self._handler is not None:
            self._handler.close()

    def _compose_http(self, source: CompositionSource, name: str | None) -> tuple[Recipe, _Bind]:
        if self._jobs or self._use_cases or self._metrics is not None:
            raise ConfigError(f"{COMPONENT} jobs=, use_cases= and metrics= run jobs, not http")
        # The http stack (FastAPI, Starlette, Mangum) needs the lambda extra.
        from loom.serverless.aws.lambda_.triggers.http import compose_http

        return compose_http(source, name, self._authenticator)

    def _compose_jobs(
        self, source: CompositionSource, name: str, trigger: JobsTrigger | EventTrigger
    ) -> tuple[Recipe, _Bind]:
        if self._authenticator is not None:
            raise ConfigError(f"{COMPONENT} authenticator= applies to an http function only")
        recipe, declared = jobs_recipe(source, self._jobs, self._use_cases, self._metrics)
        if not isinstance(trigger, EventTrigger):
            failures = trigger.reports_batch_item_failures
            return recipe, functools.partial(_bind_jobs, declared, failures)
        job_type = trigger_job(name, trigger, declared)
        if trigger.queued:
            return recipe, functools.partial(_bind_queued_events, name, trigger, job_type)
        return recipe, functools.partial(_bind_direct_events, name, trigger, job_type)


def _bind_jobs(
    declared: Mapping[str, type[Job[Any]]], report_failures: bool, application: Application
) -> LambdaHandler:
    """The SQS batch handler of a ``jobs`` function: a final failure is consumed."""
    return SQSBatch(application, declared, open_host(application), report_failures=report_failures)


def _bind_queued_events(
    name: str,
    trigger: EventTrigger,
    job_type: type[Job[Any]],
    application: Application,
) -> LambdaHandler:
    """The SQS batch handler of an event trigger via sqs: every failure is reported, then DLQ'd."""
    reader = _event_reader(name, trigger, _VIA_SQS)
    source = QueueSource(
        read=lambda body: reader(parsed(body, "the SQS message body")),
        label=trigger.job,
        retry_final=True,
        job=job_type,
    )
    failures = trigger.reports_batch_item_failures
    host = open_host(application)
    return SQSBatch(application, {}, host, report_failures=failures, source=source)


def _bind_direct_events(
    name: str, trigger: EventTrigger, job_type: type[Job[Any]], application: Application
) -> LambdaHandler:
    """The handler of a direct trigger: any failure fails the event, retried, then sent on."""
    read = _event_reader(name, trigger, _DIRECT)
    host = open_host(application)
    return _DirectEvents(application, host, job_type, read, trigger.job, name)


def _event_reader(name: str, trigger: EventTrigger, decoders: Mapping[str, Decoder]) -> Read:
    """Reads an event of *trigger*'s source into the envelopes of its job and identity."""
    source = source_of(trigger)
    decode, identity = decoders[source], _identity(name, source, trigger)

    def read(document: Any) -> tuple[tuple[JobEnvelope, str | None], ...]:
        return tuple(
            (JobEnvelope(job=trigger.job, payload=item.payload, identity=identity), item.event_id)
            for item in decode(document)
        )

    return read


def _identity(name: str, source: str, trigger: EventTrigger) -> dict[str, Any] | None:
    """The declared identity of *trigger*, encoded for its envelopes; ``None`` runs anonymous."""
    declared = trigger.identity
    if declared is None:
        return None
    return encode_identity(
        Identity(
            subject=declared.subject,
            roles=declared.roles,
            attributes=declared.attributes,
            mechanism=f"lambda:{name}:{source}",
        )
    )


class _DirectEvents:
    """The warm invocation handler of a direct trigger: fails the event at its first failed job."""

    def __init__(
        self,
        application: Application,
        host: LoopHost,
        job_type: type[Job[Any]],
        read: Read,
        label: str,
        function: str,
    ) -> None:
        self._application = application
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
                self._host.run(
                    run_envelope(self._application, envelope, self._job_type, span_id, context)
                )
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
