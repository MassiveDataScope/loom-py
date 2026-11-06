"""The Lambda entry point: serve the trigger the deployed function declares.

``create_app("config.yaml", adapter=LambdaAdapter())`` serves the function
``app.entry`` names, or ``function=``: API Gateway requests for an ``http``
trigger, SQS batches of loom jobs for ``jobs``, and the events of an
``eventbridge``, ``s3``, ``sns`` or ``schedule`` trigger, directly or
``via: sqs``. Without a declared function it serves HTTP, every interface.

An event trigger's job and the identity it runs as come from the
configuration only, never from the event. A direct trigger runs the jobs of
an event one after another and fails the invocation at the first that fails,
retryable or not, so Lambda retries the event, then sends it to the
on-failure destination: its jobs must be idempotent. Via SQS each message
runs as a ``jobs`` function runs one, reporting the messages to retry.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final, Protocol

from loom.composition import CompositionErrorCode, CompositionStateError, LoopHost, Recipe
from loom.core.config import ConfigError
from loom.core.errors import MalformedJobMessage
from loom.core.identity import Identity
from loom.core.identity.wire import encode_identity
from loom.core.job.envelope import JobEnvelope
from loom.core.job.outcome import failure_log_fields
from loom.core.logger import get_logger
from loom.serverless.aws.lambda_._refusals import refuse_unserved_config
from loom.serverless.aws.lambda_.functions import (
    EventTrigger,
    HttpTrigger,
    JobsTrigger,
    Trigger,
    served_trigger,
    source_of,
    trigger_job,
)
from loom.serverless.aws.lambda_.triggers import (
    Decoded,
    Decoder,
    eventbridge,
    parsed,
    s3,
    schedule,
    sns,
)
from loom.serverless.aws.lambda_.triggers.sqs import (
    SQSBatch,
    fail_in_span,
    jobs_recipe,
    open_host,
    run_envelope,
)

if TYPE_CHECKING:
    from loom.composition import Application, CompositionSource
    from loom.core.engine.compilable import Compilable
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.job.job import Job
    from loom.rest.auth import Authenticator

_log = get_logger(__name__)
_COMPONENT = "LambdaAdapter"
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


class _Serving(Protocol):
    def recipe(self, source: CompositionSource) -> Recipe: ...

    def bind(self, application: Application) -> LambdaHandler: ...


class LambdaAdapter:
    """Serve a composed application from AWS Lambda, as the deployed function's trigger asks.

    Swapping ``FastAPIAdapter()`` for ``LambdaAdapter()`` in ``create_app``
    serves the same routes, authentication and error bodies to API Gateway
    HTTP API (payload 2.0).

    Unlike other adapters, :meth:`bind` opens the application: Lambda calls
    ``create_app`` at Init, and the handler returned keeps it open across warm
    invocations. A failure to open propagates, so Init fails.

    One instance composes one application.

    Args:
        function: The function of ``lambda.functions`` to serve; the one
            ``app.entry`` names when omitted, so ``LOOM__APP__ENTRY`` still
            selects the function of an adapter built in code.
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
        self._serving: _Serving | None = None
        self._handler: LambdaHandler | None = None

    def recipe(self, source: CompositionSource) -> Recipe:
        """Compose what the function's trigger serves, with the Lambda pool profile.

        Raises:
            LambdaUnservedConfigError: When the configuration enables
                metrics or declares agents, before discovery; the manifest
                declares agents; or an ``http`` function mounts an SQL endpoint.
            CompositionStateError: When this instance already composed an
                application.
            ConfigError: When ``lambda.functions`` is misdeclared or names no
                *function*, an argument does not apply to its trigger, an
                event trigger's job is not discovered, declares no
                ``Input()`` command or requires another parameter, or for any
                reason the REST composition of an ``http`` function raises it.
        """
        if self._serving is not None:
            raise CompositionStateError(
                CompositionErrorCode.ADAPTER_ALREADY_COMPOSED.issue(component=_COMPONENT)
            )
        refuse_unserved_config(source, _COMPONENT)
        name, trigger = served_trigger(source.config, self._function, source.app.entry)
        self._serving = self._served(name, trigger)
        return self._serving.recipe(source)

    def bind(self, application: Application) -> LambdaHandler:
        """Open *application* once, at Init, and return the warm invocation handler.

        Raises:
            CompositionStateError: When :meth:`recipe` has not run on this
                instance, or it is already bound.
            ConfigError: When an authentication exclusion reaches a templated route.
            Exception: The failure to open the application, as itself.
        """
        if self._serving is None:
            raise CompositionStateError(
                CompositionErrorCode.ADAPTER_RECIPE_MISSING.issue(component=_COMPONENT)
            )
        if self._handler is not None:
            raise CompositionStateError(
                CompositionErrorCode.ADAPTER_ALREADY_COMPOSED.issue(component=_COMPONENT)
            )
        self._handler = self._serving.bind(application)
        return self._handler

    def close(self) -> None:
        """Close the bound handler, as :meth:`LambdaHandler.close` does."""
        if self._handler is not None:
            self._handler.close()

    def _served(self, name: str | None, trigger: Trigger | None) -> _Serving:
        if trigger is None or isinstance(trigger, HttpTrigger):
            if self._jobs or self._use_cases or self._metrics is not None:
                raise ConfigError(f"{_COMPONENT} jobs=, use_cases= and metrics= run jobs, not http")
            # The http stack (FastAPI, Starlette, Mangum) needs the lambda extra.
            from loom.serverless.aws.lambda_.triggers.http import HttpServing

            return HttpServing(_COMPONENT, name, self._authenticator)
        if self._authenticator is not None:
            raise ConfigError(f"{_COMPONENT} authenticator= applies to an http function only")
        assert name is not None  # only a declared function has a trigger
        return _JobsServing(name, trigger, self._jobs, self._use_cases, self._metrics)


@dataclass
class _JobsServing:
    """A ``jobs`` function, or an event trigger: its jobs composed, then its handler."""

    name: str
    trigger: JobsTrigger | EventTrigger
    jobs: tuple[type[Job[Any]], ...]
    use_cases: tuple[type[Compilable], ...]
    metrics: MetricsAdapter | None
    declared: dict[str, type[Job[Any]]] = field(default_factory=dict)

    def recipe(self, source: CompositionSource) -> Recipe:
        recipe, self.declared = jobs_recipe(
            source, _COMPONENT, self.jobs, self.use_cases, self.metrics
        )
        if isinstance(self.trigger, EventTrigger):
            trigger_job(self.name, self.trigger, tuple(self.declared.values()))
        return recipe

    def bind(self, application: Application) -> LambdaHandler:
        trigger, host = self.trigger, open_host(application)
        if not isinstance(trigger, EventTrigger):
            return SQSBatch(
                application,
                self.declared,
                host,
                report_failures=trigger.reports_batch_item_failures,
            )
        source = source_of(trigger)
        jobs = _EventJobs(trigger.job, _identity(self.name, source, trigger))
        if trigger.via == "sqs":
            decode = _VIA_SQS[source]
            return SQSBatch(
                application,
                self.declared,
                host,
                report_failures=trigger.reports_batch_item_failures,
                read=lambda body: jobs(decode(parsed(body, "the SQS message body"))),
                label=trigger.job,
            )
        job_type = self.declared[trigger.job]
        return _DirectEvents(application, host, job_type, _DIRECT[source], jobs, self.name)


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


@dataclass(frozen=True)
class _EventJobs:
    """The envelopes of the declared job for the payloads an event decodes to, with span ids."""

    job: str
    identity: dict[str, Any] | None

    def __call__(self, decoded: Sequence[Decoded]) -> tuple[tuple[JobEnvelope, str | None], ...]:
        return tuple(
            (
                JobEnvelope(
                    job=self.job,
                    payload=item.payload,
                    identity=self.identity,
                    trace_id=item.trace_id,
                ),
                item.event_id,
            )
            for item in decoded
        )


class _DirectEvents:
    """The warm invocation handler of a direct trigger: fails the event at its first failed job."""

    def __init__(
        self,
        application: Application,
        host: LoopHost,
        job_type: type[Job[Any]],
        decode: Decoder,
        jobs: _EventJobs,
        function: str,
    ) -> None:
        self._application = application
        self._host = host
        self._job_type = job_type
        self._decode = decode
        self._jobs = jobs
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
                job=self._jobs.job,
                error_class=type(exc).__name__,
                **failure_log_fields(exc),
            )
            raise

    def close(self) -> None:
        """Close the application and its loop; a second call does nothing."""
        self._host.close()

    def _envelopes(self, event: Any) -> tuple[tuple[JobEnvelope, str | None], ...]:
        try:
            return self._jobs(self._decode(event))
        except MalformedJobMessage as exc:
            fail_in_span(self._application, exc, self._jobs.job)
