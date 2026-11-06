"""The Lambda entry point: serve the trigger the deployed function declares.

An event trigger's job and identity come from the configuration only. A
direct trigger fails the invocation at the first job that fails, so Lambda
retries the event, then sends it to the on-failure destination; via SQS every
failed message is reported, so it is retried, then dead-lettered.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, Protocol

from loom.composition import CompositionErrorCode, CompositionStateError, LoopHost, Recipe
from loom.core.config import ConfigError
from loom.core.errors import MalformedJobMessage
from loom.core.identity import Identity
from loom.core.identity.wire import encode_identity
from loom.core.job.envelope import JobEnvelope
from loom.core.job.outcome import failure_log_fields
from loom.core.logger import get_logger
from loom.serverless.aws.lambda_._refusals import COMPONENT, refuse_unserved_config
from loom.serverless.aws.lambda_.functions import (
    EventTrigger,
    HttpTrigger,
    JobsTrigger,
    Trigger,
    served_trigger,
    source_of,
    trigger_job,
)
from loom.serverless.aws.lambda_.triggers import Decoder, eventbridge, parsed, s3, schedule, sns
from loom.serverless.aws.lambda_.triggers.sqs import (
    Read,
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
        self._serving: _Serving | None = None
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
        if self._serving is not None:
            raise CompositionStateError(
                CompositionErrorCode.ADAPTER_ALREADY_COMPOSED.issue(component=COMPONENT)
            )
        refuse_unserved_config(source)
        name, trigger = served_trigger(source.config, self._function, source.app.entry)
        serving = self._served(name, trigger)
        recipe = serving.recipe(source)
        self._serving = serving
        return recipe

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
                CompositionErrorCode.ADAPTER_RECIPE_MISSING.issue(component=COMPONENT)
            )
        if self._handler is not None:
            raise CompositionStateError(
                CompositionErrorCode.ADAPTER_ALREADY_COMPOSED.issue(component=COMPONENT)
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
                raise ConfigError(f"{COMPONENT} jobs=, use_cases= and metrics= run jobs, not http")
            # The http stack (FastAPI, Starlette, Mangum) needs the lambda extra.
            from loom.serverless.aws.lambda_.triggers.http import HttpServing

            return HttpServing(name, self._authenticator)
        if self._authenticator is not None:
            raise ConfigError(f"{COMPONENT} authenticator= applies to an http function only")
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
    declared: Mapping[str, type[Job[Any]]] | None = None
    job_type: type[Job[Any]] | None = None

    def recipe(self, source: CompositionSource) -> Recipe:
        recipe, self.declared = jobs_recipe(source, self.jobs, self.use_cases, self.metrics)
        if isinstance(self.trigger, EventTrigger):
            self.job_type = trigger_job(self.name, self.trigger, tuple(self.declared.values()))
        return recipe

    def bind(self, application: Application) -> LambdaHandler:
        trigger, declared, host = self.trigger, self.declared or {}, open_host(application)
        failures = trigger.reports_batch_item_failures
        if not isinstance(trigger, EventTrigger):
            return SQSBatch(application, declared, host, report_failures=failures)
        source = source_of(trigger)
        identity = _identity(self.name, source, trigger)
        decode = (_VIA_SQS if trigger.queued else _DIRECT)[source]

        def envelopes(document: Any) -> tuple[tuple[JobEnvelope, str | None], ...]:
            return tuple(
                (
                    JobEnvelope(job=trigger.job, payload=item.payload, identity=identity),
                    item.event_id,
                )
                for item in decode(document)
            )

        if trigger.queued:
            return SQSBatch(
                application,
                declared,
                host,
                report_failures=failures,
                read=lambda body: envelopes(parsed(body, "the SQS message body")),
                label=trigger.job,
                retry_final=True,
            )
        assert self.job_type is not None  # set by recipe for an event trigger
        return _DirectEvents(application, host, self.job_type, envelopes, trigger.job, self.name)


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
