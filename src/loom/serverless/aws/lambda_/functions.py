"""``lambda.functions``: several Lambdas served from one configuration.

``app.entry`` (or ``LOOM__APP__ENTRY``) names the function a deployment
serves, and its ``trigger`` what invokes it. An ``http`` function serves the
interfaces it lists, a Python one by its class name and a YAML one by its
``app.rest.interfaces`` key; a ``jobs`` function runs the loom jobs of a queue;
an ``eventbridge``, ``s3``, ``sns`` or ``schedule`` function runs the job it
declares for each event, invoked directly or ``via: sqs``.
"""

from __future__ import annotations

import dataclasses
import functools
import inspect
import warnings
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Annotated, Any, Literal

import msgspec

from loom.core.config import ConfigContext, ConfigError, ConfigKey
from loom.core.config.keys import INTERPOLATION
from loom.core.config.loader import convert_section
from loom.core.discovery.worker import _discover_compilables_from_config, worker_discovery
from loom.core.use_case.markers import _InputMarker

if TYPE_CHECKING:
    from loom.core.job.job import Job
    from loom.rest.fastapi.adapter import InterfaceSelector
    from loom.rest.model import RestInterface

_SECTION = "lambda.functions"
_BATCHING = ("batch_size", "batching_window_s", "max_concurrency", "report_batch_item_failures")
_RETRIES = ("maximum_retry_attempts", "maximum_event_age_s")
_MOVED = ("http", "sqs", "auth", "queue", *_BATCHING)
DEFAULT_MAXIMUM_RETRY_ATTEMPTS = 2
"""Retries of a direct trigger's failed event when undeclared, Lambda's own default."""
DEFAULT_MAXIMUM_EVENT_AGE_S = 21600
"""Age past which a direct trigger's event goes unrun to its destination, Lambda's default."""


class HttpTrigger(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
    tag="http",
    tag_field="source",
):
    """API Gateway requests to the ``interfaces`` listed.

    ``auth`` has the ``app.rest.auth`` schema and replaces it for the function.
    """

    interfaces: Annotated[tuple[str, ...], msgspec.Meta(min_length=1)]
    auth: dict[str, Any] | None = None


class _QueueMapping(
    msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True, tag_field="source"
):
    """The event source mapping of a function consuming a queue, for the deployment.

    ``queue`` is a key of ``sqs.queues`` and ``batch_size`` at most 10 on a FIFO
    queue. loom applies only ``report_batch_item_failures``, which ``false``
    turns into a failed invocation instead of a partial batch.
    """

    queue: str | None = None
    batch_size: Annotated[int, msgspec.Meta(ge=1, le=10000)] | None = None
    batching_window_s: Annotated[int, msgspec.Meta(ge=0, le=300)] | None = None
    max_concurrency: Annotated[int, msgspec.Meta(ge=2, le=1000)] | None = None
    report_batch_item_failures: bool | None = None

    @property
    def reports_batch_item_failures(self) -> bool:
        """Whether a failed message is reported alone; true by default."""
        return self.report_batch_item_failures is not False


class JobsTrigger(_QueueMapping, frozen=True, kw_only=True, tag="jobs"):
    """The ``JobEnvelope`` messages ``SQSJobService`` sends to a queue."""


class TriggerIdentity(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """The caller an event trigger's job runs as, written as values."""

    subject: Annotated[str, msgspec.Meta(min_length=1)]
    roles: tuple[str, ...] = ()
    attributes: dict[str, str] = msgspec.field(default_factory=dict)


class EventTrigger(_QueueMapping, frozen=True, kw_only=True):
    """Run ``job`` for each event of the source, as ``identity`` or anonymous.

    Invoked directly, the deployment configures ``maximum_retry_attempts``
    and ``maximum_event_age_s`` of the asynchronous invocation; ``via: sqs``,
    the event source mapping of the queue in between instead.
    """

    job: str
    identity: TriggerIdentity | None = None
    via: Literal["sqs"] | None = None
    maximum_retry_attempts: Annotated[int, msgspec.Meta(ge=0, le=2)] | None = None
    maximum_event_age_s: Annotated[int, msgspec.Meta(ge=60, le=21600)] | None = None


class EventBridgeTrigger(EventTrigger, frozen=True, kw_only=True, tag="eventbridge"):
    """An EventBridge rule's events: the job's payload is each event's ``detail``."""


class S3Trigger(EventTrigger, frozen=True, kw_only=True, tag="s3"):
    """An S3 bucket's notifications: one job per object, its payload an ``S3Object``."""


class SnsTrigger(EventTrigger, frozen=True, kw_only=True, tag="sns"):
    """An SNS topic's notifications: the job's payload is each ``Message``, a JSON object."""


class ScheduleTrigger(EventTrigger, frozen=True, kw_only=True, tag="schedule"):
    """An EventBridge Scheduler schedule: the job's payload is the schedule's input."""


Trigger = HttpTrigger | JobsTrigger | EventBridgeTrigger | S3Trigger | SnsTrigger | ScheduleTrigger


class LambdaFunction(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """One function: what invokes it."""

    trigger: Trigger


class LambdaConfig(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """The ``lambda`` section; ``handler`` is the import path every function's deployment runs."""

    handler: str | None = None
    functions: dict[str, LambdaFunction] = msgspec.field(default_factory=dict)


@dataclasses.dataclass(frozen=True)
class HttpFunction:
    """What an HTTP function changes in the REST composition; nothing by default."""

    auth_section: str | None = None
    select: InterfaceSelector | None = None


def source_of(trigger: msgspec.Struct) -> str:
    """The ``source`` *trigger* declares."""
    return str(type(trigger).__struct_config__.tag)


def declared_lambda(ctx: ConfigContext) -> LambdaConfig:
    """Decode ``lambda`` as written and refuse a function or an interface declared twice over.

    Raises:
        ConfigError: When a function declares its trigger outside ``trigger``
            or a key of another source, a trigger a queue or batching setting
            it does not consume, a retry setting via sqs or a placeholder in
            its ``job`` or ``identity``, a ``batch_size`` over 10 without a
            ``batching_window_s``, or an interface another function serves.
    """
    config = _unresolved(ctx)
    owners: dict[str, str] = {}
    for name, function in config.functions.items():
        trigger = function.trigger
        if not isinstance(trigger, HttpTrigger):
            _check_trigger(f"{_SECTION}.{name}.trigger", trigger)
            continue
        for interface in trigger.interfaces:
            owner = owners.setdefault(interface, name)
            if owner != name:
                raise ConfigError(
                    f"{interface!r} is served by both {_SECTION}.{owner} and {_SECTION}.{name}"
                )
    return config


def _check_trigger(where: str, trigger: JobsTrigger | EventTrigger) -> None:
    queued = not isinstance(trigger, EventTrigger) or trigger.via == "sqs"
    mapped = next((key for key in ("queue", *_BATCHING) if getattr(trigger, key) is not None), None)
    if mapped is not None and not queued:
        raise ConfigError(f"{where}.{mapped} applies to a trigger via sqs only")
    # AWS: a batch size over 10 needs MaximumBatchingWindowInSeconds of at least 1.
    if (trigger.batch_size or 0) > 10 and not trigger.batching_window_s:
        raise ConfigError(f"{where}.batch_size over 10 needs batching_window_s >= 1")
    if not isinstance(trigger, EventTrigger):
        return
    retry = next((key for key in _RETRIES if getattr(trigger, key) is not None), None)
    if retry is not None and queued:
        raise ConfigError(f"{where}.{retry} applies to a direct trigger only")
    identity = trigger.identity
    written = [trigger.job]
    if identity is not None:
        written += [identity.subject, *identity.roles, *identity.attributes.values()]
    if any(INTERPOLATION in value for value in written):
        raise ConfigError(f"{where}: job and identity must be written as values, not placeholders")


def _unresolved(ctx: ConfigContext) -> LambdaConfig:
    """``lambda`` as written: another function's ``auth`` may name secrets this one cannot read.

    Only the function served resolves its own ``auth``, through its section path.

    Raises:
        ConfigError: When a function declares a key of the trigger outside it.
    """
    raw = ctx.raw(ConfigKey.LAMBDA)
    if raw is None:
        return LambdaConfig()
    functions = raw.get("functions") if isinstance(raw, Mapping) else None
    for name, function in (functions if isinstance(functions, Mapping) else {}).items():
        moved = [key for key in _MOVED if isinstance(function, Mapping) and key in function]
        if moved:
            raise ConfigError(
                f"{_SECTION}.{name}.{moved[0]}: a function declares what invokes it under "
                "'trigger', such as trigger: {source: http, interfaces: [...]} or "
                "trigger: {source: jobs, queue: ...}"
            )
    return convert_section(raw, ConfigKey.LAMBDA, LambdaConfig)


def served_trigger(
    ctx: ConfigContext, function: str | None, entry: str | None
) -> tuple[str | None, Trigger | None]:
    """The function served and its trigger: *function*, else the one *entry* names.

    ``(None, None)`` when neither names a declared function: HTTP, every interface.

    Raises:
        ConfigError: When *function* names no declared function, or
            ``lambda.functions`` is misdeclared.
    """
    functions = declared_lambda(ctx).functions
    name = function if function is not None or entry not in functions else entry
    if name is None:
        return None, None
    return name, named_function(functions, name).trigger


def http_function(
    ctx: ConfigContext, name: str | None, functions: Mapping[str, LambdaFunction] | None = None
) -> HttpFunction:
    """The authentication and interfaces of the HTTP function *name*, of *functions* if decoded.

    Every interface, with ``app.rest.auth``, when *name* is ``None``.

    Raises:
        ConfigError: When no HTTP function is called *name*, or
            ``lambda.functions`` is misdeclared.
    """
    if name is None:
        return HttpFunction()
    functions = declared_lambda(ctx).functions if functions is None else functions
    trigger = named_function(functions, name).trigger
    if not isinstance(trigger, HttpTrigger):
        raise ConfigError(f"{_SECTION}.{name} is a {source_of(trigger)} function, not an http one")
    return HttpFunction(
        auth_section=None if trigger.auth is None else f"{_SECTION}.{name}.trigger.auth",
        select=functools.partial(_served_by, functions, name),
    )


def named_function(functions: Mapping[str, LambdaFunction], name: str) -> LambdaFunction:
    """The function *name* of *functions*.

    Raises:
        ConfigError: When no function is called *name*.
    """
    function = functions.get(name)
    if function is None:
        declared = ", ".join(functions) or "none"
        raise ConfigError(f"{_SECTION} declares no function {name!r} (declared: {declared})")
    return function


def trigger_job(name: str, trigger: EventTrigger, jobs: Sequence[type[Job[Any]]]) -> type[Job[Any]]:
    """The job of *jobs* the event trigger of function *name* runs.

    Raises:
        ConfigError: When no job of *jobs* is called so, or its ``execute``
            declares no ``Input()`` command or requires another parameter,
            which an event cannot give.
    """
    where = f"{_SECTION}.{name}.trigger.job"
    job_type = next((job for job in jobs if job.__qualname__ == trigger.job), None)
    if job_type is None:
        raise ConfigError(f"{where} names {trigger.job!r}, which is no discovered Job")
    parameters = [
        parameter
        for parameter in inspect.signature(job_type.execute).parameters.values()
        if parameter.name != "self"
        and parameter.kind not in (parameter.VAR_POSITIONAL, parameter.VAR_KEYWORD)
    ]
    if not any(isinstance(parameter.default, _InputMarker) for parameter in parameters):
        raise ConfigError(f"{where}: {trigger.job}.execute declares no Input() command")
    required = [parameter.name for parameter in parameters if parameter.default is parameter.empty]
    if required:
        raise ConfigError(
            f"{where}: {trigger.job}.execute requires {', '.join(required)}, "
            "which an event cannot give"
        )
    return job_type


def check_triggers(ctx: ConfigContext) -> None:
    """Refuse, as startup would, a misdeclared ``lambda`` section or an event trigger's job.

    The jobs are discovered from ``app.discovery`` as written: nothing resolves.

    Raises:
        ConfigError: When ``lambda.functions`` is misdeclared, the jobs
            cannot be discovered, or :func:`trigger_job` refuses one.
    """
    events = {
        name: function.trigger
        for name, function in declared_lambda(ctx).functions.items()
        if isinstance(function.trigger, EventTrigger)
    }
    if not events:
        return
    try:
        discovery = worker_discovery(ctx.raw("app.discovery", {}))
        jobs = _discover_compilables_from_config(discovery).jobs
    except (ImportError, RuntimeError, ValueError) as exc:
        raise ConfigError(f"the jobs of the event triggers cannot be discovered: {exc}") from exc
    for name, trigger in events.items():
        trigger_job(name, trigger, jobs)


def _served_by(
    functions: Mapping[str, LambdaFunction],
    name: str,
    named: Mapping[str, tuple[type[RestInterface[Any]], ...]],
) -> tuple[type[RestInterface[Any]], ...]:
    listed = {
        owner: function.trigger.interfaces
        for owner, function in functions.items()
        if isinstance(function.trigger, HttpTrigger)
    }
    for owner, interfaces in listed.items():
        for interface in interfaces:
            where = f"{_SECTION}.{owner}.trigger.interfaces"
            _check_reference(where, interface, named.get(interface, ()))
    unserved = sorted(set(named) - {interface for found in listed.values() for interface in found})
    if unserved:
        warnings.warn(
            f"No function of {_SECTION} serves {', '.join(unserved)}", UserWarning, stacklevel=2
        )
    return tuple(named[interface][0] for interface in listed[name])


def _check_reference(
    where: str, interface: str, found: tuple[type[RestInterface[Any]], ...]
) -> None:
    if not found:
        raise ConfigError(
            f"{where} names {interface!r}: no discovered RestInterface "
            "and no app.rest.interfaces key is called so"
        )
    if len(found) > 1:
        origins = ", ".join(sorted(f"{cls.__module__}.{cls.__qualname__}" for cls in found))
        raise ConfigError(f"{where} names {interface!r}, which is ambiguous: {origins}")
