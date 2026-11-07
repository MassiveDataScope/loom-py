"""``lambda.functions``: several Lambdas served from one configuration.

``app.entry`` (or ``LOOM__APP__ENTRY``) names the function a deployment
serves, and its ``trigger`` what invokes it. An ``http`` function serves the
interfaces it lists, a Python one by its class name and a YAML one by its
``app.rest.interfaces`` key; a ``jobs`` function runs the loom jobs of a queue;
an ``eventbridge``, ``s3``, ``sns`` or ``schedule`` function runs the job it
declares for each event, invoked directly or through a ``queue``.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Annotated, Any

import msgspec

from loom.core.config import ConfigContext, ConfigError, ConfigKey
from loom.core.config.keys import INTERPOLATION
from loom.core.config.loader import convert_section
from loom.core.identity import RESERVED_ATTRIBUTE_PREFIX

_SECTION = "lambda.functions"
_RETRIES = ("maximum_retry_attempts", "maximum_event_age_s")
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


class QueueMapping(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """The event source mapping of a trigger whose messages arrive through a queue.

    ``name`` is a key of ``sqs.queues`` and ``batch_size`` at most 10 on a FIFO
    queue. loom applies only ``report_batch_item_failures``, which ``false``
    turns into a failed invocation instead of a partial batch.
    """

    name: str
    batch_size: Annotated[int, msgspec.Meta(ge=1, le=10000)] | None = None
    batching_window_s: Annotated[int, msgspec.Meta(ge=0, le=300)] | None = None
    max_concurrency: Annotated[int, msgspec.Meta(ge=2, le=1000)] | None = None
    report_batch_item_failures: bool | None = None

    @property
    def reports_batch_item_failures(self) -> bool:
        """Whether a failed message is reported alone; true by default."""
        return self.report_batch_item_failures is not False


class JobsTrigger(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
    tag="jobs",
    tag_field="source",
):
    """The ``JobEnvelope`` messages ``SQSJobService`` sends to a queue."""

    queue: QueueMapping | None = None

    @property
    def reports_batch_item_failures(self) -> bool:
        """Whether a failed message is reported alone; true without a ``queue`` block."""
        return self.queue is None or self.queue.reports_batch_item_failures


class TriggerIdentity(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """The caller an event trigger's job runs as, written as values."""

    subject: Annotated[str, msgspec.Meta(min_length=1)]
    roles: tuple[str, ...] = ()
    attributes: dict[str, str] = msgspec.field(default_factory=dict)


class EventTrigger(
    msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True, tag_field="source"
):
    """Run ``job`` for each event of the source, as the system ``identity`` declared here.

    The identity is required: no automatic change runs without an actor, and
    the audit log records it as the actor of every change the job makes.

    Invoked directly, the deployment configures ``maximum_retry_attempts``
    and ``maximum_event_age_s`` of the asynchronous invocation; with a
    ``queue`` block, the event source mapping in between instead.
    """

    job: str
    identity: TriggerIdentity
    queue: QueueMapping | None = None
    maximum_retry_attempts: Annotated[int, msgspec.Meta(ge=0, le=2)] | None = None
    maximum_event_age_s: Annotated[int, msgspec.Meta(ge=60, le=21600)] | None = None

    @property
    def queued(self) -> bool:
        """Whether the events arrive through a queue: a ``queue`` block is declared."""
        return self.queue is not None

    @property
    def via(self) -> str | None:
        """``"sqs"`` when the events arrive through a queue, else ``None``, for the manifest."""
        return "sqs" if self.queued else None

    @property
    def reports_batch_item_failures(self) -> bool:
        """Whether a failed message is reported alone; true without a ``queue`` block."""
        return self.queue is None or self.queue.reports_batch_item_failures

    def written_values(self) -> Iterator[str]:
        """The job and every identity value, to refuse a placeholder among them."""
        yield self.job
        yield self.identity.subject
        yield from self.identity.roles
        for name, value in self.identity.attributes.items():
            yield name
            yield value


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


def source_of(trigger: msgspec.Struct) -> str:
    """The ``source`` *trigger* declares: its tag in the union."""
    return str(type(trigger).__struct_config__.tag)


def declared_lambda(ctx: ConfigContext) -> LambdaConfig:
    """Decode ``lambda`` as written and refuse a function or an interface declared twice over.

    Raises:
        ConfigError: When a function declares a key outside ``trigger`` or
            one of another source, a queued trigger a retry key, a
            placeholder in its ``job`` or ``identity``, a ``batch_size``
            over 10 without a ``batching_window_s``, or an interface or a
            queue another function serves.
    """
    config = _unresolved(ctx)
    owners: dict[str, str] = {}
    for name, function in config.functions.items():
        for item in _served(f"{_SECTION}.{name}.trigger", function.trigger):
            owner = owners.setdefault(item, name)
            if owner != name:
                raise ConfigError(
                    f"{item} is served by both {_SECTION}.{owner} and {_SECTION}.{name}"
                )
    return config


def _served(where: str, trigger: Trigger) -> list[str]:
    """What *trigger* serves, which no other function may: its interfaces or its queue."""
    if isinstance(trigger, HttpTrigger):
        return [repr(interface) for interface in trigger.interfaces]
    if isinstance(trigger, EventTrigger):
        _check_event_trigger(where, trigger)
    _check_batching(where, trigger.queue)
    return [] if trigger.queue is None else [f"queue {trigger.queue.name!r}"]


def _check_batching(where: str, queue: QueueMapping | None) -> None:
    # AWS: a batch size over 10 needs MaximumBatchingWindowInSeconds of at least 1.
    if queue is None:
        return
    if (queue.batch_size or 0) > 10 and not queue.batching_window_s:
        raise ConfigError(f"{where}.queue.batch_size over 10 needs batching_window_s >= 1")


def _check_event_trigger(where: str, trigger: EventTrigger) -> None:
    if trigger.queued:
        for key in _RETRIES:
            if getattr(trigger, key) is not None:
                raise ConfigError(f"{where}.{key} applies to a direct trigger only")
    if any(INTERPOLATION in value for value in trigger.written_values()):
        raise ConfigError(f"{where}: job and identity must be written as values, not placeholders")
    reserved = sorted(
        n for n in trigger.identity.attributes if n.startswith(RESERVED_ATTRIBUTE_PREFIX)
    )
    if reserved:
        raise ConfigError(
            f"{where}.identity.attributes: {', '.join(reserved)} is reserved to loom "
            f"(the {RESERVED_ATTRIBUTE_PREFIX} prefix)"
        )


def _unresolved(ctx: ConfigContext) -> LambdaConfig:
    """``lambda`` as written: another function's ``auth`` may name secrets this one cannot read.

    Only the function served resolves its own ``auth``, through its section path.
    """
    raw = ctx.raw(ConfigKey.LAMBDA)
    if raw is None:
        return LambdaConfig()
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
