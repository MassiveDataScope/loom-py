"""The deploy manifest: what each function of ``lambda.functions`` needs deployed.

``loom deploy manifest`` prints it for a deployment tool. It resolves no
secret, parameter or variable: a ``${secrets:...}`` or ``${ssm:...}``
placeholder appears by the name of the secret or parameter.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from typing import Any, Final

import msgspec

from loom.composition.project import Project
from loom.core.config import ConfigError
from loom.core.config.keys import INTERPOLATION
from loom.core.config.loader import config_strings, convert_section
from loom.core.config.resolver import fetched_name, referenced
from loom.jobs.aws.queue import QueueConfig
from loom.serverless.aws.lambda_.functions import (
    DEFAULT_MAXIMUM_EVENT_AGE_S,
    DEFAULT_MAXIMUM_RETRY_ATTEMPTS,
    EventTrigger,
    HttpTrigger,
    JobsTrigger,
    LambdaFunction,
    SnsTrigger,
    declared_lambda,
    named_function,
    source_of,
)
from loom.serverless.aws.lambda_.interfaces import http_function
from loom.serverless.aws.lambda_.trigger_jobs import check_triggers

MANIFEST_VERSION: Final = 3
"""Version of the manifest's schema; a change a consumer must handle bumps it."""

_HANDLER: Final = re.compile(r"[A-Za-z_][\w.]*\.\w+", re.ASCII)
_FUNCTION: Final = re.compile(r"[A-Za-z_]\w*", re.ASCII)
_RESOLVERS: Final = ("secrets", "ssm")
_MAPPING_KEYS: Final = ("batch_size", "batching_window_s", "max_concurrency")


class FunctionManifest(msgspec.Struct, frozen=True, kw_only=True):
    """What triggers one function and the secret and parameter names it resolves."""

    source: str
    via: str | None
    event: dict[str, Any]
    secrets: tuple[str, ...]
    ssm_parameters: tuple[str, ...]


class DeployManifest(msgspec.Struct, frozen=True, kw_only=True):
    """Every function a configuration deploys, for one environment."""

    manifest_version: int
    environment: str
    handler: str
    code_path: str
    queues: dict[str, QueueConfig]
    functions: dict[str, FunctionManifest]


def build_manifest(project: Project, function: str | None = None) -> DeployManifest:
    """The manifest of every function of ``lambda.functions``, or of *function* alone.

    HTTP functions discover the application's code and list their routes as
    startup composes them.

    Args:
        project: The configuration as written, from ``CheckedProject.project``;
            its scan of ``app.packages`` is the only one.
        function: The one function to describe; every function by default.

    Raises:
        ConfigError: When ``lambda.handler`` is missing or malformed, no
            function is declared, a function or a placeholder cannot be
            named, a section the functions read is misdeclared, an event
            trigger names a job startup would refuse, or an HTTP function
            would not compose at startup.
    """
    ctx = project.config
    declared = declared_lambda(ctx)
    handler = _literal("lambda.handler", declared.handler)
    if not _HANDLER.fullmatch(handler):
        raise ConfigError(f"lambda.handler {handler!r} must be 'module.attribute'")
    if not declared.functions:
        raise ConfigError("lambda.functions declares no function to deploy")
    written = ctx.raw("sqs.queues", {})
    QueueConfig.check_written_shape(written, "sqs.queues")
    queues = convert_section(written, "sqs.queues", dict[str, QueueConfig])
    for queue, config in queues.items():
        _literal(f"sqs.queues.{queue}.url", config.url)
    check_triggers(project)
    document = ctx.document()
    names = sorted(declared.functions) if function is None else [function]
    return DeployManifest(
        manifest_version=MANIFEST_VERSION,
        environment=str(project.environment),
        handler=handler,
        code_path=project.app.code_path,
        queues=queues,
        functions={
            name: _function(project, document, declared.functions, name, queues) for name in names
        },
    )


def _literal(key: str, value: str | None) -> str:
    if value is None:
        raise ConfigError(f"{key} is missing: the manifest names it")
    if INTERPOLATION in value:
        raise ConfigError(f"{key} must be written as a value, not a placeholder")
    return value


def _function(
    project: Project,
    document: dict[str, Any],
    functions: Mapping[str, LambdaFunction],
    name: str,
    queues: Mapping[str, QueueConfig],
) -> FunctionManifest:
    function = named_function(functions, name)
    if not _FUNCTION.fullmatch(name):
        raise ConfigError(f"lambda.functions.{name} must be named by a Python identifier")
    trigger = function.trigger
    event: dict[str, Any]
    if isinstance(trigger, HttpTrigger):
        event = _http_event(project, functions, name)
    elif isinstance(trigger, EventTrigger) and not trigger.queued:
        event = _direct_event(trigger)
    else:
        event = _queue_event(name, trigger, queues)
    secrets, parameters = _references(document, name)
    return FunctionManifest(
        source=source_of(trigger),
        via=trigger.via if isinstance(trigger, EventTrigger) else None,
        event=event,
        secrets=secrets,
        ssm_parameters=parameters,
    )


def _queue_event(
    name: str, trigger: JobsTrigger | EventTrigger, queues: Mapping[str, QueueConfig]
) -> dict[str, Any]:
    """The key of ``sqs.queues`` the function consumes and its event source mapping.

    The queue needs a dead-letter queue, where failed messages end. A queued
    event trigger adds its ``job`` and ``identity``; an sns one,
    ``raw_message_delivery: false``, which its subscription must keep.
    """
    queue = trigger.queue
    assert queue is not None
    if queue.name not in queues:
        declared = ", ".join(sorted(queues)) or "none"
        raise ConfigError(
            f"lambda.functions.{name}.trigger.queue.name must name the queue it consumes, a key "
            f"of sqs.queues (declared: {declared})"
        )
    event: dict[str, Any] = {"queue": queue.name}
    event.update({key: getattr(queue, key) for key in _MAPPING_KEYS})
    event["report_batch_item_failures"] = True
    event["dead_letter_required"] = True
    if isinstance(trigger, EventTrigger):
        written = msgspec.to_builtins(trigger)
        event["job"] = written["job"]
        event["identity"] = written["identity"]
    if isinstance(trigger, SnsTrigger):
        event["raw_message_delivery"] = False
    return event


def _direct_event(trigger: EventTrigger) -> dict[str, Any]:
    """The job a direct trigger runs for each event, the identity it runs as, its retries.

    The asynchronous invocation requires an on-failure destination.
    """
    written = msgspec.to_builtins(trigger)
    retries = trigger.maximum_retry_attempts
    age = trigger.maximum_event_age_s
    return {
        "job": written["job"],
        "identity": written["identity"],
        "on_failure": {
            "maximum_retry_attempts": DEFAULT_MAXIMUM_RETRY_ATTEMPTS
            if retries is None
            else retries,
            "maximum_event_age_s": DEFAULT_MAXIMUM_EVENT_AGE_S if age is None else age,
        },
    }


def _http_event(
    project: Project, functions: Mapping[str, LambdaFunction], name: str
) -> dict[str, Any]:
    """The routes of the function's interfaces, as published, and the authentication it verifies."""
    from loom.rest.compiler import InterfaceCompilationError, published_routes
    from loom.rest.fastapi.adapter import served_routes

    served = http_function(project.config, name, functions)
    try:
        _, sources = served_routes(project, served.select)
        published = published_routes(sources)
    except (InterfaceCompilationError, RuntimeError) as exc:
        raise ConfigError(f"lambda.functions.{name} cannot be composed: {exc}") from exc
    routes = sorted(published, key=lambda route: (route[1], route[0]))
    return {
        "routes": [{"method": method, "path": path} for method, path in routes],
        "auth": project.config.raw(served.auth_section or "app.rest.auth"),
    }


def _references(document: dict[str, Any], name: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """The secret and parameter names function *name* resolves, the other functions' left out.

    Raises:
        ConfigError: When a placeholder nests another, or names a wildcard.
    """
    found: dict[str, set[str]] = {resolver: set() for resolver in _RESOLVERS}
    for path, text in _function_strings(document, name):
        try:
            references = referenced(text, _RESOLVERS)
        except ConfigError as exc:
            raise ConfigError(f"{'.'.join(path)} {exc}") from exc
        for resolver, keys in references.items():
            found[resolver].update(fetched_name(key) for key in keys)
    for reference in sorted(found["secrets"] | found["ssm"]):
        if "*" in reference or "?" in reference:
            raise ConfigError(f"the secret or parameter {reference!r} holds a wildcard, '*' or '?'")
    return tuple(sorted(found["secrets"])), tuple(sorted(found["ssm"]))


def _function_strings(document: dict[str, Any], name: str) -> Iterator[tuple[tuple[str, ...], str]]:
    """Every string of *document* with its path, the other functions' sections left out."""
    for path, text in config_strings(document):
        if path[:2] == ("lambda", "functions") and path[2:3] != (name,):
            continue
        yield path, text


__all__ = ["MANIFEST_VERSION", "DeployManifest", "FunctionManifest", "build_manifest"]
