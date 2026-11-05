"""The deploy manifest: what each function of ``lambda.functions`` needs deployed.

``loom deploy manifest`` prints it for a deployment tool. It resolves no
secret, parameter or variable: a ``${secrets:...}`` or ``${ssm:...}``
placeholder appears by the name of the secret or parameter.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

import msgspec

from loom.composition.source import AppConfig, CompositionSource
from loom.core.config import ConfigContext, ConfigError, ConfigKey
from loom.core.config.keys import INTERPOLATION
from loom.core.config.loader import convert_section
from loom.core.config.secrets import SecretsManagerResolver
from loom.core.config.ssm import SsmResolver
from loom.core.discovery.worker import worker_discovery
from loom.core.observability.config import ObservabilityConfig
from loom.core.observability.runtime import ObservabilityRuntime
from loom.serverless.aws.lambda_.functions import (
    LambdaFunction,
    declared_lambda,
    http_function,
    named_function,
)

if TYPE_CHECKING:
    from loom.core.environment import Environment

MANIFEST_VERSION: Final = 1
"""Version of the manifest's schema; a change a consumer must handle bumps it."""

_HANDLER: Final = re.compile(r"[A-Za-z_][\w.]*\.\w+", re.ASCII)
_FUNCTION: Final = re.compile(r"[A-Za-z_]\w*", re.ASCII)
_OPENING: Final = re.compile(r"\$\{\s*(?:secrets|ssm)\s*:")
_REFERENCE: Final = re.compile(r"\$\{\s*(secrets|ssm)\s*:([^${}]+)\}")
_RESOLVERS: Final[dict[str, SecretsManagerResolver | SsmResolver]] = {
    "secrets": SecretsManagerResolver(),
    "ssm": SsmResolver(),
}


class Route(msgspec.Struct, frozen=True, kw_only=True):
    """One route an HTTP function serves, its path as published."""

    method: str
    path: str


class HttpEvent(msgspec.Struct, frozen=True, kw_only=True, tag="http", tag_field="type"):
    """The routes of the function's interfaces and the authentication it verifies, as written."""

    routes: tuple[Route, ...]
    auth: dict[str, Any] | None


class SqsEvent(msgspec.Struct, frozen=True, kw_only=True, tag="sqs", tag_field="type"):
    """The key of ``sqs.queues`` the function consumes and its event source mapping."""

    queue: str
    batch_size: int | None
    batching_window_s: int | None
    max_concurrency: int | None
    report_batch_item_failures: bool


class FunctionManifest(msgspec.Struct, frozen=True, kw_only=True):
    """What triggers one function and the secret and parameter names it resolves."""

    event: HttpEvent | SqsEvent
    secrets: tuple[str, ...]
    ssm_parameters: tuple[str, ...]


class DeployManifest(msgspec.Struct, frozen=True, kw_only=True):
    """Every function a configuration deploys, for one environment."""

    manifest_version: int
    environment: str
    handler: str
    code_path: str
    queues: dict[str, str]
    functions: dict[str, FunctionManifest]


def build_manifest(
    ctx: ConfigContext,
    environment: Environment,
    function: str | None = None,
    *,
    config_root: Path,
    code_path: Path,
) -> DeployManifest:
    """The manifest of every function of ``lambda.functions``, or of *function* alone.

    HTTP functions discover the application's code and list their routes as
    startup composes them.

    Raises:
        ConfigError: When ``lambda.handler`` is missing or malformed, no
            function is declared, a function or a placeholder cannot be
            named, a section the functions read is misdeclared, or an HTTP
            function would not compose at startup.
    """
    declared = declared_lambda(ctx)
    handler = _literal("lambda.handler", declared.handler)
    if not _HANDLER.fullmatch(handler):
        raise ConfigError(f"lambda.handler {handler!r} must be 'module.attribute'")
    if not declared.functions:
        raise ConfigError("lambda.functions declares no function to deploy")
    queues = convert_section(ctx.raw("sqs.queues", {}), "sqs.queues", dict[str, str])
    for queue, url in queues.items():
        _literal(f"sqs.queues.{queue}", url)
    app = convert_section(ctx.raw(ConfigKey.APP), ConfigKey.APP, AppConfig)
    source = CompositionSource(
        config=ctx,
        environment=environment,
        code_path=code_path,
        config_root=config_root,
        app=app,
        observability=ObservabilityRuntime.noop(),
        observability_config=ObservabilityConfig(),
    )
    document = ctx.document()
    names = sorted(declared.functions) if function is None else [function]
    return DeployManifest(
        manifest_version=MANIFEST_VERSION,
        environment=str(environment),
        handler=handler,
        code_path=app.code_path,
        queues=queues,
        functions={
            name: _function(source, document, declared.functions, name, queues) for name in names
        },
    )


def _literal(key: str, value: str | None) -> str:
    if value is None:
        raise ConfigError(f"{key} is missing: the manifest names it")
    if INTERPOLATION in value:
        raise ConfigError(f"{key} must be written as a value, not a placeholder")
    return value


def _function(
    source: CompositionSource,
    document: dict[str, Any],
    functions: Mapping[str, LambdaFunction],
    name: str,
    queues: Mapping[str, str],
) -> FunctionManifest:
    function = named_function(functions, name)
    if not _FUNCTION.fullmatch(name):
        raise ConfigError(f"lambda.functions.{name} must be named by a Python identifier")
    event = (
        _sqs_event(source.config, name, function, queues)
        if function.sqs
        else _http_event(source, functions, name)
    )
    secrets, parameters = _references(document, name)
    return FunctionManifest(event=event, secrets=secrets, ssm_parameters=parameters)


def _sqs_event(
    ctx: ConfigContext, name: str, function: LambdaFunction, queues: Mapping[str, str]
) -> SqsEvent:
    worker_discovery(ctx.raw("app.discovery", {}))
    if function.queue is None or function.queue not in queues:
        declared = ", ".join(sorted(queues)) or "none"
        raise ConfigError(
            f"lambda.functions.{name}.queue must name the queue it consumes, a key of "
            f"sqs.queues (declared: {declared})"
        )
    return SqsEvent(
        queue=function.queue,
        batch_size=function.batch_size,
        batching_window_s=function.batching_window_s,
        max_concurrency=function.max_concurrency,
        report_batch_item_failures=function.report_batch_item_failures is not False,
    )


def _http_event(
    source: CompositionSource, functions: Mapping[str, LambdaFunction], name: str
) -> HttpEvent:
    # The rest extra is needed only by an http function.
    from loom.rest.compiler import InterfaceCompilationError, published_routes
    from loom.rest.config import DisableRouteConfig, RestInterfaceConfig
    from loom.rest.fastapi.adapter import served_routes

    ctx = source.config
    interfaces = convert_section(
        ctx.raw("app.rest.interfaces", {}), "app.rest.interfaces", dict[str, RestInterfaceConfig]
    )
    disabled = convert_section(
        ctx.raw("app.rest.disable_routes", []),
        "app.rest.disable_routes",
        tuple[DisableRouteConfig, ...],
    )
    served = http_function(ctx, name, functions)
    try:
        _, sources = served_routes(source, interfaces, disabled, served.select)
        published = published_routes(sources)
    except (InterfaceCompilationError, RuntimeError) as exc:
        raise ConfigError(f"lambda.functions.{name} cannot be composed: {exc}") from exc
    routes = sorted(published, key=lambda route: (route[1], route[0]))
    return HttpEvent(
        routes=tuple(Route(method=method, path=path) for method, path in routes),
        auth=ctx.raw(served.auth_section or "app.rest.auth"),
    )


def _references(document: dict[str, Any], name: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """The secret and parameter names function *name* resolves, the other functions' left out."""
    found: dict[str, set[str]] = {resolver: set() for resolver in _RESOLVERS}
    for path, text in _strings(document, (), name):
        references = _REFERENCE.findall(text)
        if len(references) != len(_OPENING.findall(text)):
            raise ConfigError(f"{'.'.join(path)} nests a placeholder in a secrets or ssm key")
        for resolver, key in references:
            found[resolver].add(_RESOLVERS[resolver].reference(_as_resolved(key)))
    named = sorted(found["secrets"]), sorted(found["ssm"])
    wildcard = next((ref for refs in named for ref in refs if "*" in ref or "?" in ref), None)
    if wildcard is not None:
        raise ConfigError(f"the secret or parameter {wildcard!r} holds a wildcard, '*' or '?'")
    return tuple(named[0]), tuple(named[1])


def _as_resolved(key: str) -> str:
    """*key* as OmegaConf hands it to the resolver: stripped, one pair of quotes removed."""
    key = key.strip()
    quoted = len(key) >= 2 and key[0] == key[-1] and key[0] in "'\""
    return key[1:-1] if quoted else key


def _strings(
    node: object, path: tuple[str, ...], name: str
) -> Iterator[tuple[tuple[str, ...], str]]:
    """Every string of *node* with its path, but in the sections of functions other than *name*."""
    if isinstance(node, str):
        yield path, node
    elif isinstance(node, dict):
        for key, item in node.items():
            if path != ("lambda", "functions") or key == name:
                yield from _strings(item, (*path, str(key)), name)
    elif isinstance(node, list):
        for index, item in enumerate(node):
            yield from _strings(item, (*path, str(index)), name)


__all__ = [
    "MANIFEST_VERSION",
    "DeployManifest",
    "FunctionManifest",
    "HttpEvent",
    "Route",
    "SqsEvent",
    "build_manifest",
]
