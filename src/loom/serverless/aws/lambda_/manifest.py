"""The deploy manifest: what each function of ``lambda.functions`` needs deployed.

loom ships no infrastructure code. ``loom deploy manifest`` prints this
document, and a deployment tool (a CDK construct, for instance) turns each
function into a Lambda, its event source and its permissions.

The manifest reads the configuration as written and resolves nothing: a
``${secrets:...}`` or ``${ssm:...}`` placeholder appears by the name of the
secret or parameter, never by its value. A function lists the placeholders of
the whole configuration except those of the other functions' sections, which it
never resolves.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING, Any, Final

import msgspec

from loom.core.checks import MODULES_KEY
from loom.core.config import ConfigContext, ConfigError
from loom.core.config._resolver_utils import _split_resolver_key
from loom.core.config.keys import INTERPOLATION
from loom.core.config.loader import convert_section
from loom.core.discovery.config import DiscoveryConfig, decode_discovery, discover
from loom.core.environment import ENVIRONMENT_VARIABLE, OVERRIDE_PREFIX, Environment
from loom.core.locator import DEFAULT_CODE_PATH
from loom.core.persistence.stores import NONE_BACKEND, load_catalog
from loom.serverless.aws.lambda_.functions import (
    LambdaConfig,
    LambdaFunction,
    declared_function,
    declared_lambda,
    http_function,
)

if TYPE_CHECKING:
    from loom.rest.model import RestInterface

MANIFEST_VERSION: Final = 1
"""Version of the manifest's schema; a change a consumer must handle bumps it."""

ENTRY_VARIABLE: Final = f"{OVERRIDE_PREFIX}APP__ENTRY"
"""The variable through which the shared handler serves one function."""

_REFERENCE: Final = re.compile(r"\$\{(secrets|ssm):([^${}]+)\}")
_OPENING: Final = re.compile(r"\$\{(?:secrets|ssm):")
_FUNCTIONS: Final = ("lambda", "functions")


class DeployManifestError(ConfigError):
    """The configuration lacks something the manifest must name."""


class Route(msgspec.Struct, frozen=True, kw_only=True):
    """One route an HTTP function serves, its path as published."""

    method: str
    path: str


class HttpEvent(msgspec.Struct, frozen=True, kw_only=True, tag="http", tag_field="type"):
    """API Gateway events for the routes of the function's interfaces."""

    routes: tuple[Route, ...]


class SqsEvent(msgspec.Struct, frozen=True, kw_only=True, tag="sqs", tag_field="type"):
    """The SQS queue the function consumes and its event source mapping."""

    queue: str
    url: str
    batch_size: int | None
    batching_window_s: int | None
    max_concurrency: int | None
    report_batch_item_failures: bool


class FunctionManifest(msgspec.Struct, frozen=True, kw_only=True):
    """One function: what runs, what triggers it and what it reaches.

    Attributes:
        handler: The ``lambda.handler`` every function runs.
        variables: Environment variables the deployment sets: the function
            served and the environment.
        event: What triggers the function.
        auth: The authentication section an HTTP function verifies tokens
            with, as written.
        secrets: Secrets Manager secret names the configuration resolves.
        ssm_parameters: SSM parameter names the configuration resolves.
        stores: Backend of each store the application composes.
        queues: URL of each queue the application may send jobs to, as written.
        code_path: ``app.code_path``, relative to the configuration.
        modules: Modules the function imports at startup: discovery,
            ``checks.modules`` and a custom ``app.job_service``.
    """

    handler: str
    variables: dict[str, str]
    event: HttpEvent | SqsEvent
    auth: dict[str, Any] | None
    secrets: tuple[str, ...]
    ssm_parameters: tuple[str, ...]
    stores: dict[str, str]
    queues: dict[str, str]
    code_path: str
    modules: tuple[str, ...]


class DeployManifest(msgspec.Struct, frozen=True, kw_only=True):
    """Every function a configuration deploys, for one environment."""

    manifest_version: int
    environment: str
    functions: dict[str, FunctionManifest]


def build_manifest(
    ctx: ConfigContext, environment: Environment, function: str | None = None
) -> DeployManifest:
    """The manifest of every function of ``lambda.functions``, or of *function* alone.

    HTTP functions import the application's code to list their routes, as
    startup discovers them.

    Raises:
        DeployManifestError: When ``lambda.handler`` is missing, no function
            is declared, an sqs function names no queue of ``sqs.queues``, or
            a secrets or ssm placeholder nests another placeholder.
        ConfigError: When ``lambda``, ``stores``, ``sqs.queues``,
            ``app.discovery`` or ``app.rest`` is misdeclared, or *function*
            is not declared.
    """
    declared = declared_lambda(ctx)
    if declared.handler is None:
        raise DeployManifestError(
            "lambda.handler is missing: name the handler every function runs, e.g. entry.handler"
        )
    if not declared.functions:
        raise DeployManifestError("lambda.functions declares no function to deploy")
    if function is not None:
        declared_function(ctx, function)
    names = sorted(declared.functions) if function is None else [function]
    shared = _Shared.read(ctx)
    routes = _RouteReader(ctx, declared.functions)
    return DeployManifest(
        manifest_version=MANIFEST_VERSION,
        environment=str(environment),
        functions={
            name: _function(ctx, environment, declared, name, shared, routes) for name in names
        },
    )


class _Shared(msgspec.Struct, frozen=True, kw_only=True):
    stores: dict[str, str]
    queues: dict[str, str]
    code_path: str
    extra_modules: tuple[str, ...]

    @classmethod
    def read(cls, ctx: ConfigContext) -> _Shared:
        stores = {
            str(key): spec.backend
            for key, spec in load_catalog(ctx).specs.items()
            if spec.backend != NONE_BACKEND
        }
        queues = convert_section(ctx.raw("sqs.queues", {}), "sqs.queues", dict[str, str])
        job_service = ctx.raw("app.job_service")
        custom = _custom_module(job_service) if isinstance(job_service, str) else ()
        checks = convert_section(ctx.raw(MODULES_KEY, []), MODULES_KEY, tuple[str, ...])
        return cls(
            stores=stores,
            queues=queues,
            code_path=convert_section(
                ctx.raw("app.code_path", DEFAULT_CODE_PATH), "app.code_path", str
            ),
            extra_modules=(*checks, *custom),
        )


def _custom_module(job_service: str) -> tuple[str, ...]:
    """The module of a ``module:Class`` job service; a placeholder names none to package."""
    if ":" not in job_service or INTERPOLATION in job_service:
        return ()
    return (job_service.partition(":")[0],)


class _RouteReader:
    """Lists an HTTP function's routes; discovers the interfaces once, on first use."""

    def __init__(self, ctx: ConfigContext, functions: Mapping[str, LambdaFunction]) -> None:
        self._ctx = ctx
        self._functions = functions
        self._named: dict[str, tuple[type[RestInterface[Any]], ...]] | None = None
        self._python: frozenset[type[RestInterface[Any]]] = frozenset()
        self._disabled: frozenset[tuple[str, str]] = frozenset()

    def routes(self, name: str) -> tuple[Route, ...]:
        named = self._discover() if self._named is None else self._named
        select = http_function(self._ctx, name, self._functions).select
        assert select is not None  # http_function always narrows to the function's interfaces
        routes = (route for interface in select(named) for route in self._of(interface))
        return tuple(sorted(routes, key=lambda route: (route.path, route.method)))

    def _of(self, interface: type[RestInterface[Any]]) -> Iterator[Route]:
        from loom.rest.compiler import full_path

        for declared in interface.routes:
            route = Route(method=declared.method.upper(), path=full_path(interface, declared))
            # app.rest.disable_routes drops Python-declared routes only.
            if interface not in self._python or (route.method, route.path) not in self._disabled:
                yield route

    def _discover(self) -> dict[str, tuple[type[RestInterface[Any]], ...]]:
        # The rest extra is needed only by an http function.
        from loom.rest.config import (
            DisableRouteConfig,
            RestInterfaceConfig,
            build_interfaces_from_config,
            validate_disable_routes_config,
            validate_interfaces_config,
        )
        from loom.rest.fastapi.adapter import _interfaces_by_name

        written = self._ctx.raw("app.rest.interfaces", {})
        validate_interfaces_config(written)
        interfaces = convert_section(written, "app.rest.interfaces", dict[str, RestInterfaceConfig])
        disabled = self._ctx.raw("app.rest.disable_routes", [])
        validate_disable_routes_config(disabled)
        entries = convert_section(
            disabled, "app.rest.disable_routes", tuple[DisableRouteConfig, ...]
        )
        python = discover(_discovery(self._ctx, sqs=False)).interfaces
        self._python = frozenset(python)
        self._disabled = frozenset((entry.method.upper(), entry.path) for entry in entries)
        self._named = _interfaces_by_name(
            python, interfaces, build_interfaces_from_config(interfaces)
        )
        return self._named


def _function(
    ctx: ConfigContext,
    environment: Environment,
    declared: LambdaConfig,
    name: str,
    shared: _Shared,
    routes: _RouteReader,
) -> FunctionManifest:
    function = declared.functions[name]
    secrets, parameters = _references(ctx, set(declared.functions) - {name})
    discovery = _discovery(ctx, sqs=function.sqs)
    app_auth = None if function.sqs else ctx.raw("app.rest.auth")
    return FunctionManifest(
        handler=str(declared.handler),
        variables={ENTRY_VARIABLE: name, ENVIRONMENT_VARIABLE: str(environment)},
        event=_sqs_event(name, function, shared.queues)
        if function.sqs
        else HttpEvent(routes=routes.routes(name)),
        auth=app_auth if function.auth is None else function.auth,
        secrets=secrets,
        ssm_parameters=parameters,
        stores=shared.stores,
        queues=shared.queues,
        code_path=shared.code_path,
        modules=tuple(sorted({*_discovery_modules(discovery), *shared.extra_modules})),
    )


def _sqs_event(name: str, function: LambdaFunction, queues: Mapping[str, str]) -> SqsEvent:
    if function.queue is None or function.queue not in queues:
        declared = ", ".join(sorted(queues)) or "none"
        raise DeployManifestError(
            f"lambda.functions.{name}.queue must name the queue it consumes, a key of "
            f"sqs.queues (declared: {declared})"
        )
    return SqsEvent(
        queue=function.queue,
        url=queues[function.queue],
        batch_size=function.batch_size,
        batching_window_s=function.batching_window_s,
        max_concurrency=function.max_concurrency,
        report_batch_item_failures=function.report_batch_item_failures is not False,
    )


def _discovery(ctx: ConfigContext, *, sqs: bool) -> DiscoveryConfig:
    """``app.discovery`` as the function's adapter decodes it: a worker scans ``modules``."""
    written = ctx.raw("app.discovery", {})
    if sqs:
        return decode_discovery(written, default_mode="modules", strict=False)
    return decode_discovery(written, strict=True)


def _discovery_modules(discovery: DiscoveryConfig) -> tuple[str, ...]:
    by_mode = {
        "interfaces": tuple(discovery.interfaces.modules),
        "modules": tuple(discovery.modules.include),
        "manifest": (discovery.manifest.module,),
    }
    return by_mode.get(discovery.mode, ())


def _references(ctx: ConfigContext, others: set[str]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """The secret and parameter names a function resolves: none of the *others*' sections.

    A key with a dotted tail reads a JSON value; the name is the key without the tail.
    """
    found: dict[str, set[str]] = {"secrets": set(), "ssm": set()}
    for path, text in _strings(ctx.document(), (), others):
        references = _REFERENCE.findall(text)
        if len(references) != len(_OPENING.findall(text)):
            raise DeployManifestError(
                f"{'.'.join(path)} nests a placeholder in a secrets or ssm key, "
                "which the manifest cannot name"
            )
        for resolver, key in references:
            found[resolver].add(_split_resolver_key(_as_resolved(key))[0])
    return tuple(sorted(found["secrets"])), tuple(sorted(found["ssm"]))


def _as_resolved(key: str) -> str:
    """*key* as OmegaConf hands it to the resolver: stripped, one pair of quotes removed."""
    key = key.strip()
    quoted = len(key) >= 2 and key[0] == key[-1] and key[0] in "'\""
    return key[1:-1] if quoted else key


def _strings(
    node: object, path: tuple[str, ...], others: set[str]
) -> Iterator[tuple[tuple[str, ...], str]]:
    if isinstance(node, str):
        yield path, node
    elif isinstance(node, dict):
        for key, item in node.items():
            if path == _FUNCTIONS and key in others:
                continue
            yield from _strings(item, (*path, str(key)), others)
    elif isinstance(node, list):
        for index, item in enumerate(node):
            yield from _strings(item, (*path, str(index)), others)


__all__ = [
    "MANIFEST_VERSION",
    "DeployManifest",
    "DeployManifestError",
    "FunctionManifest",
    "HttpEvent",
    "Route",
    "SqsEvent",
    "build_manifest",
]
