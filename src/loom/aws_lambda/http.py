"""Serve a loom application from AWS Lambda behind API Gateway.

Install the ``lambda`` extra. The entry point changes one line::

    handler = create_app("config.yaml", adapter=LambdaHTTPAdapter())

The recipe is the FastAPI adapter's, so the configuration, the authentication,
discovery and the CRUD gating have one source of truth. The transport is a bare
Starlette application with one route per compiled route, reusing FastAPI's error
handlers and the router runtime: a response matches the FastAPI one by
construction, without FastAPI's routing or its OpenAPI.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Mapping
from typing import Any

from fastapi.exception_handlers import http_exception_handler
from fastapi.exceptions import RequestValidationError
from mangum import Mangum
from pydantic import TypeAdapter, ValidationError
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from loom.aws_lambda._refusals import LambdaUnservedConfigError
from loom.composition import (
    Application,
    ApplicationHost,
    CompositionErrorCode,
    CompositionSource,
    CompositionStateError,
    Recipe,
)
from loom.core.config import ConfigContext, ConfigKey
from loom.core.discovery.base import DiscoveryResult
from loom.core.engine.executor import RuntimeExecutor
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.persistence import LAMBDA_PROFILE
from loom.core.sql import SqlConfig
from loom.core.use_case.factory import UseCaseFactory
from loom.rest.auth import Authenticator
from loom.rest.compiler import CompiledRoute, RestInterfaceCompiler
from loom.rest.fastapi._auto_auth import _mount_authentication
from loom.rest.fastapi._errors import register_error_handlers
from loom.rest.fastapi._exclusions import verify_exclusion_paths
from loom.rest.fastapi.adapter import (
    FastAPIAdapter,
    _Declared,
    _mount_cors,
    _mount_request_guards,
)
from loom.rest.fastapi.app import _resolve_executor
from loom.rest.fastapi.router_runtime import (
    _build_route_runtime,
    _dispatch_route,
    _normalize_path_param_annotation,
)
from loom.rest.fastapi.sql import mounted_sql_endpoints

_COMPONENT = "LambdaHTTPAdapter"
_PATH_LOCATION = "path"


class LambdaHTTPAdapter:
    """Serve a composed application to API Gateway HTTP API (payload 2.0) from AWS Lambda.

    Swapping ``FastAPIAdapter()`` for ``LambdaHTTPAdapter()`` in
    ``create_app`` is the whole change: the same configuration and code serve
    the same routes, authentication and error bodies. Health, metrics, SQL
    endpoints, agents and OpenAPI are not served; a configuration declaring
    metrics, an SQL endpoint or agents is refused.

    Unlike other adapters, :meth:`bind` opens the application: Lambda calls
    ``create_app`` at Init, and the returned :class:`LambdaHTTPHandler` keeps
    it open across warm invocations.

    One instance composes one application.

    Args:
        authenticator: Custom authentication mechanism; requires
            ``app.rest.auth.custom: true``.
    """

    def __init__(self, *, authenticator: Authenticator | None = None) -> None:
        self._authenticator = authenticator
        self._declared: _Declared | None = None

    def recipe(self, source: CompositionSource) -> Recipe:
        """Compose as :meth:`FastAPIAdapter.recipe` does, with the Lambda pool profile.

        Raises:
            LambdaUnservedConfigError: When the configuration enables
                metrics, declares agents or mounts an SQL endpoint.
            CompositionStateError: When this instance already composed an
                application.
            ConfigError: For any reason :meth:`FastAPIAdapter.recipe` raises it.
        """
        if self._declared is not None:
            raise CompositionStateError(
                CompositionErrorCode.ADAPTER_ALREADY_COMPOSED.issue(component=_COMPONENT)
            )
        _refuse_metrics(source)
        _refuse_ai_section(source.config)
        rest = FastAPIAdapter(authenticator=self._authenticator)
        recipe = rest.recipe(source)
        _refuse_sql_endpoints(source.config)
        _refuse_manifest_agents(recipe.discovered)
        self._declared = rest._declared
        return dataclasses.replace(recipe, profile=LAMBDA_PROFILE, agents=False, pool_metrics=None)

    def bind(self, application: Application) -> LambdaHTTPHandler:
        """Mount the routes, error handlers and middleware, then open *application*.

        Raises:
            CompositionStateError: When :meth:`recipe` has not run on this instance.
            ConfigError: When an authentication exclusion reaches a templated route.
            Exception: The failure to open the application, as itself.
        """
        if self._declared is None:
            raise CompositionStateError(
                CompositionErrorCode.ADAPTER_RECIPE_MISSING.issue(component=_COMPONENT)
            )
        return LambdaHTTPHandler(_starlette_app(application, self._declared), application)


def _refuse_metrics(source: CompositionSource) -> None:
    if source.observability_config.prometheus.enabled:
        raise LambdaUnservedConfigError(
            "observability.prometheus.enabled",
            "a Lambda function serves no metrics scrape. Disable it for this entry point.",
        )


def _refuse_ai_section(ctx: ConfigContext) -> None:
    if ctx.has(ConfigKey.AI):
        raise LambdaUnservedConfigError(
            ConfigKey.AI,
            "LambdaHTTPAdapter does not host agents. Serve them with FastAPIAdapter, "
            "or drop the section from this entry point's configuration.",
        )


def _refuse_manifest_agents(discovered: DiscoveryResult) -> None:
    if discovered.agent_specs:
        raise LambdaUnservedConfigError(
            "AGENTS",
            "the manifest declares agents, which LambdaHTTPAdapter does not host. "
            "Serve them with FastAPIAdapter.",
        )


def _refuse_sql_endpoints(ctx: ConfigContext) -> None:
    sql = ctx.section_optional(ConfigKey.SQL, SqlConfig)
    mounted = mounted_sql_endpoints(sql) if sql is not None else ()
    if mounted:
        raise LambdaUnservedConfigError(
            f"sql.connections.{mounted[0]}.sql_endpoint",
            "LambdaHTTPAdapter does not mount SQL endpoints. Disable the endpoint "
            "(sql_endpoint.enabled: false) or serve it with FastAPIAdapter.",
        )


def _starlette_app(application: Application, declared: _Declared) -> Starlette:
    """Build the Starlette application of *declared*, as FastAPI would serve it."""
    kernel = application.kernel
    compiled = RestInterfaceCompiler(kernel.compiler).compile_sources(declared.routes)
    executor = _resolve_executor(kernel)
    app = Starlette(
        routes=[
            _route(route, kernel.factory, executor, application.observability) for route in compiled
        ]
    )
    register_error_handlers(app)
    app.add_exception_handler(HTTPException, _routing_error)
    _mount_authentication(app, declared.auth)
    _mount_request_guards(app, declared.rest)
    _mount_cors(app, declared.rest.cors)
    verify_exclusion_paths(app, declared.auth.exclude_paths)
    return app


async def _routing_error(request: Request, exc: Exception) -> Response:
    """Answer the routing 404 and 405 with FastAPI's own handler: same body, same Allow."""
    if not isinstance(exc, HTTPException):  # pragma: no cover - registered per type
        raise exc
    return await http_exception_handler(request, exc)


def _route(
    compiled: CompiledRoute,
    factory: UseCaseFactory,
    executor: RuntimeExecutor,
    observability: ObservabilityRuntime,
) -> Route:
    """One Starlette route running *compiled* through the router runtime.

    The path keeps bare ``{name}`` parameters: a Starlette convertor such as
    ``{id:int}`` would answer ``404`` where FastAPI answers ``422``.
    """
    runtime = _build_route_runtime(compiled)
    param_types = dict(compiled.execute_param_types)
    coercers: dict[str, TypeAdapter[Any]] = {
        name: TypeAdapter(_normalize_path_param_annotation(param_types.get(name, str)))
        for name in runtime.path_params
    }

    async def endpoint(request: Request) -> Response:
        path_kwargs = _coerce_path_params(request.path_params, coercers)
        return await _dispatch_route(
            request, path_kwargs, runtime, factory, executor, observability
        )

    method = compiled.route.method.upper()
    route = Route(compiled.full_path, endpoint, methods=[method])
    # Starlette adds HEAD to a GET route; FastAPI does not, so neither its 405
    # nor its Allow header would match.
    route.methods = {method}
    return route


def _coerce_path_params(
    raw: Mapping[str, Any], coercers: Mapping[str, TypeAdapter[Any]]
) -> dict[str, Any]:
    """Validate the path parameters as FastAPI does, with the same error locations.

    Raises:
        RequestValidationError: Listing every parameter that fails, each at
            ``("path", name)``.
    """
    values: dict[str, Any] = {}
    errors: list[dict[str, Any]] = []
    for name, coercer in coercers.items():
        try:
            values[name] = coercer.validate_python(raw[name], from_attributes=True)
        except ValidationError as exc:
            errors.extend(
                {**error, "loc": (_PATH_LOCATION, name, *error["loc"])}
                for error in exc.errors(include_url=False)
            )
    if errors:
        raise RequestValidationError(errors)
    return values


class LambdaHTTPHandler:
    """Serve a Starlette application to API Gateway events, keeping its application open.

    Built during the execution environment's Init (a module-level
    ``handler = create_app(...)``): it creates the event loop of the thread,
    opens the application on it in an :class:`~loom.composition.ApplicationHost`
    with its startup checks, and leaves it open across warm invocations. A
    failure to open propagates, so the execution environment fails its Init.

    Each call translates one event with Mangum, its ASGI lifespan off, on that
    same loop: a request runs in a task of its own, with its own identity,
    trace id and unit of work.

    Args:
        app: The application serving the requests.
        application: The composed application the requests run against.
    """

    def __init__(self, app: Starlette, application: Application) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        host = ApplicationHost(application)
        try:
            loop.run_until_complete(host.start())
        except BaseException:
            asyncio.set_event_loop(None)
            loop.close()
            raise
        self._loop = loop
        self._host = host
        self._mangum = Mangum(app, lifespan="off")

    def __call__(self, event: dict[str, Any], context: Any) -> dict[str, Any]:
        """Answer one API Gateway HTTP API event."""
        # Mangum runs on the thread's current loop: make sure it is the one
        # the application was opened on.
        asyncio.set_event_loop(self._loop)
        return self._mangum(event, context)

    def close(self) -> None:
        """Close the application and its loop, from a shutdown hook or a test.

        Raises:
            Exception: The failure to close the application; the loop is
                closed all the same.
        """
        try:
            self._loop.run_until_complete(self._host.stop())
        finally:
            asyncio.set_event_loop(None)
            self._loop.close()
