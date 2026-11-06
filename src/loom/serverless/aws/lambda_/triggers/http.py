"""Serve a loom application from AWS Lambda behind API Gateway: an ``http`` trigger.

Install the ``lambda`` extra. The entry point changes one line::

    handler = create_app("config.yaml", adapter=LambdaAdapter())

The recipe is the FastAPI adapter's, so the configuration, the authentication,
discovery and the CRUD gating have one source of truth. The transport is a bare
Starlette application with one route per compiled route, reusing FastAPI's error
handlers and the router runtime: a response matches the FastAPI one by
construction, without FastAPI's routing or its OpenAPI.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from typing import Any

from fastapi.exceptions import RequestValidationError
from mangum import Mangum
from pydantic import TypeAdapter, ValidationError
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from loom.composition import Application, CompositionSource, LoopHost, Recipe
from loom.core.config import ConfigContext, ConfigKey
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.persistence import LAMBDA_PROFILE
from loom.core.sql import SqlConfig
from loom.rest.auth import Authenticator
from loom.rest.compiler import CompiledRoute
from loom.rest.fastapi._errors import register_error_handlers
from loom.rest.fastapi._exclusions import verify_exclusion_paths
from loom.rest.fastapi.adapter import RestDeclaration, compose_rest, mount_request_stack
from loom.rest.fastapi.app import CompiledApi, compile_api
from loom.rest.fastapi.router_runtime import (
    _build_route_runtime,
    _dispatch_route,
    path_param_annotation,
)
from loom.rest.fastapi.sql import mounted_sql_endpoints
from loom.serverless.aws.lambda_._refusals import (
    COMPONENT,
    LambdaUnservedConfigError,
    refuse_manifest_agents,
)
from loom.serverless.aws.lambda_.functions import http_function

_PATH_LOCATION = "path"


class HttpServing:
    """Compose as :class:`~loom.rest.fastapi.FastAPIAdapter` does, with the Lambda pool profile.

    Health, metrics, SQL endpoints, agents and OpenAPI are not served; a
    configuration declaring metrics, an SQL endpoint or agents is refused.
    Left at their default, the authentication exclusions are therefore none:
    a route at ``/health`` or ``/docs`` requires a token. An explicit
    ``exclude_paths`` is honoured as written.

    Args:
        function: The ``http`` function of ``lambda.functions`` served: its
            interfaces only, with its ``auth`` when it declares one; every
            interface when ``None``.
        authenticator: Custom authentication mechanism; requires
            ``custom: true`` in the authentication in effect.
    """

    def __init__(self, function: str | None, authenticator: Authenticator | None) -> None:
        self._function = function
        self._authenticator = authenticator
        self._declared: RestDeclaration | None = None

    def recipe(self, source: CompositionSource) -> Recipe:
        """The FastAPI recipe, with the Lambda pool profile and no agents.

        Raises:
            LambdaUnservedConfigError: When the configuration mounts an SQL
                endpoint, or the manifest declares agents.
            ConfigError: For any reason :func:`~loom.rest.fastapi.adapter.compose_rest`
                raises it.
        """
        _refuse_sql_endpoints(source.config)
        served = http_function(source.config, self._function)
        recipe, declared = compose_rest(
            source,
            authenticator=self._authenticator,
            serves_documentation=False,
            auth_section=served.auth_section,
            select_interfaces=served.select,
        )
        refuse_manifest_agents(recipe.discovered.agent_specs)
        self._declared = declared
        return dataclasses.replace(recipe, profile=LAMBDA_PROFILE, agents=False, pool_metrics=None)

    def bind(self, application: Application) -> LambdaHTTPHandler:
        """Mount the routes, error handlers and middleware, then open *application*.

        :class:`LambdaAdapter` binds only after :meth:`recipe` succeeded.

        Raises:
            ConfigError: When an authentication exclusion reaches a templated route.
            Exception: The failure to open the application, as itself.
        """
        assert self._declared is not None  # set by recipe
        return LambdaHTTPHandler(_starlette_app(application, self._declared), application)


def _refuse_sql_endpoints(ctx: ConfigContext) -> None:
    sql = ctx.section_optional(ConfigKey.SQL, SqlConfig)
    mounted = mounted_sql_endpoints(sql) if sql is not None else ()
    if mounted:
        raise LambdaUnservedConfigError(
            f"sql.connections.{mounted[0]}.sql_endpoint",
            f"{COMPONENT} does not mount SQL endpoints. Disable the endpoint "
            "(sql_endpoint.enabled: false) or serve it with FastAPIAdapter.",
        )


def _starlette_app(application: Application, declared: RestDeclaration) -> Starlette:
    """Build the Starlette application of *declared*, as FastAPI would serve it."""
    api = compile_api(application.kernel, declared.routes)
    app = Starlette(
        routes=[_route(compiled, api, application.observability) for compiled in api.routes]
    )
    register_error_handlers(app)
    mount_request_stack(app, declared)
    verify_exclusion_paths(app, declared.auth.exclude_paths)
    return app


def _route(compiled: CompiledRoute, api: CompiledApi, observability: ObservabilityRuntime) -> Route:
    """One Starlette route running *compiled* through the router runtime.

    The path keeps bare ``{name}`` parameters: a Starlette convertor such as
    ``{id:int}`` would answer ``404`` where FastAPI answers ``422``.
    """
    method = compiled.route.method.upper()
    endpoint = _RouteEndpoint(compiled, api, observability)
    route = Route(compiled.full_path, endpoint.handle, methods=[method])
    # Starlette adds HEAD to a GET route; FastAPI does not, so neither its 405
    # nor its Allow header would match.
    route.methods = {method}
    return route


class _RouteEndpoint:
    """Validates the path parameters of one route as FastAPI does, then runs it."""

    def __init__(
        self, compiled: CompiledRoute, api: CompiledApi, observability: ObservabilityRuntime
    ) -> None:
        self._runtime = _build_route_runtime(compiled)
        self._api = api
        self._observability = observability
        param_types = dict(compiled.execute_param_types)
        self._coercers: dict[str, TypeAdapter[Any]] = {
            name: TypeAdapter(path_param_annotation(name, param_types))
            for name in self._runtime.path_params
        }

    async def handle(self, request: Request) -> Response:
        """Answer *request*.

        Raises:
            RequestValidationError: Listing every path parameter that fails,
                each at ``("path", name)``.
        """
        return await _dispatch_route(
            request,
            self._path_kwargs(request.path_params),
            self._runtime,
            self._api.factory,
            self._api.executor,
            self._observability,
        )

    def _path_kwargs(self, raw: Mapping[str, Any]) -> dict[str, Any]:
        values: dict[str, Any] = {}
        errors: list[dict[str, Any]] = []
        for name, coercer in self._coercers.items():
            try:
                values[name] = coercer.validate_python(raw[name])
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
    ``handler = create_app(...)``): it opens the application, with its startup
    checks, on a :class:`~loom.composition.LoopHost` and leaves it open across
    warm invocations. A failure to open propagates, so the execution
    environment fails its Init.

    Each call translates one event with Mangum, its ASGI lifespan off, on that
    same loop: a request runs in a task of its own, with its own identity,
    trace id and unit of work.

    Args:
        app: The application serving the requests.
        application: The composed application the requests run against.
    """

    def __init__(self, app: Starlette, application: Application) -> None:
        host = LoopHost(application)
        # Built on the host's loop: without a current loop Mangum makes one of its own.
        self._mangum = Mangum(app, lifespan="off")
        host.start()
        self._host = host

    def __call__(self, event: dict[str, Any], context: Any) -> dict[str, Any]:
        """Answer one API Gateway HTTP API event."""
        self._host.make_current()
        return self._mangum(event, context)

    def close(self) -> None:
        """Close the application and its loop, from a shutdown hook or a test; idempotent.

        Raises:
            Exception: The failure to close the application; the loop is
                closed all the same.
        """
        self._host.close()
