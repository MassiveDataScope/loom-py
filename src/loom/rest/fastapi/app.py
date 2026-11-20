"""FastAPI application factory.

:func:`create_fastapi_app` is the composition root that wires together the
domain bootstrap result and REST interface declarations into a runnable
``FastAPI`` instance.

It is intentionally kept thin — route validation happens during
:class:`~loom.rest.compiler.RestInterfaceCompiler` compilation, the cursor-key
check in :func:`compile_api` (both fail-fast at startup; a route that merely lets
callers choose cursor pages is refused per request without a key), and all request
handling is delegated to :func:`~loom.rest.fastapi.router_runtime.bind_interfaces`.

An application normally gets it through :func:`~loom.composition.create_app`,
which composes the kernel from configuration. Directly::

    kernel = create_kernel(
        config=cfg,
        use_cases=[CreateOrderUseCase, GetOrderUseCase],
        modules=[register_repositories],
    )
    app = create_fastapi_app(
        kernel,
        RouteSources(python=[OrderRestInterface]),
        observability_runtime=ObservabilityRuntime.noop(),
        title="Orders API",
        version="1.0.0",
    )
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI

from loom.core.bootstrap.bootstrap import BootstrapResult
from loom.core.engine.executor import RuntimeExecutor
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.stores.repository import CursorCodec
from loom.core.stores.repository.cursor import CURSORS_SECTION
from loom.core.stores.repository.query import PaginationMode
from loom.core.use_case.factory import UseCaseFactory
from loom.rest.compiler import (
    CompiledRoute,
    InterfaceCompilationError,
    RestInterfaceCompiler,
    RouteSources,
)
from loom.rest.fastapi._errors import register_error_handlers
from loom.rest.fastapi.router_runtime import bind_interfaces
from loom.rest.model import RestApiDefaults

_MiddlewareClass = Any


def _require_route_sources(routes: object) -> RouteSources:
    """Return *routes*, refusing anything that is not a ``RouteSources``.

    A bare sequence of interfaces would otherwise surface much later as an
    attribute error on the sequence, which says nothing about how to fix the
    call.

    Raises:
        TypeError: If *routes* is not a ``RouteSources`` instance.
    """
    if not isinstance(routes, RouteSources):
        raise TypeError(
            "create_fastapi_app()'s 'routes' must be a RouteSources, "
            f"got {type(routes).__name__}. Pass RouteSources(python=[...])."
        )
    return routes


@dataclass(frozen=True)
class CompiledApi:
    """The compiled routes of an application, and what runs their use cases.

    Args:
        routes: Fully resolved routes, in mounting order.
        factory: Builds the use case of each request.
        executor: Drives the use-case pipeline.
    """

    routes: tuple[CompiledRoute, ...]
    factory: UseCaseFactory
    executor: RuntimeExecutor


def compile_api(
    result: BootstrapResult,
    routes: RouteSources,
    *,
    defaults: RestApiDefaults | None = None,
) -> CompiledApi:
    """Compile *routes* against *result*, as every HTTP transport serves them.

    Raises:
        TypeError: If *routes* is not a ``RouteSources``.
        RuntimeError: If no ``RuntimeExecutor`` is registered.
        InterfaceCompilationError: If any interface fails structural validation,
            or a route declares ``pagination: cursor`` while no cursor key is
            configured under ``stores.settings.cursors``.
    """
    resolved_routes = _require_route_sources(routes)
    executor = result.container.resolve_or_none(RuntimeExecutor)
    if executor is None:
        raise RuntimeError(
            "RuntimeExecutor is not registered in the container. "
            "Use create_kernel(...) to build BootstrapResult."
        )
    compiled = RestInterfaceCompiler(result.compiler, defaults=defaults).compile_sources(
        resolved_routes
    )
    codec = result.container.resolve_or_none(CursorCodec) or CursorCodec.disabled()
    if not codec.is_enabled:
        for route in compiled:
            if route.effective_pagination_mode is PaginationMode.CURSOR:
                raise InterfaceCompilationError(
                    f"{route.interface_name}: route ({route.route.method.upper()}, "
                    f"{route.route.path!r}) declares pagination: cursor, but no cursor key is "
                    f"configured: set keys under {CURSORS_SECTION}"
                )
    return CompiledApi(routes=tuple(compiled), factory=result.factory, executor=executor)


def create_fastapi_app(
    result: BootstrapResult,
    routes: RouteSources,
    *,
    observability_runtime: ObservabilityRuntime | None = None,
    middleware: Sequence[_MiddlewareClass] = (),
    defaults: RestApiDefaults | None = None,
    **fastapi_kwargs: Any,
) -> FastAPI:
    """Create a FastAPI application from a bootstrap result and REST interfaces.

    Compiles all ``RestInterface`` declarations via
    :class:`~loom.rest.compiler.RestInterfaceCompiler`, binds each compiled
    route to the ``FastAPI`` instance, and returns the ready application.

    Compilation is fail-fast: any structural error (missing use-case plan,
    duplicate route, missing prefix) raises
    :class:`~loom.rest.compiler.InterfaceCompilationError` before the app
    starts accepting requests.

    Args:
        result: Fully initialised :class:`~loom.core.bootstrap.bootstrap.BootstrapResult`
            from :func:`~loom.core.bootstrap.kernel.create_kernel`.
        routes: Which interfaces to mount and from which origin — see
            :class:`~loom.rest.compiler.RouteSources`. ``routes.config``
            compiles after ``routes.python``, deterministically; a
            ``(method, path)`` collision between the two aborts naming both
            — neither side takes precedence. ``routes.disabled`` is applied
            to ``routes.python`` before the merge, so a disabled Python
            route can be redeclared in ``routes.config`` without colliding
            with itself; an entry matching no Python-declared route aborts
            startup instead of doing nothing.
        observability_runtime: Shared runtime used to emit lifecycle events
            around each request.
        middleware: ASGI middleware classes to register on the application.
            Added in declaration order (first = outermost wrapper).
            Accepts any class compatible with ``FastAPI.add_middleware``.
            Example::

                from loom.rest.middleware import TraceIdMiddleware
                from loom.prometheus import PrometheusMiddleware

                app = create_fastapi_app(
                    result,
                    RouteSources(python=[...]),
                    observability_runtime=ObservabilityRuntime.noop(),
                    middleware=[TraceIdMiddleware, PrometheusMiddleware],
                )
        defaults: Global REST API defaults (pagination mode, profile policy).
            Falls back to :class:`~loom.rest.model.RestApiDefaults` when not
            provided.
        **fastapi_kwargs: Additional keyword arguments forwarded to the
            ``FastAPI`` constructor (e.g. ``title``, ``version``,
            ``docs_url``).

    Returns:
        Configured :class:`fastapi.FastAPI` instance ready to serve requests.

    Raises:
        InterfaceCompilationError: If any interface fails structural validation.
        TypeError: If *routes* is not a ``RouteSources``.

    Example::

        app = create_fastapi_app(
            result,
            RouteSources(python=[UserRestInterface, OrderRestInterface]),
            defaults=RestApiDefaults(pagination_mode=PaginationMode.CURSOR),
            observability_runtime=ObservabilityRuntime.noop(),
            title="My API",
            version="2.0.0",
        )
    """
    api = compile_api(result, routes, defaults=defaults)
    runtime = observability_runtime or ObservabilityRuntime.noop()
    app = FastAPI(**fastapi_kwargs)
    register_error_handlers(app)

    for mw_class in middleware:
        app.add_middleware(mw_class)

    component_registry = bind_interfaces(
        app,
        api.routes,
        api.factory,
        api.executor,
        observability_runtime=runtime,
    )
    if component_registry:
        _register_openapi_components(app, component_registry)

    return app


def _register_openapi_components(app: FastAPI, schemas: dict[str, Any]) -> None:
    """Patch ``app.openapi`` to inject ``schemas`` into ``components.schemas``.

    Args:
        app: FastAPI application whose OpenAPI generator to patch.
        schemas: Mapping of component name → JSON Schema fragment collected
            during route binding (nested ``$defs`` from msgspec/pydantic).
    """
    original_openapi = app.openapi

    def _openapi() -> dict[str, Any]:
        doc = original_openapi()
        doc.setdefault("components", {}).setdefault("schemas", {}).update(schemas)
        return doc

    app.openapi = _openapi  # type: ignore[method-assign]
