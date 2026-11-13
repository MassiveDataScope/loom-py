"""The adapter the HTTP test harness composes its application with."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from fastapi import FastAPI

from loom.composition.application import Application
from loom.composition.recipe import Recipe
from loom.composition.source import CompositionSource
from loom.core.di.container import LoomContainer
from loom.core.discovery.base import DiscoveryResult
from loom.core.persistence import SERVER_PROFILE
from loom.rest.compiler import RouteSources
from loom.rest.fastapi.app import create_fastapi_app


class HarnessAdapter:
    """Compose the use cases *routes* serve, with *overrides*, leniently; mount *routes*.

    Discovers no model, so no store registers a repository: *overrides* bind
    the fakes.

    Args:
        routes: The interfaces to serve; their routes' use cases are composed.
        overrides: DI modules binding the fake repositories.
        **fastapi_kwargs: Forwarded to the ``FastAPI`` constructor.
    """

    def __init__(
        self,
        routes: RouteSources,
        *,
        overrides: tuple[Callable[[LoomContainer], None], ...],
        **fastapi_kwargs: Any,
    ) -> None:
        self._routes = routes
        self._overrides = overrides
        self._fastapi_kwargs = fastapi_kwargs

    def recipe(self, source: CompositionSource) -> Recipe:
        """The routes' use cases over no model, leniently."""
        del source
        interfaces = (*self._routes.python, *self._routes.config)
        use_cases = dict.fromkeys(route.use_case for iface in interfaces for route in iface.routes)
        return Recipe(
            profile=SERVER_PROFILE,
            discovered=DiscoveryResult(models=(), use_cases=tuple(use_cases), interfaces=()),
            overrides=self._overrides,
            lenient=True,
        )

    def bind(self, application: Application) -> FastAPI:
        """Mount the routes on *application*'s kernel."""
        return create_fastapi_app(application.kernel, self._routes, **self._fastapi_kwargs)
