"""HTTP integration test harness for Loom use cases.

:class:`HttpTestHarness` builds a minimal FastAPI application from REST
interface declarations and fake repository instances, and returns a
:class:`~fastapi.testclient.TestClient` ready for HTTP-level assertions —
no real database or running server required.

Usage::

    harness = HttpTestHarness()
    harness.inject_repo(Product, InMemoryRepository(Product))
    client = harness.build_app(interfaces=[ProductRestInterface])

    resp = client.get("/products/1")
    assert resp.status_code == 200
    assert resp.json()["name"] == "Widget"
"""

from __future__ import annotations

import warnings
from collections.abc import Callable, Sequence
from typing import Any

from fastapi.testclient import TestClient

from loom.core.bootstrap.bootstrap import bootstrap_app
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.use_case.use_case import UseCase
from loom.rest.compiler import RouteSources
from loom.rest.fastapi.app import create_fastapi_app
from loom.rest.model import RestInterface
from loom.testing.golden import _ErrorProxy


class _NullConfig:
    """Placeholder config used when no real config is needed in tests."""


class HttpTestHarness:
    """Builds a FastAPI ``TestClient`` with injected fake repositories.

    Combines :func:`~loom.core.bootstrap.bootstrap.bootstrap_app` and
    :func:`~loom.rest.fastapi.app.create_fastapi_app` into a single
    test-oriented entry point.

    Repositories are keyed by **domain model type** (the ``TModel`` generic
    parameter of the use case) rather than by interface.  The harness derives
    the correct binding automatically by scanning the use cases declared in
    the REST interfaces.

    Args:
        None

    Example::

        harness = HttpTestHarness()
        harness.inject_repo(Product, InMemoryRepository(Product))
        harness.force_error(Product, "get_by_id", NotFound("not found"))
        client = harness.build_app(interfaces=[ProductRestInterface])

        resp = client.get("/products/99")
        assert resp.status_code == 404
    """

    def __init__(self) -> None:
        self._repos: dict[type[Any], Any] = {}
        self._error_overrides: dict[tuple[type[Any], str], Exception] = {}

    def inject_repo(self, model: type[Any], fake: Any) -> None:
        """Register a fake repository for a domain model type.

        The harness resolves the model from the use-case generic parameter
        (``UseCase[Model, …]``) at bootstrap time and injects ``fake`` as its
        repository.

        Args:
            model: ``msgspec.Struct`` subclass used as the resolution key.
                Must match the ``TModel`` parameter of the use cases that need
                this repository.
            fake: Fake repository instance to inject.

        Example::

            harness.inject_repo(Product, InMemoryRepository(Product))
            harness.inject_repo(Order, FakeOrderRepo())
        """
        self._repos[model] = fake

    def force_error(self, model: type[Any], method: str, error: Exception) -> None:
        """Force a repository method to raise a specific error on every call.

        Errors that are :class:`~loom.core.errors.LoomError` subclasses are
        mapped to the corresponding HTTP status code by the router.

        Args:
            model: Domain model type identifying the repository.
            method: Method name to intercept (e.g. ``"get_by_id"``).
            error: Exception instance to raise.

        Example::

            from loom.core.errors import NotFound
            harness.force_error(Product, "get_by_id", NotFound("not found"))
        """
        self._error_overrides[(model, method)] = error

    def simulate_system_error(self, model: type[Any], method: str) -> None:
        """Simulate an unhandled ``SystemError`` on a repository method.

        The resulting HTTP response will have status ``500``.

        Args:
            model: Domain model type identifying the repository.
            method: Method name to intercept.
        """
        self._error_overrides[(model, method)] = SystemError(
            f"simulated system error in repo for {model.__name__}.{method}"
        )

    def _build_module(self) -> Callable[[LoomContainer], None]:
        repos = dict(self._repos)
        error_overrides = dict(self._error_overrides)

        def _make_provider(instance: Any) -> Callable[[], Any]:
            def _provider() -> Any:
                return instance

            return _provider

        def _module(container: LoomContainer) -> None:
            for model, fake in repos.items():
                overrides = {
                    method: err for (m, method), err in error_overrides.items() if m is model
                }
                effective = _ErrorProxy(fake, overrides) if overrides else fake
                # Use an explicit per-model token to avoid DI key collisions
                # when multiple models share the same fake repository type.
                token = ("repo", model)
                container.register(
                    token,
                    _make_provider(effective),
                    scope=Scope.APPLICATION,
                )
                container.register_repo(model, token)

        return _module

    def build_app(
        self,
        interfaces: Sequence[type[RestInterface[Any]]],
        *,
        config_interfaces: Sequence[type[RestInterface[Any]]] = (),
        disable_routes: Sequence[tuple[str, str]] = (),
        **fastapi_kwargs: Any,
    ) -> TestClient:
        """Build a ``TestClient`` from REST interface declarations.

        Extracts use cases from every interface's route declarations
        (Python-declared and config-declared alike), bootstraps the
        application with the injected fake repositories, and returns a
        :class:`~fastapi.testclient.TestClient` ready for HTTP assertions.

        The client is configured with ``raise_server_exceptions=False`` so
        that unhandled server errors are returned as ``500`` responses instead
        of being re-raised in the test.

        Args:
            interfaces: Python-declared ``RestInterface`` subclasses whose
                routes should be exposed.
            config_interfaces: ``RestInterface`` subclasses built from
                ``app.rest.interfaces`` (see
                :func:`~loom.rest.config.build_interfaces_from_config`),
                exercised the same way a Python-declared interface is.
            disable_routes: ``(method, full_path)`` pairs to drop from the
                routes declared by *interfaces* — forwarded to
                :func:`~loom.rest.fastapi.app.create_fastapi_app` unchanged.
            **fastapi_kwargs: Forwarded to the ``FastAPI`` constructor
                (e.g. ``title``, ``version``).

        Returns:
            :class:`~fastapi.testclient.TestClient` wrapping the test app.

        Example::

            client = harness.build_app(interfaces=[ProductRestInterface])
            resp = client.post("/products/", json={"name": "Widget"})
            assert resp.status_code == 201
        """
        all_interfaces = (*interfaces, *config_interfaces)
        use_cases: list[type[UseCase[Any, Any]]] = list(
            dict.fromkeys(route.use_case for iface in all_interfaces for route in iface.routes)
        )
        with warnings.catch_warnings():  # removed in T01e
            # The harness itself still builds through bootstrap_app: its
            # deprecation is for the harness's callers to never see.
            warnings.filterwarnings("ignore", "bootstrap_app is deprecated", DeprecationWarning)
            result = bootstrap_app(
                config=_NullConfig(),
                use_cases=use_cases,
                modules=[self._build_module()],
            )
        app = create_fastapi_app(
            result,
            RouteSources(python=interfaces, config=config_interfaces, disabled=disable_routes),
            **fastapi_kwargs,
        )
        return TestClient(app, raise_server_exceptions=False)
