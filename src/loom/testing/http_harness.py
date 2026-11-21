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

from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.types import ASGIApp, Receive, Scope, Send

from loom.composition.application import Application
from loom.composition.project import AppConfig, Project
from loom.composition.recipe import Recipe
from loom.connections.aws import AwsClients
from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer
from loom.core.environment import resolve_environment
from loom.core.job.service import JobService
from loom.core.observability.config import ObservabilityConfig
from loom.core.observability.observability import Observability
from loom.core.stores import SERVER_PROFILE
from loom.rest.compiler import RouteSources
from loom.rest.fastapi.app import create_fastapi_app
from loom.rest.model import RestInterface
from loom.testing.golden import _ErrorProxy
from loom.testing.overrides import Override


class _JobsIdleAfterRequest:
    """ASGI middleware that returns from a request once the inline jobs it dispatched have run.

    Args:
        app: The application serving the request.
        jobs: The job service whose inline jobs are waited for.
    """

    def __init__(self, app: ASGIApp, *, jobs: JobService) -> None:
        self._app = app
        self._jobs = jobs

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await self._app(scope, receive, send)
        if scope["type"] == "http":
            await self._jobs.wait_until_idle()


class _HarnessRuntime:
    """Compose the use cases *routes* serve, with *overrides*, leniently; mount *routes*.

    Discovers no model, so no store registers a repository: *overrides* bind
    the fakes.

    Args:
        routes: The interfaces to serve; their routes' use cases are composed.
        overrides: DI modules binding the fake repositories.
        aws: The AWS clients the composition uses; a fresh set without it.
        **fastapi_kwargs: Forwarded to the ``FastAPI`` constructor.
    """

    def __init__(
        self,
        routes: RouteSources,
        *,
        overrides: tuple[Callable[[LoomContainer], None], ...],
        aws: AwsClients | None = None,
        **fastapi_kwargs: Any,
    ) -> None:
        self._routes = routes
        self._overrides = overrides
        self._aws = AwsClients() if aws is None else aws
        self._fastapi_kwargs = fastapi_kwargs

    def project(self) -> Project:
        """The ``test-harness`` application, its jobs inline, in the ``LOOM_ENV`` environment.

        Raises:
            ConfigError: When ``LOOM_ENV`` is not an environment.
        """
        app = AppConfig(name="test-harness", job_service="inline")
        return Project(
            config=ConfigContext.from_dict(
                {"app": {"name": app.name, "job_service": app.job_service}}
            ),
            environment=resolve_environment(),
            code_path=Path.cwd(),
            config_root=Path.cwd(),
            app=app,
            observability=Observability.noop(),
            observability_config=ObservabilityConfig(),
        )

    def recipe(self, project: Project) -> Recipe:
        """The routes' use cases over no model, leniently."""
        interfaces = (*self._routes.python, *self._routes.config)
        use_cases = dict.fromkeys(route.use_case for iface in interfaces for route in iface.routes)
        return Recipe(
            profile=SERVER_PROFILE,
            discovered=project.discovered.with_changes(models=(), use_cases=tuple(use_cases)),
            aws=self._aws,
            overrides=self._overrides,
            lenient=True,
        )

    def mount(self, application: Application) -> FastAPI:
        """Mount the routes on *application*'s kernel; each request returns once its jobs ran."""
        app = create_fastapi_app(application.kernel, self._routes, **self._fastapi_kwargs)
        app.add_middleware(_JobsIdleAfterRequest, jobs=application.jobs)
        return app


class HttpTestHarness:
    """Builds a FastAPI ``TestClient`` with injected fake repositories.

    Composes the application through the same pipeline as
    :func:`~loom.composition.create_app`, from an in-memory configuration and
    leniently (see :class:`~loom.composition.Recipe`), then mounts the routes
    with :func:`~loom.rest.fastapi.app.create_fastapi_app`.

    Repositories are keyed by **domain model type** (the ``TModel`` generic
    parameter of the use case) rather than by interface.  The harness derives
    the correct binding automatically by scanning the use cases declared in
    the REST interfaces.

    Its jobs run inline, and each request returns once the jobs it
    dispatched, and the jobs those dispatch, have run: a test asserts their
    effects right after the call. A job with a countdown is waited for too; bind
    a :class:`~loom.testing.ManualTimer` to hold it until the test moves the clock.

    Args:
        overrides: Substitutions applied while the application is composed.
        aws: Recording or fake AWS clients; the stores, the job transports and
            the container's ``AwsClients`` all use this one object.

    Example::

        harness = HttpTestHarness()
        harness.inject_repo(Product, InMemoryRepository(Product))
        harness.force_error(Product, "get_by_id", NotFound("not found"))
        client = harness.build_app(interfaces=[ProductRestInterface])

        resp = client.get("/products/99")
        assert resp.status_code == 404
    """

    def __init__(
        self, *, overrides: Sequence[Override] = (), aws: AwsClients | None = None
    ) -> None:
        self._repos: dict[type[Any], Any] = {}
        self._error_overrides: dict[tuple[type[Any], str], Exception] = {}
        self._overrides = tuple(overrides)
        self._aws = aws
        self.application: Application | None = None

    @contextmanager
    def overriding(self, *overrides: Override) -> Iterator[HttpTestHarness]:
        """Apply substitutions to the built application until the block exits.

        Args:
            *overrides: The substitutions; they exit in reverse order.

        Yields:
            This harness.

        Raises:
            RuntimeError: When the application is not built yet.
            ResolutionError: As :meth:`Override.apply` raises it.
        """
        if self.application is None:
            raise RuntimeError("Build the harness application before overriding dependencies")
        with Override.apply_all(self.application.kernel.container, overrides):
            yield self

    def inject_repo(self, model: type[Any], fake: Any) -> None:
        """Register a fake repository for a domain model type.

        The harness resolves the model from the use-case generic parameter
        (``UseCase[Model, …]``) when the application is composed and injects
        ``fake`` as its repository. Like a store's repository, ``fake`` is
        also bound under each capability its class implements: the standard
        ones (``Readable[Model]``, …) and its own repository ``Protocol``
        bases, so a use case injecting such a protocol receives it.

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

        def _module(container: LoomContainer) -> None:
            for override in self._overrides:
                override.install(container)
            for model, fake in repos.items():
                overrides = {
                    method: err for (m, method), err in error_overrides.items() if m is model
                }
                effective = _ErrorProxy(fake, overrides) if overrides else fake
                Override.repository(model, effective).install(container)

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
        (Python-declared and config-declared alike), composes the
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
        runtime = _HarnessRuntime(
            RouteSources(python=interfaces, config=config_interfaces, disabled=disable_routes),
            overrides=(self._build_module(),),
            aws=self._aws,
            **fastapi_kwargs,
        )
        self.application = runtime.project().compose(runtime)
        app = runtime.mount(self.application)
        return TestClient(app, raise_server_exceptions=False)
