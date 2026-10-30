"""``create_app``: the HTTP entry point composed from YAML configuration.

A shortcut over :func:`loom.composition.create_app` with
:class:`~loom.rest.fastapi.adapter.FastAPIAdapter`; the composition owns the
resources, the adapter mounts the routes.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from fastapi import FastAPI
from prometheus_client import CollectorRegistry

from loom.composition import create_app as compose
from loom.core.config import ConfigResolver
from loom.core.di.container import LoomContainer
from loom.core.environment import Environment
from loom.core.introspection import (
    INTROSPECTION_STATE_ATTR,
    AppIntrospection,
    IntrospectionError,
    describe_app,
)
from loom.rest.auth import Authenticator
from loom.rest.fastapi.adapter import FastAPIAdapter


def create_app(
    *config_paths: str,
    config_dir: str | None = None,
    environment: Environment | str | None = None,
    code_path: str | None = None,
    metrics_registry: CollectorRegistry | None = None,
    authenticator: Authenticator | None = None,
    resolvers: Sequence[ConfigResolver] = (),
    modules: Sequence[Callable[[LoomContainer], None]] = (),
) -> FastAPI:
    """Create a FastAPI application from a config directory or explicit YAML files.

    The environment is resolved once: *environment* when given, else the
    ``LOOM_ENV`` variable, else production (see :mod:`loom.core.environment`).
    It picks the configuration overlay, drives the logger defaults, and is
    registered in the container for use cases to inject.

    Configuration comes from exactly one of two forms. *config_dir* loads its
    ``base.yaml``, then ``<environment>.yaml`` when present. Explicit
    *config_paths* are merged left-to-right as given. Either way later files
    override earlier ones, each file's ``includes`` merge before its own
    values, and the ``LOOM__SECTION__KEY`` variables merge last (see
    :mod:`loom.core.config.overrides`).

    In production and staging the deploy checks run once the logger is
    configured and the code path is importable, before the kernel is built
    (see :mod:`loom.core.checks`); ``checks.modules`` names the application
    modules registering its own checks:
    an error stops startup, a warning is logged, and ``checks.silenced``
    lists the ids accepted on purpose.

    ``${secrets:...}`` and ``${ssm:...}`` placeholders resolve through loom's
    built-in AWS resolvers unless *resolvers* supplies one with the same name.

    ``TraceIdMiddleware`` is mounted automatically. Structured logging and
    OTEL come from the top-level ``observability:`` section. Prometheus
    middleware is mounted when ``observability.prometheus.enabled`` is
    ``true``.

    Authentication is mechanism-agnostic: configure ``app.rest.auth.jwt`` for
    the built-in stateless JWT mechanism, or set ``app.rest.auth.custom:
    true`` and pass *authenticator* for any other.  Exactly one of the two
    may be active, and the configuration always names it.

    The optional ``sql:`` section wires the SQL subsystem: its connections
    open inside the app lifespan and two collaborators are registered in the
    container (null implementations raising an actionable ``ConfigError`` when
    the section is absent). ``CallerBoundSql`` derives the query roles from the
    verified caller and is what a use case serving a request should inject;
    ``SqlQueryService`` takes the roles as an argument and is for system work
    that has no caller. Connections opting in with
    ``sql_endpoint.enabled`` plus an explicit ``sql_endpoint.auth`` mount a
    ``POST /sql/{name}`` endpoint; ``auth: identity`` additionally requires a
    configured authentication mechanism, and a non-empty ``allowed_roles``
    also requires that mechanism to bind roles to the verified caller
    identity (for JWT, ``app.rest.auth.jwt.roles_claim``).

    Args:
        *config_paths: Paths to YAML configuration files, merged left to
            right. Mutually exclusive with *config_dir*.
        config_dir: Local directory holding ``base.yaml`` and the
            per-environment overlays. Mutually exclusive with *config_paths*.
        environment: Environment of the application; overrides ``LOOM_ENV``.
        code_path: Optional override for ``app.code_path``.  Resolved relative
            to the first config file when not absolute.
        metrics_registry: Optional Prometheus ``CollectorRegistry`` used for
            ``PrometheusMiddleware`` and the scrape endpoint.  Defaults to the
            global registry.  Pass a fresh ``CollectorRegistry()`` in tests to
            avoid ``ValueError: Duplicated timeseries`` when multiple apps with
            ``observability.prometheus.enabled: true`` are created in the same
            process.
        authenticator: Custom authentication mechanism.  Requires
            ``app.rest.auth.custom: true`` and excludes ``app.rest.auth.jwt``.
        resolvers: Resolvers for ``${name:key}`` placeholders, registered
            before the built-in ``secrets`` and ``ssm`` defaults.  A resolver
            named like a default replaces it.
        modules: Optional DI registration modules, each called with the
            container in order before repositories are registered and before
            use cases are verified.  Use them to bind ports no repository
            provides (a clock, an identity verifier, a bridge to another
            bounded context).  Same semantics as the Celery ``create_app``.
            The framework registers its own bindings after these modules.
            ``JobService`` and repositories (``repository_for``,
            ``DefaultRepositoryBuilder``) are overridable: a module's binding
            is kept.  ``SessionManager``, ``SqlQueryService``,
            ``CallerBoundSql``, ``ObservabilityRuntime``, ``ElevationSink``,
            ``RuntimeExecutor``, ``UseCaseRegistry`` and
            ``ApplicationInvoker`` are reserved: a module binding one fails
            start-up with
            :class:`~loom.composition.errors.ReservedBindingError`.

    Returns:
        Configured :class:`fastapi.FastAPI` application, ready to serve.

    Raises:
        ConfigError: When neither or both of *config_paths* and *config_dir*
            are given, when the environment is unknown, or when *authenticator*
            and ``app.rest.auth.custom: true`` do not come together.
        DeployCheckError: When a deploy check of level error fails in
            production or staging.
        ReservedBindingError: When a module binds a framework-owned key.

    Example — config directory, overlay chosen by ``LOOM_ENV``::

        # config/base.yaml, config/development.yaml, config/production.yaml
        app = create_app(config_dir="config")

    Example — single config::

        app = create_app("config/app.yaml")

    Example — a mechanism of your own (``app.rest.auth.custom: true``)::

        app = create_app("config/app.yaml", authenticator=MyMtlsAuthenticator())

    Example — bind a port a use case injects::

        def clock_module(container: LoomContainer) -> None:
            container.register(Clock, SystemClock, scope=Scope.APPLICATION)

        app = create_app("config/app.yaml", modules=[clock_module])

    Example — explicit layers, the environment named in code::

        app = create_app(
            "config/base.yaml", "config/production.yaml", environment="production"
        )

    Example — single file using inline includes::

        # config/app.yaml
        # includes:
        #   - base.yaml
        #   - secrets.yaml
        app = create_app("config/app.yaml")
    """
    return compose(
        *config_paths,
        adapter=FastAPIAdapter(metrics_registry=metrics_registry, authenticator=authenticator),
        config_dir=config_dir,
        environment=environment,
        code_path=code_path,
        resolvers=resolvers,
        modules=modules,
    )


def describe_fastapi_app(app: FastAPI) -> dict[str, Any]:
    """Describe an application built by :func:`create_app`.

    The document always carries the application identity under ``"app"``, plus
    one section per wired pillar — ``"agents"`` when an ``ai:`` section is
    configured.  Only publishable values appear: no instructions, no model
    binding, no URL and no credential reference.

    Args:
        app: Application previously built by :func:`create_app`.

    Returns:
        JSON-encodable self-description of the application.

    Raises:
        IntrospectionError: When *app* was not built by :func:`create_app`, or
            when a pillar contribution cannot be resolved.

    Example::

        app = create_app("config/app.yaml")
        describe_fastapi_app(app)["app"]
        # {'name': 'billing', 'version': '1.4.0'}
    """
    introspection = getattr(app.state, INTROSPECTION_STATE_ATTR, None)
    if not isinstance(introspection, AppIntrospection):
        raise IntrospectionError(
            "this application carries no introspection state; only applications built by "
            "'create_app' can describe themselves."
        )
    return describe_app(introspection)
