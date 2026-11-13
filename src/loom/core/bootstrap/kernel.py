"""Shared kernel runtime assembly for transport adapters.

This module builds a reusable runtime composed of container, compiler,
factory, and executor. Transport adapters (REST, Celery, CLI) can consume
the same runtime without duplicating bootstrap logic.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from loom.core.authz.elevation import ElevationSink
from loom.core.bootstrap.bootstrap import BootstrapResult, bootstrap_app
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.compilable import Compilable
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.metrics import MetricsAdapter
from loom.core.environment import Environment
from loom.core.logger import LoggerPort, get_logger
from loom.core.uow.coordinator import Coordinator
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.invoker import AppInvoker, ApplicationInvoker
from loom.core.use_case.registry import UseCaseRegistry


@dataclass(frozen=True, kw_only=True)
class KernelRuntime(BootstrapResult):
    """Reusable application runtime built on top of :func:`bootstrap_app`.

    Attributes:
        executor: Runtime executor shared by transport adapters.
        registry: Read-only use-case registry for name-based resolution.
        app: App-level invoker facade. Typed as the concrete
            :class:`~loom.core.use_case.invoker.AppInvoker` because a
            composition root binds a copy of it to one caller with
            :meth:`~loom.core.use_case.invoker.AppInvoker.for_identity`, which
            is a property of this implementation rather than of the protocol.
    """

    executor: RuntimeExecutor
    registry: UseCaseRegistry
    app: AppInvoker


def create_kernel(
    *,
    config: object | None = None,
    use_cases: Sequence[type[Compilable]],
    modules: Sequence[Callable[[LoomContainer], None]] = (),
    logger: LoggerPort | None = None,
    metrics: MetricsAdapter | None = None,
    coordinator: Coordinator | None = None,
    repo_resolver: Callable[[type[Any]], Any] | None = None,
    environment: Environment | str | None = None,
    container: LoomContainer | None = None,
) -> KernelRuntime:
    """Create a shared kernel runtime for application adapters.

    Without *container* it builds one: registers *config* and the
    environment, runs *modules*, compiles, and validates the result. With
    *container* the caller has registered everything already and validates
    it afterwards: only the kernel is added.

    Args:
        config: Application config object registered in DI at ``APPLICATION``
            scope under its concrete type. Required without *container*.
        use_cases: Compilable classes to compile at startup.
        modules: Container registration modules; not with *container*.
        logger: Optional logger override.
        metrics: Optional metrics adapter.
        coordinator: Coordinator of the application's stores, which runs
            each execution in their units of work; ``None`` runs without one.
        repo_resolver: Optional repository resolver used by marker-based loads.
            Defaults to ``container.resolve_repo``.
        environment: Environment of the application, resolved like
            :func:`~loom.core.environment.resolve_environment` when ``None``.
            Registered at ``APPLICATION`` scope before the modules run, so a
            module's service or a use case can inject
            :class:`~loom.core.environment.Environment`. Not with *container*.
        container: A container the caller seeded and registered; the kernel's
            keys are added to it and it is not validated here.

    Returns:
        Fully built :class:`KernelRuntime`.

    Raises:
        TypeError: When *config* is missing without *container*, or given
            with it, as are *modules* or *environment*.
        BootstrapError: Without *container*, as :func:`bootstrap_app`.
        CompilationError: With *container*, when a use case does not compile.
    """
    _check_arguments(container, config, modules, environment)
    if container is None:
        base = bootstrap_app(
            config=config,
            use_cases=use_cases,
            modules=modules,
            logger=logger,
            metrics=metrics,
            environment=environment,
        )
    else:
        base = _compiled(container, use_cases, logger=logger, metrics=metrics)
    resolver = repo_resolver or base.container.resolve_repo
    executor = RuntimeExecutor(
        base.compiler,
        coordinator=coordinator,
        metrics=base.metrics,
        repo_resolver=resolver,
        elevation_sink=_registered_sink(base.container),
    )
    registry = UseCaseRegistry.build(list(use_cases))
    app = AppInvoker(
        factory=base.factory,
        executor=executor,
        registry=registry,
    )
    base.container.register(RuntimeExecutor, lambda: executor, scope=Scope.APPLICATION)
    base.container.register(UseCaseRegistry, lambda: registry, scope=Scope.APPLICATION)
    base.container.register(ApplicationInvoker, lambda: app, scope=Scope.APPLICATION)
    if container is None:
        base.container.validate()

    return KernelRuntime(
        container=base.container,
        compiler=base.compiler,
        factory=base.factory,
        metrics=base.metrics,
        executor=executor,
        registry=registry,
        app=app,
    )


def _check_arguments(
    container: LoomContainer | None,
    config: object | None,
    modules: Sequence[Callable[[LoomContainer], None]],
    environment: Environment | str | None,
) -> None:
    if container is None and config is None:
        raise TypeError("create_kernel() missing required keyword argument: 'config'")
    if container is not None and (config is not None or modules or environment is not None):
        raise TypeError(
            "create_kernel(container=...) takes no config, modules or environment: "
            "register them in the container before passing it"
        )


def _compiled(
    container: LoomContainer,
    use_cases: Sequence[type[Compilable]],
    *,
    logger: LoggerPort | None,
    metrics: MetricsAdapter | None,
) -> BootstrapResult:
    """Compile *use_cases* and register their factory in *container*, which is already filled."""
    compiler = UseCaseCompiler(logger=logger or get_logger("loom.bootstrap"), metrics=metrics)
    factory = UseCaseFactory(container)
    for use_case in use_cases:
        compiler.compile(use_case)
    for use_case in use_cases:
        factory.register(use_case)
    container.register(UseCaseFactory, lambda: factory, scope=Scope.APPLICATION)
    return BootstrapResult(container=container, compiler=compiler, factory=factory, metrics=metrics)


def _registered_sink(container: LoomContainer) -> ElevationSink | None:
    if container.is_registered(ElevationSink):
        sink: ElevationSink = container.resolve(ElevationSink)
        return sink
    return None
