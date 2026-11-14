"""Shared kernel runtime assembly for transport adapters.

This module builds a reusable runtime composed of container, compiler,
factory, and executor. Transport adapters (REST, Celery, CLI) can consume
the same runtime without duplicating bootstrap logic.

:func:`create_kernel` is the public entry point. :func:`seeded_container`,
:func:`install_use_cases` and :func:`install_kernel` are its steps.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from loom.core.authz.elevation import ElevationSink
from loom.core.bootstrap.bootstrap import BootstrapError, BootstrapResult
from loom.core.di.container import BindingKey, LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.compilable import Compilable
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.kinds import declare_ai_kinds
from loom.core.engine.metrics import MetricsAdapter
from loom.core.environment import Environment, resolve_environment
from loom.core.execution import ExecutionResolver
from loom.core.identity import Identity
from loom.core.logger import LoggerPort, get_logger
from loom.core.repository.dependency import classify_key
from loom.core.uow.coordinator import Coordinator
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.invoker import AppInvoker, ApplicationInvoker
from loom.core.use_case.kinds import CALLER, CallerResolver, declare_use_case_kinds
from loom.core.use_case.registry import UseCaseRegistry


@dataclass(frozen=True, kw_only=True)
class KernelRuntime(BootstrapResult):
    """Reusable application runtime: the bootstrap result plus its executor and invoker.

    Attributes:
        executor: Runtime executor shared by transport adapters.
        registry: Read-only use-case registry for name-based resolution.
        app: App-level invoker facade, bound to no identity: each use case it
            invokes acts as the identity acting where it is called. Typed as
            the concrete :class:`~loom.core.use_case.invoker.AppInvoker`, whose
            :meth:`~loom.core.use_case.invoker.AppInvoker.for_identity` binds a
            copy to one caller, a method the protocol does not declare.
    """

    executor: RuntimeExecutor
    registry: UseCaseRegistry
    app: AppInvoker


def create_kernel(
    *,
    config: object,
    use_cases: Sequence[type[Compilable]],
    modules: Sequence[Callable[[LoomContainer], None]] = (),
    logger: LoggerPort | None = None,
    metrics: MetricsAdapter | None = None,
    coordinator: Coordinator | None = None,
    repo_resolver: Callable[[type[Any]], Any] | None = None,
    environment: Environment | str | None = None,
    agent_resolver: ExecutionResolver | Callable[[str, Identity], Any] | None = None,
    mcp_resolver: ExecutionResolver | Callable[[str, tuple[str, ...], Identity], Any] | None = None,
) -> KernelRuntime:
    """Create a shared kernel runtime for application adapters.

    Registers *config* and the environment in a new container, runs
    *modules*, adds the kernel (:func:`install_kernel`) and validates every
    ``APPLICATION`` binding.

    Args:
        config: Application config object registered in DI at ``APPLICATION``
            scope under its concrete type.
        use_cases: Compilable classes to compile at startup.
        modules: Container registration modules, run in order.
        logger: Optional logger override; defaults to ``loom.bootstrap``.
        metrics: Optional metrics adapter.
        coordinator: Coordinator of the application's stores, which runs
            each execution in their units of work; ``None`` runs without one.
        repo_resolver: Optional repository resolver used by marker-based loads.
            Defaults to ``container.resolve_repo``.
        environment: Environment of the application, resolved like
            :func:`~loom.core.environment.resolve_environment` when ``None``.
            Registered at ``APPLICATION`` scope before the modules run, so a
            module's service or a use case can inject
            :class:`~loom.core.environment.Environment`.
        agent_resolver: Bound on the executor as by
            :meth:`~loom.core.engine.executor.RuntimeExecutor.bind_agent_resolver`.
            A singleton holding ``Agent()`` is built only with an
            :class:`~loom.core.execution.ExecutionResolver`; a legacy callable
            reads the identity when it resolves, so it cannot build one.
        mcp_resolver: Bound as by
            :meth:`~loom.core.engine.executor.RuntimeExecutor.bind_mcp_resolver`;
            the same holds for a singleton holding ``Mcp()``.

    Returns:
        Fully built :class:`KernelRuntime`.

    Raises:
        ConfigError: When *environment* is not an environment; raised as is.
        BootstrapError: Wrapping a module's failure, a use case's
            compilation error or an ``APPLICATION`` binding that cannot be
            built.
    """
    log = logger or get_logger("loom.bootstrap")
    seeds = {type(config): config, Environment: resolve_environment(environment)}
    with seeded_container(seeds, modules=modules, logger=log) as container:
        kernel = install_kernel(
            container,
            use_cases=use_cases,
            logger=log,
            metrics=metrics,
            coordinator=coordinator,
            repo_resolver=repo_resolver,
            agent_resolver=agent_resolver,
            mcp_resolver=mcp_resolver,
        )
        log.info("[BOOT] Validating container bindings")
        container.validate()
    log.info("[BOOT] Bootstrap complete")
    return kernel


@contextmanager
def seeded_container(
    seeds: Mapping[BindingKey, object],
    *,
    modules: Sequence[Callable[[LoomContainer], None]],
    logger: LoggerPort,
) -> Iterator[LoomContainer]:
    """A new container with each of *seeds* bound under its key, then filled by *modules*.

    Internal to loom: not public API, and it may change without deprecation.

    The container reads constructor annotations with
    :func:`~loom.core.repository.dependency.classify_key`. The seeds are bound
    at ``APPLICATION`` scope before any module runs, so a module can resolve
    them. The caller finishes the container inside the
    ``with`` block.

    Raises:
        BootstrapError: Wrapping any failure of a module or of the ``with``
            block, cause chained; a ``BootstrapError`` passes through as is.
    """
    try:
        logger.info("[BOOT] Initialising DI container")
        container = LoomContainer(classify_key=classify_key)
        for key, seed in seeds.items():
            container.register_instance(key, seed)
        for module in modules:
            name = getattr(module, "__qualname__", None) or repr(module)
            logger.info(f"[BOOT] Registering module: {name}")
            module(container)
        yield container
    except BootstrapError:
        raise
    except Exception as exc:
        raise BootstrapError(f"Bootstrap failed: {exc}") from exc


def install_use_cases(
    container: LoomContainer,
    use_cases: Sequence[type[Compilable]],
    *,
    logger: LoggerPort,
    metrics: MetricsAdapter | None,
) -> BootstrapResult:
    """Compile *use_cases* and register their :class:`UseCaseFactory` in the filled *container*.

    Internal to loom, called by :func:`install_kernel` and
    :func:`~loom.core.bootstrap.bootstrap_app`: not public API, and it may
    change without deprecation.

    Raises:
        CompilationError: When a use case does not compile.
    """
    logger.info(f"[BOOT] Compiling {len(use_cases)} use case(s)")
    compiler = UseCaseCompiler(logger=logger, metrics=metrics)
    for use_case in use_cases:
        compiler.compile(use_case)
    factory = UseCaseFactory(container)
    for use_case in use_cases:
        factory.register(use_case)
    container.register(UseCaseFactory, lambda: factory, scope=Scope.APPLICATION)
    return BootstrapResult(container=container, compiler=compiler, factory=factory, metrics=metrics)


def install_kernel(
    container: LoomContainer,
    *,
    use_cases: Sequence[type[Compilable]],
    logger: LoggerPort,
    metrics: MetricsAdapter | None = None,
    coordinator: Coordinator | None = None,
    repo_resolver: Callable[[type[Any]], Any] | None = None,
    agent_resolver: ExecutionResolver | Callable[[str, Identity], Any] | None = None,
    mcp_resolver: ExecutionResolver | Callable[[str, tuple[str, ...], Identity], Any] | None = None,
) -> KernelRuntime:
    """Compile *use_cases* and register the kernel's keys in the filled *container*.

    Internal to loom: not public API, and it may change without deprecation.

    Registers :class:`~loom.core.use_case.factory.UseCaseFactory`, one
    :class:`~loom.core.engine.executor.RuntimeExecutor` (whose elevation sink
    is the bound :class:`~loom.core.authz.elevation.ElevationSink`, if any),
    :class:`~loom.core.use_case.registry.UseCaseRegistry` and
    :class:`~loom.core.use_case.invoker.ApplicationInvoker`. Declares the
    use-case and AI kinds on ``container.kinds``, binds the ``Caller()``
    resolver there and gives the executor that registry, the factory's
    ``build`` as its builder, and the agent and MCP resolvers given, if any.
    Validates nothing:
    the caller does once the container is complete.

    Raises:
        CompilationError: When a use case does not compile.
    """
    kinds = container.kinds
    declare_use_case_kinds(kinds)
    declare_ai_kinds(kinds)
    kinds.bind(CALLER, CallerResolver())
    base = install_use_cases(container, use_cases, logger=logger, metrics=metrics)
    executor = RuntimeExecutor(
        base.compiler,
        coordinator=coordinator,
        metrics=metrics,
        repo_resolver=repo_resolver or container.resolve_repo,
        resolvers=kinds,
        builder=base.factory.build,
        elevation_sink=container.resolve_or_none(ElevationSink),
        agent_resolver=agent_resolver,
        mcp_resolver=mcp_resolver,
    )
    registry = UseCaseRegistry.build(list(use_cases))
    app = AppInvoker(factory=base.factory, executor=executor, registry=registry)
    container.register(RuntimeExecutor, lambda: executor, scope=Scope.APPLICATION)
    container.register(UseCaseRegistry, lambda: registry, scope=Scope.APPLICATION)
    container.register(ApplicationInvoker, lambda: app, scope=Scope.APPLICATION)
    return KernelRuntime(
        container=container,
        compiler=base.compiler,
        factory=base.factory,
        metrics=metrics,
        executor=executor,
        registry=registry,
        app=app,
    )
