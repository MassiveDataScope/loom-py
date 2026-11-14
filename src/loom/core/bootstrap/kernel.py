"""Shared kernel runtime assembly for transport adapters.

This module builds a reusable runtime composed of container, compiler,
factory, and executor. Transport adapters (REST, Celery, CLI) can consume
the same runtime without duplicating bootstrap logic.

:func:`create_kernel` is the public entry point. :func:`seeded_container`
and :meth:`KernelRecipe.install` are its steps.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from loom.core.authz.elevation import ElevationSink
from loom.core.bootstrap.bootstrap import BootstrapError, BootstrapResult
from loom.core.di.container import BindingKey, LoomContainer
from loom.core.di.errors import describe_key
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
from loom.core.use_case.factory import UseCaseFactory, main_repository
from loom.core.use_case.invoker import AppInvoker, ApplicationInvoker
from loom.core.use_case.kinds import CALLER, CallerResolver, declare_use_case_kinds
from loom.core.use_case.registry import UseCaseRegistry

KERNEL_KEYS: tuple[BindingKey, ...] = (
    UseCaseFactory,
    RuntimeExecutor,
    UseCaseRegistry,
    ApplicationInvoker,
)
"""The keys the kernel binds itself, which no caller's module may bind."""


@dataclass(frozen=True, kw_only=True)
class KernelRuntime(BootstrapResult):
    """Reusable application runtime: the bootstrap result plus its executor and invoker.

    Attributes:
        executor: Runtime executor shared by transport adapters.
        registry: Read-only use-case registry for name-based resolution.
        app: App-level invoker facade, bound to no identity: each use case it
            invokes acts as the identity acting where it is called;
            :meth:`~loom.core.use_case.invoker.AppInvoker.for_identity` binds a
            copy to one caller.
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
    *modules*, adds the kernel (:meth:`KernelRecipe.install`) and validates
    every ``APPLICATION`` binding. A module may not bind a key of
    :data:`KERNEL_KEYS`. The container stays unsealed: a key, the kernel's
    included, can still be registered once this returns.

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
            built; raised directly, naming the module, when a module binds a
            key of :data:`KERNEL_KEYS`.
    """
    log = logger or get_logger("loom.bootstrap")
    seeds = {type(config): config, Environment: resolve_environment(environment)}
    recipe = KernelRecipe(
        use_cases,
        log,
        metrics=metrics,
        coordinator=coordinator,
        repo_resolver=repo_resolver,
        agent_resolver=agent_resolver,
        mcp_resolver=mcp_resolver,
    )
    with seeded_container(
        KernelRecipe.container(), seeds, modules=modules, logger=log, reserved=KERNEL_KEYS
    ) as container:
        kernel = recipe.install(container)
        log.info("[BOOT] Validating container bindings")
        container.validate()
    log.info("[BOOT] Bootstrap complete")
    return kernel


@contextmanager
def seeded_container(
    container: LoomContainer,
    seeds: Mapping[BindingKey, object],
    *,
    modules: Sequence[Callable[[LoomContainer], None]],
    logger: LoggerPort,
    reserved: Collection[BindingKey] = (),
) -> Iterator[LoomContainer]:
    """*container* with each of *seeds* bound under its key, then filled by *modules*.

    The seeds are bound at ``APPLICATION`` scope before any module runs, so a
    module can resolve them. The caller finishes the container inside the
    ``with`` block.

    Raises:
        BootstrapError: Naming the module, when one binds or replaces a key
            of *reserved*; wrapping any other failure of a module or of the
            ``with`` block, cause chained. A ``BootstrapError`` passes through.
    """
    try:
        logger.info("[BOOT] Initialising DI container")
        for key, seed in seeds.items():
            container.register_instance(key, seed)
        view = container.bindings()
        for module in modules:
            name = getattr(module, "__qualname__", None) or repr(module)
            logger.info(f"[BOOT] Registering module: {name}")
            before = {key: view.get(key) for key in reserved}
            module(container)
            for key in reserved:
                if view.get(key) is not before[key]:
                    raise BootstrapError(
                        f"{name} binds {describe_key(key)}, a key the kernel binds itself"
                    )
        yield container
    except BootstrapError:
        raise
    except Exception as exc:
        raise BootstrapError(f"Bootstrap failed: {exc}") from exc


@dataclass(frozen=True)
class KernelRecipe:
    """The use cases a kernel compiles and the collaborators its executor runs with.

    Args:
        use_cases: Compilable classes to compile.
        logger: Logger of the compilation.
        metrics: Optional metrics adapter.
        coordinator: Coordinator of the application's stores; ``None`` runs without one.
        repo_resolver: Repository resolver of marker-based loads; defaults to
            the container's ``resolve_repo``.
        agent_resolver: As :func:`create_kernel`'s.
        mcp_resolver: As :func:`create_kernel`'s.
    """

    use_cases: Sequence[type[Compilable]]
    logger: LoggerPort
    metrics: MetricsAdapter | None = None
    coordinator: Coordinator | None = None
    repo_resolver: Callable[[type[Any]], Any] | None = None
    agent_resolver: ExecutionResolver | Callable[[str, Identity], Any] | None = None
    mcp_resolver: ExecutionResolver | Callable[[str, tuple[str, ...], Identity], Any] | None = None

    @staticmethod
    def container() -> LoomContainer:
        """Return a new container that reads constructors as loom builds a use case.

        Annotations are classified with
        :func:`~loom.core.repository.dependency.classify_key`, a use case
        built through ``UseCase.__init__`` gets its main repository
        (:func:`~loom.core.use_case.factory.main_repository`), and the
        use-case and AI kinds are declared before anything is registered.
        """
        container = LoomContainer(classify_key=classify_key, implicit=main_repository)
        declare_use_case_kinds(container.kinds)
        declare_ai_kinds(container.kinds)
        return container

    def install(self, container: LoomContainer) -> KernelRuntime:
        """Compile the use cases and register the kernel's keys in the filled *container*.

        Registers :class:`~loom.core.use_case.factory.UseCaseFactory`, one
        :class:`~loom.core.engine.executor.RuntimeExecutor` (whose elevation
        sink is the bound :class:`~loom.core.authz.elevation.ElevationSink`,
        if any), :class:`~loom.core.use_case.registry.UseCaseRegistry` and
        :class:`~loom.core.use_case.invoker.ApplicationInvoker`. Binds the
        ``Caller()`` resolver on ``container.kinds`` and gives the executor
        that registry, the factory's ``build`` as its builder, and the agent
        and MCP resolvers, if any. Validates nothing.

        Raises:
            CompilationError: When a use case does not compile.
        """
        kinds = container.kinds
        kinds.bind(CALLER, CallerResolver())
        base = self._install_use_cases(container)
        executor = RuntimeExecutor(
            base.compiler,
            coordinator=self.coordinator,
            metrics=self.metrics,
            repo_resolver=self.repo_resolver or container.resolve_repo,
            resolvers=kinds,
            builder=base.factory.build,
            elevation_sink=container.resolve_or_none(ElevationSink),
            agent_resolver=self.agent_resolver,
            mcp_resolver=self.mcp_resolver,
        )
        registry = UseCaseRegistry.build(list(self.use_cases))
        app = AppInvoker(factory=base.factory, executor=executor, registry=registry)
        container.register_instance(RuntimeExecutor, executor)
        container.register_instance(UseCaseRegistry, registry)
        container.register_instance(ApplicationInvoker, app)
        return KernelRuntime(
            container=container,
            compiler=base.compiler,
            factory=base.factory,
            metrics=self.metrics,
            executor=executor,
            registry=registry,
            app=app,
        )

    def _install_use_cases(self, container: LoomContainer) -> BootstrapResult:
        """Compile the use cases and register their :class:`UseCaseFactory` in *container*.

        Raises:
            CompilationError: When a use case does not compile.
        """
        self.logger.info(f"[BOOT] Compiling {len(self.use_cases)} use case(s)")
        compiler = UseCaseCompiler(logger=self.logger, metrics=self.metrics)
        for use_case in self.use_cases:
            compiler.compile(use_case)
        factory = UseCaseFactory(container)
        for use_case in self.use_cases:
            factory.register(use_case)
        container.register_instance(UseCaseFactory, factory)
        return BootstrapResult(
            container=container, compiler=compiler, factory=factory, metrics=self.metrics
        )
