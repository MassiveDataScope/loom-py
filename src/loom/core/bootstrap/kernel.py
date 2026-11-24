"""The kernel of an application, built once for every transport adapter.

A :class:`Kernel` holds the container, compiler, factory, executor, registry
and invoker. Transport adapters (REST, Celery, CLI) consume the same kernel
without duplicating bootstrap logic.

:func:`create_kernel` is the public entry point. :func:`seeded_container`
and :meth:`KernelRecipe.install` are its steps.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from loom.core.authz.elevation import ElevationSink
from loom.core.bootstrap.bootstrap import BootstrapError
from loom.core.di.container import BindingKey, LoomContainer
from loom.core.di.errors import describe_key
from loom.core.engine.compilable import Compilable
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.execution_observer import ExecutionObserver
from loom.core.engine.executor import UseCaseExecutor
from loom.core.environment import Environment, resolve_environment
from loom.core.errors.summary import ErrorSummary
from loom.core.execution import ExecutionResolver
from loom.core.logger import LogEvent, LogField, LogKey
from loom.core.stores.repository.registration import classify_key
from loom.core.stores.uow.coordinator import Coordinator
from loom.core.use_case.factory import UseCaseFactory, main_repository
from loom.core.use_case.invoker import AppInvoker, ApplicationInvoker
from loom.core.use_case.kinds import declare_kinds
from loom.core.use_case.registry import UseCaseRegistry

_log = logging.getLogger(__name__)


class BootEvent(LogEvent):
    """What the bootstrap logs while it composes the container and the kernel."""

    CONTAINER_INITIALISING = "bootstrap.container_initialising"
    MODULE_REGISTERING = "bootstrap.module_registering"
    USE_CASES_COMPILING = "bootstrap.use_cases_compiling"
    CONTAINER_VALIDATING = "bootstrap.container_validating"
    KERNEL_COMPLETED = "bootstrap.kernel_completed"


class BootKey(LogKey):
    """The fields of a bootstrap record."""

    MODULE = "loom.bootstrap.module"


KERNEL_KEYS: tuple[BindingKey, ...] = (
    UseCaseFactory,
    UseCaseExecutor,
    UseCaseRegistry,
    ApplicationInvoker,
)
"""The keys the kernel binds itself, which no caller's module may bind."""


@dataclass(frozen=True, kw_only=True)
class Kernel:
    """The compiled core of an application, the same under every runtime.

    Attributes:
        container: Validated DI container with all bindings resolved.
        compiler: Use-case compiler with all plans cached.
        factory: Factory for constructing use-case instances per request.
        executor: Runtime executor shared by transport adapters.
        registry: Read-only use-case registry for name-based resolution.
        invoker: App-level invoker facade, bound to no identity: each use case
            it invokes acts as the identity acting where it is called;
            :meth:`~loom.core.use_case.invoker.AppInvoker.for_identity` binds a
            copy to one caller.
        execution_observer: Optional execution observer the executor reports to.
    """

    container: LoomContainer
    compiler: UseCaseCompiler
    factory: UseCaseFactory
    executor: UseCaseExecutor
    registry: UseCaseRegistry
    invoker: AppInvoker
    execution_observer: ExecutionObserver | None = None


def create_kernel(
    *,
    config: object,
    use_cases: Sequence[type[Compilable]],
    modules: Sequence[Callable[[LoomContainer], None]] = (),
    execution_observer: ExecutionObserver | None = None,
    coordinator: Coordinator | None = None,
    repo_resolver: Callable[[type[Any]], Any] | None = None,
    environment: Environment | str | None = None,
    agent_resolver: ExecutionResolver | None = None,
    mcp_resolver: ExecutionResolver | None = None,
) -> Kernel:
    """Create the kernel of an application: the one way to build its core.

    Registers *config* and the environment in a new container, runs
    *modules*, adds the kernel (:meth:`KernelRecipe.install`), validates
    every ``APPLICATION`` binding and verifies the use cases
    (:meth:`~loom.core.use_case.factory.UseCaseFactory.verify`). A module may
    not bind a key of :data:`KERNEL_KEYS`. The container stays unsealed: a
    key, the kernel's included, can still be registered once this returns.

    Args:
        config: Application config object registered in DI at ``APPLICATION``
            scope under its concrete type.
        use_cases: Compilable classes to compile at startup.
        modules: Container registration modules, run in order.
        execution_observer: Optional execution observer.
        coordinator: Coordinator of the application's stores, which runs
            each execution in their units of work; ``None`` runs without one.
        repo_resolver: Optional repository resolver used by marker-based loads.
            Defaults to ``container.resolve_repo``.
        environment: Environment of the application, resolved like
            :func:`~loom.core.environment.resolve_environment` when ``None``.
            Registered at ``APPLICATION`` scope before the modules run, so a
            module's service or a use case can inject
            :class:`~loom.core.environment.Environment`.
        agent_resolver: An :class:`~loom.core.execution.ExecutionResolver`
            bound as by :meth:`~loom.core.engine.executor.UseCaseExecutor.bind_agent_resolver`.
        mcp_resolver: An :class:`~loom.core.execution.ExecutionResolver`
            bound as by :meth:`~loom.core.engine.executor.UseCaseExecutor.bind_mcp_resolver`.

    Returns:
        Fully built :class:`Kernel`.

    Raises:
        ConfigError: When *environment* is not an environment; raised as is.
        BootstrapError: Wrapping a module's failure, a use case's
            compilation error, an ``APPLICATION`` binding that cannot be
            built or a use case's dependency the container cannot provide
            (:class:`~loom.core.di.graph.DependencyGraphError`, naming its
            path, or :class:`~loom.core.use_case.errors.UnservedModelError` for a use
            case whose main model has no repository), or an agent or MCP
            resolver without ``resolve(declaration, annotation)``; raised
            directly, naming the module, when a module binds a key of
            :data:`KERNEL_KEYS`.
    """
    seeds = {type(config): config, Environment: resolve_environment(environment)}
    recipe = KernelRecipe(
        use_cases,
        execution_observer=execution_observer,
        coordinator=coordinator,
        repo_resolver=repo_resolver,
        agent_resolver=agent_resolver,
        mcp_resolver=mcp_resolver,
    )
    with seeded_container(
        KernelRecipe.container(), seeds, modules=modules, reserved=KERNEL_KEYS
    ) as container:
        kernel = recipe.install(container)
        _log.info(BootEvent.CONTAINER_VALIDATING)
        container.validate()
        kernel.factory.verify()
    _log.info(BootEvent.KERNEL_COMPLETED)
    return kernel


def _kernel_key_bound(module: Callable[..., object], key: BindingKey) -> BootstrapError:
    name = getattr(module, "__qualname__", None) or repr(module)
    return BootstrapError(f"{name} binds {describe_key(key)}, a key the kernel binds itself")


@contextmanager
def seeded_container(
    container: LoomContainer,
    seeds: Mapping[BindingKey, object],
    *,
    modules: Sequence[Callable[[LoomContainer], None]],
    reserved: Collection[BindingKey] = (),
    error: Callable[[Callable[..., object], BindingKey], BootstrapError] = _kernel_key_bound,
) -> Iterator[LoomContainer]:
    """*container* with each of *seeds* bound under its key, then filled by *modules*.

    The seeds are bound at ``APPLICATION`` scope before any module runs, so a
    module can resolve them. The caller finishes the container inside the
    ``with`` block.

    Args:
        container: The container to fill.
        seeds: Instances bound under their key before any module runs.
        modules: DI modules run in order.
        reserved: Keys no module may bind or replace.
        error: Builds the error raised for a module and the reserved key it bound.

    Raises:
        BootstrapError: Built by *error* when a module binds or replaces a
            key of *reserved*; wrapping any other failure of a module or of
            the ``with`` block, cause chained. A ``BootstrapError`` passes through.
    """
    try:
        _log.info(BootEvent.CONTAINER_INITIALISING)
        for key, seed in seeds.items():
            container.register_instance(key, seed)
        view = container.bindings()
        for module in modules:
            name = getattr(module, "__qualname__", None) or repr(module)
            _log.info(BootEvent.MODULE_REGISTERING, extra={BootKey.MODULE: name})
            before = {key: view.get(key) for key in reserved}
            module(container)
            for key in reserved:
                if view.get(key) is not before[key]:
                    raise error(module, key)
        yield container
    except BootstrapError:
        raise
    except Exception as exc:
        raise BootstrapError(f"Bootstrap failed: {ErrorSummary.message_for(exc)}") from exc


@dataclass(frozen=True)
class KernelRecipe:
    """The use cases a kernel compiles and the collaborators its executor runs with.

    Args:
        use_cases: Compilable classes to compile.
        execution_observer: Optional execution observer.
        coordinator: Coordinator of the application's stores; ``None`` runs without one.
        repo_resolver: Repository resolver of marker-based loads; defaults to
            the container's ``resolve_repo``.
        agent_resolver: As :func:`create_kernel`'s.
        mcp_resolver: As :func:`create_kernel`'s.
    """

    use_cases: Sequence[type[Compilable]]
    execution_observer: ExecutionObserver | None = None
    coordinator: Coordinator | None = None
    repo_resolver: Callable[[type[Any]], Any] | None = None
    agent_resolver: ExecutionResolver | None = None
    mcp_resolver: ExecutionResolver | None = None

    @staticmethod
    def container() -> LoomContainer:
        """Return a new container that reads constructors as loom builds a use case.

        Annotations are classified with
        :func:`~loom.core.stores.repository.registration.classify_key`, a use case
        built through ``UseCase.__init__`` gets its main repository
        (:func:`~loom.core.use_case.factory.main_repository`), and the kinds
        are declared (:func:`~loom.core.use_case.kinds.declare_kinds`) before
        anything is registered.
        """
        container = LoomContainer(classify_key=classify_key, implicit=main_repository)
        declare_kinds(container.kinds)
        return container

    def install(self, container: LoomContainer) -> Kernel:
        """Compile the use cases and register the kernel's keys in the filled *container*.

        Registers :class:`~loom.core.use_case.factory.UseCaseFactory`, one
        :class:`~loom.core.engine.executor.UseCaseExecutor` (whose elevation
        sink is the bound :class:`~loom.core.authz.elevation.ElevationSink`,
        if any), :class:`~loom.core.use_case.registry.UseCaseRegistry` and
        :class:`~loom.core.use_case.invoker.ApplicationInvoker`. Gives the
        executor ``container.kinds``, that registry, the factory's ``build``
        as its builder, and the agent and MCP resolvers, if any. Validates
        nothing.

        Raises:
            CompilationError: When a use case does not compile.
        """
        compiler, factory = self._install_use_cases(container)
        executor = UseCaseExecutor(
            compiler,
            coordinator=self.coordinator,
            execution_observer=self.execution_observer,
            repo_resolver=self.repo_resolver or container.resolve_repo,
            resolvers=container.kinds,
            builder=factory.build,
            elevation_sink=container.resolve_or_none(ElevationSink),
            agent_resolver=self.agent_resolver,
            mcp_resolver=self.mcp_resolver,
        )
        registry = UseCaseRegistry.build(list(self.use_cases))
        invoker = AppInvoker(factory=factory, executor=executor, registry=registry)
        container.register_instance(UseCaseExecutor, executor)
        container.register_instance(UseCaseRegistry, registry)
        container.register_instance(ApplicationInvoker, invoker)
        return Kernel(
            container=container,
            compiler=compiler,
            factory=factory,
            executor=executor,
            registry=registry,
            invoker=invoker,
            execution_observer=self.execution_observer,
        )

    def _install_use_cases(
        self, container: LoomContainer
    ) -> tuple[UseCaseCompiler, UseCaseFactory]:
        """Compile the use cases and register their :class:`UseCaseFactory` in *container*.

        Raises:
            CompilationError: When a use case does not compile.
        """
        _log.info(
            BootEvent.USE_CASES_COMPILING, extra={LogField.USE_CASE_COUNT: len(self.use_cases)}
        )
        compiler = UseCaseCompiler(execution_observer=self.execution_observer)
        for use_case in self.use_cases:
            compiler.compile(use_case)
        factory = UseCaseFactory(container)
        for use_case in self.use_cases:
            factory.register(use_case)
        container.register_instance(UseCaseFactory, factory)
        return compiler, factory
