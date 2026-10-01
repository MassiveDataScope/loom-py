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
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.metrics import MetricsAdapter
from loom.core.logger import LoggerPort
from loom.core.uow.abc import UnitOfWorkFactory
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
    config: object,
    use_cases: Sequence[type[Compilable]],
    modules: Sequence[Callable[[LoomContainer], None]] = (),
    logger: LoggerPort | None = None,
    metrics: MetricsAdapter | None = None,
    uow_factory: UnitOfWorkFactory | None = None,
    repo_resolver: Callable[[type[Any]], Any] | None = None,
) -> KernelRuntime:
    """Create a shared kernel runtime for application adapters.

    Args:
        config: Application config object registered in DI at ``APPLICATION``
            scope under its concrete type.
        use_cases: Compilable classes to compile at startup.
        modules: Container registration modules.
        logger: Optional logger override.
        metrics: Optional metrics adapter.
        uow_factory: Optional UnitOfWork factory for transactional execution.
        repo_resolver: Optional repository resolver used by marker-based loads.
            Defaults to ``container.resolve_repo``.

    Returns:
        Fully built :class:`KernelRuntime`.
    """
    base = bootstrap_app(
        config=config,
        use_cases=use_cases,
        modules=modules,
        logger=logger,
        metrics=metrics,
    )
    resolver = repo_resolver or base.container.resolve_repo
    executor = RuntimeExecutor(
        base.compiler,
        uow_factory=uow_factory,
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


def _registered_sink(container: LoomContainer) -> ElevationSink | None:
    if container.is_registered(ElevationSink):
        sink: ElevationSink = container.resolve(ElevationSink)
        return sink
    return None
