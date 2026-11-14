"""Deprecated shim over :mod:`loom.core.bootstrap.kernel`; use :func:`loom.composition.create_app`.

Also defines :class:`BootstrapError` and :class:`BootstrapResult`.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.compilable import Compilable
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.metrics import MetricsAdapter
from loom.core.environment import Environment, resolve_environment
from loom.core.logger import LoggerPort, get_logger
from loom.core.use_case.factory import UseCaseFactory


class BootstrapError(Exception):
    """Raised when the application bootstrap pipeline fails.

    Args:
        message: Human-readable description of the failure.

    Example::

        raise BootstrapError("Failed to compile CreateOrderUseCase") from exc
    """


@dataclass(frozen=True)
class BootstrapResult:
    """Result of a successful :func:`bootstrap_app` call.

    Contains the fully initialised container and compiler, ready for the
    infrastructure layer to bind to a transport (e.g. FastAPI).

    Attributes:
        container: Validated DI container with all bindings resolved.
        compiler: Use-case compiler with all plans cached.
        factory: Factory for constructing use-case instances per request.
        metrics: Optional metrics adapter passed to :func:`bootstrap_app`.
            Propagated to the transport layer so it can wire it into the
            :class:`~loom.core.engine.executor.RuntimeExecutor`.
    """

    container: LoomContainer
    compiler: UseCaseCompiler
    factory: UseCaseFactory
    metrics: MetricsAdapter | None = None


def bootstrap_app(
    config: object,
    use_cases: Sequence[type[Compilable]],
    modules: Sequence[Callable[[LoomContainer], None]] = (),
    logger: LoggerPort | None = None,
    metrics: MetricsAdapter | None = None,
    environment: Environment | str | None = None,
) -> BootstrapResult:
    """Run the deterministic Loom bootstrap pipeline.

    .. deprecated::
        Compose the application with :func:`loom.composition.create_app`, or
        build a kernel with :func:`~loom.core.bootstrap.kernel.create_kernel`.

    The framework imposes no shape on ``config`` — pass any object (omegaConf
    ``DictConfig``, ``msgspec.Struct``, dataclass, plain dict, etc.).  It is
    registered in the container under ``type(config)`` so modules can resolve
    it by that type.

    Args:
        config: Application configuration object.  Registered as an
            ``APPLICATION``-scope singleton under ``type(config)``.
        use_cases: Concrete compilable classes to compile at startup.
            Any class implementing the ``Compilable`` protocol is accepted,
            including ``UseCase`` and ``Job`` subclasses.
            All plans are cached in the returned compiler.
        modules: Callables that receive the container and register
            infrastructure bindings (repositories, services, etc.).
            Executed in declaration order before compilation.
        logger: Optional logger.  Falls back to the framework default.
        metrics: Optional metrics adapter passed to the compiler.
        environment: Environment of the application, resolved like
            :func:`~loom.core.environment.resolve_environment` (``LOOM_ENV``,
            else production) when ``None``. Registered at ``APPLICATION``
            scope with the configuration, before the modules run, so a
            module's service can resolve it.

    Returns:
        :class:`BootstrapResult` with container, compiler, and factory.

    Raises:
        BootstrapError: If any pipeline step fails (compilation error,
            missing binding, singleton instantiation failure, etc.).
    """
    warnings.warn(
        "bootstrap_app is deprecated; compose the application with "
        "loom.composition.create_app instead",
        DeprecationWarning,
        stacklevel=2,
    )
    from loom.core.bootstrap.kernel import bootstrapping, install_use_cases

    log = logger or get_logger("loom.bootstrap")
    seeds = {type(config): config, Environment: resolve_environment(environment)}
    with bootstrapping(seeds, modules=modules, logger=log) as container:
        result = install_use_cases(container, use_cases, logger=log, metrics=metrics)
        executor = RuntimeExecutor(
            result.compiler, metrics=metrics, repo_resolver=container.resolve_repo
        )
        container.register(RuntimeExecutor, lambda: executor, scope=Scope.APPLICATION)
        log.info("[BOOT] Validating container bindings")
        container.validate()
    log.info("[BOOT] Bootstrap complete")
    return result
