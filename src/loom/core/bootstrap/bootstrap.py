"""Core-agnostic application bootstrap pipeline.

:func:`bootstrap_app` is the single entry point for starting a Loom
application.  It executes a deterministic pipeline:

1. Initialise logger.
2. Create :class:`~loom.core.di.container.LoomContainer` and register the
   config object as an ``APPLICATION``-scope singleton under its concrete type.
3. Execute user-supplied module callables to register infrastructure bindings.
4. Compile all declared ``UseCase`` subclasses via
   :class:`~loom.core.engine.compiler.UseCaseCompiler`.
5. Register :class:`~loom.core.use_case.factory.UseCaseFactory` in the container.
6. Validate the container (fail-fast).
7. Return :class:`BootstrapResult`.

The framework does **not** impose any shape on the config object.  Pass any
object — an omegaConf :class:`~omegaconf.DictConfig`, a ``msgspec.Struct``,
a dataclass, or a plain dict — and it will be registered under its concrete
type so modules can resolve it from the container.

The returned result is **framework-agnostic**.  Binding it to FastAPI (or any
other transport) is done separately in the infrastructure layer.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.compilable import Compilable
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.metrics import MetricsAdapter
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
) -> BootstrapResult:
    """Run the deterministic Loom bootstrap pipeline.

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

    Returns:
        :class:`BootstrapResult` with container, compiler, and factory.

    Raises:
        BootstrapError: If any pipeline step fails (compilation error,
            missing binding, singleton instantiation failure, etc.).

    Example::

        cfg = load_config("config/base.yaml", "config/production.yaml")

        result = bootstrap_app(
            config=cfg,
            use_cases=[CreateOrderUseCase, CancelOrderUseCase],
            modules=[register_repositories],
        )
        app = create_fastapi_app(result, RouteSources(python=[OrderRestInterface]))
    """
    _logger = logger or get_logger("loom.bootstrap")

    try:
        # Step 1 — Container + config binding
        _logger.info("[BOOT] Initialising DI container")
        container = LoomContainer()
        container.register(type(config), lambda: config, scope=Scope.APPLICATION)

        # Step 2 — User module bindings
        for module in modules:
            _logger.info(
                f"[BOOT] Registering module: {getattr(module, '__qualname__', repr(module))}"
            )
            module(container)

        # Step 3 — Compile use cases
        _logger.info(f"[BOOT] Compiling {len(use_cases)} use case(s)")
        compiler = UseCaseCompiler(logger=_logger, metrics=metrics)
        for uc_type in use_cases:
            compiler.compile(uc_type)

        # Step 4 — Factory
        factory = UseCaseFactory(container)
        for uc_type in use_cases:
            factory.register(uc_type)
        container.register(UseCaseFactory, lambda: factory, scope=Scope.APPLICATION)

        # Step 5 — Runtime executor (transport adapters consume this singleton)
        executor = RuntimeExecutor(
            compiler,
            metrics=metrics,
            repo_resolver=container.resolve_repo,
        )
        container.register(RuntimeExecutor, lambda: executor, scope=Scope.APPLICATION)

        # Step 6 — Validate container (fail-fast for APPLICATION scope)
        _logger.info("[BOOT] Validating container bindings")
        container.validate()

        _logger.info("[BOOT] Bootstrap complete")
        return BootstrapResult(
            container=container,
            compiler=compiler,
            factory=factory,
            metrics=metrics,
        )

    except BootstrapError:
        raise
    except Exception as exc:
        raise BootstrapError(f"Bootstrap failed: {exc}") from exc
