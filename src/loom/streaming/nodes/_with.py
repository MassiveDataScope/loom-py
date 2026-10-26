"""Scoped dependency declarations for streaming tasks."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Generic, TypeGuard, TypeVar

from loom.core.config import Configurable
from loom.core.logger import LoggerPort, get_logger
from loom.core.model import LoomFrozenStruct, LoomStruct

if TYPE_CHECKING:
    from loom.streaming.graph._flow import Process

InT = TypeVar("InT", bound=LoomStruct | LoomFrozenStruct)
OutT = TypeVar("OutT", bound=LoomStruct | LoomFrozenStruct)

SyncContextDependency = AbstractContextManager[object]
AsyncContextDependency = AbstractAsyncContextManager[object]
ContextDependency = SyncContextDependency | AsyncContextDependency


class ResourceScope(StrEnum):
    """Context-manager lifecycle for dependencies declared through ``With``.

    Pattern:
        Wrapper scope.
    """

    WORKER = "worker"
    BATCH = "batch"


class ContextFactory(Configurable):
    """Factory that creates a fresh context manager on demand.

    Pattern:
        Wrapper dependency.

    Use this when you need a new context-manager instance per batch
    (``scope=BATCH``) or when you want to configure the factory from YAML.

    Args:
        factory: Callable that returns a context-manager instance.

    Example::

        db = ContextFactory(lambda: SessionLocal())
        With(process=Process(ValidateOrder, IntoTopic("validated")), scope=BATCH, db=db)
    """

    __slots__ = ("_factory", "_log")

    def __init__(self, factory: Callable[[], ContextDependency]) -> None:
        self._factory = factory
        self._log: LoggerPort | None = None

    def create(self) -> ContextDependency:
        """Return a fresh context-manager instance."""
        return self._factory()

    @property
    def log(self) -> LoggerPort:
        """Structured logger bound to this factory class and module."""
        logger = self._log
        if logger is None:
            cls = type(self)
            logger = get_logger(cls.__qualname__).bind(
                component="context_factory",
                class_name=cls.__qualname__,
                module=cls.__module__,
                factory_name=cls.__qualname__,
            )
            self._log = logger
        return logger


class _WithBase(Generic[InT, OutT]):
    """Shared dependency scope and lifecycle management for streaming nodes.

    Pattern:
        Wrapper node.

    Classifies keyword dependencies into async context managers, sync context
    managers, :class:`ContextFactory` instances, and plain values.  Subclasses
    are responsible for declaring and validating their primary execution target
    (a step or a process).

    Args:
        scope: Lifecycle used when opening context-manager dependencies.
        process: Inner process executed by the wrapper node.
        **dependencies: Named dependencies injected at execution time.
    """

    __slots__ = (
        "_async_contexts",
        "_context_factories",
        "_plain_deps",
        "_sync_contexts",
        "process",
        "scope",
    )

    def __init__(
        self,
        scope: ResourceScope = ResourceScope.WORKER,
        *,
        process: Process[Any, Any],
        **dependencies: object,
    ) -> None:
        self.process = process
        self.scope = scope
        sync_contexts: dict[str, SyncContextDependency] = {}
        async_contexts: dict[str, AsyncContextDependency] = {}
        context_factories: dict[str, ContextFactory] = {}
        plain_deps: dict[str, object] = {}
        for name, dependency in dependencies.items():
            if isinstance(dependency, ContextFactory):
                context_factories[name] = dependency
            elif _is_sync_context_manager(dependency):
                sync_contexts[name] = dependency
            elif _is_async_context_manager(dependency):
                async_contexts[name] = dependency
            else:
                plain_deps[name] = dependency
        self._sync_contexts = sync_contexts
        self._async_contexts = async_contexts
        self._context_factories = context_factories
        self._plain_deps = plain_deps

    @property
    def sync_contexts(self) -> Mapping[str, SyncContextDependency]:
        """Sync context-manager dependencies keyed by injection name."""
        return MappingProxyType(self._sync_contexts)

    @property
    def async_contexts(self) -> Mapping[str, AsyncContextDependency]:
        """Async context-manager dependencies keyed by injection name."""
        return MappingProxyType(self._async_contexts)

    @property
    def context_factories(self) -> Mapping[str, ContextFactory]:
        """Context-manager factories keyed by injection name."""
        return MappingProxyType(self._context_factories)

    @property
    def plain_deps(self) -> Mapping[str, object]:
        """Plain dependencies keyed by injection name."""
        return MappingProxyType(self._plain_deps)


class With(_WithBase[InT, OutT]):
    """Declare a sync dependency scope around an inner process.

    Pattern:
        Sync wrapper.

    Each incoming message flows through the inner
    :class:`~loom.streaming.graph.Process` synchronously.  If the last
    node of the inner process is an
    :class:`~loom.streaming.nodes._boundary.IntoTopic`, results are written
    directly to Kafka as each message completes — no ``ForEach`` or outer
    ``IntoTopic`` is required.  The outer stream is drained after this node
    (``StreamShape.NONE``).

    Args:
        scope: Lifecycle used when opening context-manager dependencies.
        process: Inner process executed per message.
        **dependencies: Named sync context managers, factories, or plain
            values injected into step execution.

    Raises:
        TypeError: If an async context manager is passed directly.

    Example::

        With(
            process=Process(ValidateOrder(), IntoTopic("validated", payload=Validated)),
            scope=ResourceScope.WORKER,
            db=SessionLocal(),
        )
    """

    __slots__ = ()

    def __init__(
        self,
        scope: ResourceScope = ResourceScope.WORKER,
        *,
        process: Process[Any, Any],
        **dependencies: object,
    ) -> None:
        super().__init__(scope=scope, process=process, **dependencies)
        if self._async_contexts:
            names = ", ".join(self._async_contexts.keys())
            raise TypeError(
                f"With only accepts sync context managers. "
                f"Async context manager(s) found: {names}. "
                f"Use WithAsync for async context managers."
            )


class WithAsync(_WithBase[InT, OutT]):
    """Declare an async dependency scope around an inner process.

    Pattern:
        Async wrapper.

    Each incoming message flows through the inner
    :class:`~loom.streaming.graph.Process` asynchronously.  If the last
    node of the inner process is an
    :class:`~loom.streaming.nodes._boundary.IntoTopic`, results are written
    directly to Kafka as each message completes — no ``ForEach`` or outer
    ``IntoTopic`` is required.  The outer stream is drained after this node
    (``StreamShape.NONE``).

    Args:
        scope: Lifecycle used when opening context-manager dependencies.
        max_concurrency: Maximum concurrent message executions.
        task_timeout_ms: Optional per-message wall-clock deadline in
            milliseconds.  When set, each message execution is cancelled with
            :class:`TimeoutError` if it exceeds the deadline.
        process: Inner process executed per message.
        **dependencies: Named async context managers, factories, or plain
            values injected into step execution.

    Raises:
        TypeError: If a sync context manager is passed directly.
        ValueError: If *max_concurrency* is not positive.
        ValueError: If *task_timeout_ms* is not positive when provided.

    Example::

        WithAsync(
            process=Process(FetchTask(), IntoTopic("results", payload=Result)),
            scope=ResourceScope.WORKER,
            max_concurrency=50,
            http=ContextFactory(lambda: httpx.AsyncClient()),
        )
    """

    __slots__ = ("max_concurrency", "task_timeout_ms")

    def __init__(
        self,
        scope: ResourceScope = ResourceScope.WORKER,
        max_concurrency: int = 10,
        task_timeout_ms: int | None = None,
        *,
        process: Process[Any, Any],
        **dependencies: object,
    ) -> None:
        if max_concurrency <= 0:
            raise ValueError("max_concurrency must be greater than zero")
        if task_timeout_ms is not None and task_timeout_ms <= 0:
            raise ValueError("task_timeout_ms must be greater than zero when provided")
        super().__init__(scope=scope, process=process, **dependencies)
        if self._sync_contexts:
            names = ", ".join(self._sync_contexts.keys())
            raise TypeError(
                f"WithAsync only accepts async context managers. "
                f"Sync context manager(s) found: {names}. "
                f"Use With for sync context managers."
            )
        self.max_concurrency = max_concurrency
        self.task_timeout_ms: int | None = task_timeout_ms


def _is_sync_context_manager(value: object) -> TypeGuard[SyncContextDependency]:
    return isinstance(value, AbstractContextManager)


def _is_async_context_manager(value: object) -> TypeGuard[AsyncContextDependency]:
    return isinstance(value, AbstractAsyncContextManager)


__all__ = [
    "AsyncContextDependency",
    "ContextDependency",
    "ContextFactory",
    "ResourceScope",
    "SyncContextDependency",
    "With",
    "WithAsync",
]
