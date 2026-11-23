"""Resource lifecycle management for streaming Bytewax operators."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import (
    AbstractAsyncContextManager,
    AbstractContextManager,
    AsyncExitStack,
    ExitStack,
    suppress,
)
from typing import Any, Protocol, TypeVar, overload

from loom.core.async_bridge import AsyncBridge
from loom.core.errors import InvalidUse
from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming.core._exceptions import MissingBridgeError
from loom.streaming.nodes._with import (
    ContextSupplier,
    ResourceScope,
    With,
    WithAsync,
)

InT = TypeVar("InT", bound=LoomStruct | LoomFrozenStruct)
OutT = TypeVar("OutT", bound=LoomStruct | LoomFrozenStruct)


class ResourceLifecycle(Protocol):
    """Open and close scoped context-manager dependencies."""

    def open_worker(self) -> dict[str, object]:
        """Open worker-scoped resources and return injectable dependencies."""
        ...

    def open_batch(self) -> dict[str, object]:
        """Open batch-scoped resources and return injectable dependencies."""
        ...

    def close_batch(self) -> None:
        """Close batch-scoped resources."""
        ...

    def shutdown(self) -> None:
        """Close worker-scoped resources."""
        ...


def _exit_entered(
    entered: list[tuple[str, AbstractContextManager[object]]],
) -> None:
    """Exit previously entered sync CMs in reverse order, suppressing errors."""
    for _, cm in reversed(entered):
        with suppress(Exception):
            cm.__exit__(None, None, None)


async def _aexit_entered(
    entered: list[tuple[str, AbstractAsyncContextManager[object]]],
) -> None:
    """Exit previously entered async CMs in reverse order, suppressing errors."""
    for _, cm in reversed(entered):
        with suppress(Exception):
            await cm.__aexit__(None, None, None)


class SyncResourceLifecycle:
    """Open and close sync context-manager dependencies.

    For ``WORKER`` scope an :class:`ExitStack` groups all CMs so they
    are entered once and closed together at shutdown.

    For ``BATCH`` scope each CM (or factory-produced CM) is entered/exited
    directly per batch.  Factories are called via ``create()`` to obtain a
    fresh instance every time.
    """

    __slots__ = (
        "_context_suppliers",
        "_contexts",
        "_entered",
        "_plain_deps",
        "_resources",
        "_scope",
        "_stack",
    )

    def __init__(
        self,
        *,
        scope: ResourceScope,
        contexts: Mapping[str, AbstractContextManager[object]],
        context_suppliers: Mapping[str, ContextSupplier],
        plain_deps: Mapping[str, object],
    ) -> None:
        self._scope = scope
        self._contexts = dict(contexts)
        self._context_suppliers = dict(context_suppliers)
        self._plain_deps = dict(plain_deps)
        self._stack: ExitStack | None = None
        self._entered: list[tuple[str, AbstractContextManager[object]]] = []
        self._resources: dict[str, object] = {}

    def open_worker(self) -> dict[str, object]:
        """Open worker-scoped resources and return injectable dependencies."""
        if self._scope == ResourceScope.WORKER:
            self._resources = self._enter_with_stack()
        self._resources.update(self._plain_deps)
        return dict(self._resources)

    def open_batch(self) -> dict[str, object]:
        """Open batch-scoped resources and return injectable dependencies."""
        if self._scope == ResourceScope.BATCH:
            self._resources = self._enter_direct()
            self._resources.update(self._plain_deps)
        return dict(self._resources)

    def close_batch(self) -> None:
        """Close batch-scoped resources."""
        if self._scope == ResourceScope.BATCH:
            self._exit_direct()

    def shutdown(self) -> None:
        """Close worker-scoped resources."""
        if self._scope == ResourceScope.WORKER:
            self._exit_stack()

    def _resolve_all_contexts(self) -> dict[str, AbstractContextManager[object]]:
        """Return direct CMs merged with freshly-created factory CMs."""
        resolved = dict(self._contexts)
        for name, factory in self._context_suppliers.items():
            resolved[name] = factory.create()  # type: ignore[assignment]
        return resolved

    def _enter_with_stack(self) -> dict[str, object]:
        """Enter all CMs via ExitStack (worker scope — entered once)."""
        self._stack = ExitStack()
        contexts = self._resolve_all_contexts()
        return {name: self._stack.enter_context(cm) for name, cm in contexts.items()}

    def _exit_stack(self) -> None:
        if self._stack is not None:
            self._stack.close()
            self._stack = None
        self._resources = {}

    def _enter_direct(self) -> dict[str, object]:
        """Enter each CM directly (batch scope — fresh instance per batch)."""
        resources: dict[str, object] = {}
        entered: list[tuple[str, AbstractContextManager[object]]] = []
        for name, cm in self._resolve_all_contexts().items():
            try:
                value = cm.__enter__()
            except Exception:
                _exit_entered(entered)
                raise
            resources[name] = value
            entered.append((name, cm))
        self._entered = entered
        return resources

    def _exit_direct(self) -> None:
        _exit_entered(self._entered)
        self._entered = []
        self._resources = {}


class AsyncResourceLifecycle:
    """Open and close async context-manager dependencies through AsyncBridge.

    For ``WORKER`` scope an :class:`AsyncExitStack` groups all CMs so
    they are entered once and closed together at shutdown.

    For ``BATCH`` scope each CM (or factory-produced CM) is entered/exited
    directly per batch.  Factories are called via ``create()`` to obtain a
    fresh instance every time.
    """

    __slots__ = (
        "_bridge",
        "_context_suppliers",
        "_contexts",
        "_entered",
        "_plain_deps",
        "_resources",
        "_scope",
        "_stack",
    )

    def __init__(
        self,
        *,
        scope: ResourceScope,
        contexts: Mapping[str, AbstractAsyncContextManager[object]],
        context_suppliers: Mapping[str, ContextSupplier],
        plain_deps: Mapping[str, object],
        bridge: AsyncBridge,
    ) -> None:
        self._scope = scope
        self._contexts = dict(contexts)
        self._context_suppliers = dict(context_suppliers)
        self._plain_deps = dict(plain_deps)
        self._bridge = bridge
        self._stack: AsyncExitStack | None = None
        self._entered: list[tuple[str, AbstractAsyncContextManager[object]]] = []
        self._resources: dict[str, object] = {}

    def open_worker(self) -> dict[str, object]:
        """Open worker-scoped resources and return injectable dependencies."""
        if self._scope == ResourceScope.WORKER:
            self._resources = self._enter_with_stack()
        self._resources.update(self._plain_deps)
        return dict(self._resources)

    def open_batch(self) -> dict[str, object]:
        """Open batch-scoped resources and return injectable dependencies."""
        if self._scope == ResourceScope.BATCH:
            self._resources = self._enter_direct()
            self._resources.update(self._plain_deps)
        return dict(self._resources)

    def close_batch(self) -> None:
        """Close batch-scoped resources."""
        if self._scope == ResourceScope.BATCH:
            self._exit_direct()

    def shutdown(self) -> None:
        """Close worker-scoped resources."""
        if self._scope == ResourceScope.WORKER:
            self._exit_stack()

    def _resolve_all_contexts(self) -> dict[str, AbstractAsyncContextManager[object]]:
        """Return direct CMs merged with freshly-created factory CMs."""
        resolved = dict(self._contexts)
        for name, factory in self._context_suppliers.items():
            resolved[name] = factory.create()  # type: ignore[assignment]
        return resolved

    def _enter_with_stack(self) -> dict[str, object]:
        """Enter all CMs via AsyncExitStack (worker scope — entered once)."""
        self._stack = AsyncExitStack()
        contexts = self._resolve_all_contexts()

        async def _open_all() -> dict[str, object]:
            if self._stack is None:
                raise InvalidUse("AsyncExitStack was not initialized.")
            return {
                name: await self._stack.enter_async_context(cm) for name, cm in contexts.items()
            }

        return self._bridge.run(_open_all())

    def _exit_stack(self) -> None:
        if self._stack is not None:
            self._bridge.run(self._stack.aclose())
            self._stack = None
        self._resources = {}

    def _enter_direct(self) -> dict[str, object]:
        """Enter each CM directly (batch scope — fresh instance per batch)."""
        bridge = self._bridge
        contexts = self._resolve_all_contexts()

        async def _open_all() -> dict[str, object]:
            resources: dict[str, object] = {}
            entered: list[tuple[str, AbstractAsyncContextManager[object]]] = []
            for name, cm in contexts.items():
                try:
                    value = await cm.__aenter__()
                except Exception:
                    await _aexit_entered(entered)
                    raise
                resources[name] = value
                entered.append((name, cm))
            self._entered = entered
            return resources

        return bridge.run(_open_all())

    def _exit_direct(self) -> None:
        entered = self._entered
        self._entered = []
        self._resources = {}
        if entered:
            self._bridge.run(_aexit_entered(entered))


@overload
def lifecycle_for(
    node: With[InT, OutT],
    *,
    bridge: AsyncBridge | None = None,
) -> SyncResourceLifecycle: ...


@overload
def lifecycle_for(
    node: WithAsync[InT, OutT],
    *,
    bridge: AsyncBridge | None = None,
) -> AsyncResourceLifecycle: ...


def lifecycle_for(
    node: With[Any, Any] | WithAsync[Any, Any],
    *,
    bridge: AsyncBridge | None = None,
) -> ResourceLifecycle:
    """Create the resource lifecycle matching a scoped dependency node.

    Args:
        node: DSL node carrying scope, contexts and plain dependencies.
        bridge: Async bridge required for ``WithAsync``.

    Returns:
        Configured lifecycle ready for open/close calls.
    """
    if isinstance(node, WithAsync):
        if bridge is None:
            raise MissingBridgeError("WithAsync dependencies require an AsyncBridge.")
        return AsyncResourceLifecycle(
            scope=node.scope,
            contexts=node.async_contexts,
            context_suppliers=node.context_suppliers,
            plain_deps=node.plain_deps,
            bridge=bridge,
        )
    return SyncResourceLifecycle(
        scope=node.scope,
        contexts=node.sync_contexts,
        context_suppliers=node.context_suppliers,
        plain_deps=node.plain_deps,
    )


__all__ = [
    "AsyncResourceLifecycle",
    "ResourceLifecycle",
    "SyncResourceLifecycle",
    "lifecycle_for",
]
