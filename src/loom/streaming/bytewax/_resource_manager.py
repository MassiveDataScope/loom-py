"""Resource lifecycle management for the Bytewax adapter."""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any, Protocol, cast, runtime_checkable

from loom.core.async_bridge import AsyncBridge
from loom.core.stores.database import DatabaseConfig
from loom.core.stores.profile import STREAMING_PROFILE, RuntimeProfile
from loom.streaming.bytewax._operators import ResourceLifecycle, lifecycle_for
from loom.streaming.core._exceptions import MissingBridgeError
from loom.streaming.core._typing import StreamPayload
from loom.streaming.nodes._with import With, WithAsync


@runtime_checkable
class _ResourceManagerProtocol(Protocol):
    """Manage per-node lifecycle objects during adapter wiring."""

    def manager_for(
        self,
        idx: int,
        node: With[StreamPayload, StreamPayload] | WithAsync[StreamPayload, StreamPayload],
    ) -> ResourceLifecycle:
        """Return the lifecycle manager for one scoped node."""
        ...

    def shutdown_all(self) -> None:
        """Shutdown all lifecycle managers."""
        ...


@runtime_checkable
class _SharedSessionManagerProtocol(Protocol):
    """Minimal SQLAlchemy session-manager contract cached by the adapter."""

    def close(self) -> Coroutine[Any, Any, None]:
        """Close pooled connections and release engine resources."""
        ...


class ResourceManager:
    """Own the resource lifecycle objects used by scoped nodes."""

    __slots__ = ("_bridge", "_managers", "_session_managers")

    def __init__(self, bridge: AsyncBridge | None) -> None:
        self._bridge = bridge
        self._managers: dict[int, ResourceLifecycle] = {}
        self._session_managers: dict[DatabaseConfig, _SharedSessionManagerProtocol] = {}

    def manager_for(
        self,
        idx: int,
        node: With[StreamPayload, StreamPayload] | WithAsync[StreamPayload, StreamPayload],
    ) -> ResourceLifecycle:
        """Get or create the lifecycle manager for one scoped node."""
        if idx not in self._managers:
            self._managers[idx] = lifecycle_for(node, bridge=self._bridge)
        return self._managers[idx]

    def session_manager_for(self, config: DatabaseConfig) -> _SharedSessionManagerProtocol:
        """Get or create one shared SQLAlchemy session manager for a database section."""
        if self._bridge is None:
            raise MissingBridgeError("SQLAlchemy session managers require an async bridge.")
        if config not in self._session_managers:
            self._session_managers[config] = _session_manager_builder()(config, STREAMING_PROFILE)
        return self._session_managers[config]

    def shutdown_all(self) -> None:
        """Shutdown all managed resources and any associated bridge."""
        errors: list[Exception] = []
        for manager in self._managers.values():
            try:
                manager.shutdown()
            except Exception as exc:
                errors.append(exc)
        bridge = self._bridge
        for session_manager in self._session_managers.values():
            if bridge is None:
                break
            try:
                bridge.run(session_manager.close())
            except Exception as exc:
                errors.append(exc)
        self._managers.clear()
        self._session_managers.clear()
        if self._bridge is not None:
            try:
                self._bridge.shutdown()
            except Exception as exc:
                errors.append(exc)
            finally:
                self._bridge = None
        if errors:
            raise ExceptionGroup("shutdown errors", errors)


def _session_manager_builder() -> Callable[
    [DatabaseConfig, RuntimeProfile], _SharedSessionManagerProtocol
]:
    """Return the configured session-manager constructor, or fail naming its extra."""
    try:
        from loom.stores.sqlalchemy.session_manager import build_session_manager
    except ImportError as exc:
        raise ImportError(
            "IntoTable(engine=SQLALCHEMY) requires SQLAlchemy. "
            "Install loom-kernel[sqlalchemy] to write to SQLAlchemy tables."
        ) from exc
    return cast(
        Callable[[DatabaseConfig, RuntimeProfile], _SharedSessionManagerProtocol],
        build_session_manager,
    )


__all__ = ["ResourceManager", "_ResourceManagerProtocol"]
