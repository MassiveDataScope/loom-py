"""Persistent async portal for synchronous runtimes.

Wraps anyio's blocking portal so that synchronous workers (Celery prefork,
Bytewax operators) can execute async code without managing event loops directly.

Lifecycle::

    bridge = AsyncBridge()
    result = bridge.run(some_coroutine())
    bridge.shutdown()

Backend selection::

    # asyncio (default)
    bridge = AsyncBridge()

    # asyncio + uvloop
    bridge = AsyncBridge(backend="asyncio", backend_options={"loop_factory": uvloop.new_event_loop})

    # trio
    bridge = AsyncBridge(backend="trio")

Thread safety:
    :meth:`run` is safe to call from the thread that owns the bridge.
    :meth:`shutdown` must be called once at worker exit.
"""

from __future__ import annotations

import logging
import sys
import threading
from collections.abc import Coroutine
from typing import Any, TypeVar, cast

import anyio
from anyio.from_thread import BlockingPortal, start_blocking_portal

from loom.core.errors import InvalidUse
from loom.core.log_names import LogEvent, LogKey, ValueKind

logger = logging.getLogger(__name__)


class BridgeEvent(LogEvent):
    """What the async bridge logs."""

    SHUTDOWN_TIMED_OUT = "bridge.shutdown_timed_out"


class BridgeKey(LogKey):
    """The fields of an async bridge record."""

    TIMEOUT_MS = "loom.bridge.timeout_ms", ValueKind.NUMBER


T = TypeVar("T")


def build_backend_options(backend: str, use_uvloop: bool) -> dict[str, Any]:
    """Build AnyIO backend options for the shared async bridge contract.

    Args:
        backend: AnyIO backend name.
        use_uvloop: Whether asyncio should use uvloop when available.

    Returns:
        A backend-options mapping suitable for :class:`AsyncBridge`.
    """
    if backend == "asyncio" and use_uvloop and sys.platform != "win32":
        import uvloop

        return {"loop_factory": uvloop.new_event_loop}
    return {}


class AsyncBridge:
    """Anyio blocking portal wrapper for sync-to-async execution.

    Args:
        backend: Anyio backend name — ``"asyncio"`` or ``"trio"``.
        backend_options: Backend-specific options passed to anyio.
            For the asyncio backend, ``{"loop_factory": uvloop.new_event_loop}``
            enables uvloop.
        shutdown_timeout_ms: Maximum milliseconds to wait for in-flight tasks
            to finish during :meth:`shutdown`.  When exceeded, the portal thread
            is abandoned (it will be killed on process exit).  ``None`` waits
            indefinitely.
    """

    __slots__ = (
        "_backend",
        "_backend_options",
        "_closed",
        "_portal",
        "_portal_cm",
        "_shutdown_timeout_ms",
    )

    def __init__(
        self,
        *,
        backend: str = "asyncio",
        backend_options: dict[str, Any] | None = None,
        shutdown_timeout_ms: int | None = None,
    ) -> None:
        self._backend = backend
        self._backend_options: dict[str, Any] = backend_options or {}
        self._shutdown_timeout_ms = shutdown_timeout_ms
        self._closed = False
        self._portal_cm = start_blocking_portal(self._backend, self._backend_options)
        self._portal: BlockingPortal = self._portal_cm.__enter__()

    def run(self, coro: Coroutine[Any, Any, T], *, timeout: float | None = None) -> T:
        """Submit *coro* to the portal and block until done.

        Args:
            coro: Awaitable coroutine.
            timeout: Maximum seconds to wait.  ``None`` waits indefinitely.

        Returns:
            The coroutine's return value.

        Raises:
            InvalidUse: If the bridge has been shut down.
            TimeoutError: If *timeout* elapses before completion.
            Exception: Any exception raised by *coro*.
        """
        if self._closed:
            coro.close()
            raise InvalidUse("AsyncBridge has been shut down.")

        async def _await_coro_with_timeout() -> T:
            with anyio.fail_after(timeout):
                return await coro

        if timeout is not None:
            return self._portal.call(_await_coro_with_timeout)
        return cast(T, self._portal.call(_await_coro, coro))

    @property
    def is_alive(self) -> bool:
        """Whether the portal is still open."""
        return not self._closed

    def shutdown(self) -> None:
        """Close the portal and release the background thread.

        Idempotent — safe to call multiple times.  When *shutdown_timeout_ms*
        was provided at construction, the portal exit runs in a daemon thread
        and the call returns after the timeout regardless of whether in-flight
        tasks have finished.  The daemon thread is silently abandoned and will
        be killed when the process exits.
        """
        if self._closed:
            return
        self._closed = True
        if self._shutdown_timeout_ms is None:
            self._portal_cm.__exit__(None, None, None)
            return
        _exit_portal_with_timeout(self._portal_cm, self._shutdown_timeout_ms)


def _exit_portal_with_timeout(portal_cm: Any, timeout_ms: int) -> None:
    """Run portal.__exit__ in a daemon thread, return after *timeout_ms* ms."""
    done = threading.Event()

    def _do_exit() -> None:
        portal_cm.__exit__(None, None, None)
        done.set()

    thread = threading.Thread(target=_do_exit, daemon=True)
    thread.start()
    if not done.wait(timeout=timeout_ms / 1000):
        logger.warning(BridgeEvent.SHUTDOWN_TIMED_OUT, extra={BridgeKey.TIMEOUT_MS: timeout_ms})


async def _await_coro(coro: Coroutine[Any, Any, T]) -> T:
    return await coro


AsyncWorker = AsyncBridge

__all__ = ["AsyncBridge", "AsyncWorker", "build_backend_options"]
