"""Elevation frames: which scopes an execution may write beyond its own rows.

A frame lives exactly as long as one use-case execution. ``elevate_scope``
records a scope in the open frame after checking the decision that justifies
it, and asks the sink to flag the open transaction when there is one; the
session-settings provider reads ``elevated_scopes`` for every later
transaction. A nested frame starts with no scopes: an elevation is never
inherited, and the executor refuses to start a nested execution while its
parent holds one. Tasks spawned inside the execution share the frame object
and lose the elevation when it closes. Nothing here knows a database.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import NoReturn, Protocol

from loom.core.authz._decide import Decision
from loom.core.authz._roles import Permission, RoleCatalog
from loom.core.authz._scope import Scope

_logger = logging.getLogger(__name__)

_FLAG_STEP_TIMEOUT_S = 30.0
"""How long setting or clearing flags, then invalidating the session, may take."""


class ReadOnlyElevation(RuntimeError):
    """``elevate`` was called in a read-only execution.

    An elevable scope opens writes only, and a read-only execution writes
    nothing: a flag it set would land on the transaction it shares with its
    parent, or on the sessions its other tasks read with.

    Args:
        scope: The scope asked for.
    """

    def __init__(self, scope: str) -> None:
        super().__init__(
            f"scope {scope!r} cannot be elevated in a read-only execution; "
            "elevate in the use case that writes"
        )


class ElevationSink(Protocol):
    """How the frame reaches the open transactions of the stores that declare a scope.

    Each method finds those transactions when it is called, in the calling
    task, and returns only the driver call left to await. The frame may await
    that call in a task of its own, so a cancellation cannot cut it short;
    finding the transactions there would fail the owner-task check of the
    invocation scope.
    """

    def in_transaction(self, scopes: frozenset[str]) -> bool: ...

    def set_flag(self, scope: str) -> Awaitable[object]: ...

    def clear_flags(self, scopes: frozenset[str]) -> Awaitable[object]: ...

    def invalidate(self, scopes: frozenset[str], cause: BaseException) -> Awaitable[object]:
        """Invalidate the sessions holding ``scopes``; their invocation then never commits."""
        ...


@dataclass(slots=True)
class _Frame:
    owns_transaction: bool
    sink: ElevationSink | None
    parent: _Frame | None
    read_only: bool = False
    scopes: set[str] = field(default_factory=set)
    is_open: bool = True


_elevation: ContextVar[_Frame | None] = ContextVar("_elevation", default=None)


def in_execution_frame() -> bool:
    """Whether an execution frame is open in this context."""
    return _open_frame() is not None


def elevated_scopes() -> frozenset[str]:
    """Scopes elevated by the execution running in this context; empty outside one."""
    frame = _open_frame()
    return frozenset(frame.scopes) if frame is not None else frozenset()


@asynccontextmanager
async def elevation_scope(
    *, owns_transaction: bool, sink: ElevationSink | None = None, read_only: bool = False
) -> AsyncIterator[None]:
    """Open a frame for one execution; a nested frame starts empty and never mutates its parent.

    A ``read_only`` execution's frame refuses every elevation.
    """
    parent = _open_frame()
    inherited = parent.sink if parent is not None else None
    frame = _Frame(
        owns_transaction=owns_transaction,
        sink=sink or inherited,
        parent=parent,
        read_only=read_only,
    )
    token = _elevation.set(frame)
    try:
        yield
    finally:
        try:
            await _clear_added(frame)
        finally:
            frame.is_open = False
            _elevation.reset(token)


async def elevate_scope(
    scope: str,
    decision: Decision,
    *,
    at: Scope,
    permission: Permission,
    catalog: RoleCatalog,
) -> None:
    """Record ``scope`` as elevated for the rest of the execution.

    Raises:
        RuntimeError: Outside an execution frame.
        ReadOnlyElevation: In a read-only execution.
        PermissionError: When the decision is denied, its grant's role lacks
            ``permission``, or its grant does not cover ``at``.
    """
    frame = _open_frame()
    if frame is None:
        raise RuntimeError("elevate requires an open execution frame")
    if frame.read_only:
        raise ReadOnlyElevation(scope)
    _check(decision, scope, at, permission, catalog)
    if scope in frame.scopes:
        return
    frame.scopes.add(scope)
    if frame.sink is None:
        return
    sink, scopes = frame.sink, frozenset({scope})
    try:
        open_now = sink.in_transaction(scopes)
    except BaseException:
        # Refused before any statement: the scope stays unelevated.
        frame.scopes.discard(scope)
        raise
    if not open_now:
        return
    failure, cancelled = await _settle(lambda: sink.set_flag(scope))
    if failure is None and cancelled is None:
        return
    # The flag may be on: the session goes, and its invocation cannot commit.
    frame.scopes.discard(scope)
    if failure is None:
        failure = RuntimeError(f"elevating scope {scope!r} was interrupted")
        failure.__cause__ = cancelled
    await _fail_closed(sink, scopes, failure, cancelled)


def _check(
    decision: Decision, scope: str, at: Scope, permission: Permission, catalog: RoleCatalog
) -> None:
    grant = decision.grant
    if not decision or grant is None:
        raise PermissionError(f"scope {scope!r} cannot be elevated on a denied decision")
    if grant.role not in catalog.roles_with(permission):
        raise PermissionError(
            f"role {grant.role!r} lacks {permission} and cannot elevate scope {scope!r}"
        )
    if not grant.scope.covers(at):
        raise PermissionError(
            f"grant on {grant.scope} does not cover {at}; scope {scope!r} stays unelevated"
        )


async def _clear_added(frame: _Frame) -> None:
    """Clear the flags ``frame`` added to the open transactions, or fail closed.

    A cancellation arriving meanwhile waits for the clearing and is re-raised
    after: the outer execution keeps the transaction, so it must never get it
    back with a flag still on.
    """
    if frame.owns_transaction or frame.sink is None:
        return
    held_by_parent = frame.parent.scopes if frame.parent is not None else set()
    added = frozenset(frame.scopes - held_by_parent)
    # The sink is asked only when there is something to clear: a frame that
    # elevated nothing closes without touching the session.
    if not added or not frame.sink.in_transaction(added):
        return
    sink = frame.sink
    failure, cancelled = await _settle(lambda: sink.clear_flags(added))
    if failure is not None:
        await _fail_closed(sink, added, failure, cancelled)
    if cancelled is not None:
        raise cancelled


async def _fail_closed(
    sink: ElevationSink,
    scopes: frozenset[str],
    failure: BaseException,
    cancelled: asyncio.CancelledError | None,
) -> NoReturn:
    """Invalidate the sessions that may hold ``scopes`` on, so their invocation cannot commit.

    Raises:
        asyncio.CancelledError: The first cancellation received, if any.
        BaseException: Otherwise ``failure``, or what the invalidation failed
            with, chained from it.
    """
    # The type only: a driver error's parameters may carry the settings' values.
    _logger.error(
        "elevation flags for %s may be left on (%s); invalidating the session",
        sorted(scopes),
        type(failure).__name__,
    )
    invalidation, cancelled_again = await _settle(lambda: sink.invalidate(scopes, failure))
    cancelled = cancelled or cancelled_again
    if invalidation is not None:
        _logger.error("the session could not be invalidated (%s)", type(invalidation).__name__)
    if cancelled is not None:
        raise cancelled
    if invalidation is not None:
        raise invalidation from failure
    raise failure


async def _settle(
    call: Callable[[], Awaitable[object]],
) -> tuple[BaseException | None, asyncio.CancelledError | None]:
    """Run ``call`` to completion or to :data:`_FLAG_STEP_TIMEOUT_S`, whatever cancels the caller.

    ``call`` itself runs here, in the caller's task, where the sink finds the
    session; only the driver call it returns is awaited in a task of its own.

    Returns:
        What ``call`` failed with, anything at all, and the first
        cancellation the caller received while waiting.
    """
    try:
        step = call()
    except BaseException as exc:  # NOSONAR S5754 - returned, the caller fails closed
        return exc, None
    pending = asyncio.ensure_future(_failure_of(step))
    cancelled: asyncio.CancelledError | None = None
    while not pending.done():
        try:
            await asyncio.wait([pending])
        except asyncio.CancelledError as exc:  # NOSONAR S7497 - re-raised by the caller
            cancelled = cancelled or exc
    return pending.result(), cancelled


async def _failure_of(step: Awaitable[object]) -> BaseException | None:
    try:
        await asyncio.wait_for(step, _FLAG_STEP_TIMEOUT_S)
    except BaseException as exc:  # NOSONAR S5754 - returned, the caller fails closed
        return exc
    return None


def _open_frame() -> _Frame | None:
    frame = _elevation.get()
    return frame if frame is not None and frame.is_open else None
