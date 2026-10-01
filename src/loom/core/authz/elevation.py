"""Elevation frames: which scopes an execution may write beyond its own rows.

A frame lives exactly as long as one use-case execution. ``elevate_scope``
records a scope in the open frame after checking the decision that justifies
it, and asks the sink to flag the open transaction when there is one; the
session-settings provider reads ``elevated_scopes`` for every later
transaction. Tasks spawned inside the execution share the frame object and
lose the elevation when it closes. Nothing here knows a database.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Protocol

from loom.core.authz._decide import Decision
from loom.core.authz._roles import Permission, RoleCatalog
from loom.core.authz._scope import Scope

_logger = logging.getLogger(__name__)


class ElevationSink(Protocol):
    """How the frame reaches the transaction that is open right now."""

    def in_transaction(self) -> bool: ...

    async def set_flag(self, scope: str) -> None: ...

    async def clear_flags(self, scopes: frozenset[str]) -> None: ...

    async def invalidate(self) -> None: ...


@dataclass(slots=True)
class _Frame:
    owns_transaction: bool
    sink: ElevationSink | None
    parent: _Frame | None
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
    *, owns_transaction: bool, sink: ElevationSink | None = None
) -> AsyncIterator[None]:
    """Open a frame for one execution; nested frames inherit and never mutate their parent."""
    parent = _open_frame()
    inherited = parent.sink if parent is not None else None
    frame = _Frame(
        owns_transaction=owns_transaction,
        sink=sink or inherited,
        parent=parent,
        scopes=set(parent.scopes) if parent else set(),
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
        PermissionError: When the decision is denied, its grant's role lacks
            ``permission``, or its grant does not cover ``at``.
    """
    frame = _open_frame()
    if frame is None:
        raise RuntimeError("elevate requires an open execution frame")
    _check(decision, scope, at, permission, catalog)
    if scope in frame.scopes:
        return
    frame.scopes.add(scope)
    if frame.sink is not None and frame.sink.in_transaction():
        try:
            await frame.sink.set_flag(scope)
        except BaseException:
            frame.scopes.discard(scope)
            raise


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
    if frame.owns_transaction or frame.sink is None or not frame.sink.in_transaction():
        return
    inherited = frame.parent.scopes if frame.parent is not None else set()
    added = frozenset(frame.scopes - inherited)
    if not added:
        return
    try:
        await asyncio.shield(frame.sink.clear_flags(added))
    except Exception:
        _logger.exception(
            "elevation flags could not be cleared for %s; invalidating the session", sorted(added)
        )
        await frame.sink.invalidate()


def _open_frame() -> _Frame | None:
    frame = _elevation.get()
    return frame if frame is not None and frame.is_open else None
