"""What an application opens and closes, and in which order."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from enum import StrEnum


class ResourceKind(StrEnum):
    """What a resource is. The order of the members is the order of opening.

    The cache gateways only need closing, so they open first and close last;
    the SQL registry opens before the agent runtime, which may query it; the
    stores open last, after their startup checks.
    """

    CACHE = "cache"
    SQL = "sql"
    AGENTS = "agents"
    STORE = "store"


_ORDER = {kind: position for position, kind in enumerate(ResourceKind)}


@dataclass(frozen=True, kw_only=True)
class Resource:
    """One thing an :class:`~loom.composition.Application` owns.

    Args:
        kind: What it is; decides when it opens.
        name: Its name among resources of the same kind (a store's name).
        manager: Called on every open; returns the context manager that opens
            and closes it. Reopening works only if what it returns supports it.
        check: Startup checks, run right before it opens and by
            :meth:`~loom.composition.Application.check`; they leave no
            connection open.
        readiness: Probe answering whether it can serve now.
    """

    kind: ResourceKind
    name: str
    manager: Callable[[], AbstractAsyncContextManager[object]]
    check: Callable[[], Awaitable[None]] | None = None
    readiness: Callable[[], Awaitable[bool]] | None = None

    @property
    def label(self) -> str:
        """``kind:name``, the way failures and logs name it."""
        return f"{self.kind}:{self.name}"


def in_opening_order(resources: tuple[Resource, ...]) -> tuple[Resource, ...]:
    """Sort by kind; resources of one kind keep the order they were given in."""
    return tuple(sorted(resources, key=lambda resource: _ORDER[resource.kind]))


def closing(
    close: Callable[[], Awaitable[object]],
) -> Callable[[], AbstractAsyncContextManager[None]]:
    """Return a :attr:`Resource.manager` for something that only needs closing."""

    @asynccontextmanager
    async def manager() -> AsyncIterator[None]:
        try:
            yield
        finally:
            await close()

    return manager
