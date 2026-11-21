"""What an application opens and closes, and in which order.

Loom's own resources and the ones an :class:`~loom.core.di.installer.Installer`
declares share this one lifecycle.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from enum import StrEnum
from types import TracebackType

_Exit = Callable[
    [type[BaseException] | None, BaseException | None, TracebackType | None],
    Awaitable[bool | None],
]
"""The ``__aexit__`` shape :meth:`AsyncExitStack.push_async_exit` takes."""


class ResourceKind(StrEnum):
    """What a resource is. The order of the members is the order of opening.

    The AWS clients and the cache gateways only need closing, so they open first
    and close last, the clients after everything that calls AWS;
    the Celery producer opens next and closes after the stores, agents and SQL;
    the SQL registry opens before the agent host, which may query it; the
    stores open next, after their startup checks; the resources an application
    declares open after the stores, so they close before them and may use them
    while closing; the job timer opens last, so it closes first and no delayed
    job runs against a closed store or a closed application resource.
    """

    AWS = "aws"
    CACHE = "cache"
    CELERY = "celery"
    SQL = "sql"
    AGENTS = "agents"
    STORE = "store"
    APPLICATION = "application"
    TIMER = "timer"


_ORDER = {kind: position for position, kind in enumerate(ResourceKind)}
_PROBED_KINDS = frozenset({ResourceKind.STORE, ResourceKind.APPLICATION})


@dataclass(frozen=True, kw_only=True)
class Resource:
    """One thing an :class:`~loom.composition.Application` owns.

    Args:
        kind: What it is; decides when it opens. An application's own
            resource is ``APPLICATION``, the default.
        name: Its name among resources of the same kind (a store's name).
        manager: Called on every open; returns the context manager that opens
            and closes it. Reopening works only if what it returns supports it.
        check: Startup checks, run right before it opens and by
            :meth:`~loom.composition.Application.check`; they leave no
            connection open.
        readiness: Probe answering whether it can serve now; the health probe
            asks it when the kind is ``STORE`` or ``APPLICATION``.
        readiness_key: Key its readiness is reported under; its name when empty.
    """

    kind: ResourceKind = ResourceKind.APPLICATION
    name: str
    manager: Callable[[], AbstractAsyncContextManager[object]]
    check: Callable[[], Awaitable[None]] | None = None
    readiness: Callable[[], Awaitable[bool]] | None = None
    readiness_key: str = ""

    def __post_init__(self) -> None:
        if not self.readiness_key:
            object.__setattr__(self, "readiness_key", self.name)

    @property
    def answers_health_probe(self) -> bool:
        """Whether the health probe asks it: a store or an application resource with a readiness."""
        return self.kind in _PROBED_KINDS and self.readiness is not None

    @property
    def label(self) -> str:
        """``kind:name``, the way failures and logs name it."""
        return f"{self.kind}:{self.name}"

    def recorded_exit(self, exit_: _Exit, failures: list[BaseException]) -> _Exit:
        """*exit_*, appending a failure to close to *failures* instead of raising it.

        Args:
            exit_: The ``__aexit__`` of what :attr:`manager` returned.
            failures: Where a failure to close is appended, noted with :attr:`label`.

        Returns:
            An ``__aexit__`` that never raises and never suppresses the error in flight.
        """

        async def recorded(
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            traceback: TracebackType | None,
        ) -> bool:
            try:
                await exit_(exc_type, exc, traceback)
            except BaseException as failure:  # NOSONAR S5754
                failure.add_note(f"while closing {self.label}")
                failures.append(failure)
            return False

        return recorded


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
