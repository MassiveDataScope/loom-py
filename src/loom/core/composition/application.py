"""The composed application and the one place that opens and closes its resources."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field

from loom.core.bootstrap.kernel import KernelRuntime
from loom.core.cache.wiring import CacheGateways
from loom.core.persistence.abc import PersistenceWiring

_Close = Callable[[], Awaitable[object]]


def _no_resources() -> Sequence[AbstractAsyncContextManager[object]]:
    return ()


@dataclass(eq=False)
class Application:
    """A compiled kernel and the resources it owns, independent of any transport.

    :meth:`running` opens, in this order, the cache gateways (they only need
    closing), each of *resources*, the persistence startup checks and the
    persistence lifespan. Leaving it closes them in reverse. A transport
    adapter enters :meth:`running` and never opens these resources itself.

    Args:
        kernel: Compiled kernel; its container holds the cache gateways.
        persistence: Wiring of the persistence backend.
        resources: Called on every open; returns the context managers to
            enter, in order, before persistence (the SQL registry, the agent
            runtime). A fresh call per open leaves reopening to whoever
            supplies them.
    """

    kernel: KernelRuntime
    persistence: PersistenceWiring
    resources: Callable[[], Sequence[AbstractAsyncContextManager[object]]] = _no_resources
    _open: bool = field(default=False, init=False)

    @property
    def is_open(self) -> bool:
        """Whether :meth:`running` is between opening and closing."""
        return self._open

    async def check(self) -> None:
        """Run the startup checks without opening anything.

        Raises:
            ConfigError: When the schema or the roles do not pass.
        """
        await self.persistence.startup_checks()

    @asynccontextmanager
    async def running(self, *, check: bool = True) -> AsyncIterator[Application]:
        """Open the resources, yield the application, and close them in reverse.

        Args:
            check: Run :meth:`check` before opening persistence. A prefork
                worker checks once in the parent and opens each child with
                ``check=False``.

        Raises:
            RuntimeError: When the application is already open.
            ExceptionGroup: When several resources fail to close. A single
                failure is raised as itself, after every other resource closed.
        """
        if self._open:
            raise RuntimeError("Application is already open; close it before opening it again")
        self._open = True
        closers: list[_Close] = []
        try:
            await self._open_all(closers, check=check)
        except BaseException as error:
            for failure in await _close_all(closers):
                error.add_note(f"also failed to close: {failure!r}")
            self._open = False
            raise
        try:
            yield self
        finally:
            failures = await _close_all(closers)
            self._open = False
            if len(failures) == 1:
                raise failures[0]
            if failures:
                raise ExceptionGroup("Application failed to close", failures)

    async def _open_all(self, closers: list[_Close], *, check: bool) -> None:
        container = self.kernel.container
        if container.is_registered(CacheGateways):
            closers.extend(gateway.close for gateway in container.resolve(CacheGateways).distinct())
        for resource in self.resources():
            await _enter(resource, closers)
        if check:
            await self.check()
        await _enter(self.persistence.lifespan_init(), closers)


async def _enter(resource: AbstractAsyncContextManager[object], closers: list[_Close]) -> None:
    await resource.__aenter__()
    closers.append(lambda: resource.__aexit__(None, None, None))


async def _close_all(closers: list[_Close]) -> list[Exception]:
    failures: list[Exception] = []
    while closers:
        try:
            await closers.pop()()
        except Exception as failure:
            failures.append(failure)
    return failures
