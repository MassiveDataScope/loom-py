"""The composed application and the one place that opens and closes its resources."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field

from loom.core.bootstrap.kernel import KernelRuntime
from loom.core.cache.wiring import CacheGateways
from loom.core.persistence.abc import PersistenceWiring

_Close = Callable[[BaseException | None], Awaitable[object]]
"""Closes one resource, told the error that ended its use, if any."""


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
            runtime). The application reopens only if these resources do:
            a factory returning the same instances each time reopens only
            what supports reentry.
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

        Every resource is closed even when another fails to close or the task
        is cancelled while closing, and the application is closed afterwards.
        Each resource is told the error that ended the block, if any.

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
            try:
                for failure in await _close_all(closers, error):
                    error.add_note(f"also failed to close: {failure!r}")
            finally:
                self._open = False
            raise
        ended: BaseException | None = None
        try:
            yield self
        except BaseException as error:
            ended = error
            raise
        finally:
            try:
                failures = await _close_all(closers, ended)
            finally:
                self._open = False
            if len(failures) == 1:
                raise failures[0]
            if failures:
                raise BaseExceptionGroup("Application failed to close", failures)

    async def _open_all(self, closers: list[_Close], *, check: bool) -> None:
        container = self.kernel.container
        if container.is_registered(CacheGateways):
            closers.extend(
                _closing(gateway.close) for gateway in container.resolve(CacheGateways).distinct()
            )
        for resource in self.resources():
            await _enter(resource, closers)
        if check:
            await self.check()
        await _enter(self.persistence.lifespan_init(), closers)


def _closing(close: Callable[[], Awaitable[object]]) -> _Close:
    async def closer(_error: BaseException | None) -> None:
        await close()

    return closer


async def _enter(resource: AbstractAsyncContextManager[object], closers: list[_Close]) -> None:
    await resource.__aenter__()

    async def close(error: BaseException | None) -> None:
        if error is None:
            await resource.__aexit__(None, None, None)
            return
        try:
            await resource.__aexit__(type(error), error, error.__traceback__)
        except BaseException as raised:
            # A generator-based manager re-raises the error it was given;
            # that is the error being handled, not a failure to close.
            if raised is not error:
                raise

    closers.append(close)


async def _close_all(closers: list[_Close], error: BaseException | None) -> list[BaseException]:
    """Close every resource in reverse; a cancellation does not skip the rest."""
    failures: list[BaseException] = []
    while closers:
        try:
            await closers.pop()(error)
        except BaseException as failure:
            failures.append(failure)
    return failures
