"""The composed application and the one place that opens and closes its resources."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from types import TracebackType
from typing import TYPE_CHECKING

from loom.core.cache.wiring import CacheGateways

if TYPE_CHECKING:
    from loom.core.bootstrap.kernel import KernelRuntime
    from loom.core.persistence.abc import PersistenceWiring

_Exit = Callable[
    [type[BaseException] | None, BaseException | None, TracebackType | None],
    Awaitable[bool | None],
]
"""The ``__aexit__`` shape :meth:`AsyncExitStack.push_async_exit` takes."""


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

        An :class:`~contextlib.AsyncExitStack` closes them, so each one is told
        the error that ended the block. Unlike a bare stack, a failure to close
        hides neither that error nor the other failures, and no resource can
        suppress it.

        Args:
            check: Run :meth:`check` before opening persistence. A prefork
                worker checks once in the parent and opens each child with
                ``check=False``.

        Raises:
            RuntimeError: When the application is already open.
            ExceptionGroup: When several resources fail to close. A single
                failure is raised as itself, after every other resource
                closed. A failure to open is raised as itself, with the
                failures to close what had opened as notes.
        """
        if self._open:
            raise RuntimeError("Application is already open; close it before opening it again")
        self._open = True
        failures: list[BaseException] = []
        opened = False
        try:
            async with AsyncExitStack() as stack:
                await self._open_all(stack, failures, check=check)
                opened = True
                yield self
        except BaseException as error:
            if not opened:
                for failure in failures:
                    error.add_note(f"also failed to close: {failure!r}")
            elif failures:
                raise _close_error(failures) from error
            raise
        finally:
            self._open = False
        if failures:
            raise _close_error(failures)

    async def _open_all(
        self, stack: AsyncExitStack, failures: list[BaseException], *, check: bool
    ) -> None:
        container = self.kernel.container
        if container.is_registered(CacheGateways):
            for gateway in container.resolve(CacheGateways).distinct():
                stack.push_async_exit(_recorded(_closing(gateway.close), failures))
        for resource in self.resources():
            await _enter(stack, resource, failures)
        if check:
            await self.check()
        await _enter(stack, self.persistence.lifespan_init(), failures)


async def _enter(
    stack: AsyncExitStack,
    resource: AbstractAsyncContextManager[object],
    failures: list[BaseException],
) -> None:
    await resource.__aenter__()
    stack.push_async_exit(_recorded(resource.__aexit__, failures))


def _closing(close: Callable[[], Awaitable[object]]) -> _Exit:
    async def exit_(*_: object) -> None:
        await close()

    return exit_


def _recorded(exit_: _Exit, failures: list[BaseException]) -> _Exit:
    """Record a failure to close instead of letting it replace the error in flight."""

    async def recorded(
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        try:
            await exit_(exc_type, exc, traceback)
        except BaseException as failure:
            failures.append(failure)
        return False

    return recorded


def _close_error(failures: list[BaseException]) -> BaseException:
    if len(failures) == 1:
        return failures[0]
    return BaseExceptionGroup("Application failed to close", failures)
