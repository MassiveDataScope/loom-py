"""The composed application and the one place that opens and closes its resources."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from types import TracebackType
from typing import TYPE_CHECKING

from loom.composition.resources import Resource, in_opening_order

if TYPE_CHECKING:
    from pathlib import Path

    from loom.composition._ai import AiWiring
    from loom.composition._sql import SqlWiring
    from loom.core.bootstrap.kernel import KernelRuntime
    from loom.core.config import ConfigContext
    from loom.core.discovery.base import DiscoveryResult
    from loom.core.environment import Environment
    from loom.core.observability.runtime import ObservabilityRuntime
    from loom.core.persistence.abc import PersistenceWiring

_Exit = Callable[
    [type[BaseException] | None, BaseException | None, TracebackType | None],
    Awaitable[bool | None],
]
"""The ``__aexit__`` shape :meth:`AsyncExitStack.push_async_exit` takes."""


@dataclass(eq=False, kw_only=True)
class Application:
    """A compiled kernel and the resources it owns, independent of any transport.

    :meth:`running` opens the resources in the order of their
    :class:`~loom.composition.ResourceKind`, running each one's startup checks
    right before it opens, and closes them in reverse. A transport adapter
    enters :meth:`running` and never opens these resources itself.

    Args:
        kernel: Compiled kernel.
        config: Loaded configuration.
        environment: Environment the application runs in.
        code_path: Application code directory.
        discovered: What discovery found, as the adapter's recipe left it.
        observability: Logging, tracing and metrics runtime.
        sql: SQL registry and services.
        ai: Agent runtime and its plans.
        persistence: Wiring of the one store this delivery serves; the
            catalogue of named stores replaces it.
        resources: What the application owns, in any order.
    """

    kernel: KernelRuntime
    config: ConfigContext
    environment: Environment
    code_path: Path
    discovered: DiscoveryResult
    observability: ObservabilityRuntime
    sql: SqlWiring
    ai: AiWiring
    persistence: PersistenceWiring
    resources: tuple[Resource, ...] = ()
    _open: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        self.resources = in_opening_order(self.resources)

    @property
    def is_open(self) -> bool:
        """Whether :meth:`running` is between opening and closing."""
        return self._open

    async def check(self) -> None:
        """Run every resource's startup checks without opening anything.

        Raises:
            ConfigError: When the schema or the roles of a store do not pass.
        """
        for resource in self.resources:
            if resource.check is not None:
                await resource.check()

    @asynccontextmanager
    async def running(self, *, check: bool = True) -> AsyncIterator[Application]:
        """Open the resources, yield the application, and close them in reverse.

        An :class:`~contextlib.AsyncExitStack` closes them, so each one is told
        the error that ended the block. Unlike a bare stack, a failure to close
        hides neither that error nor the other failures, and no resource can
        suppress it.

        Args:
            check: Run each resource's startup checks before it opens. A
                prefork worker checks once in the parent and opens each child
                with ``check=False``.

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
                for resource in self.resources:
                    if check and resource.check is not None:
                        await resource.check()
                    manager = resource.manager()
                    await manager.__aenter__()
                    stack.push_async_exit(_recorded(manager.__aexit__, resource, failures))
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


def _recorded(exit_: _Exit, resource: Resource, failures: list[BaseException]) -> _Exit:
    """Record a failure to close instead of letting it replace the error in flight."""

    async def recorded(
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        try:
            await exit_(exc_type, exc, traceback)
        except BaseException as failure:
            failure.add_note(f"while closing {resource.label}")
            failures.append(failure)
        return False

    return recorded


def _close_error(failures: list[BaseException]) -> BaseException:
    if len(failures) == 1:
        return failures[0]
    return BaseExceptionGroup("Application failed to close", failures)
