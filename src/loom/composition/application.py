"""The composed application and the one place that opens and closes its resources."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from loom.composition.errors import ApplicationAlreadyOpenError
from loom.core.job.config import JobSettings
from loom.core.resources import Resource, in_opening_order

if TYPE_CHECKING:
    from pathlib import Path

    from loom.composition._ai import AiWiring
    from loom.composition._sql import SqlWiring
    from loom.core.bootstrap.kernel import Kernel
    from loom.core.config import ConfigContext
    from loom.core.discovery.base import DiscoveryResult
    from loom.core.environment import Environment
    from loom.core.observability.observability import Observability


@dataclass(eq=False, kw_only=True)
class Application:
    """A compiled kernel and the resources it owns, independent of any transport.

    :meth:`running` opens the resources in the order of their
    :class:`~loom.core.resources.ResourceKind`, running each one's startup checks
    right before it opens, and closes them in reverse. A transport runtime
    enters :meth:`running` and never opens these resources itself.

    Args:
        kernel: Compiled kernel.
        config: Loaded configuration, carrying the environment the application runs in.
        code_path: Application code directory.
        discovered: What discovery found, as the runtime's recipe left it.
        observability: Logging, tracing and metrics runtime.
        sql: SQL registry and services.
        ai: Agent host and its plans.
        job_settings: Each job's queue, retries, countdown, timeout and
            priority in this application.
        resources: What the application owns, in any order.
    """

    kernel: Kernel
    config: ConfigContext
    code_path: Path
    discovered: DiscoveryResult
    observability: Observability
    sql: SqlWiring
    ai: AiWiring
    job_settings: JobSettings = field(default_factory=JobSettings)
    resources: tuple[Resource, ...] = ()
    _open: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        self.resources = in_opening_order(self.resources)

    @property
    def environment(self) -> Environment:
        """The environment the application runs in."""
        return self.config.environment

    @property
    def is_open(self) -> bool:
        """Whether :meth:`running` is between opening and closing."""
        return self._open

    def readiness_probes(self) -> dict[str, Callable[[], Awaitable[bool]]]:
        """Return the readiness probe of each store and application resource, by its reported key.

        A resource without a probe is left out. Each probe answers whether its
        resource can serve now and never raises; a transport bounds and caches
        each one on its own, so one slow resource does not hide the others.
        """
        return {
            resource.readiness_key: resource.readiness
            for resource in self.resources
            if resource.readiness is not None and resource.answers_health_probe
        }

    async def check(self) -> None:
        """Run every resource's startup checks without opening anything.

        Raises:
            ConfigError: When the schema or the roles of a store do not pass,
                or a ClickHouse connection fails its driver checks or its
                role-enforcement probe.
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
            ApplicationAlreadyOpenError: When the application is already open.
            BaseExceptionGroup: When several resources fail to close. A single
                failure is raised as itself, after every other resource
                closed. A failure to open is raised as itself, with the
                failures to close what had opened as notes.
        """
        if self._open:
            raise ApplicationAlreadyOpenError
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
                    stack.push_async_exit(resource.recorded_exit(manager.__aexit__, failures))
                opened = True
                yield self
        except BaseException as error:  # NOSONAR S5754
            if not opened:
                for failure in failures:
                    error.add_note(f"also failed to close: {failure!r}")
            elif failures:
                raise _close_error(failures) from error  # NOSONAR S112
            raise
        finally:
            self._open = False
        if failures:
            raise _close_error(failures)  # NOSONAR S112


def _close_error(failures: list[BaseException]) -> BaseException:
    if len(failures) == 1:
        return failures[0]
    return BaseExceptionGroup("Application failed to close", failures)
