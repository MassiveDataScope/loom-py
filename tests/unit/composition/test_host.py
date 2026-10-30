"""A host opens and closes its application in one task of its own, whoever starts and stops it."""

from __future__ import annotations

import asyncio
import gc
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest

from loom.composition import ApplicationHost, Resource, ResourceKind, closing
from tests.helpers.composition import TaskAffine, application_of


def _failing_to_close(message: str) -> Resource:
    async def close() -> None:
        raise RuntimeError(message)

    return Resource(kind=ResourceKind.CACHE, name="cache", manager=closing(close))


def _failing_to_open(message: str) -> Resource:
    @asynccontextmanager
    async def resource() -> AsyncIterator[None]:
        raise RuntimeError(message)
        yield

    return Resource(kind=ResourceKind.STORE, name="default", manager=resource)


class _Gate:
    """A store whose opening or closing blocks until released, recording when it closed."""

    def __init__(self) -> None:
        self.entering = asyncio.Event()
        self.closing = asyncio.Event()
        self.release = asyncio.Event()
        self.closed = False

    def blocking_open(self) -> Resource:
        @asynccontextmanager
        async def resource() -> AsyncIterator[None]:
            self.entering.set()
            await self.release.wait()
            try:
                yield
            finally:
                self.closed = True

        return Resource(kind=ResourceKind.STORE, name="default", manager=resource)

    def blocking_close(self, *, error: str | None = None) -> Resource:
        async def close() -> None:
            self.closing.set()
            await self.release.wait()
            self.closed = True
            if error is not None:
                raise RuntimeError(error)

        return Resource(kind=ResourceKind.STORE, name="default", manager=closing(close))


class TestOpening:
    async def test_start_returns_once_the_application_is_open(self) -> None:
        application = application_of()
        host = ApplicationHost(application)

        await host.start()
        try:
            assert application.is_open
        finally:
            await host.stop()

        assert not application.is_open

    async def test_start_runs_the_startup_checks_unless_told_not_to(self) -> None:
        checks: list[str] = []

        async def check() -> None:
            checks.append("check")

        store = Resource(
            kind=ResourceKind.STORE, name="default", manager=closing(_noop), check=check
        )
        checked, unchecked = (
            ApplicationHost(application_of(store)),
            ApplicationHost(application_of(store), check=False),
        )

        for host in (checked, unchecked):
            await host.start()
            await host.stop()

        assert checks == ["check"]

    async def test_a_failure_to_open_is_raised_from_start_as_itself(self) -> None:
        application = application_of(_failing_to_open("store unreachable"))
        host = ApplicationHost(application)

        with pytest.raises(RuntimeError, match="store unreachable"):
            await host.start()

        assert not application.is_open
        await host.stop()

    async def test_a_host_starts_once(self) -> None:
        host = ApplicationHost(application_of())
        await host.start()
        await host.stop()

        with pytest.raises(RuntimeError, match="once"):
            await host.start()

    async def test_a_cancelled_start_closes_what_opened(self) -> None:
        opened = asyncio.Event()
        affine = TaskAffine()

        async def slow_check() -> None:
            opened.set()
            await asyncio.sleep(3600)

        store = Resource(
            kind=ResourceKind.STORE, name="default", manager=closing(_noop), check=slow_check
        )
        application = application_of(affine.resource(), store)
        host = ApplicationHost(application)
        starting = asyncio.create_task(host.start())
        await opened.wait()

        starting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await starting

        assert affine.exited_in is affine.entered_in is not None
        assert not application.is_open

    async def test_a_stop_while_opening_fails_the_start_once_closed(self) -> None:
        gate = _Gate()
        application = application_of(gate.blocking_open())
        host = ApplicationHost(application)
        starting = asyncio.create_task(host.start())
        await gate.entering.wait()

        stopping = asyncio.create_task(host.stop())
        await asyncio.sleep(0)
        assert not stopping.done()
        gate.release.set()

        with pytest.raises(RuntimeError, match="stopped while opening"):
            await starting
        assert gate.closed
        assert not application.is_open
        await stopping

    async def test_a_failure_to_close_after_a_cancelled_start_is_logged(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        cache = _failing_to_close("pool gone")

        async def hanging_check() -> None:
            await asyncio.sleep(3600)

        store = Resource(
            kind=ResourceKind.STORE, name="default", manager=closing(_noop), check=hanging_check
        )
        host = ApplicationHost(application_of(cache, store))
        loop = asyncio.get_running_loop()
        handler = loop.get_exception_handler()
        reported: list[dict[str, object]] = []
        loop.set_exception_handler(lambda _, context: reported.append(context))
        try:
            starting = asyncio.create_task(host.start())
            await asyncio.sleep(0.01)
            with caplog.at_level(logging.WARNING, logger="loom.composition.host"):
                starting.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await starting
                await asyncio.sleep(0)
                del starting
                gc.collect()
        finally:
            loop.set_exception_handler(handler)

        assert "pool gone" in caplog.text
        # Nothing reached the loop's handler: no "exception was never retrieved".
        assert reported == []


class TestTaskAffinity:
    async def test_enter_and_exit_run_in_the_same_task_across_calls(self) -> None:
        affine = TaskAffine()
        host = ApplicationHost(application_of(affine.resource()))

        # Separate tasks, as each call of a blocking portal or each Lambda
        # invocation runs in a task of its own.
        await asyncio.create_task(host.start())
        await asyncio.create_task(host.stop())

        assert affine.entered_in is not None
        assert affine.exited_in is affine.entered_in

    async def test_the_owner_task_is_neither_the_starter_nor_the_stopper(self) -> None:
        affine = TaskAffine()
        host = ApplicationHost(application_of(affine.resource()))

        await host.start()
        await host.stop()

        assert affine.entered_in is not asyncio.current_task()

    async def test_a_real_agent_runtime_closes_cleanly(self) -> None:
        # Local imports: the AI pillar is an optional extra.
        pytest.importorskip("loom.ai.runtime")
        from loom.ai.runtime import AgentRuntime
        from loom.core.di import LoomContainer
        from tests.integration.ai.conftest import (
            CountingEngineProvider,
            StubDepsFactory,
            make_ai_config,
        )

        runtime = AgentRuntime(
            plans=[],
            config=make_ai_config(),
            engine_provider=CountingEngineProvider(engines={}),  # type: ignore[arg-type]
            deps=StubDepsFactory(),
            container=LoomContainer(),
        )
        agents = Resource(kind=ResourceKind.AGENTS, name="agents", manager=lambda: runtime)
        host = ApplicationHost(application_of(agents))

        await asyncio.create_task(host.start())
        await asyncio.create_task(host.stop())


class TestStopping:
    async def test_stop_without_start_does_nothing(self) -> None:
        await ApplicationHost(application_of()).stop()

    async def test_stop_is_idempotent(self) -> None:
        affine = TaskAffine()
        host = ApplicationHost(application_of(affine.resource()))
        await host.start()

        await host.stop()
        await host.stop()

        assert affine.exited_in is affine.entered_in

    async def test_a_failure_to_close_is_raised_from_stop_once(self) -> None:
        application = application_of(_failing_to_close("pool gone"))
        host = ApplicationHost(application)
        await host.start()

        with pytest.raises(RuntimeError, match="pool gone"):
            await host.stop()

        assert not application.is_open
        await host.stop()

    async def test_concurrent_stops_all_wait_for_the_close(self) -> None:
        gate = _Gate()
        host = ApplicationHost(application_of(gate.blocking_close()))
        await host.start()

        first = asyncio.create_task(host.stop())
        second = asyncio.create_task(host.stop())
        await gate.closing.wait()
        await asyncio.sleep(0)
        assert not first.done() and not second.done()
        gate.release.set()
        await asyncio.gather(first, second)

        assert gate.closed

    async def test_the_close_error_is_raised_to_the_first_stop_only(self) -> None:
        gate = _Gate()
        host = ApplicationHost(application_of(gate.blocking_close(error="pool gone")))
        await host.start()

        first = asyncio.create_task(host.stop())
        second = asyncio.create_task(host.stop())
        await gate.closing.wait()
        gate.release.set()
        outcomes = await asyncio.gather(first, second, return_exceptions=True)

        assert isinstance(outcomes[0], RuntimeError) and str(outcomes[0]) == "pool gone"
        assert outcomes[1] is None

    async def test_a_failure_to_close_after_a_cancelled_stop_is_logged(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        gate = _Gate()
        host = ApplicationHost(application_of(gate.blocking_close(error="pool gone")))
        await host.start()
        stopping = asyncio.create_task(host.stop())
        await gate.closing.wait()

        with caplog.at_level(logging.WARNING, logger="loom.composition.host"):
            stopping.cancel()
            with pytest.raises(asyncio.CancelledError):
                await stopping
            gate.release.set()
            await host.stop()
            await asyncio.sleep(0)

        assert gate.closed
        assert caplog.text.count("pool gone") == 1

    def test_a_cancelled_owner_still_closes_in_its_own_task(self) -> None:
        affine = TaskAffine()
        application = application_of(affine.resource())
        host = ApplicationHost(application)

        # The loop closes without a stop and cancels the owner task, the way a
        # process shutting its loop down would.
        with asyncio.Runner() as runner:
            runner.run(host.start())
            assert application.is_open

        assert affine.exited_in is affine.entered_in is not None
        assert not application.is_open


async def _noop() -> None:
    return None
