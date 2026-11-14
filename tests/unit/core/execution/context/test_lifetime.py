"""An execution owns its context: leaving it closes it for every task and thread holding a copy.

Work awaited inside the execution (``gather``, ``TaskGroup``, an awaited task,
``asyncio.to_thread``) ends before the execution does and is unaffected.
"""

from __future__ import annotations

import asyncio
import gc
import threading
import weakref
from collections.abc import Awaitable, Callable

import pytest

from loom.core.execution import (
    ExecutionClosed,
    ExecutionRequired,
    ExecutionScope,
    current_execution,
    execution_identity,
    per_execution,
    refuse_ended_execution,
)
from loom.core.identity import Identity
from tests.helpers.loggers import RecordingLogger
from tests.unit.core.execution.context.doubles import ACME_ALICE, ACME_BOB, AcmeSession

Outliving = Callable[..., Awaitable["asyncio.Task[object]"]]


class TestATaskOutlivingItsExecution:
    async def test_reading_the_identity_raises_execution_closed(self, outliving: Outliving) -> None:
        task = await outliving(execution_identity)

        with pytest.raises(ExecutionClosed):
            await task

    async def test_an_execution_with_no_caller_raises_execution_closed_not_unauthenticated(
        self, outliving: Outliving
    ) -> None:
        task = await outliving(execution_identity, identity=None)

        with pytest.raises(ExecutionClosed):
            await task

    async def test_sees_no_execution_in_progress(self, outliving: Outliving) -> None:
        task = await outliving(current_execution)

        assert await task is None

    def test_a_context_kept_past_its_end_refuses_its_identity(self) -> None:
        with ExecutionScope(ACME_ALICE):
            context = current_execution()
        assert context is not None

        with pytest.raises(ExecutionClosed):
            _ = context.identity

    async def test_a_per_execution_value_built_before_the_end_raises_execution_closed(
        self,
    ) -> None:
        session = per_execution(AcmeSession, AcmeSession)
        release = asyncio.Event()

        async def late() -> object:
            await release.wait()
            return session()

        with ExecutionScope(ACME_ALICE):
            session()
            task = asyncio.create_task(late())
        release.set()

        with pytest.raises(ExecutionClosed):
            await task

    async def test_a_per_execution_value_never_built_raises_execution_closed(
        self, outliving: Outliving
    ) -> None:
        task = await outliving(per_execution(AcmeSession, AcmeSession))

        with pytest.raises(ExecutionClosed):
            await task

    def test_a_context_kept_past_its_end_refuses_get_and_store(self) -> None:
        with ExecutionScope(ACME_ALICE):
            context = current_execution()
            assert context is not None
            context.store("acme-report", 0)

        with pytest.raises(ExecutionClosed):
            context.get("acme-report")
        with pytest.raises(ExecutionClosed):
            context.store("acme-report", 1)

    async def test_work_on_what_it_owned_is_refused(self, outliving: Outliving) -> None:
        task = await outliving(lambda: refuse_ended_execution("job dispatch"))

        with pytest.raises(ExecutionClosed, match="dispatch a job from inside the execution"):
            await task

    async def test_the_error_names_the_supported_paths_for_background_work(
        self, outliving: Outliving
    ) -> None:
        task = await outliving(execution_identity)

        with pytest.raises(ExecutionClosed) as caught:
            await task

        assert "dispatch a job" in str(caught.value)
        assert "executor.run(UseCase, identity=...)" in str(caught.value)

    async def test_each_refused_read_logs_the_guidance_once_without_the_caller(
        self, outliving: Outliving, recorded: RecordingLogger
    ) -> None:
        task = await outliving(execution_identity)

        with pytest.raises(ExecutionClosed):
            await task

        [(event, fields)] = recorded.calls
        assert (event, fields["read"]) == ("execution_closed", "identity")
        assert "dispatch a job" in fields["guidance"]
        assert "executor.run(UseCase, identity=...)" in fields["guidance"]
        assert ACME_ALICE.subject not in repr(recorded.calls)

    async def test_execution_closed_is_an_execution_required(self, outliving: Outliving) -> None:
        task = await outliving(execution_identity)

        with pytest.raises(ExecutionRequired):
            await task

    async def test_the_values_of_the_ended_execution_are_released(self) -> None:
        release = asyncio.Event()

        async def late() -> None:
            await release.wait()

        with ExecutionScope(ACME_ALICE):
            value = AcmeSession()
            kept = weakref.ref(value)
            context = current_execution()
            assert context is not None
            context.store(AcmeSession, value)
            task = asyncio.create_task(late())
        del value
        gc.collect()

        assert kept() is None
        release.set()
        await task

    async def test_a_thread_outliving_its_execution_raises_execution_closed(self) -> None:
        release = threading.Event()

        def late() -> object:
            release.wait(timeout=5)
            return execution_identity()

        with ExecutionScope(ACME_ALICE):
            thread = asyncio.create_task(asyncio.to_thread(late))
        release.set()

        with pytest.raises(ExecutionClosed):
            await thread


class TestWorkAwaitedInsideItsExecution:
    def test_work_on_what_it_owns_is_accepted(self) -> None:
        with ExecutionScope(ACME_ALICE):
            refuse_ended_execution("job dispatch")

    async def test_gathered_children_read_the_identity_and_share_the_cache(self) -> None:
        session = per_execution(AcmeSession, AcmeSession)

        async def child() -> tuple[Identity, object]:
            await asyncio.sleep(0)
            return execution_identity(), session()

        with ExecutionScope(ACME_ALICE):
            seen = await asyncio.gather(child(), child())

        assert [identity for identity, _ in seen] == [ACME_ALICE, ACME_ALICE]
        assert seen[0][1] is seen[1][1]

    async def test_task_group_children_read_the_identity(self) -> None:
        async def child() -> Identity:
            await asyncio.sleep(0)
            return execution_identity()

        with ExecutionScope(ACME_ALICE):
            async with asyncio.TaskGroup() as group:
                tasks = [group.create_task(child()) for _ in range(2)]

        assert [task.result() for task in tasks] == [ACME_ALICE, ACME_ALICE]

    async def test_an_awaited_task_reads_the_identity(self) -> None:
        async def child() -> Identity:
            await asyncio.sleep(0)
            return execution_identity()

        with ExecutionScope(ACME_ALICE):
            seen = await asyncio.create_task(child())

        assert seen is ACME_ALICE

    async def test_to_thread_reads_the_identity_and_the_execution_value(self) -> None:
        session = per_execution(AcmeSession, AcmeSession)

        with ExecutionScope(ACME_ALICE):
            built = session()
            seen = await asyncio.to_thread(lambda: (execution_identity(), session()))

        assert seen == (ACME_ALICE, built)

    async def test_a_cancelled_execution_reads_its_identity_in_its_own_teardown(self) -> None:
        started = asyncio.Event()
        teardown: list[Identity] = []

        async def body() -> None:
            with ExecutionScope(ACME_ALICE):
                try:
                    started.set()
                    await asyncio.Event().wait()
                finally:
                    teardown.append(execution_identity())

        running = asyncio.create_task(body())
        await started.wait()
        running.cancel()

        with pytest.raises(asyncio.CancelledError):
            await running
        assert teardown == [ACME_ALICE]

    async def test_a_task_a_cancelled_execution_left_behind_raises_execution_closed(
        self,
    ) -> None:
        started, release = asyncio.Event(), asyncio.Event()
        children: list[asyncio.Task[Identity]] = []

        async def child() -> Identity:
            await release.wait()
            return execution_identity()

        async def body() -> None:
            with ExecutionScope(ACME_ALICE):
                children.append(asyncio.create_task(child()))
                started.set()
                await asyncio.Event().wait()

        running = asyncio.create_task(body())
        await started.wait()
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
        release.set()

        with pytest.raises(ExecutionClosed):
            await children[0]


class TestANestedExecutionEnding:
    async def test_closes_only_the_nested_execution(self) -> None:
        release = asyncio.Event()

        async def late() -> Identity:
            await release.wait()
            return execution_identity()

        with ExecutionScope(ACME_ALICE):
            with ExecutionScope(ACME_BOB):
                task = asyncio.create_task(late())
            release.set()

            with pytest.raises(ExecutionClosed):
                await task
            assert execution_identity() is ACME_ALICE

    def test_the_enclosing_execution_keeps_its_cache(self) -> None:
        session = per_execution(AcmeSession, AcmeSession)

        with ExecutionScope(ACME_ALICE):
            outer = session()
            with ExecutionScope(ACME_ALICE):
                inner = session()
            assert session() is outer

        assert inner is not outer
