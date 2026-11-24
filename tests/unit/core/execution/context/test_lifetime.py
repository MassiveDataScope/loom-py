"""An execution owns its resources; its identity governs everything it caused (T09a).

Leaving an execution closes its resources for every task and thread holding a
copy, which keep acting as the same caller, within the delegation window.
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

from loom.core.errors import Unauthenticated
from loom.core.execution import (
    ExecutionClosed,
    ExecutionContext,
    ExecutionRequired,
    ExecutionScope,
    NoExecution,
    current_execution,
    execution_ended,
    execution_identity,
    per_execution,
)
from loom.core.execution.context import ExecutionContextEvent, ExecutionContextKey
from loom.core.identity import (
    Identity,
    IdentityFixed,
    current_caller,
    set_identity,
)
from loom.core.identity.context import IdentityEvent, IdentityKey
from loom.core.log_names import LogField
from tests.helpers.loggers import CapturedLog, user_hash
from tests.unit.core.execution.context.doubles import ACME_ALICE, ACME_BOB, AcmePerExecutionValue

Outliving = Callable[..., Awaitable["asyncio.Task[object]"]]


class TestATaskOutlivingItsExecution:
    async def test_keeps_acting_as_the_caller(self, outliving: Outliving) -> None:
        task = await outliving(execution_identity)

        assert await task is ACME_ALICE

    async def test_reading_its_caller_past_the_window_is_refused(
        self, outliving: Outliving, past_the_window: Callable[[], None]
    ) -> None:
        """What an Agent or MCP handle reads in such a task: bounded by the delegation."""

        def late() -> object:
            past_the_window()
            return execution_identity()

        task = await outliving(late)

        with pytest.raises(Unauthenticated, match="system identity"):
            await task

    async def test_an_execution_with_no_caller_still_has_none(self, outliving: Outliving) -> None:
        task = await outliving(execution_identity, identity=None)

        with pytest.raises(Unauthenticated):
            await task

    async def test_sees_no_execution_in_progress(self, outliving: Outliving) -> None:
        task = await outliving(current_execution)

        assert await task is None

    async def test_knows_its_execution_ended(self, outliving: Outliving) -> None:
        task = await outliving(execution_ended)

        assert await task is True

    async def test_cannot_change_who_it_acts_as(self, outliving: Outliving) -> None:
        task = await outliving(lambda: set_identity(ACME_BOB))

        with pytest.raises(IdentityFixed):
            await task

    async def test_an_execution_scope_as_another_is_a_logged_switch(
        self, outliving: Outliving, switches: CapturedLog
    ) -> None:
        def as_bob() -> Identity:
            with ExecutionScope(ACME_BOB):
                return execution_identity()

        task = await outliving(as_bob)

        assert await task is ACME_BOB
        [switch] = switches.named(IdentityEvent.CALLER_SWITCHED)
        assert (
            switch[IdentityKey.PREVIOUS_USER_HASH],
            switch[LogField.USER_HASH],
            switch[LogField.USE_CASE],
        ) == (
            user_hash(ACME_ALICE.subject),
            user_hash(ACME_BOB.subject),
            "ExecutionScope",
        )

    async def test_opens_an_execution_as_the_caller_within_the_window(
        self, outliving: Outliving
    ) -> None:
        def reopen() -> Identity:
            token = ExecutionContext.entered(ACME_ALICE)
            try:
                return execution_identity()
            finally:
                token.close()

        task = await outliving(reopen)

        assert await task is ACME_ALICE

    async def test_opening_an_execution_past_the_window_is_refused(
        self,
        outliving: Outliving,
        recorded: CapturedLog,
        past_the_window: Callable[[], None],
    ) -> None:
        def late() -> object:
            past_the_window()
            return ExecutionContext.entered(ACME_ALICE)

        task = await outliving(late)

        with pytest.raises(Unauthenticated, match="system identity"):
            await task
        [(event, fields)] = recorded.events
        assert event == IdentityEvent.DELEGATION_EXPIRED
        assert ACME_ALICE.subject not in repr(fields)

    async def test_a_per_execution_value_built_before_the_end_raises_execution_closed(
        self, outliving: Outliving
    ) -> None:
        session = per_execution(AcmePerExecutionValue, AcmePerExecutionValue)

        task = await outliving(session, inside=session)

        with pytest.raises(ExecutionClosed):
            await task

    async def test_a_per_execution_value_never_built_raises_execution_closed(
        self, outliving: Outliving
    ) -> None:
        task = await outliving(per_execution(AcmePerExecutionValue, AcmePerExecutionValue))

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

    async def test_the_error_names_the_supported_paths_for_background_work(
        self, outliving: Outliving
    ) -> None:
        task = await outliving(per_execution(AcmePerExecutionValue, AcmePerExecutionValue))

        with pytest.raises(ExecutionClosed) as caught:
            await task

        assert "executor.run(UseCase)" in str(caught.value)
        assert "dispatch a job" in str(caught.value)
        assert "they act as the same caller" in str(caught.value)

    async def test_each_refused_read_logs_the_guidance_once_without_the_caller(
        self, outliving: Outliving, recorded: CapturedLog
    ) -> None:
        task = await outliving(per_execution(AcmePerExecutionValue, AcmePerExecutionValue))

        with pytest.raises(ExecutionClosed):
            await task

        [(event, fields)] = recorded.events
        assert (event, fields[ExecutionContextKey.READ]) == (
            ExecutionContextEvent.CLOSED,
            "per-execution value",
        )
        assert ACME_ALICE.subject not in repr(recorded.events)

    async def test_execution_closed_is_an_execution_required(self, outliving: Outliving) -> None:
        task = await outliving(per_execution(AcmePerExecutionValue, AcmePerExecutionValue))

        with pytest.raises(ExecutionRequired):
            await task

    async def test_the_values_of_the_ended_execution_are_released(self) -> None:
        release = asyncio.Event()

        async def late() -> None:
            await release.wait()

        with ExecutionScope(ACME_ALICE):
            value = AcmePerExecutionValue()
            kept = weakref.ref(value)
            context = current_execution()
            assert context is not None
            context.store(AcmePerExecutionValue, value)
            task = asyncio.create_task(late())
        del value
        gc.collect()

        assert kept() is None
        release.set()
        await task

    async def test_a_thread_outliving_its_execution_keeps_the_caller_not_the_resources(
        self,
    ) -> None:
        release = threading.Event()
        session = per_execution(AcmePerExecutionValue, AcmePerExecutionValue)

        def late() -> object:
            release.wait(timeout=5)
            identity = execution_identity()
            try:
                session()
            except ExecutionClosed:
                return identity
            return None

        with ExecutionScope(ACME_ALICE):
            thread = asyncio.create_task(asyncio.to_thread(late))
        release.set()

        assert await thread is ACME_ALICE


class TestWorkAwaitedInsideItsExecution:
    def test_it_has_not_ended(self) -> None:
        with ExecutionScope(ACME_ALICE):
            assert execution_ended() is False
        assert execution_ended() is False

    async def test_gathered_children_read_the_identity_and_share_the_cache(self) -> None:
        session = per_execution(AcmePerExecutionValue, AcmePerExecutionValue)

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
        session = per_execution(AcmePerExecutionValue, AcmePerExecutionValue)

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

    async def test_a_task_a_cancelled_execution_left_behind_loses_its_resources(
        self,
    ) -> None:
        started, release = asyncio.Event(), asyncio.Event()
        children: list[asyncio.Task[object]] = []
        session = per_execution(AcmePerExecutionValue, AcmePerExecutionValue)

        async def child() -> object:
            await release.wait()
            return session()

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
        session = per_execution(AcmePerExecutionValue, AcmePerExecutionValue)

        async def late() -> object:
            await release.wait()
            return session()

        with ExecutionScope(ACME_ALICE):
            outer = session()
            with ExecutionScope(ACME_ALICE):
                task = asyncio.create_task(late())
            release.set()

            with pytest.raises(ExecutionClosed):
                await task
            assert session() is outer
            assert execution_identity() is ACME_ALICE

    @pytest.mark.parametrize(
        ("outer", "from_hash"),
        [(ExecutionScope(ACME_ALICE), user_hash(ACME_ALICE.subject)), (NoExecution(), None)],
        ids=["inside-an-execution", "inside-no-execution"],
    )
    def test_another_identity_opened_inside_it_is_a_logged_switch(
        self,
        switches: CapturedLog,
        outer: ExecutionScope | NoExecution,
        from_hash: str | None,
    ) -> None:
        """One switch policy, as ``executor.run(identity=)``; singletons are built in the second."""
        with outer:
            with ExecutionScope(ACME_BOB):
                assert execution_identity() is ACME_BOB
            assert current_caller() is (None if from_hash is None else ACME_ALICE)

        [switch] = switches.named(IdentityEvent.CALLER_SWITCHED)
        assert (switch.get(IdentityKey.PREVIOUS_USER_HASH), switch[LogField.USER_HASH]) == (
            from_hash,
            user_hash(ACME_BOB.subject),
        )

    def test_the_enclosing_execution_keeps_its_cache(self) -> None:
        session = per_execution(AcmePerExecutionValue, AcmePerExecutionValue)

        with ExecutionScope(ACME_ALICE):
            outer = session()
            with ExecutionScope(ACME_ALICE):
                inner = session()
            assert session() is outer

        assert inner is not outer
