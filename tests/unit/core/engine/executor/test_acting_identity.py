"""One acting identity: everything a request causes acts as its caller (spec 011, T09a).

An execution runs as the identity it is given, else as the one acting
(``current_caller()``), else with no caller. Each case reads the three places
the identity reaches: ``Caller()`` (the arguments), ``current_identity()`` and
``current_caller()``. Only an explicit switch changes it, and it is logged.
The rule itself is tested in ``tests/unit/core/identity``, a task outliving
its execution in ``tests/unit/core/execution/context/test_lifetime.py``; this
file keeps what the executor adds.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import AbstractContextManager, nullcontext
from typing import Any, cast

import pytest
from opentelemetry.sdk.trace import TracerProvider

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope as DiScope
from loom.core.engine.executor import UseCaseExecutor
from loom.core.engine.post_commit import active_channel
from loom.core.errors import Unauthenticated
from loom.core.execution import ExecutionScope, execution_identity
from loom.core.identity import ANONYMOUS, Identity, current_caller, current_identity
from loom.core.job.context import add_pending_dispatch, flush_pending_dispatches
from loom.core.stores.uow.scope import current_scope
from loom.core.tracing import active_trace_id
from loom.core.use_case.invoker import AppInvoker
from loom.core.use_case.registry import UseCaseRegistry
from tests.helpers.delegation import acting_as
from tests.helpers.jobs import InlineServiceFixture
from tests.helpers.loggers import RecordingLogger
from tests.helpers.outliving import outlive

from .._lifecycle_doubles import Run
from .doubles import (
    ALICE,
    ANA,
    BOB,
    BUILDING,
    PROMOTED_ALICE,
    TICKET,
    AcmeAnyone,
    AcmeAsCaller,
    AcmeNests,
    AcmeStores,
    AcmeWho,
    AcmeWhoJob,
    channel_now,
)

Outliving = Callable[..., Awaitable["asyncio.Task[Any]"]]


async def acting_now() -> tuple[Identity, Identity, Identity | None]:
    """The execution identity, the acting identity and the caller, read where it runs."""
    return execution_identity(), current_identity(), current_caller()


class TestEffectiveIdentity:
    """Given, else the one acting, else no caller."""

    async def test_an_explicit_identity_reaches_all_three(self, executor: UseCaseExecutor) -> None:
        assert await executor.execute(AcmeWho(), identity=ALICE) == (ALICE,) * 3

    async def test_a_nested_execution_acts_as_the_enclosing_identity(
        self, executor: UseCaseExecutor
    ) -> None:
        outer = AcmeNests(executor, AcmeWho(), None)

        assert await executor.execute(outer, identity=ALICE) == (ALICE,) * 3

    async def test_given_no_identity_it_acts_as_the_caller_a_transport_installed(
        self, executor: UseCaseExecutor
    ) -> None:
        with acting_as(ALICE):
            assert await executor.execute(AcmeWho()) == (ALICE,) * 3

    async def test_with_nothing_given_and_nothing_acting_it_has_no_caller(
        self, executor: UseCaseExecutor
    ) -> None:
        assert await executor.execute(AcmeAnyone()) == (None, ANONYMOUS, None)
        with pytest.raises(Unauthenticated, match="carried no identity"):
            await executor.execute(AcmeWho())

    async def test_a_nested_execution_under_one_with_no_caller_has_none(
        self, executor: UseCaseExecutor
    ) -> None:
        outer = AcmeNests(executor, AcmeAnyone(), None)

        assert await executor.execute(outer) == (None, ANONYMOUS, None)


class TestExplicitSwitch:
    """``executor.run(..., identity=X)`` inside work acting as someone else: logged, isolated."""

    async def test_the_given_identity_governs_the_nested_execution(
        self, executor: UseCaseExecutor
    ) -> None:
        outer = AcmeNests(executor, AcmeWho(), BOB)

        assert await executor.execute(outer, identity=ALICE) == (BOB,) * 3

    async def test_work_caused_under_the_switch_acts_as_the_switched_identity(
        self, executor: UseCaseExecutor
    ) -> None:
        outer = AcmeNests(executor, AcmeNests(executor, AcmeWho(), None), BOB)

        assert await executor.execute(outer, identity=ALICE) == (BOB,) * 3

    async def test_it_is_logged_with_subjects_roles_the_use_case_and_the_trace(
        self, executor: UseCaseExecutor, switches: RecordingLogger
    ) -> None:
        with active_trace_id("acme-trace"):
            await executor.execute(AcmeNests(executor, AcmeWho(), BOB), identity=ALICE)

        assert switches.calls == [
            (
                "identity_switched",
                {
                    "from_subject": ALICE.subject,
                    "to_subject": BOB.subject,
                    "from_roles": ALICE.roles,
                    "to_roles": BOB.roles,
                    "attributes_changed": False,
                    "use_case": "AcmeWho",
                    "trace_id": "acme-trace",
                },
            )
        ]

    async def test_a_roles_change_under_the_same_subject_is_a_switch(
        self, executor: UseCaseExecutor, switches: RecordingLogger
    ) -> None:
        await executor.execute(AcmeNests(executor, AcmeWho(), PROMOTED_ALICE), identity=ALICE)

        [line] = switches.named("identity_switched")
        assert (line["from_subject"], line["to_subject"]) == (ALICE.subject, ALICE.subject)
        assert (line["from_roles"], line["to_roles"]) == (ALICE.roles, ("admin",))

    async def test_a_roles_change_keeps_the_delegation_and_is_refused_past_it(
        self, executor: UseCaseExecutor, past_the_window: Callable[[], None]
    ) -> None:
        """Other roles under the same subject never renew the window."""

        async def promote_late() -> Any:
            past_the_window()
            return await executor.execute(AcmeWho(), identity=PROMOTED_ALICE)

        with acting_as(ALICE), pytest.raises(Unauthenticated, match="system identity"):
            await executor.execute(Run(promote_late))

    async def test_another_subject_starts_a_delegation_of_its_own(
        self, executor: UseCaseExecutor, past_the_window: Callable[[], None]
    ) -> None:
        """The documented path for long work: run it as a system identity."""

        async def as_the_system_late() -> Any:
            past_the_window()
            return await executor.execute(AcmeWho(), identity=BOB)

        with acting_as(ALICE):
            assert await executor.execute(Run(as_the_system_late)) == (BOB,) * 3

    async def test_an_invoker_bound_with_for_identity_is_a_logged_switch(
        self, executor: UseCaseExecutor, switches: RecordingLogger
    ) -> None:
        bound = AppInvoker(BUILDING, executor, cast(UseCaseRegistry, None)).for_identity(BOB)

        async def invoke() -> Any:
            return await bound.invoke(AcmeWho)

        assert await executor.execute(Run(invoke), identity=ALICE) == (BOB,) * 3
        [line] = switches.named("identity_switched")
        assert (line["from_subject"], line["to_subject"]) == (ALICE.subject, BOB.subject)

    async def test_an_execution_scope_acting_as_another_than_the_transport_is_logged(
        self, switches: RecordingLogger
    ) -> None:
        with acting_as(ALICE), ExecutionScope(BOB):
            assert current_caller() is BOB

        [line] = switches.named("identity_switched")
        assert (line["from_subject"], line["to_subject"], line["use_case"]) == (
            ALICE.subject,
            BOB.subject,
            "ExecutionScope",
        )

    async def test_it_annotates_the_current_span(self, executor: UseCaseExecutor) -> None:
        with TracerProvider().get_tracer("acme").start_as_current_span("acme-request") as span:
            await executor.execute(AcmeNests(executor, AcmeWho(), BOB), identity=ALICE)

        assert dict(cast(Any, span).attributes or {}) == {
            "loom.identity.switched_from": ALICE.subject,
            "loom.identity.switched_to": BOB.subject,
        }

    @pytest.mark.parametrize(
        "around",
        [lambda: acting_as(ALICE), nullcontext],
        ids=["given-the-acting-identity", "given-one-while-nothing-acts"],
    )
    async def test_acting_as_the_identity_already_acting_is_no_switch(
        self,
        executor: UseCaseExecutor,
        switches: RecordingLogger,
        around: Callable[[], AbstractContextManager[object]],
    ) -> None:
        with around():
            await executor.execute(AcmeWho(), identity=ALICE)

        assert switches.calls == []


class TestInsideAnExecution:
    """Work awaited inside the execution acts as its caller."""

    @staticmethod
    async def gathered() -> Any:
        [seen] = await asyncio.gather(acting_now())
        return seen

    @staticmethod
    async def in_a_task_group() -> Any:
        async with asyncio.TaskGroup() as group:
            task = group.create_task(acting_now())
        return task.result()

    @staticmethod
    async def in_a_thread() -> Any:
        return await asyncio.to_thread(
            lambda: (execution_identity(), current_identity(), current_caller())
        )

    @pytest.mark.parametrize(
        "spawn",
        [gathered, in_a_task_group, in_a_thread],
        ids=["gather", "task-group", "to-thread"],
    )
    async def test_children_act_as_the_caller(
        self, executor: UseCaseExecutor, spawn: Callable[[], Awaitable[Any]]
    ) -> None:
        assert await executor.execute(Run(spawn), identity=ALICE) == (ALICE,) * 3


class TestATaskOutlivingItsExecution:
    """What the executor adds for it: a unit of work, a channel and jobs of its own."""

    @pytest.fixture
    def executor(self, transactional_executor: UseCaseExecutor) -> UseCaseExecutor:
        """These executions open an invocation scope over the phased stores."""
        return transactional_executor

    @pytest.fixture
    def outliving(self, executor: UseCaseExecutor) -> Outliving:
        """Run ``late`` in a task created by an execution as bob, once that execution ended."""

        async def start(
            late: Callable[[], object],
            inside: Callable[[], object] = lambda: None,
            seen: list[object] | None = None,
        ) -> asyncio.Task[object]:
            def opens(body: Callable[[], Awaitable[None]]) -> Awaitable[object]:
                return executor.execute(Run(body), identity=BOB)

            return await outlive(opens, late, inside=inside, seen=seen)

        return start

    async def test_a_use_case_it_runs_acts_as_the_caller_in_its_own_execution(
        self, outliving: Outliving, stores: AcmeStores, executor: UseCaseExecutor
    ) -> None:
        seen: list[object] = []

        task = await outliving(
            lambda: executor.run(AcmeAsCaller, factory=BUILDING),
            inside=lambda: (TICKET(), current_scope()),
            seen=seen,
        )
        identity, ticket, scope = await task

        [outer] = seen
        outer_ticket, outer_scope = cast(tuple[object, object], outer)
        assert identity is BOB
        assert ticket is not outer_ticket
        assert scope is not None
        assert scope is not outer_scope
        assert len(stores.created) == 2

    async def test_a_use_case_it_runs_as_another_identity_is_a_switch(
        self, outliving: Outliving, executor: UseCaseExecutor
    ) -> None:
        task = await outliving(lambda: executor.run(AcmeAsCaller, factory=BUILDING, identity=ANA))

        identity, _, _ = await task

        assert identity is ANA

    async def test_does_not_reuse_the_departed_post_commit_channel(
        self, outliving: Outliving, executor: UseCaseExecutor
    ) -> None:
        seen: list[object] = []

        task = await outliving(
            lambda: executor.execute(Run(channel_now), read_only=True),
            inside=active_channel,
            seen=seen,
        )
        channel = await task

        assert seen[0] is not None
        assert channel is not seen[0]

    async def test_a_job_dispatched_inside_a_use_case_it_runs_runs(
        self, outliving: Outliving, executor: UseCaseExecutor
    ) -> None:
        sent: list[str] = []

        async def dispatch() -> None:
            add_pending_dispatch(lambda: sent.append("acme-job"))

        task = await outliving(lambda: executor.execute(Run(dispatch)))
        await task

        assert sent == ["acme-job"]

    async def test_a_job_it_dispatches_goes_out_detached_as_the_caller(
        self, outliving: Outliving, executor: UseCaseExecutor
    ) -> None:
        jobs = InlineServiceFixture(BUILDING, executor)

        async def dispatch() -> Identity:
            handle = jobs.dispatch_after_commit(AcmeWhoJob)
            await flush_pending_dispatches()
            return cast(Identity, handle.wait())

        task = await outliving(dispatch)

        assert await task is BOB


class TestATaskOutlivingItsTransport:
    """A task started at the transport, outside any execution, is bounded by the request too."""

    async def test_runs_as_the_caller_within_the_window_and_is_refused_past_it(
        self, executor: UseCaseExecutor, past_the_window: Callable[[], None]
    ) -> None:
        go = asyncio.Event()

        async def after_the_request() -> Any:
            await go.wait()
            within = await executor.execute(AcmeWho())
            past_the_window()
            return within, await executor.execute(AcmeWho())

        with acting_as(ALICE):
            task = asyncio.create_task(after_the_request())
        go.set()

        with pytest.raises(Unauthenticated, match="system identity"):
            await task


class TestSingletons:
    """A singleton built lazily inside an execution acts as no one (D2)."""

    async def test_a_lazily_built_singleton_sees_anonymous(self, executor: UseCaseExecutor) -> None:
        class AcmeLazy:
            def __init__(self) -> None:
                self.saw = (current_identity(), current_caller())

        container = LoomContainer()
        container.register(AcmeLazy, AcmeLazy, scope=DiScope.APPLICATION)

        async def build() -> Any:
            lazy = container.resolve(AcmeLazy)
            return lazy.saw, current_caller()

        saw, after = await executor.execute(Run(build), identity=ALICE)

        assert saw == (ANONYMOUS, None)
        assert after is ALICE
