"""Who an execution runs as: the identity given, else the enclosing one's, else no one.

``loom.core.engine.principal``. For each case the test reads the three places
the effective identity reaches: ``Caller()`` (the arguments),
``current_identity()`` (``set_identity``) and ``current_execution().identity``
(the context). A task that outlived its execution runs as at top level.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, cast

import pytest

from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.post_commit import active_channel
from loom.core.errors import NestedIdentityMismatch, Unauthenticated
from loom.core.execution import ExecutionClosed, ExecutionScope, execution_identity
from loom.core.identity import ANONYMOUS
from loom.core.job.context import add_pending_dispatch
from loom.core.uow.scope import current_scope

from .._lifecycle_doubles import Run
from .doubles import (
    ALICE,
    ANA,
    BOB,
    BUILDING,
    TICKET,
    AcmeAnyone,
    AcmeAsCaller,
    AcmeNests,
    AcmeStores,
    AcmeTampers,
    AcmeWho,
    ambient_now,
    channel_now,
    identity_now,
)


class TestEffectiveIdentity:
    """Explicit, else the enclosing execution's, else ``None`` (NS2), checked for tampering."""

    async def test_an_explicit_identity_reaches_all_three(self, executor: RuntimeExecutor) -> None:
        assert await executor.execute(AcmeWho(), identity=ALICE) == (ALICE,) * 3

    async def test_an_explicit_identity_wins_over_the_enclosing_one(
        self, executor: RuntimeExecutor
    ) -> None:
        outer = AcmeNests(executor, AcmeWho(), BOB)

        assert await executor.execute(outer, identity=ALICE) == (BOB,) * 3

    async def test_a_nested_execution_inherits_the_enclosing_identity(
        self,
        executor: RuntimeExecutor,
    ) -> None:
        outer = AcmeNests(executor, AcmeWho(), None)

        assert await executor.execute(outer, identity=ALICE) == (ALICE,) * 3

    async def test_no_identity_and_no_enclosing_execution_runs_with_none(
        self,
        executor: RuntimeExecutor,
    ) -> None:
        assert await executor.execute(AcmeAnyone()) == (None, ANONYMOUS, None)
        with pytest.raises(Unauthenticated, match="carried no identity"):
            await executor.execute(AcmeWho())

    async def test_a_nested_execution_under_one_with_no_caller_has_none(
        self,
        executor: RuntimeExecutor,
    ) -> None:
        outer = AcmeNests(executor, AcmeAnyone(), None)

        assert await executor.execute(outer) == (None, ANONYMOUS, None)

    async def test_an_ambient_identity_changed_mid_execution_refuses_an_inheriting_child(
        self,
        executor: RuntimeExecutor,
    ) -> None:
        outer = AcmeTampers(executor, AcmeWho(), None)

        with pytest.raises(NestedIdentityMismatch, match="AcmeWho was given no identity"):
            await executor.execute(outer, identity=ALICE)

    async def test_an_execution_scope_opened_without_the_ambient_identity_is_named_as_a_cause(
        self,
        executor: RuntimeExecutor,
    ) -> None:
        with (
            pytest.raises(NestedIdentityMismatch, match="opened .* without set_identity"),
            ExecutionScope(ALICE),
        ):
            await executor.execute(AcmeWho())


Outliving = Callable[..., Awaitable["asyncio.Task[Any]"]]


class TestATaskOutlivingItsExecution:
    """An ended execution is no execution: the executor opens a new one, as at top level.

    Nothing of the departed caller survives in it: identity, scope or channel.
    """

    @pytest.fixture
    def executor(self, transactional_executor: RuntimeExecutor) -> RuntimeExecutor:
        """These executions open an invocation scope over the phased stores."""
        return transactional_executor

    @pytest.fixture
    def outliving(self) -> Outliving:
        """Run ``late`` in a task created by an execution as bob, once that execution ended.

        ``inside`` runs in that execution first; what it returns is appended to ``seen``.
        """

        async def start(
            executor: RuntimeExecutor,
            late: Callable[[], Awaitable[Any]],
            inside: Callable[[], object] = lambda: None,
            seen: list[object] | None = None,
        ) -> asyncio.Task[Any]:
            go = asyncio.Event()
            survivors: list[asyncio.Task[Any]] = []

            async def after_the_end() -> Any:
                await go.wait()
                return await late()

            async def outer() -> None:
                if seen is not None:
                    seen.append(inside())
                survivors.append(asyncio.create_task(after_the_end()))

            await executor.execute(Run(outer), identity=BOB)
            go.set()
            return survivors[0]

        return start

    async def test_runs_a_use_case_as_the_given_identity_in_its_own_execution(
        self, outliving: Outliving, stores: AcmeStores, executor: RuntimeExecutor
    ) -> None:
        seen: list[object] = []

        task = await outliving(
            executor,
            lambda: executor.run(AcmeAsCaller, factory=BUILDING, identity=ANA),
            inside=lambda: (TICKET(), current_scope()),
            seen=seen,
        )
        identity, session, scope = await task

        [outer] = seen
        outer_session, outer_scope = cast(tuple[object, object], outer)
        assert identity is ANA
        assert session is not outer_session
        assert scope is not None and scope is not outer_scope
        assert len(stores.created) == 2

    async def test_given_no_identity_runs_with_no_caller(
        self, outliving: Outliving, executor: RuntimeExecutor
    ) -> None:

        task = await outliving(executor, lambda: executor.execute(Run(identity_now)))

        with pytest.raises(Unauthenticated, match="identity="):
            await task

    async def test_given_no_identity_the_ambient_identity_is_not_the_departed_caller(
        self, outliving: Outliving, executor: RuntimeExecutor
    ) -> None:

        task = await outliving(executor, lambda: executor.execute(Run(ambient_now)))

        assert await task is ANONYMOUS

    async def test_given_an_identity_the_ambient_identity_is_that_one(
        self, outliving: Outliving, executor: RuntimeExecutor
    ) -> None:

        task = await outliving(executor, lambda: executor.execute(Run(ambient_now), identity=ANA))

        assert await task is ANA

    async def test_does_not_reuse_the_departed_post_commit_channel(
        self, outliving: Outliving, executor: RuntimeExecutor
    ) -> None:
        seen: list[object] = []

        task = await outliving(
            executor,
            lambda: executor.execute(Run(channel_now), identity=ANA, read_only=True),
            inside=active_channel,
            seen=seen,
        )
        channel = await task

        assert seen[0] is not None
        assert channel is not seen[0]

    async def test_a_job_dispatched_inside_it_runs(
        self, outliving: Outliving, executor: RuntimeExecutor
    ) -> None:
        sent: list[str] = []

        async def dispatch() -> None:
            add_pending_dispatch(lambda: sent.append("acme-job"))

        task = await outliving(executor, lambda: executor.execute(Run(dispatch), identity=ANA))
        await task

        assert sent == ["acme-job"]

    async def test_reading_the_identity_directly_still_raises_execution_closed(
        self, outliving: Outliving, executor: RuntimeExecutor
    ) -> None:

        async def read() -> Any:
            return execution_identity()

        task = await outliving(executor, read)

        with pytest.raises(ExecutionClosed, match="executor.run"):
            await task

    async def test_dispatching_a_job_directly_raises_execution_closed(
        self, outliving: Outliving, executor: RuntimeExecutor
    ) -> None:
        sent: list[str] = []

        async def dispatch() -> None:
            add_pending_dispatch(lambda: sent.append("acme-job"))

        task = await outliving(executor, dispatch)

        with pytest.raises(ExecutionClosed, match="dispatch a job from inside the execution"):
            await task
        assert sent == []
