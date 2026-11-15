"""The public contract of ``run`` and ``execute``: one execution, built inside it.

``run`` builds the use case through the given factory, else the executor's
builder, inside the execution it opens, before ``EXEC_START``; ``execute``
runs an instance it is handed through the same lifecycle. The constructor,
the resolvers and ``execute()`` see one execution context.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from loom.core.bootstrap.kernel import create_kernel
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.errors import UndeclaredStoreWrite
from loom.core.execution import ExecutionContext, current_execution
from loom.core.identity import ANONYMOUS
from loom.core.persistence.stores import DEFAULT_STORE
from loom.core.uow.coordinator import OrderedCommits
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.use_case import UseCase

from .._lifecycle_doubles import Log, StubUnitOfWorkFactory
from .conftest import MakeExecutor
from .doubles import (
    ALICE,
    JOURNAL,
    AcmeAssistant,
    AcmeEveryInputUseCase,
    AcmeOrder,
    AcmeProbe,
    AcmeRecordingLogger,
    AcmeSession,
    AcmeSettings,
    AcmeUser,
    fake_repo,
    refusing_builder,
    serves_session,
)


class TestRun:
    """``run(type, factory=None)`` (L9): a given factory overrides the executor's builder."""

    async def test_without_a_factory_the_builder_builds(
        self, factory: UseCaseFactory, make_executor: MakeExecutor
    ) -> None:
        executor = make_executor(builder=factory.build)

        probe = await executor.run(AcmeProbe, identity=ALICE)

        assert probe.session.caller is ALICE

    async def test_a_given_factory_overrides_the_builder(
        self, make_executor: MakeExecutor, factory: UseCaseFactory
    ) -> None:
        executor = make_executor(builder=refusing_builder)

        probe = await executor.run(AcmeProbe, factory=factory, identity=ALICE)

        assert probe.session.caller is ALICE

    async def test_with_neither_run_names_both(self, executor: RuntimeExecutor) -> None:
        with pytest.raises(TypeError, match=r"factory=.*builder="):
            await executor.run(AcmeProbe, identity=ALICE)

        assert JOURNAL.entries == []

    async def test_the_kernel_executor_builds_with_its_factory(self) -> None:
        kernel = create_kernel(
            config=AcmeSettings(), use_cases=[AcmeProbe], modules=[serves_session]
        )

        probe = await kernel.executor.run(AcmeProbe, identity=ALICE)

        assert probe.session.caller is ALICE

    async def test_builds_the_use_case_through_the_factory_and_forwards_every_argument(
        self,
    ) -> None:
        instance = AcmeEveryInputUseCase()
        factory = MagicMock()
        factory.build = MagicMock(return_value=instance)
        stores = StubUnitOfWorkFactory(Log())
        executor = RuntimeExecutor(
            UseCaseCompiler(),
            coordinator=OrderedCommits({DEFAULT_STORE: stores}),
            logger=AcmeRecordingLogger(),
        )
        loaded = AcmeUser(id=1, email="u@u.com")
        order_repo = fake_repo(AcmeOrder(id=7))

        result = await executor.run(
            AcmeEveryInputUseCase,
            factory=factory,
            params={"user_id": 1, "order_id": 7},
            payload={"email": "a@b.com"},
            dependencies={AcmeOrder: order_repo},
            load_overrides={AcmeUser: loaded},
            read_only=True,
            identity=ANONYMOUS,
        )

        factory.build.assert_called_once_with(AcmeEveryInputUseCase)
        user_id, email, user, order_exists, caller, read_only = result
        assert (user_id, email, user, order_exists, caller) == (
            1,
            "a@b.com",
            loaded,
            True,
            ANONYMOUS,
        )
        order_repo.exists_by.assert_awaited_once_with("id", 7)
        assert read_only is True
        assert stores.created == []


class TestBuiltInsideTheExecution:
    """The use case is built inside its execution, before ``EXEC_START``, events unchanged."""

    async def test_run_builds_inside_the_execution_and_before_it_starts(
        self, executor: RuntimeExecutor, factory: UseCaseFactory
    ) -> None:
        probe = await executor.run(AcmeProbe, factory=factory, identity=ALICE)

        assert probe.built_in is not None
        assert JOURNAL.entries == ["built", "EXEC_START", "EXEC_DONE"]

    async def test_execute_given_an_instance_emits_the_same_events(
        self, executor: RuntimeExecutor
    ) -> None:
        probe = AcmeProbe(AcmeSession(ALICE))
        JOURNAL.entries.clear()

        await executor.execute(probe, identity=ALICE)

        assert JOURNAL.entries == ["EXEC_START", "EXEC_DONE"]

    async def test_the_constructor_runs_in_the_execute_context(
        self, executor: RuntimeExecutor, factory: UseCaseFactory
    ) -> None:
        probe = await executor.run(AcmeProbe, factory=factory, identity=ALICE)

        assert probe.built_in is not None
        assert probe.built_in is probe.ran_in
        assert probe.session.execution is probe.ran_in

    async def test_a_per_execution_adapter_declaring_the_caller_gets_the_execution_identity(
        self, executor: RuntimeExecutor, factory: UseCaseFactory
    ) -> None:
        probe = await executor.run(AcmeProbe, factory=factory, identity=ALICE)

        assert probe.session.caller is ALICE

    async def test_execute_markers_resolve_in_the_execution_itself(self) -> None:
        seen: list[ExecutionContext | None] = []

        class Resolver:
            def resolve(self, declaration: object, annotation: object) -> ExecutionContext | None:
                assert current_execution() is not None
                seen.append(current_execution())
                return seen[-1]

        executor = RuntimeExecutor(
            UseCaseCompiler(), agent_resolver=Resolver(), mcp_resolver=Resolver()
        )

        ran_in = await executor.execute(AcmeAssistant(), identity=ALICE)

        assert ran_in is not None
        assert seen == [ran_in, ran_in]

    async def test_a_writer_under_a_read_only_execution_is_refused_before_it_is_built(
        self, executor: RuntimeExecutor, factory: UseCaseFactory
    ) -> None:
        class _ReadOnlyOuter(UseCase[Any, None]):
            async def execute(self) -> None:
                await executor.run(AcmeProbe, factory=factory)

        outer = _ReadOnlyOuter()

        with pytest.raises(UndeclaredStoreWrite, match="the enclosing execution is read-only"):
            await executor.execute(outer, read_only=True, identity=ALICE)

        assert JOURNAL.entries == ["EXEC_START", "EXEC_ERROR"]


class TestExecute:
    """``execute`` runs an instance under the plan compiled once for its class."""

    async def test_compiler_compile_called_once_per_class(self) -> None:
        # Use a locally-defined class so __execution_plan__ is always None
        # at the start of this test, regardless of other tests' execution order.
        class _FreshUseCase(UseCase[Any, str]):
            async def execute(self, value: int) -> str:
                return f"value={value}"

        compiler = UseCaseCompiler()
        ex = RuntimeExecutor(compiler, logger=AcmeRecordingLogger())

        # Execute twice — _build_plan should only be called once
        with patch.object(compiler, "_build_plan", wraps=compiler._build_plan) as mock_build:
            await ex.execute(_FreshUseCase(), params={"value": 1})
            await ex.execute(_FreshUseCase(), params={"value": 2})

        mock_build.assert_called_once()
