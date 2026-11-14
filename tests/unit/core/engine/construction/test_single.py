"""One execution context: the constructor's, the resolvers' and ``execute()``'s are the same."""

from __future__ import annotations

from typing import Any

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.execution import ExecutionContext, current_execution
from loom.core.identity import Identity
from loom.core.use_case.markers import Agent, Mcp
from loom.core.use_case.use_case import UseCase
from tests.unit.core.engine.construction import acme


async def test_the_constructor_runs_in_the_execute_context() -> None:
    probe = await acme.executor().run(acme.AcmeProbe, factory=acme.factory(), identity=acme.ALICE)

    assert probe.built_in is not None
    assert probe.built_in is probe.ran_in
    assert probe.session.execution is probe.ran_in


async def test_a_per_execution_adapter_declaring_the_caller_gets_the_execution_identity() -> None:
    probe = await acme.executor().run(acme.AcmeProbe, factory=acme.factory(), identity=acme.ALICE)

    assert probe.session.caller is acme.ALICE


class AcmeAssistant(UseCase[Any, ExecutionContext | None]):
    async def execute(
        self,
        agent: Any = Agent("acme-triage"),
        docs: Any = Mcp("acme-docs", include=["search"]),
    ) -> ExecutionContext | None:
        assert agent == docs
        return current_execution()


async def test_execute_markers_resolve_in_the_execution_itself() -> None:
    seen: list[ExecutionContext | None] = []

    def resolve(*declaration: Any) -> ExecutionContext | None:
        identity: Identity = declaration[-1]
        assert identity is acme.ALICE
        seen.append(current_execution())
        return seen[-1]

    executor = RuntimeExecutor(UseCaseCompiler(), agent_resolver=resolve, mcp_resolver=resolve)

    ran_in = await executor.execute(AcmeAssistant(), identity=acme.ALICE)

    assert ran_in is not None
    assert seen == [ran_in, ran_in]
