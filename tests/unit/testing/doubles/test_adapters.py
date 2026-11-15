from __future__ import annotations

import warnings
from collections.abc import Callable
from typing import Any

import pytest

from loom.core.di.scope import Scope
from loom.core.errors import Unauthenticated
from loom.core.identity import Identity
from loom.core.use_case.use_case import UseCase
from loom.testing.golden import GoldenHarness
from loom.testing.runner import AgentHandleDouble, McpHandleDouble, UseCaseTest
from tests.unit.testing.doubles import (
    AgentTriage,
    AgentUseCase,
    McpTriage,
    McpUseCase,
    SearchOnlyUseCase,
    Triage,
    TriageUseCase,
)

Registration = Callable[[UseCaseTest[object], AgentHandleDouble, McpHandleDouble], Any]


class TestUseCaseTestResolvers:
    async def test_binding_the_doubles_warns_nothing(
        self, caller: Identity, triage_agent: AgentHandleDouble, runbooks: McpHandleDouble
    ) -> None:
        runner = (
            UseCaseTest(AgentUseCase())
            .with_caller(caller)
            .with_agent("triage", triage_agent)
            .with_mcp("runbooks", runbooks)
        )
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            assert await runner.run() is triage_agent

    async def test_the_mcp_double_is_narrowed_to_the_markers_include(
        self, caller: Identity
    ) -> None:
        runbooks = McpHandleDouble("runbooks").with_tools("search", "delete")
        runner = UseCaseTest(SearchOnlyUseCase()).with_caller(caller).with_mcp("runbooks", runbooks)

        assert await runner.run() == ("search",)

    @pytest.mark.parametrize(
        ("use_case", "register"),
        [
            (AgentUseCase(), lambda runner, agent, mcp: runner.with_agent("triage", agent)),
            (McpUseCase(), lambda runner, agent, mcp: runner.with_mcp("runbooks", mcp)),
        ],
        ids=["agent", "mcp"],
    )
    async def test_a_marker_without_a_caller_fails_closed(
        self,
        use_case: UseCase[Any, object],
        register: Registration,
        triage_agent: AgentHandleDouble,
        runbooks: McpHandleDouble,
    ) -> None:
        runner = register(UseCaseTest(use_case), triage_agent, runbooks)
        with pytest.raises(Unauthenticated):
            await runner.run()


class TestGoldenHarnessAdapterMarkers:
    @pytest.mark.parametrize("scope", [Scope.REQUEST, Scope.APPLICATION])
    async def test_an_agent_adapter_resolves_through_its_double(
        self,
        harness: GoldenHarness,
        caller: Identity,
        triage_agent: AgentHandleDouble,
        scope: Scope,
    ) -> None:
        harness.register(Triage, AgentTriage, scope=scope)
        harness.with_agent("triage", triage_agent)

        result = await harness.run(TriageUseCase, params={"incident": "INC-1"}, identity=caller)

        assert result == "sev-2"
        assert [run.prompt for run in triage_agent.run_calls] == ["INC-1"]
        assert AgentTriage.built == 1

    async def test_an_mcp_adapter_resolves_through_its_narrowed_double(
        self, harness: GoldenHarness, caller: Identity, runbooks: McpHandleDouble
    ) -> None:
        harness.register(Triage, McpTriage)
        harness.with_mcp("runbooks", runbooks)

        result = await harness.run(TriageUseCase, params={"incident": "INC-1"}, identity=caller)

        assert result == "restart the broker"

    async def test_an_execute_marker_resolves_through_its_double(
        self, harness: GoldenHarness, caller: Identity, triage_agent: AgentHandleDouble
    ) -> None:
        harness.with_agent("triage", triage_agent)

        assert await harness.run(AgentUseCase, identity=caller) is triage_agent

    @pytest.mark.parametrize(
        ("adapter", "declared"),
        [(AgentTriage, "Agent('triage')"), (McpTriage, "Mcp('runbooks', ...)")],
        ids=["agent", "mcp"],
    )
    async def test_an_adapter_marker_without_a_double_fails_closed(
        self, harness: GoldenHarness, caller: Identity, adapter: type, declared: str
    ) -> None:
        harness.register(Triage, adapter)

        with pytest.raises(RuntimeError) as raised:
            await harness.run(TriageUseCase, params={"incident": "INC-1"}, identity=caller)

        assert declared in str(raised.value)
        assert "TriageUseCase" in str(raised.value)

    async def test_a_request_adapter_marker_without_a_caller_fails_closed(
        self, harness: GoldenHarness, triage_agent: AgentHandleDouble
    ) -> None:
        harness.register(Triage, AgentTriage)
        harness.with_agent("triage", triage_agent)

        with pytest.raises(Unauthenticated):
            await harness.run(TriageUseCase, params={"incident": "INC-1"})

        assert triage_agent.run_calls == []
