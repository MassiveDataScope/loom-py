from __future__ import annotations

import inspect
from typing import Any
from unittest.mock import AsyncMock

import msgspec
import pytest

from loom.ai.abc import AgentHandle, McpHandle
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.composition._ai import AgentDepsFactory
from loom.core.command import Command
from loom.core.engine.plan import ExecutionPlan
from loom.core.errors import NotFound, RuleViolation, RuleViolations
from loom.core.identity import Identity
from loom.core.model import LoomStruct
from loom.core.use_case.markers import Agent, Caller, Input, LoadById, Mcp
from loom.core.use_case.use_case import UseCase
from loom.testing.runner import AgentHandleDouble, DepsFactoryDouble, McpHandleDouble, UseCaseTest

# ---------------------------------------------------------------------------
# Domain fixtures
# ---------------------------------------------------------------------------


class Cmd(Command, frozen=True):
    email: str
    name: str


class Entity:
    def __init__(self, name: str) -> None:
        self.name = name


class _SimpleUseCase(UseCase[Any, str]):
    async def execute(self, cmd: Cmd = Input()) -> str:
        return cmd.email


class _ParamOnlyUseCase(UseCase[Any, str]):
    async def execute(self, user_id: int) -> str:
        return f"id={user_id}"


class _ParamAndInputUseCase(UseCase[Any, str]):
    async def execute(
        self,
        tenant_id: str,
        cmd: Cmd = Input(),
    ) -> str:
        return f"{tenant_id}:{cmd.email}"


class _LoadUseCase(UseCase[Any, str]):
    async def execute(
        self,
        eid: int,
        entity: Entity = LoadById(Entity, by="eid"),
    ) -> str:
        return entity.name


def _always_fail_rule(
    command: Command,
    fields_set: frozenset[str],
    context: dict[str, object] | None = None,
) -> None:
    raise RuleViolation("email", "bad")


class _RuleFailUseCase(UseCase[Any, str]):
    rules = [_always_fail_rule]

    async def execute(self, cmd: Cmd = Input()) -> str:
        return cmd.email


class _Product(LoomStruct):
    id: int
    name: str


class _MainRepoUseCase(UseCase[_Product, str]):
    async def execute(self, product_id: int) -> str:
        product: _Product | None = await self.main_repo.get_by_id(product_id)
        if product is None:
            return "missing"
        return product.name


class _SeverityAssessment(msgspec.Struct, frozen=True):
    severity: int


class _TriageUseCase(UseCase[Any, dict[str, Any]]):
    """Mirrors the markers.md example: query, tool call, model run, branch."""

    async def execute(
        self,
        incident_id: str,
        caller: Identity = Caller(),
        triage: AgentHandle[_SeverityAssessment] = Agent("incident-triage"),
    ) -> dict[str, Any]:
        obs = triage.sql("observability_readonly")
        deploys = await obs.query("select * from deploys where incident = :id")

        runbooks = triage.mcp("runbooks")
        runbook = await runbooks.call("search_incident", {"incident_id": incident_id}, expect=dict)

        assessment = await triage.run(f"Assess {incident_id}.")

        escalate = assessment.output.severity >= 4
        return {
            "caller": caller.subject,
            "deploys": deploys,
            "runbook": runbook,
            "severity": assessment.output.severity,
            "escalate": escalate,
        }


class _NoDoubleUseCase(UseCase[Any, object]):
    async def execute(
        self, triage: AgentHandle[_SeverityAssessment] = Agent("incident-triage")
    ) -> object:
        return triage


class _McpUseCase(UseCase[Any, dict[str, Any]]):
    """Declares an Mcp() marker narrowed to a single tool."""

    async def execute(
        self,
        incident_id: str,
        runbooks: McpHandle = Mcp("runbooks", include=("search_incident",)),
    ) -> dict[str, Any]:
        result = await runbooks.call_untyped("search_incident", {"incident_id": incident_id})
        return dict(result)


class _NoMcpDoubleUseCase(UseCase[Any, object]):
    async def execute(self, runbooks: McpHandle = Mcp("runbooks", include=("*",))) -> object:
        return runbooks


class _McpTypedCallUseCase(UseCase[Any, Any]):
    """Declares an Mcp() marker and calls the typed ``call()`` method."""

    async def execute(
        self,
        tool: str,
        runbooks: McpHandle = Mcp("runbooks", include=("search_incident",)),
    ) -> Any:
        return await runbooks.call(tool, {}, expect=dict)


class _WideningMcpUseCase(UseCase[Any, object]):
    """Calls a tool its own Mcp() include does not admit."""

    async def execute(self, runbooks: McpHandle = Mcp("runbooks", include=("search_*",))) -> object:
        return await runbooks.call_untyped("delete_incident", {})


class _TwoMcpServersUseCase(UseCase[Any, dict[str, Any]]):
    """Declares two independent Mcp() markers — exercises per-server routing."""

    async def execute(
        self,
        runbooks: McpHandle = Mcp("runbooks", include=("search_incident",)),
        docs: McpHandle = Mcp("docs", include=("lookup",)),
    ) -> dict[str, Any]:
        runbook = await runbooks.call_untyped("search_incident", {})
        doc = await docs.call_untyped("lookup", {})
        return {"runbook": runbook, "doc": doc}


# ---------------------------------------------------------------------------
# Basic execution
# ---------------------------------------------------------------------------


class TestUseCaseTestRun:
    async def test_run_simple_input_use_case(self) -> None:
        result = await (
            UseCaseTest(_SimpleUseCase()).with_input(email="alice@corp.com", name="Alice").run()
        )
        assert result == "alice@corp.com"

    async def test_run_param_only_use_case(self) -> None:
        result = await UseCaseTest(_ParamOnlyUseCase()).with_params(user_id=42).run()
        assert result == "id=42"

    async def test_run_param_and_input(self) -> None:
        result = await (
            UseCaseTest(_ParamAndInputUseCase())
            .with_params(tenant_id="t-1")
            .with_input(email="b@corp.com", name="Bob")
            .run()
        )
        assert result == "t-1:b@corp.com"

    async def test_run_with_command(self) -> None:
        cmd = Cmd(email="pre@corp.com", name="Pre")
        result = await UseCaseTest(_SimpleUseCase()).with_command(cmd).run()
        assert result == "pre@corp.com"

    async def test_run_with_loaded_entity(self) -> None:
        entity = Entity(name="preloaded")
        result = await (
            UseCaseTest(_LoadUseCase()).with_params(eid=1).with_loaded(Entity, entity).run()
        )
        assert result == "preloaded"

    async def test_run_with_deps(self) -> None:
        entity = Entity(name="from_repo")
        repo = AsyncMock()
        repo.get_by_id = AsyncMock(return_value=entity)
        result = await UseCaseTest(_LoadUseCase()).with_params(eid=5).with_deps(Entity, repo).run()
        assert result == "from_repo"
        repo.get_by_id.assert_awaited_once_with(5, profile="default")


# ---------------------------------------------------------------------------
# Error propagation
# ---------------------------------------------------------------------------


class TestUseCaseTestErrors:
    async def test_rule_violation_propagated(self) -> None:
        test_case = UseCaseTest(_RuleFailUseCase()).with_input(email="bad@corp.com", name="X")
        with pytest.raises(RuleViolations):
            await test_case.run()

    async def test_not_found_propagated(self) -> None:
        repo = AsyncMock()
        repo.get_by_id = AsyncMock(return_value=None)
        test_case = UseCaseTest(_LoadUseCase()).with_params(eid=99).with_deps(Entity, repo)
        with pytest.raises(NotFound):
            await test_case.run()


# ---------------------------------------------------------------------------
# Plan property
# ---------------------------------------------------------------------------


class TestUseCaseTestPlan:
    def test_plan_returns_execution_plan(self) -> None:
        runner = UseCaseTest(_SimpleUseCase())
        assert isinstance(runner.plan, ExecutionPlan)

    def test_plan_has_correct_use_case_type(self) -> None:
        runner = UseCaseTest(_SimpleUseCase())
        assert runner.plan.use_case_type is _SimpleUseCase

    def test_plan_has_input_binding(self) -> None:
        runner = UseCaseTest(_SimpleUseCase())
        assert runner.plan.input_binding is not None

    async def test_plan_after_run_is_same_instance(self) -> None:
        runner = UseCaseTest(_SimpleUseCase())
        plan_before = runner.plan
        await runner.with_input(email="x@corp.com", name="X").run()
        assert runner.plan is plan_before


# ---------------------------------------------------------------------------
# Builder fluent API
# ---------------------------------------------------------------------------


class TestUseCaseTestBuilder:
    def test_with_params_returns_self(self) -> None:
        runner = UseCaseTest(_SimpleUseCase())
        assert runner.with_params() is runner

    def test_with_input_returns_self(self) -> None:
        runner = UseCaseTest(_SimpleUseCase())
        assert runner.with_input(email="x@c.com", name="Y") is runner

    def test_with_command_returns_self(self) -> None:
        runner = UseCaseTest(_SimpleUseCase())
        assert runner.with_command(Cmd(email="x@c.com", name="Y")) is runner

    def test_with_loaded_returns_self(self) -> None:
        runner = UseCaseTest(_LoadUseCase())
        assert runner.with_loaded(Entity, Entity("e")) is runner

    def test_with_deps_returns_self(self) -> None:
        runner = UseCaseTest(_LoadUseCase())
        assert runner.with_deps(Entity, AsyncMock()) is runner

    def test_with_main_repo_returns_self(self) -> None:
        runner = UseCaseTest(_MainRepoUseCase())
        repo = AsyncMock()
        assert runner.with_main_repo(repo) is runner

    async def test_with_input_merges_multiple_calls(self) -> None:
        result = await (
            UseCaseTest(_SimpleUseCase())
            .with_input(email="a@corp.com")
            .with_input(name="Alice")
            .run()
        )
        assert result == "a@corp.com"

    async def test_with_params_merges_multiple_calls(self) -> None:
        result = await (
            UseCaseTest(_ParamAndInputUseCase())
            .with_params(tenant_id="t-1")
            .with_input(email="c@corp.com", name="C")
            .run()
        )
        assert result == "t-1:c@corp.com"

    async def test_with_command_overrides_previous_input(self) -> None:
        cmd = Cmd(email="cmd@corp.com", name="Cmd")
        result = await (
            UseCaseTest(_SimpleUseCase())
            .with_input(email="original@corp.com", name="Original")
            .with_command(cmd)
            .run()
        )
        assert result == "cmd@corp.com"

    async def test_with_main_repo_injects_repo_for_use_case_logic(self) -> None:
        repo = AsyncMock()
        repo.get_by_id = AsyncMock(return_value=_Product(id=1, name="keyboard"))

        result = await (
            UseCaseTest(_MainRepoUseCase()).with_main_repo(repo).with_params(product_id=1).run()
        )

        assert result == "keyboard"
        repo.get_by_id.assert_awaited_once_with(1)


# ---------------------------------------------------------------------------
# Agent() marker doubles (spec 014, T401)
# ---------------------------------------------------------------------------


class TestAgentMarkerFailsClosedWithoutADouble:
    async def test_declaring_agent_with_no_double_raises(self) -> None:
        runner = UseCaseTest(_NoDoubleUseCase()).with_caller(Identity(subject="ada"))
        with pytest.raises(RuntimeError, match="incident-triage"):
            await runner.run()

    async def test_error_names_the_use_case_and_the_agent(self) -> None:
        runner = UseCaseTest(_NoDoubleUseCase()).with_caller(Identity(subject="ada"))
        with pytest.raises(RuntimeError) as excinfo:
            await runner.run()
        message = str(excinfo.value)
        assert "_NoDoubleUseCase" in message
        assert "incident-triage" in message

    async def test_a_use_case_with_no_marker_at_all_never_needs_a_double(self) -> None:
        result = await UseCaseTest(_SimpleUseCase()).with_input(email="x@corp.com", name="X").run()
        assert result == "x@corp.com"


class TestAgentHandleDoubleRequiresNoNetworkModelOrDatabase:
    """The closing criterion of T401: the whole example use case, doubled fully."""

    async def test_the_escape_hatch_says_a_double_has_no_engine_behind_it(self) -> None:
        """``native()`` is the one call a double cannot honour."""
        triage = AgentHandleDouble("incident-triage")

        with pytest.raises(NotImplementedError) as excinfo:
            triage.native()

        assert "no engine behind it" in str(excinfo.value)

    async def test_the_full_example_use_case_runs_with_no_network_no_model_no_database(
        self,
    ) -> None:
        triage = AgentHandleDouble("incident-triage").on_run(_SeverityAssessment(severity=5))
        triage.sql("observability_readonly").on_query([{"service": "checkout", "status": "bad"}])
        triage.mcp("runbooks").on_call("search_incident", {"title": "checkout runbook"})

        result = await (
            UseCaseTest(_TriageUseCase())
            .with_caller(Identity(subject="ada"))
            .with_agent("incident-triage", triage)
            .with_params(incident_id="INC-1")
            .run()
        )

        assert result == {
            "caller": "ada",
            "deploys": [{"service": "checkout", "status": "bad"}],
            "runbook": {"title": "checkout runbook"},
            "severity": 5,
            "escalate": True,
        }

    async def test_the_double_records_the_prompt_it_was_asked(self) -> None:
        triage = AgentHandleDouble("incident-triage").on_run(_SeverityAssessment(severity=1))
        triage.sql("observability_readonly").on_query([])
        triage.mcp("runbooks").on_call("search_incident", {})

        await (
            UseCaseTest(_TriageUseCase())
            .with_caller(Identity(subject="ada"))
            .with_agent("incident-triage", triage)
            .with_params(incident_id="INC-2")
            .run()
        )

        assert triage.run_calls[0].prompt == "Assess INC-2."
        assert triage.sql("observability_readonly").calls[0].statement.startswith("select")
        assert triage.mcp("runbooks").calls[0].tool == "search_incident"

    async def test_a_below_threshold_severity_does_not_escalate(self) -> None:
        triage = AgentHandleDouble("incident-triage").on_run(_SeverityAssessment(severity=1))
        triage.sql("observability_readonly").on_query([])
        triage.mcp("runbooks").on_call("search_incident", {})

        result = await (
            UseCaseTest(_TriageUseCase())
            .with_caller(Identity(subject="ada"))
            .with_agent("incident-triage", triage)
            .with_params(incident_id="INC-3")
            .run()
        )

        assert result["escalate"] is False

    async def test_with_agent_returns_self_for_chaining(self) -> None:
        runner = UseCaseTest(_NoDoubleUseCase())
        double = AgentHandleDouble("incident-triage").on_run(_SeverityAssessment(severity=1))
        assert runner.with_agent("incident-triage", double) is runner

    async def test_run_text_double_returns_the_scripted_prose(self) -> None:
        double = AgentHandleDouble("writer").on_run_text("the write-up")
        answer = await double.run_text("summarise")
        assert answer.output == "the write-up"
        assert double.run_text_calls[0].prompt == "summarise"

    async def test_a_single_scheduled_answer_serves_every_call(self) -> None:
        double = AgentHandleDouble("writer").on_run(_SeverityAssessment(severity=2))
        first = await double.run("first")
        second = await double.run("second")
        assert first.output.severity == 2
        assert second.output.severity == 2
        assert len(double.run_calls) == 2

    async def test_an_unscripted_run_raises(self) -> None:
        double = AgentHandleDouble("incident-triage")
        with pytest.raises(AssertionError, match="incident-triage"):
            await double.run("Assess INC-4.")

    async def test_an_unscripted_tool_call_raises(self) -> None:
        double = AgentHandleDouble("incident-triage")
        mcp_handle = double.mcp("runbooks")
        with pytest.raises(AssertionError, match="search_incident"):
            await mcp_handle.call("search_incident", {}, expect=dict)

    async def test_an_unscripted_query_raises(self) -> None:
        double = AgentHandleDouble("incident-triage")
        sql_handle = double.sql("observability_readonly")
        with pytest.raises(AssertionError, match="observability_readonly"):
            await sql_handle.query("select 1")

    async def test_grants_lists_every_mcp_and_sql_name_reached(self) -> None:
        double = AgentHandleDouble("incident-triage")
        double.mcp("runbooks")
        double.sql("observability_readonly")
        assert double.grants() == ("runbooks", "observability_readonly")


# ---------------------------------------------------------------------------
# Mcp() marker doubles (spec 015, T401)
# ---------------------------------------------------------------------------


class TestMcpMarkerFailsClosedWithoutADouble:
    async def test_declaring_mcp_with_no_double_raises(self) -> None:
        runner = UseCaseTest(_NoMcpDoubleUseCase()).with_caller(Identity(subject="ada"))
        with pytest.raises(RuntimeError, match="runbooks"):
            await runner.run()

    async def test_error_names_the_use_case_and_the_server(self) -> None:
        runner = UseCaseTest(_NoMcpDoubleUseCase()).with_caller(Identity(subject="ada"))
        with pytest.raises(RuntimeError) as excinfo:
            await runner.run()
        message = str(excinfo.value)
        assert "_NoMcpDoubleUseCase" in message
        assert "runbooks" in message


class TestMcpHandleDoubleEnforcesInclude:
    """The closing criterion of T401: the double refuses what production refuses."""

    async def test_the_use_case_runs_with_no_network_and_no_mcp_server(self) -> None:
        runbooks = McpHandleDouble("runbooks").with_tools("search_incident")
        runbooks.on_call_untyped("search_incident", {"title": "checkout runbook"})

        result = await (
            UseCaseTest(_McpUseCase())
            .with_caller(Identity(subject="ada"))
            .with_mcp("runbooks", runbooks)
            .with_params(incident_id="INC-1")
            .run()
        )

        assert result == {"title": "checkout runbook"}

    async def test_with_tools_drives_tools_through_the_filter(self) -> None:
        runbooks = McpHandleDouble("runbooks").with_tools("search_incident", "delete_incident")
        runner = UseCaseTest(_McpUseCase()).with_mcp("runbooks", runbooks)
        resolved = runner._resolve_mcp_double(
            "runbooks", ("search_incident",), Identity(subject="ada")
        )
        assert resolved.tools() == ("search_incident",)

    async def test_an_unscripted_in_filter_tool_call_raises_assertion_error(self) -> None:
        runbooks = McpHandleDouble("runbooks").with_tools("search_incident")
        # No on_call_untyped scripted: admitted by the filter, refused by the double.
        runner = (
            UseCaseTest(_McpUseCase())
            .with_caller(Identity(subject="ada"))
            .with_mcp("runbooks", runbooks)
            .with_params(incident_id="INC-1")
        )
        with pytest.raises(AssertionError, match="search_incident"):
            await runner.run()

    async def test_a_use_case_cannot_widen_its_own_include_by_calling_outside_it(self) -> None:
        """B1 / M2: the include a call is checked against must come from the
        resolving marker's own binding, exercised end to end through
        :meth:`UseCaseTest.run` — not a tuple the test hands the resolver
        by hand."""
        runbooks = McpHandleDouble("runbooks").with_tools("search_incident", "delete_incident")
        runbooks.on_call_untyped("delete_incident", {"ok": True})
        runner = (
            UseCaseTest(_WideningMcpUseCase())
            .with_caller(Identity(subject="ada"))
            .with_mcp("runbooks", runbooks)
        )
        with pytest.raises(AgentRunError) as excinfo:
            await runner.run()
        assert excinfo.value.code is AgentRunErrorCode.TOOL_UNKNOWN
        message = str(excinfo.value)
        assert "delete_incident" in message
        assert "search_incident" in message  # the only tool this include actually admits

    async def test_an_in_filter_tool_absent_from_with_tools_raises_tool_unknown(self) -> None:
        runbooks = McpHandleDouble("runbooks")  # nothing scripted through with_tools
        runner = (
            UseCaseTest(_McpUseCase())
            .with_caller(Identity(subject="ada"))
            .with_mcp("runbooks", runbooks)
            .with_params(incident_id="INC-1")
        )
        with pytest.raises(AgentRunError) as excinfo:
            await runner.run()
        assert excinfo.value.code is AgentRunErrorCode.TOOL_UNKNOWN
        assert "none" in str(excinfo.value)  # nothing published, nothing admitted

    async def test_with_mcp_returns_self_for_chaining(self) -> None:
        runner = UseCaseTest(_NoMcpDoubleUseCase())
        double = McpHandleDouble("runbooks")
        assert runner.with_mcp("runbooks", double) is runner


class TestMcpMarkerTypedCallEnforcesInclude:
    """H1: the typed ``call()`` path enforces the same guard as ``call_untyped()``."""

    async def test_a_typed_call_outside_include_raises_tool_unknown(self) -> None:
        runbooks = McpHandleDouble("runbooks").with_tools("search_incident", "delete_incident")
        runbooks.on_call("delete_incident", {"ok": True})
        runner = (
            UseCaseTest(_McpTypedCallUseCase())
            .with_caller(Identity(subject="ada"))
            .with_mcp("runbooks", runbooks)
            .with_params(tool="delete_incident")
        )
        with pytest.raises(AgentRunError) as excinfo:
            await runner.run()
        assert excinfo.value.code is AgentRunErrorCode.TOOL_UNKNOWN

    async def test_a_typed_call_inside_include_but_unpublished_raises_tool_unknown(self) -> None:
        runbooks = McpHandleDouble("runbooks")  # nothing scripted through with_tools
        runner = (
            UseCaseTest(_McpTypedCallUseCase())
            .with_caller(Identity(subject="ada"))
            .with_mcp("runbooks", runbooks)
            .with_params(tool="search_incident")
        )
        with pytest.raises(AgentRunError) as excinfo:
            await runner.run()
        assert excinfo.value.code is AgentRunErrorCode.TOOL_UNKNOWN


class TestMcpMarkerRefusalNamesTheDeclaredServer:
    """B2: the refusal names the server the marker declared, not the double's own name."""

    async def test_the_message_names_the_markers_server_not_the_doubles_name(self) -> None:
        mistyped = McpHandleDouble("runbook").with_tools("search_incident", "delete_incident")
        runner = (
            UseCaseTest(_WideningMcpUseCase())
            .with_caller(Identity(subject="ada"))
            .with_mcp("runbooks", mistyped)
        )
        with pytest.raises(AgentRunError) as excinfo:
            await runner.run()
        message = str(excinfo.value)
        assert "mcp server 'runbooks' grants" in message
        assert "mcp server 'runbook' grants" not in message


class TestMcpMarkerRoutesEachServerToItsOwnDouble:
    """M1: a double registered under a different name never stands in for the
    marker's own server, and two servers each reach their own double."""

    async def test_a_double_registered_under_another_name_fails_closed(self) -> None:
        other = McpHandleDouble("other").with_tools("search_incident")
        other.on_call_untyped("search_incident", {"title": "wrong server"})
        runner = (
            UseCaseTest(_McpUseCase())
            .with_caller(Identity(subject="ada"))
            .with_mcp("other", other)
            .with_params(incident_id="INC-1")
        )
        with pytest.raises(RuntimeError, match="runbooks"):
            await runner.run()

    async def test_two_servers_each_reach_their_own_double(self) -> None:
        runbooks = McpHandleDouble("runbooks").with_tools("search_incident")
        runbooks.on_call_untyped("search_incident", {"title": "checkout runbook"})
        docs = McpHandleDouble("docs").with_tools("lookup")
        docs.on_call_untyped("lookup", {"title": "checkout doc"})

        result = await (
            UseCaseTest(_TwoMcpServersUseCase())
            .with_caller(Identity(subject="ada"))
            .with_mcp("runbooks", runbooks)
            .with_mcp("docs", docs)
            .run()
        )

        assert result == {
            "runbook": {"title": "checkout runbook"},
            "doc": {"title": "checkout doc"},
        }


class TestDepsFactoryDoubleMatchesProduction:
    """R3: the double drifting from ``DepsFactory`` must be caught by a test,
    not rediscovered at run time."""

    def test_double_and_production_factory_accept_the_same_call(self) -> None:
        double_signature = inspect.signature(DepsFactoryDouble.build)
        production_signature = inspect.signature(AgentDepsFactory.build)
        assert double_signature.parameters.keys() == production_signature.parameters.keys()
        for name in double_signature.parameters:
            assert (
                double_signature.parameters[name].kind == production_signature.parameters[name].kind
            )
