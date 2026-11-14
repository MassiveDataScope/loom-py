"""Start-up verification of the ``Agent()`` marker (spec 014, T202).

White-box unit tests of the two steps ``create_app`` runs after
``AiWiring.build``: ``MarkerDeclarations.verify`` aborts start-up on an unknown
agent or a mismatched output type, and ``AiWiring.bind_resolvers`` wires the
executor's resolvers once (and only once) an AI runtime exists.
"""

from __future__ import annotations

from typing import Any

import msgspec
import pytest

from loom.ai.abc import AgentHandle
from loom.ai.compiler._plan import AgentPlan, CompiledInstruction, CompiledOutput
from loom.ai.declarative import PolicySpec
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.inference import InferenceTarget
from loom.composition._ai import AiWiring
from loom.core.bootstrap import KernelRuntime
from loom.core.di import LoomContainer
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.identity import Identity
from loom.core.model import msgspec_type
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.sql.service import NullSqlQueryService, SqlQueryService
from loom.core.use_case import Agent
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.invoker import AppInvoker
from loom.core.use_case.registry import UseCaseRegistry
from loom.core.use_case.use_case import UseCase
from tests.helpers.ai import declared_markers


def _plan(name: str, *, output_type: type[Any] = dict) -> AgentPlan:
    """Build a minimal compiled plan naming its own declared output type."""
    return AgentPlan(
        name=name,
        description=f"{name} test agent",
        instructions=(CompiledInstruction(text="answer"),),
        spec_version=1,
        inference=InferenceTarget(provider="fake", model="fake-model"),
        output=CompiledOutput(
            schema={"type": "object"},
            loom_type=msgspec_type(output_type),
        ),
        capabilities=(),
        policies=PolicySpec(),
        metadata={},
    )


def _by_name(*plans: AgentPlan) -> dict[str, AgentPlan]:
    return {plan.name: plan for plan in plans}


class _Severity(msgspec.Struct):
    level: int


class KnownAgentUseCase(UseCase[object, object]):
    """Declares an agent whose plan will exist, with a matching output type."""

    async def execute(self, triage: AgentHandle[dict] = Agent("triage")) -> object:
        return triage


class MismatchedOutputUseCase(UseCase[object, object]):
    """The annotation names a type the artefact does not declare (dict)."""

    async def execute(self, triage: AgentHandle[_Severity] = Agent("triage")) -> object:
        return triage


class UnknownAgentUseCase(UseCase[object, object]):
    """Names an agent no plan in this deployment compiles."""

    async def execute(self, triage: AgentHandle[dict] = Agent("does-not-exist")) -> object:
        return triage


class AcmeAgentUseCaseWithoutMarker(UseCase[object, object]):
    """Declares no Agent() marker at all — must never be inspected."""

    async def execute(self) -> object:
        return None


class TestUseCaseWithoutAnAgentMarker:
    def test_no_ai_section_and_no_markers_does_not_fail(self) -> None:
        declared_markers(AcmeAgentUseCaseWithoutMarker).verify({}, {})  # no raise


class TestUnknownAgent:
    def test_an_unknown_agent_name_aborts_startup(self) -> None:
        with pytest.raises(AgentCompilationError) as excinfo:
            declared_markers(UnknownAgentUseCase).verify(_by_name(_plan("triage")), {})

        codes = {issue.code for issue in excinfo.value.issues}
        assert codes == {AgentErrorCode.AGENT_MARKER_UNKNOWN}
        message = str(excinfo.value)
        assert "does-not-exist" in message
        assert "triage" in message

    def test_without_an_ai_section_the_message_says_there_are_no_agents(self) -> None:
        with pytest.raises(AgentCompilationError) as excinfo:
            declared_markers(UnknownAgentUseCase).verify({}, {})

        assert "none" in str(excinfo.value)


class TestMismatchedAgentOutput:
    def test_an_annotation_that_mismatches_the_declared_output_aborts(self) -> None:
        plans = _by_name(_plan("triage", output_type=dict))

        with pytest.raises(AgentCompilationError) as excinfo:
            declared_markers(MismatchedOutputUseCase).verify(plans, {})

        codes = {issue.code for issue in excinfo.value.issues}
        assert codes == {AgentErrorCode.AGENT_MARKER_OUTPUT_MISMATCH}

    def test_a_matching_annotation_does_not_fail(self) -> None:
        plans = _by_name(_plan("triage", output_type=dict))

        declared_markers(KnownAgentUseCase).verify(plans, {})  # no raise


class TestAgentMarkerProblemsAreReportedTogether:
    def test_two_distinct_problems_are_reported_together(self) -> None:
        declared = declared_markers(UnknownAgentUseCase, MismatchedOutputUseCase)

        with pytest.raises(AgentCompilationError) as excinfo:
            declared.verify(_by_name(_plan("triage", output_type=dict)), {})

        codes = {issue.code for issue in excinfo.value.issues}
        assert codes == {
            AgentErrorCode.AGENT_MARKER_UNKNOWN,
            AgentErrorCode.AGENT_MARKER_OUTPUT_MISMATCH,
        }


def _kernel_runtime() -> KernelRuntime:
    """Build a real, minimal ``KernelRuntime`` — every collaborator genuine, none stubbed.

    ``SqlQueryService`` is registered the same way ``create_app`` always
    registers it, before ``AiWiring.bind_resolvers`` ever runs: that
    method resolves it unconditionally, so an unregistered container
    here would fail every test in this class for a reason unrelated to what
    each one is pinning.
    """
    container = LoomContainer()
    container.register_instance(SqlQueryService, NullSqlQueryService())
    compiler = UseCaseCompiler()
    factory = UseCaseFactory(container)
    executor = RuntimeExecutor(compiler)
    registry = UseCaseRegistry.build([])
    app = AppInvoker(factory=factory, executor=executor, registry=registry)
    return KernelRuntime(
        container=container,
        compiler=compiler,
        factory=factory,
        executor=executor,
        registry=registry,
        app=app,
    )


def _fake_agent_runtime() -> Any:
    """An unentered ``AgentRuntime``: enough to close over, never called here.

    ``AiWiring.bind_resolvers`` only reads ``runtime is None`` and hands the
    value on to the marker resolvers, which store it and never call any of
    its methods until the handle they build actually runs.
    So a real ``AgentRuntime`` never entered as a context manager is a
    faithful double for this seam — no fake class needed.
    """
    from loom.ai.runtime import AgentRuntime

    return object.__new__(AgentRuntime)


class TestResolversBindOnlyWithAnAiRuntime:
    @pytest.mark.parametrize("resolver", ["_agent_resolver", "_mcp_resolver"])
    def test_without_an_ai_section_the_executor_receives_no_resolver(self, resolver: str) -> None:
        result = _kernel_runtime()

        AiWiring.ABSENT.bind_resolvers(result)

        assert getattr(result.executor, resolver) is None

    def test_with_an_ai_runtime_both_resolvers_are_bound_once(self) -> None:
        result = _kernel_runtime()
        ai = AiWiring(config=None, runtime=_fake_agent_runtime())

        ai.bind_resolvers(result)

        assert result.executor._agent_resolver is not None  # noqa: SLF001
        assert result.executor._mcp_resolver is not None  # noqa: SLF001
        with pytest.raises(RuntimeError, match="more than once"):
            result.executor.bind_agent_resolver(lambda name, identity: object())
        with pytest.raises(RuntimeError, match="more than once"):
            result.executor.bind_mcp_resolver(lambda server, include, identity: object())

    @pytest.mark.parametrize("registered", [True, False], ids=["registered", "unregistered"])
    def test_the_handle_opens_spans_on_the_registered_observability_runtime(
        self, registered: bool
    ) -> None:
        result = _kernel_runtime()
        runtime = ObservabilityRuntime([])
        if registered:
            result.container.register_instance(ObservabilityRuntime, runtime)

        AiWiring(config=None, runtime=_fake_agent_runtime()).bind_resolvers(result)

        resolver = result.executor._agent_resolver  # noqa: SLF001
        assert resolver is not None
        handle = resolver("triage", Identity(subject="ada", mechanism="test"))
        assert handle._observability is (runtime if registered else None)  # noqa: SLF001
