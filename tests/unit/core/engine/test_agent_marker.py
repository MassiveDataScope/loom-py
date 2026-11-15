"""The ``Agent()`` marker: resolved to a handle closed over the verified caller.

Mirrors ``test_caller_binding.py`` — same shape of coverage, one level up: a
use case declares ``Agent("name")``, the compiler records where it landed,
and the executor resolves it through a callable the AI pillar supplies,
never through a parameter the caller could pass.
"""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.errors import Unauthenticated
from loom.core.execution import execution_identity
from loom.core.identity import ANONYMOUS, Identity
from loom.core.use_case.markers import Agent, Caller
from loom.core.use_case.use_case import UseCase

_ALICE = Identity(subject="alice", mechanism="test")


class _FakeHandle:
    """Stands in for an ``AgentHandle`` without pulling in the AI pillar."""

    def __init__(self, agent: str, identity: Identity) -> None:
        self.agent = agent
        self.identity = identity


class TriageUseCase(UseCase[object, tuple[str, str]]):
    """Declares one Agent() marker alongside the caller."""

    async def execute(
        self,
        caller: Identity = Caller(),
        triage: Any = Agent("incident-triage"),
    ) -> tuple[str, str]:
        return caller.subject, triage.agent


class TwoAgentsUseCase(UseCase[object, tuple[str, str]]):
    """Two distinct named agents in one signature."""

    async def execute(
        self,
        first: Any = Agent("triage"),
        second: Any = Agent("summariser"),
    ) -> tuple[str, str]:
        return first.agent, second.agent


class NoAgentUseCase(UseCase[object, str]):
    """Declares no Agent() marker at all."""

    async def execute(self) -> str:
        return "no agents here"


def _resolver(calls: list[tuple[str, Identity]]) -> Any:
    class Resolver:
        def resolve(self, declaration: object, annotation: object) -> _FakeHandle:
            name = declaration.name
            identity = execution_identity()
            calls.append((name, identity))
            return _FakeHandle(name, identity)

    return Resolver()


# ---------------------------------------------------------------------------
# Compilation
# ---------------------------------------------------------------------------


class TestAgentMarker:
    def test_the_marker_compiles_into_an_agent_binding(self) -> None:
        plan = UseCaseCompiler().compile(TriageUseCase)
        assert [ab.name for ab in plan.agent_bindings] == ["triage"]
        assert plan.agent_bindings[0].agent == "incident-triage"

    def test_the_agent_parameter_is_not_a_primitive_parameter(self) -> None:
        """It must not fall through to ``ParamBinding``, or callers would supply it."""
        plan = UseCaseCompiler().compile(TriageUseCase)
        assert [pb.name for pb in plan.param_bindings] == []

    def test_several_agent_markers_compile_in_declaration_order(self) -> None:
        plan = UseCaseCompiler().compile(TwoAgentsUseCase)
        assert [(ab.name, ab.agent) for ab in plan.agent_bindings] == [
            ("first", "triage"),
            ("second", "summariser"),
        ]

    def test_a_use_case_without_the_marker_has_no_agent_bindings(self) -> None:
        plan = UseCaseCompiler().compile(NoAgentUseCase)
        assert plan.agent_bindings == ()

    # ---------------------------------------------------------------------------
    # Execution
    # ---------------------------------------------------------------------------

    async def test_the_resolved_handle_is_bound_to_the_verified_caller(self) -> None:
        """The identity closed over by the handle is this execution's caller, not

        a parameter the use case body could have gotten wrong: there is no
        identity argument in the signature for the author to pass at all.
        """
        calls: list[tuple[str, Identity]] = []
        compiler = UseCaseCompiler()
        compiler.compile(TriageUseCase)
        executor = RuntimeExecutor(compiler)
        executor.bind_agent_resolver(_resolver(calls))

        subject, agent_name = await executor.execute(TriageUseCase(), identity=_ALICE)

        assert subject == "alice"
        assert agent_name == "incident-triage"
        assert calls == [("incident-triage", _ALICE)]

    async def test_an_anonymous_identity_still_resolves_a_handle(self) -> None:
        """Binding succeeds even for ANONYMOUS: the handle itself must refuse it,

        not the executor — the refusal belongs right before the model call, not
        merely because a parameter is bound (see the AI-pillar handle tests).
        """
        calls: list[tuple[str, Identity]] = []
        compiler = UseCaseCompiler()
        compiler.compile(TriageUseCase)
        executor = RuntimeExecutor(compiler)
        executor.bind_agent_resolver(_resolver(calls))

        subject, _ = await executor.execute(TriageUseCase(), identity=ANONYMOUS)

        assert subject == ""
        assert calls == [("incident-triage", ANONYMOUS)]

    async def test_a_missing_identity_is_refused_instead_of_defaulted(self) -> None:
        """Fail-closed, exactly like ``Caller()``: no identity is a transport bug."""
        compiler = UseCaseCompiler()
        compiler.compile(TriageUseCase)
        executor = RuntimeExecutor(compiler)
        executor.bind_agent_resolver(_resolver([]))
        use_case = TriageUseCase()

        with pytest.raises(Unauthenticated, match="TriageUseCase"):
            await executor.execute(use_case)

    async def test_no_resolver_bound_fails_with_a_named_error(self) -> None:
        """A use case declaring Agent() before the AI runtime is wired fails loudly."""
        compiler = UseCaseCompiler()
        compiler.compile(TriageUseCase)
        executor = RuntimeExecutor(compiler)
        use_case = TriageUseCase()

        with pytest.raises(RuntimeError, match="no 'agent' resolver is bound"):
            await executor.execute(use_case, identity=_ALICE)

    async def test_binding_a_resolver_twice_is_refused(self) -> None:
        executor = RuntimeExecutor(UseCaseCompiler())
        executor.bind_agent_resolver(_resolver([]))
        second_resolver = _resolver([])

        with pytest.raises(RuntimeError):
            executor.bind_agent_resolver(second_resolver)

    async def test_a_use_case_with_no_agent_marker_never_calls_the_resolver(self) -> None:
        calls: list[tuple[str, Identity]] = []
        compiler = UseCaseCompiler()
        compiler.compile(NoAgentUseCase)
        executor = RuntimeExecutor(compiler)
        executor.bind_agent_resolver(_resolver(calls))

        result = await executor.execute(NoAgentUseCase(), identity=_ALICE)

        assert result == "no agents here"
        assert calls == []
