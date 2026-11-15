"""The kinds registry, the declared kinds, and the executor's one dispatch loop."""

from __future__ import annotations

from typing import Any, Generic, TypeVar

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.execution import ExecutionKinds, Kind
from loom.core.identity import Identity
from loom.core.use_case.kinds import AGENT, CALLER, MCP, declare_kinds
from loom.core.use_case.markers import Agent, Caller, Exists, Input, Load, LoadById, Mcp, Query
from loom.core.use_case.use_case import UseCase

_ACME = Identity(subject="acme-alice", mechanism="test")
_T = TypeVar("_T")
ACME = Kind("acme")


class AcmeMarker:
    pass


class AcmeKindCall(Generic[_T]):
    pass


class AcmeRecordingResolver:
    def __init__(self) -> None:
        self.calls: list[tuple[object, object]] = []

    def resolve(self, declaration: object, annotation: object) -> object:
        self.calls.append((declaration, annotation))
        return "acme-handle"


class AcmeNamedResolver:
    def __init__(self, calls: list[str], field: str) -> None:
        self.calls, self.field = calls, field

    def resolve(self, declaration: object, annotation: object) -> object:
        name = getattr(declaration, self.field)
        self.calls.append(name)
        return name


class AcmeMixedUseCase(UseCase[object, list[str]]):
    async def execute(
        self, docs: Any = Mcp("acme-docs", include=["search"]), triage: Any = Agent("acme-triage")
    ) -> list[str]:
        return [docs, triage]


class TestTheKindsRegistry:
    def test_a_declared_default_type_resolves_to_its_kind_before_the_annotation(self) -> None:
        kinds = ExecutionKinds()
        kinds.declare_type(AcmeMarker, ACME)
        kinds.declare_origin(AcmeKindCall, MCP)

        assert kinds.kind_for(AcmeMarker(), AcmeKindCall[int]) is ACME
        assert kinds.kind_for(None, AcmeKindCall[int]) is MCP
        assert kinds.kind_for(None, int) is None

    def test_a_refused_default_type_has_its_reason_and_no_kind(self) -> None:
        kinds = ExecutionKinds()
        kinds.refuse(AcmeMarker, "AcmeMarker is no dependency")

        assert kinds.refusal_for(AcmeMarker()) == "AcmeMarker is no dependency"
        assert kinds.refusal_for(None) is None
        assert kinds.kind_for(AcmeMarker(), Any) is None

    def test_declaring_again_is_idempotent_and_a_conflict_is_refused(self) -> None:
        kinds = ExecutionKinds()
        kinds.declare_type(AcmeMarker, ACME)
        kinds.declare_type(AcmeMarker, ACME)

        with pytest.raises(ValueError, match="already declared"):
            kinds.declare_type(AcmeMarker, Kind("acme"))

    def test_a_resolver_is_bound_once_and_receives_the_declaration_and_annotation(self) -> None:
        kinds = ExecutionKinds()
        resolver = AcmeRecordingResolver()
        marker = AcmeMarker()
        assert not kinds.is_bound(ACME)

        kinds.bind(ACME, resolver)

        assert kinds.is_bound(ACME)
        assert kinds.resolve(ACME, marker, int) == "acme-handle"
        assert resolver.calls == [(marker, int)]
        another = AcmeRecordingResolver()
        with pytest.raises(RuntimeError, match="already bound"):
            kinds.bind(ACME, another)

    def test_called_classes_are_plain_classes_and_nothing_by_default(self) -> None:
        calling = Kind("acme-call", called_classes=lambda annotation: (AcmeMarker,))

        assert calling.called_classes(AcmeKindCall[AcmeMarker]) == (AcmeMarker,)
        assert ACME.called_classes(AcmeKindCall[AcmeMarker]) == ()

    @pytest.mark.parametrize(
        "marker",
        [
            Input(),
            Query(),
            LoadById(AcmeMarker),
            Load(AcmeMarker, from_param="acme_id", against="id"),
            Exists(AcmeMarker, from_param="acme_id", against="id"),
        ],
        ids=["Input", "Query", "LoadById", "Load", "Exists"],
    )
    def test_use_case_kinds_refuse_every_input_marker(self, marker: object) -> None:
        kinds = ExecutionKinds()
        declare_kinds(kinds)

        assert "input of execute" in (kinds.refusal_for(marker) or "")
        assert kinds.kind_for(marker, Any) is None

    def test_use_case_kinds_declare_the_caller_as_not_singleton_safe(self) -> None:
        kinds = ExecutionKinds()
        declare_kinds(kinds)

        assert kinds.kind_for(Caller(), Identity) is CALLER
        assert not CALLER.singleton_safe

    def test_ai_kinds_declare_the_agent_and_mcp_markers(self) -> None:
        kinds = ExecutionKinds()
        declare_kinds(kinds)

        assert kinds.kind_for(Agent("acme-triage"), Any) is AGENT
        assert kinds.kind_for(Mcp("acme-docs", include=["search"]), Any) is MCP

    async def test_the_executor_resolves_every_kind_in_one_loop_in_declaration_order(self) -> None:
        calls: list[str] = []
        compiler = UseCaseCompiler()
        plan = compiler.compile(AcmeMixedUseCase)
        executor = RuntimeExecutor(compiler)
        executor.bind_agent_resolver(AcmeNamedResolver(calls, "name"))
        executor.bind_mcp_resolver(AcmeNamedResolver(calls, "server"))

        result = await executor.execute(AcmeMixedUseCase(), identity=_ACME)

        assert [binding.kind for binding in plan.execution_bindings] == [MCP, AGENT]
        assert [binding.name for binding in plan.mcp_bindings] == ["docs"]
        assert [binding.agent for binding in plan.agent_bindings] == ["acme-triage"]
        assert calls == result == ["acme-docs", "acme-triage"]
