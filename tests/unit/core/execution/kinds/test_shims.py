"""The legacy ``bind_*`` resolvers bound to the ``AGENT`` and ``MCP`` kinds."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.kinds import AGENT, MCP
from loom.core.execution import ExecutionKinds, ExecutionRequired, ExecutionScope
from loom.core.identity import Identity
from loom.core.use_case.markers import Agent, Mcp
from loom.core.use_case.use_case import UseCase

_ACME = Identity(subject="acme-alice", mechanism="test")


class AcmeTriageUseCase(UseCase[object, object]):
    async def execute(self, triage: Any = Agent("acme-triage")) -> object:
        return triage


class AcmeCallTimeResolver:
    """Implements both shapes, as the AI runtime's resolver will (T12)."""

    def __call__(self, name: str, identity: Identity) -> object:
        raise AssertionError("the call-time form must be bound, not the legacy call")

    def resolve(self, declaration: object, annotation: object) -> object:
        return ("call-time", declaration)


class TestLegacyResolverBinding:
    async def test_a_call_time_resolver_is_bound_as_is_and_kept_as_given(self) -> None:
        resolver = AcmeCallTimeResolver()
        compiler = UseCaseCompiler()
        marker = compiler.compile(AcmeTriageUseCase).execution_bindings[0].declaration
        executor = RuntimeExecutor(compiler)
        executor.bind_agent_resolver(resolver)

        assert await executor.execute(AcmeTriageUseCase(), identity=_ACME) == ("call-time", marker)
        assert executor._agent_resolver is resolver  # noqa: SLF001 - the legacy shape

    def test_a_plain_callable_reads_the_execution_identity_at_resolve_time(self) -> None:
        kinds = ExecutionKinds()
        calls: list[tuple[str, tuple[str, ...], Identity]] = []

        def legacy(server: str, include: tuple[str, ...], identity: Identity) -> str:
            calls.append((server, include, identity))
            return server

        RuntimeExecutor(UseCaseCompiler(), resolvers=kinds, mcp_resolver=legacy)
        marker = Mcp("acme-docs", include=["search"])

        with pytest.raises(ExecutionRequired):
            kinds.resolve(MCP, marker, Any)
        with ExecutionScope(_ACME):
            assert kinds.resolve(MCP, marker, Any) == "acme-docs"
        assert calls == [("acme-docs", ("search",), _ACME)]

    def test_the_constructor_kwargs_bind_write_once_like_the_methods(self) -> None:
        kinds = ExecutionKinds()
        executor = RuntimeExecutor(
            UseCaseCompiler(), resolvers=kinds, agent_resolver=lambda name, identity: name
        )

        assert kinds.is_bound(AGENT)
        assert not kinds.is_bound(MCP)
        with pytest.raises(RuntimeError, match="more than once"):
            executor.bind_agent_resolver(lambda name, identity: name)
