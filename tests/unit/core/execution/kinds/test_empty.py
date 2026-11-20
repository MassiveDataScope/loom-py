"""A plan with no execution dependency reads no registry; its one execution is the executor's."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import UseCaseExecutor
from loom.core.execution import ExecutionContext, ExecutionKinds, Kind
from loom.core.identity import Identity
from loom.core.use_case.use_case import UseCase

_ACME = Identity(subject="acme-alice", mechanism="test")


class AcmeCountingKinds(ExecutionKinds):
    __slots__ = ("reads",)

    def __init__(self) -> None:
        super().__init__()
        self.reads = 0

    def is_bound(self, kind: Kind) -> bool:
        self.reads += 1
        return super().is_bound(kind)

    def resolve(self, kind: Kind, declaration: object, annotation: object) -> object:
        self.reads += 1
        return super().resolve(kind, declaration, annotation)


class AcmePongUseCase(UseCase[object, str]):
    async def execute(self) -> str:
        return "pong"


class TestAnExecutionFreePlan:
    @pytest.mark.parametrize("identity", [None, _ACME], ids=["no-identity", "identity"])
    async def test_no_execution_binding_enters_one_execution_and_reads_no_registry(
        self, identity: Identity | None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        entered: list[Identity | None] = []

        original = ExecutionContext.entered

        def counting(given: Identity | None, *args: Any) -> ExecutionContext:
            entered.append(given)
            return original(given, *args)

        monkeypatch.setattr(ExecutionContext, "entered", counting)
        kinds = AcmeCountingKinds()
        compiler = UseCaseCompiler()
        plan = compiler.compile(AcmePongUseCase)
        executor = UseCaseExecutor(compiler, resolvers=kinds)

        assert await executor.execute(AcmePongUseCase(), identity=identity) == "pong"
        assert plan.execution_bindings == ()
        assert kinds.reads == 0
        assert entered == [identity]
