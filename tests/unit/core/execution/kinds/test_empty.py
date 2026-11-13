"""A plan with no execution dependency opens no execution and reads no registry."""

from __future__ import annotations

import pytest

from loom.core.engine import executor as executor_module
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.execution import ExecutionKinds, Kind
from loom.core.identity import Identity
from loom.core.use_case.use_case import UseCase

_ACME = Identity(subject="acme-alice", mechanism="test")


class AcmeCountingKinds(ExecutionKinds):
    __slots__ = ("reads",)

    def __init__(self) -> None:
        super().__init__()
        self.reads = 0

    def bound(self, kind: Kind) -> bool:
        self.reads += 1
        return super().bound(kind)

    def resolve(self, kind: Kind, declaration: object, annotation: object) -> object:
        self.reads += 1
        return super().resolve(kind, declaration, annotation)


class AcmePingUseCase(UseCase[object, str]):
    async def execute(self) -> str:
        return "pong"


@pytest.mark.parametrize("identity", [None, _ACME], ids=["no-identity", "identity"])
async def test_no_execution_binding_enters_no_execution_and_reads_no_registry(
    identity: Identity | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered: list[Identity | None] = []
    monkeypatch.setattr(executor_module, "enter", entered.append)
    kinds = AcmeCountingKinds()
    compiler = UseCaseCompiler()
    plan = compiler.compile(AcmePingUseCase)
    executor = RuntimeExecutor(compiler, resolvers=kinds)

    assert await executor.execute(AcmePingUseCase(), identity=identity) == "pong"
    assert plan.execution_bindings == ()
    assert kinds.reads == 0
    assert entered == []
