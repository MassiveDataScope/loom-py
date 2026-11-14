"""The effective identity is computed once and reaches the inputs, the ambient and the context.

Explicit, else the enclosing execution's, else ``None`` (NS2). For each case the
test reads the three places: ``Caller()`` (the inputs), ``current_identity()``
(``set_identity``) and ``current_execution().identity`` (the context).
"""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.errors import NestedIdentityMismatch, Unauthenticated
from loom.core.execution import ExecutionScope, current_execution
from loom.core.identity import ANONYMOUS, Identity, current_identity, reset_identity, set_identity
from loom.core.use_case.markers import Caller
from loom.core.use_case.use_case import UseCase
from tests.unit.core.engine.construction import acme

Seen = tuple[Identity | None, Identity, Identity | None]


def _seen(caller: Identity | None) -> Seen:
    context = current_execution()
    assert context is not None
    return caller, current_identity(), context.identity


class AcmeWho(UseCase[Any, Seen]):
    async def execute(self, caller: Identity = Caller()) -> Seen:
        return _seen(caller)


class AcmeAnyone(UseCase[Any, Seen]):
    async def execute(self) -> Seen:
        return _seen(None)


class AcmeNests(UseCase[Any, Seen]):
    """Runs ``inner`` nested, as ``identity`` (``None`` to inherit)."""

    def __init__(self, executor: RuntimeExecutor, inner: UseCase[Any, Seen], identity: Any) -> None:
        self.executor, self.inner, self.identity = executor, inner, identity

    async def execute(self) -> Seen:
        seen: Seen = await self.executor.execute(self.inner, identity=self.identity)
        return seen


@pytest.fixture
def executor() -> RuntimeExecutor:
    return RuntimeExecutor(UseCaseCompiler())


async def test_an_explicit_identity_reaches_all_three(executor: RuntimeExecutor) -> None:
    assert await executor.execute(AcmeWho(), identity=acme.ALICE) == (acme.ALICE,) * 3


async def test_an_explicit_identity_wins_over_the_enclosing_one(executor: RuntimeExecutor) -> None:
    outer = AcmeNests(executor, AcmeWho(), acme.BOB)

    assert await executor.execute(outer, identity=acme.ALICE) == (acme.BOB,) * 3


async def test_a_nested_execution_inherits_the_enclosing_identity(
    executor: RuntimeExecutor,
) -> None:
    outer = AcmeNests(executor, AcmeWho(), None)

    assert await executor.execute(outer, identity=acme.ALICE) == (acme.ALICE,) * 3


async def test_no_identity_and_no_enclosing_execution_runs_with_none(
    executor: RuntimeExecutor,
) -> None:
    assert await executor.execute(AcmeAnyone()) == (None, ANONYMOUS, None)
    with pytest.raises(Unauthenticated, match="carried no identity"):
        await executor.execute(AcmeWho())


async def test_a_nested_execution_under_one_with_no_caller_has_none(
    executor: RuntimeExecutor,
) -> None:
    outer = AcmeNests(executor, AcmeAnyone(), None)

    assert await executor.execute(outer) == (None, ANONYMOUS, None)


class AcmeTampers(AcmeNests):
    """Changes the ambient identity, then runs ``inner`` nested with no identity."""

    async def execute(self) -> Seen:
        token = set_identity(acme.BOB)
        try:
            return await super().execute()
        finally:
            reset_identity(token)


async def test_an_ambient_identity_changed_mid_execution_refuses_an_inheriting_child(
    executor: RuntimeExecutor,
) -> None:
    outer = AcmeTampers(executor, AcmeWho(), None)

    with pytest.raises(NestedIdentityMismatch, match="AcmeWho was given no identity"):
        await executor.execute(outer, identity=acme.ALICE)


async def test_an_execution_scope_opened_without_the_ambient_identity_is_named_as_a_cause(
    executor: RuntimeExecutor,
) -> None:
    with (
        pytest.raises(NestedIdentityMismatch, match="opened .* without set_identity"),
        ExecutionScope(acme.ALICE),
    ):
        await executor.execute(AcmeWho())
