"""The executor installs the identity it was given, and refuses nestings that would leak it."""

from __future__ import annotations

import asyncio
import pickle
from collections.abc import Iterator
from typing import Any, ClassVar

import pytest

from loom.core.authz import Decision, Grant, Permission, Role, RoleCatalog, Scope
from loom.core.authz.elevation import elevate_scope, elevated_scopes
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.errors.codes import ErrorCode
from loom.core.identity import Identity, current_identity, reset_identity, set_identity
from loom.core.uow.scope import NestedElevation, NestedIdentityMismatch
from tests.unit.core.authz._elevation_doubles import FakeSink
from tests.unit.core.engine._lifecycle_doubles import Body, Log, Run, StubUnitOfWorkFactory

Built = tuple[NestedIdentityMismatch | NestedElevation, ErrorCode]
ANA = Identity(subject="ana", mechanism="test")
BOB = Identity(subject="bob", mechanism="test")
WRITE = Permission("rows.write")
CATALOG = RoleCatalog((WRITE,), (Role("editor", (WRITE,)),))
BOUNDARY = Scope.of("b1")
DECISION = Decision(allowed=True, grant=Grant(subject="ana", role="editor", scope=BOUNDARY))


class ReadRun(Run):
    read_only: ClassVar[bool] = True


class Interrupted(BaseException):
    """Stands in for an interruption the executor must not swallow."""


async def _elevate() -> None:
    await elevate_scope("editor", DECISION, at=BOUNDARY, permission=WRITE, catalog=CATALOG)


async def _whoami() -> Identity:
    return current_identity()


@pytest.fixture
def executor() -> RuntimeExecutor:
    return RuntimeExecutor(
        UseCaseCompiler(), uow_factory=StubUnitOfWorkFactory(Log()), elevation_sink=FakeSink()
    )


@pytest.fixture
def ambient_bob() -> Iterator[None]:
    token = set_identity(BOB)
    try:
        yield
    finally:
        reset_identity(token)


def _nesting(executor: RuntimeExecutor, child: Run, identity: Identity | None) -> Run:
    async def body() -> Any:
        return await executor.execute(child, identity=identity)

    return Run(body)


# --------------------------------------------------------------- ambient binding


@pytest.mark.usefixtures("ambient_bob")
async def test_an_execution_sees_its_identity_as_the_ambient_one(executor: RuntimeExecutor) -> None:
    assert await executor.execute(Run(_whoami), identity=ANA) == ANA


@pytest.mark.usefixtures("ambient_bob")
async def test_an_execution_given_no_identity_keeps_the_ambient_one(
    executor: RuntimeExecutor,
) -> None:
    assert await executor.execute(Run(_whoami)) == BOB


@pytest.mark.usefixtures("ambient_bob")
async def test_gather_children_see_the_bound_identity(executor: RuntimeExecutor) -> None:
    async def body() -> list[Identity]:
        return list(await asyncio.gather(_whoami(), _whoami()))

    assert await executor.execute(Run(body), identity=ANA) == [ANA, ANA]


async def _raise_runtime() -> None:
    raise RuntimeError("boom")


async def _raise_cancelled() -> None:
    raise asyncio.CancelledError


async def _raise_interrupted() -> None:
    raise Interrupted


@pytest.mark.usefixtures("ambient_bob")
@pytest.mark.parametrize(
    ("body", "raised"),
    [
        (_raise_runtime, RuntimeError),
        (_raise_cancelled, asyncio.CancelledError),
        (_raise_interrupted, Interrupted),
    ],
    ids=["exception", "cancellation", "interruption"],
)
async def test_the_ambient_identity_is_restored_when_the_execution_fails(
    executor: RuntimeExecutor, body: Body, raised: type[BaseException]
) -> None:
    use_case = Run(body)
    with pytest.raises(raised):
        await executor.execute(use_case, identity=ANA)

    assert await executor.execute(Run(_whoami)) == BOB


# --------------------------------------------------------------- nested identity


async def test_a_nested_execution_joining_as_the_same_identity_is_allowed(
    executor: RuntimeExecutor,
) -> None:
    parent = _nesting(executor, Run(_whoami), ANA)

    assert await executor.execute(parent, identity=ANA) == ANA


@pytest.mark.parametrize("child_type", [Run, ReadRun], ids=["write", "read-only"])
async def test_a_nested_execution_joining_as_another_identity_is_refused(
    executor: RuntimeExecutor, child_type: type[Run]
) -> None:
    parent = _nesting(executor, child_type(_whoami), BOB)

    with pytest.raises(NestedIdentityMismatch, match="Run would join the unit of work of Run"):
        await executor.execute(parent, identity=ANA)


async def test_a_refused_nesting_leaves_the_parent_identity_in_place(
    executor: RuntimeExecutor,
) -> None:
    child = _nesting(executor, Run(_whoami), BOB)

    async def body() -> Identity:
        with pytest.raises(NestedIdentityMismatch):
            await child.execute()
        return current_identity()

    assert await executor.execute(Run(body), identity=ANA) == ANA


async def test_a_nested_execution_opening_its_own_unit_of_work_may_use_another_identity(
    executor: RuntimeExecutor,
) -> None:
    parent = ReadRun(_nesting(executor, Run(_whoami), BOB).execute)

    assert await executor.execute(parent, identity=ANA) == BOB


async def test_a_per_call_read_from_a_gather_child_may_use_another_identity(
    executor: RuntimeExecutor,
) -> None:
    nested = _nesting(executor, ReadRun(_whoami), BOB)

    async def body() -> list[Identity]:
        return list(await asyncio.gather(nested.execute()))

    assert await executor.execute(Run(body), identity=ANA) == [BOB]


# --------------------------------------------------------------- nested elevation


async def test_a_nested_execution_after_elevating_is_refused(executor: RuntimeExecutor) -> None:
    child = _nesting(executor, Run(_whoami), None)

    async def body() -> Any:
        await _elevate()
        return await child.execute()

    with pytest.raises(NestedElevation, match="Run cannot run inside Run, which elevated editor"):
        await executor.execute(Run(body))


async def test_a_nested_execution_may_elevate_itself(executor: RuntimeExecutor) -> None:
    async def elevating() -> frozenset[str]:
        await _elevate()
        return elevated_scopes()

    async def body() -> tuple[frozenset[str], frozenset[str]]:
        inner = await executor.execute(Run(elevating))
        return inner, elevated_scopes()

    assert await executor.execute(Run(body)) == (frozenset({"editor"}), frozenset())


# --------------------------------------------------------------- errors


@pytest.fixture(
    params=[
        (NestedIdentityMismatch.joining("Child", "Parent"), ErrorCode.NESTED_IDENTITY_MISMATCH),
        (
            NestedElevation.opening("Child", "Parent", frozenset({"b", "a"})),
            ErrorCode.NESTED_ELEVATION,
        ),
    ],
    ids=["identity", "elevation"],
)
def built(request: pytest.FixtureRequest) -> Built:
    built: Built = request.param
    return built


def test_each_error_carries_its_code(built: Built) -> None:
    error, code = built

    assert (error.code, error.retryable) == (code, False)


def test_each_error_names_both_use_cases(built: Built) -> None:
    error, _ = built

    assert "Child" in error.message
    assert "Parent" in error.message


def test_each_error_pickles_to_its_type_and_message(built: Built) -> None:
    error, _ = built

    restored = pickle.loads(pickle.dumps(error))

    assert (type(restored), str(restored)) == (type(error), str(error))


def test_the_identity_error_tells_what_to_do_instead() -> None:
    error = NestedIdentityMismatch.joining("Child", "Parent")

    assert "same identity" in error.message and "dispatch a job" in error.message


def test_the_elevation_error_names_the_scopes_in_order_and_what_to_do_instead() -> None:
    error = NestedElevation.opening("Child", "Parent", frozenset({"b", "a"}))

    assert "elevated a, b; elevate inside the nested use case" in error.message
