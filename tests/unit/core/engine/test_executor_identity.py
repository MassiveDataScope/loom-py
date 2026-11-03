"""The executor installs the identity it was given, and refuses nestings that would leak it."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from typing import Any, ClassVar, cast

import pytest

from loom.ai._usecase import invoke_as
from loom.core.authz import Decision, Grant, Permission, Role, RoleCatalog, Scope
from loom.core.authz.elevation import elevate_scope, elevated_scopes
from loom.core.engine.compilable import Compilable
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.identity import Identity, current_identity, reset_identity, set_identity
from loom.core.job.job import Job
from loom.core.job.service import InlineJobService
from loom.core.persistence.stores import DEFAULT_STORE
from loom.core.uow.coordinator import OrderedCommits
from loom.core.uow.scope import NestedElevation, NestedIdentityMismatch, invocation
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.invoker import ApplicationInvoker
from tests.unit.core.authz._elevation_doubles import FakeSink
from tests.unit.core.engine._lifecycle_doubles import Body, Log, Run, StubUnitOfWorkFactory

ANA = Identity(subject="ana", mechanism="test")
BOB = Identity(subject="bob", mechanism="test")
WRITE = Permission("rows.write")
CATALOG = RoleCatalog((WRITE,), (Role("editor", (WRITE,)),))
BOUNDARY = Scope.of("b1")
DECISION = Decision(allowed=True, grant=Grant(subject="ana", role="editor", scope=BOUNDARY))
REFUSED_JOIN = "Child would join the unit of work of Parent"


class Parent(Run):
    pass


class ReadParent(Run):
    read_only: ClassVar[bool] = True


class Child(Run):
    pass


class ReadChild(Run):
    read_only: ClassVar[bool] = True


class Interrupted(BaseException):
    """Stands in for an interruption the executor must not swallow."""


class WhoAmIJob(Job[Identity]):
    async def execute(self) -> Identity:
        return current_identity()


class _Factory:
    def build(self, use_case_type: type[Any]) -> Any:
        return use_case_type()


class _Invoker:
    """An invoker bound to one identity, as the composition root hands one to an agent."""

    def __init__(self, executor: RuntimeExecutor, identity: Identity) -> None:
        self._executor = executor
        self._identity = identity

    async def invoke(self, use_case: type[Compilable], **_: Any) -> Any:
        return await self._executor.execute(Child(_whoami), identity=self._identity)


async def _elevate() -> None:
    await elevate_scope("editor", DECISION, at=BOUNDARY, permission=WRITE, catalog=CATALOG)


async def _whoami() -> Identity:
    return current_identity()


async def _caught(call: Body) -> BaseException | None:
    try:
        await call()
    except (NestedElevation, NestedIdentityMismatch) as exc:
        return exc
    return None


@pytest.fixture
def sink() -> FakeSink:
    return FakeSink(open=True)


@pytest.fixture
def executor(sink: FakeSink) -> RuntimeExecutor:
    return RuntimeExecutor(
        UseCaseCompiler(), uow_factory=StubUnitOfWorkFactory(Log()), elevation_sink=sink
    )


@pytest.fixture
def ambient_bob() -> Iterator[None]:
    token = set_identity(BOB)
    try:
        yield
    finally:
        reset_identity(token)


def _child_call(executor: RuntimeExecutor, child: Run, identity: Identity | None) -> Body:
    async def call() -> Any:
        return await executor.execute(child, identity=identity)

    return call


def _catching(call: Body) -> Parent:
    async def body() -> BaseException | None:
        return await _caught(call)

    return Parent(body)


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
    parent = Parent(_child_call(executor, Child(_whoami), ANA))

    assert await executor.execute(parent, identity=ANA) == ANA


@pytest.mark.parametrize("child_type", [Child, ReadChild], ids=["write", "read-only"])
async def test_a_nested_execution_joining_as_another_identity_is_refused(
    executor: RuntimeExecutor, child_type: type[Run]
) -> None:
    parent = Parent(_child_call(executor, child_type(_whoami), BOB))

    with pytest.raises(NestedIdentityMismatch, match=REFUSED_JOIN):
        await executor.execute(parent, identity=ANA)


async def test_a_refused_nesting_is_logged_with_the_use_cases_only(
    executor: RuntimeExecutor, caplog: pytest.LogCaptureFixture
) -> None:
    parent = _catching(_child_call(executor, Child(_whoami), BOB))

    await executor.execute(parent, identity=ANA)

    assert REFUSED_JOIN in caplog.text
    assert BOB.subject not in caplog.text


@pytest.mark.parametrize("identity", [BOB, None], ids=["given", "ambient"])
async def test_changing_the_ambient_identity_mid_execution_does_not_unlock_a_join(
    executor: RuntimeExecutor, identity: Identity | None
) -> None:
    call = _child_call(executor, Child(_whoami), identity)

    async def body() -> BaseException | None:
        token = set_identity(BOB)
        try:
            return await _caught(call)
        finally:
            reset_identity(token)

    refused = await executor.execute(Parent(body), identity=ANA)

    assert isinstance(refused, NestedIdentityMismatch)


async def test_invoking_as_another_identity_from_inside_a_use_case_is_refused(
    executor: RuntimeExecutor,
) -> None:
    invoker = cast(ApplicationInvoker, _Invoker(executor, BOB))

    async def call() -> object:
        return await invoke_as(invoker, Child, BOB, params=None, payload=None)

    refused = await executor.execute(_catching(call), identity=ANA)

    assert isinstance(refused, NestedIdentityMismatch)


async def test_a_refused_nesting_leaves_the_parent_identity_in_place(
    executor: RuntimeExecutor,
) -> None:
    call = _child_call(executor, Child(_whoami), BOB)

    async def body() -> tuple[BaseException | None, Identity]:
        return await _caught(call), current_identity()

    refused, after = await executor.execute(Parent(body), identity=ANA)

    assert (type(refused), after) == (NestedIdentityMismatch, ANA)


async def test_joining_a_hand_opened_unit_of_work_as_another_identity_is_refused(
    executor: RuntimeExecutor,
) -> None:
    child = Child(_whoami)
    coordinator = OrderedCommits({DEFAULT_STORE: StubUnitOfWorkFactory(Log())})

    async with invocation(coordinator):
        with pytest.raises(NestedIdentityMismatch, match="the enclosing invocation"):
            await executor.execute(child, identity=ANA)


async def test_a_nested_execution_opening_its_own_unit_of_work_may_use_another_identity(
    executor: RuntimeExecutor,
) -> None:
    parent = ReadParent(_child_call(executor, Child(_whoami), BOB))

    assert await executor.execute(parent, identity=ANA) == BOB


async def test_a_per_call_read_from_a_gather_child_may_use_another_identity(
    executor: RuntimeExecutor,
) -> None:
    call = _child_call(executor, ReadChild(_whoami), BOB)

    async def body() -> list[Identity]:
        return list(await asyncio.gather(call()))

    assert await executor.execute(Parent(body), identity=ANA) == [BOB]


# --------------------------------------------------------------- nested elevation


def _elevating_then(call: Body) -> Parent:
    async def body() -> BaseException | None:
        await _elevate()
        return await _caught(call)

    return Parent(body)


async def test_a_nested_execution_after_elevating_is_refused(executor: RuntimeExecutor) -> None:
    parent = _elevating_then(_child_call(executor, Child(_whoami), None))

    refused = await executor.execute(parent)

    assert isinstance(refused, NestedElevation)
    assert "Child cannot run inside Parent, which elevated editor" in refused.message


async def test_an_inline_job_run_after_elevating_is_refused(executor: RuntimeExecutor) -> None:
    jobs = InlineJobService(cast(UseCaseFactory, _Factory()), executor)

    async def call() -> Identity:
        return await jobs.run(WhoAmIJob)

    refused = await executor.execute(_elevating_then(call))

    assert isinstance(refused, NestedElevation)


async def test_a_task_created_before_elevating_is_refused_a_nested_execution(
    executor: RuntimeExecutor,
) -> None:
    call = _child_call(executor, ReadChild(_whoami), None)
    elevated = asyncio.Event()

    async def later() -> BaseException | None:
        await elevated.wait()
        return await _caught(call)

    async def body() -> BaseException | None:
        task = asyncio.create_task(later())
        await _elevate()
        elevated.set()
        return await task

    refused = await executor.execute(Parent(body))

    assert isinstance(refused, NestedElevation)


@pytest.mark.parametrize("child_type", [Child, ReadChild], ids=["write", "read-only"])
async def test_a_nested_execution_may_elevate_itself_and_the_flag_ends_with_it(
    executor: RuntimeExecutor, sink: FakeSink, child_type: type[Run]
) -> None:
    async def elevating() -> frozenset[str]:
        await _elevate()
        return elevated_scopes()

    call = _child_call(executor, child_type(elevating), None)

    async def body() -> tuple[frozenset[str], frozenset[str]]:
        return await call(), elevated_scopes()

    inner, after = await executor.execute(Parent(body))

    assert (inner, after, sink.cleared) == ({"editor"}, frozenset(), [{"editor"}])
