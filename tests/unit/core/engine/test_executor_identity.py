"""The executor fixes the identity an execution acts as, and refuses nestings that would leak it."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, nullcontext
from typing import Any, ClassVar, cast

import pytest

from loom.core.authz import Decision, Grant, Permission, Role, RoleCatalog, Scope
from loom.core.authz.elevation import ReadOnlyElevation, elevate_scope, elevated_scopes
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.errors import NestedElevation, NestedIdentityMismatch, UndeclaredStoreWrite
from loom.core.identity import (
    Identity,
    IdentityFixed,
    current_caller,
    current_identity,
    set_identity,
)
from loom.core.job.job import Job
from loom.core.stores.catalog import DEFAULT_STORE
from loom.core.stores.uow.coordinator import OrderedCommits
from loom.core.stores.uow.scope import invocation
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.invoker import AppInvoker
from loom.core.use_case.registry import UseCaseRegistry
from tests.helpers.delegation import acting_as
from tests.helpers.jobs import InlineServiceFixture
from tests.unit.core.authz._elevation_doubles import FakeSink
from tests.unit.core.engine._lifecycle_doubles import Body, Log, Run, StubUnitOfWorkFactory

ANA = Identity(subject="ana", mechanism="test")
BOB = Identity(subject="bob", mechanism="test")
WRITE = Permission("rows.write")
CATALOG = RoleCatalog((WRITE,), (Role("editor", (WRITE,)),))
BOUNDARY = Scope.of("b1")
DECISION = Decision(allowed=True, grant=Grant(subject="ana", role="editor", scope=BOUNDARY))
REFUSED_JOIN = "Child would join the open unit of work as another identity"


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
        return use_case_type() if issubclass(use_case_type, Job) else use_case_type(_whoami)


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
        UseCaseCompiler(),
        coordinator=OrderedCommits({DEFAULT_STORE: StubUnitOfWorkFactory(Log())}),
        elevation_sink=sink,
    )


@pytest.fixture
def ambient_bob() -> Iterator[None]:
    with acting_as(BOB):
        yield


def _child_call(executor: RuntimeExecutor, child: Run, identity: Identity | None) -> Body:
    async def call() -> Any:
        return await executor.execute(child, identity=identity)

    return call


def _invoking_as(executor: RuntimeExecutor, identity: Identity) -> Body:
    invoker = AppInvoker(
        cast(UseCaseFactory, _Factory()), executor, cast(UseCaseRegistry, None), identity=identity
    )

    async def call() -> object:
        return await invoker.invoke(Child, params=None, payload=None)

    return call


def _catching(call: Body) -> Parent:
    async def body() -> BaseException | None:
        return await _caught(call)

    return Parent(body)


async def _raise_runtime() -> None:
    raise RuntimeError("boom")


async def _raise_cancelled() -> None:
    raise asyncio.CancelledError


async def _raise_interrupted() -> None:
    raise Interrupted


def _elevating_then(call: Body) -> Parent:
    async def body() -> BaseException | None:
        await _elevate()
        return await _caught(call)

    return Parent(body)


class TestAmbientBinding:
    """The transport's caller acts again once an execution as another ends, however it ends."""

    @pytest.mark.usefixtures("ambient_bob")
    @pytest.mark.parametrize(
        ("body", "ends"),
        [
            (_whoami, nullcontext),
            (_raise_runtime, lambda: pytest.raises(RuntimeError)),
            (_raise_cancelled, lambda: pytest.raises(asyncio.CancelledError)),
            (_raise_interrupted, lambda: pytest.raises(Interrupted)),
        ],
        ids=["success", "exception", "cancellation", "interruption"],
    )
    async def test_the_transport_caller_is_back_when_the_execution_ends(
        self,
        executor: RuntimeExecutor,
        body: Body,
        ends: Callable[[], AbstractContextManager[object]],
    ) -> None:
        with ends():
            await executor.execute(Run(body), identity=ANA)

        assert current_caller() is BOB


class TestNestedIdentity:
    """A nested execution never joins a unit of work as another identity."""

    async def test_a_nested_execution_joining_as_the_same_identity_is_allowed(
        self,
        executor: RuntimeExecutor,
    ) -> None:
        parent = Parent(_child_call(executor, Child(_whoami), ANA))

        assert await executor.execute(parent, identity=ANA) == ANA

    @pytest.mark.parametrize("child_type", [Child, ReadChild], ids=["write", "read-only"])
    async def test_a_nested_execution_joining_as_another_identity_is_refused(
        self, executor: RuntimeExecutor, child_type: type[Run]
    ) -> None:
        call = _child_call(executor, child_type(_whoami), BOB)

        async def body() -> tuple[BaseException | None, Identity]:
            return await _caught(call), current_identity()

        refused, after = await executor.execute(Parent(body), identity=ANA)

        assert isinstance(refused, NestedIdentityMismatch)
        assert REFUSED_JOIN in refused.message
        assert after == ANA

    async def test_a_refused_nesting_is_logged_with_the_use_cases_only(
        self, executor: RuntimeExecutor, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The refusal names no identity; the switch before it is logged by subject (T09a)."""
        parent = _catching(_child_call(executor, Child(_whoami), BOB))

        await executor.execute(parent, identity=ANA)

        refusals = [
            record.getMessage() for record in caplog.records if REFUSED_JOIN in record.getMessage()
        ]
        assert refusals
        assert all(BOB.subject not in refusal for refusal in refusals)

    async def test_changing_the_ambient_identity_mid_execution_does_not_unlock_a_join(
        self,
        executor: RuntimeExecutor,
    ) -> None:
        """No way to join ana's unit of work as bob: neither a silent switch nor an explicit one.

        The silent switch (``set_identity`` inside the work) is refused outright,
        and changes nothing: the child given no identity joins as ana. The
        explicit switch to bob is refused at the join.
        """
        inheriting = _child_call(executor, Child(_whoami), None)
        as_bob = _child_call(executor, Child(_whoami), BOB)

        async def body() -> tuple[bool, Identity | None, object, BaseException | None]:
            try:
                set_identity(BOB)
            except IdentityFixed:
                silent_refused = True
            else:
                silent_refused = False
            return silent_refused, current_caller(), await inheriting(), await _caught(as_bob)

        silent_refused, caller, joined_as, refused = await executor.execute(
            Parent(body), identity=ANA
        )

        assert silent_refused
        assert caller is ANA
        assert joined_as is ANA
        assert isinstance(refused, NestedIdentityMismatch)

    async def test_invoking_as_another_identity_from_inside_a_use_case_is_refused(
        self,
        executor: RuntimeExecutor,
    ) -> None:
        refused = await executor.execute(_catching(_invoking_as(executor, BOB)), identity=ANA)

        assert isinstance(refused, NestedIdentityMismatch)

    async def test_joining_a_hand_opened_unit_of_work_as_another_identity_is_refused(
        self,
        executor: RuntimeExecutor,
    ) -> None:
        child = Child(_whoami)
        coordinator = OrderedCommits({DEFAULT_STORE: StubUnitOfWorkFactory(Log())})

        async with invocation(coordinator):
            with pytest.raises(NestedIdentityMismatch, match=REFUSED_JOIN):
                await executor.execute(child, identity=ANA)

    async def test_a_read_only_execution_may_run_a_read_only_one_as_another_identity(
        self,
        executor: RuntimeExecutor,
    ) -> None:
        """Its scope enlists no store, so the child's reads carry the child's identity."""
        parent = ReadParent(_child_call(executor, ReadChild(_whoami), BOB))

        assert await executor.execute(parent, identity=ANA) == BOB

    @pytest.mark.parametrize(
        "call",
        [
            lambda executor: _child_call(executor, Child(_whoami), ANA),
            lambda executor: _child_call(executor, Child(_whoami), BOB),
            lambda executor: _invoking_as(executor, BOB),
        ],
        ids=["same-identity", "another-identity", "invoker"],
    )
    async def test_a_writing_execution_under_a_read_only_one_is_refused_whatever_its_identity(
        self, executor: RuntimeExecutor, call: Callable[[RuntimeExecutor], Body]
    ) -> None:
        parent = ReadParent(call(executor))

        with pytest.raises(UndeclaredStoreWrite, match="the enclosing execution is read-only"):
            await executor.execute(parent, identity=ANA)

    async def test_a_per_call_read_from_a_gather_child_may_use_another_identity(
        self,
        executor: RuntimeExecutor,
    ) -> None:
        call = _child_call(executor, ReadChild(_whoami), BOB)

        async def body() -> list[Identity]:
            return list(await asyncio.gather(call()))

        assert await executor.execute(Parent(body), identity=ANA) == [BOB]


class TestNestedElevation:
    """No execution nests under one that elevated a scope."""

    async def test_a_nested_execution_after_elevating_is_refused(
        self, executor: RuntimeExecutor
    ) -> None:
        parent = _elevating_then(_child_call(executor, Child(_whoami), None))

        refused = await executor.execute(parent)

        assert isinstance(refused, NestedElevation)
        assert "Child cannot run inside an execution that elevated editor" in refused.message

    async def test_an_inline_job_run_after_elevating_is_refused(
        self, executor: RuntimeExecutor
    ) -> None:
        jobs = InlineServiceFixture(cast(UseCaseFactory, _Factory()), executor)

        async def call() -> Identity:
            return await jobs.run(WhoAmIJob)

        refused = await executor.execute(_elevating_then(call))

        assert isinstance(refused, NestedElevation)

    async def test_a_task_created_before_elevating_is_refused_a_nested_execution(
        self,
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

    async def test_a_nested_execution_may_elevate_itself_and_the_flag_ends_with_it(
        self, executor: RuntimeExecutor, sink: FakeSink
    ) -> None:
        async def elevating() -> frozenset[str]:
            await _elevate()
            return elevated_scopes()

        call = _child_call(executor, Child(elevating), None)

        async def body() -> tuple[frozenset[str], frozenset[str]]:
            return await call(), elevated_scopes()

        inner, after = await executor.execute(Parent(body))

        assert (inner, after, sink.cleared) == ({"editor"}, frozenset(), [{"editor"}])

    @pytest.mark.parametrize("nested", [True, False], ids=["nested", "top-level"])
    async def test_a_read_only_execution_cannot_elevate(
        self, executor: RuntimeExecutor, sink: FakeSink, nested: bool
    ) -> None:
        read_only = ReadChild(_elevate)
        run = Parent(_child_call(executor, read_only, None)) if nested else read_only

        with pytest.raises(ReadOnlyElevation, match="'editor'"):
            await executor.execute(run)

        assert sink.set == []
