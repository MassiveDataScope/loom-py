"""Post-commit actions belong to the store that owns them (ADR 0008, T049, D4).

Once the invocation ended, each store's actions run by that store's outcome:
a store that committed, or wrote without a transaction, runs every lane; an
``Uncertain`` one only its priority lane; a rolled-back one none. The
executor owns an invocation of two SQLite stores, plus a store without
transactions, the way the composition will once several stores compose
(PR 11). A store's commit fails where a test injects it.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, field
from typing import Any, ClassVar, cast

import pytest

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.cache.abc.config import CacheConfig
from loom.core.cache.dependency import GenerationalDependencyResolver
from loom.core.cache.repository import CachedRepository
from loom.core.command import Command
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.post_commit import store_channel
from loom.core.job.context import add_pending_dispatch, add_pending_publish
from loom.core.model import BaseModel, ColumnField
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey
from loom.core.repository.abc import CursorCodec, RepoFor
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.transactional import transactional
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWork
from loom.core.uow.abc import UnitOfWork
from loom.core.uow.coordinator import CommitUncertain, OrderedCommits
from loom.core.uow.scope import StoresNotCommitted, current_scope
from loom.core.use_case import Joined, UseCase
from tests.unit.core.cache._doubles import CountingCacheBackend

pytestmark = pytest.mark.timeout(30)

AUDIT = StoreKey("audit")
EVENTS = StoreKey("events")

Step = Callable[[], Awaitable[None]]


class _Order(BaseModel):
    __tablename__ = "owned_orders"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    ref: str = ColumnField(length=16)


class _AuditEntry(BaseModel):
    __tablename__ = "owned_audit_entries"
    __store__ = "audit"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    note: str = ColumnField(length=16)


class _Event(BaseModel):
    __tablename__ = "owned_events"
    __store__ = "events"

    id: int = ColumnField(primary_key=True, autoincrement=True)


class _NewOrder(Command, frozen=True):
    ref: str


class _NewEntry(Command, frozen=True):
    note: str


class _CommitRefused(Exception):
    """A commit the store answered with a defined failure."""


class _UseCaseFailed(Exception):
    pass


class _CommitFails(SQLAlchemyUnitOfWork):
    """A SQLite unit of work whose commit raises ``error`` instead of committing."""

    def __init__(self, manager: SessionManager, error: BaseException) -> None:
        super().__init__(manager)
        self._error = error

    async def commit(self) -> None:
        raise self._error


class _Autocommit:
    """A protocol-only unit of work of a store without transactions."""

    transactional: ClassVar[bool] = False

    async def begin(self) -> None:
        return None

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None

    async def __aenter__(self) -> _Autocommit:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        return None


@dataclass
class _Factory:
    """Builds a store's unit of work; its commit raises ``failures[store]`` when set."""

    manager: SessionManager
    failures: dict[StoreKey, BaseException]

    def create(self) -> UnitOfWork:
        error = self.failures.get(self.manager.store)
        if error is None:
            return SQLAlchemyUnitOfWork(self.manager)
        return _CommitFails(self.manager, error)


class _AutocommitFactory:
    def create(self) -> UnitOfWork:
        return _Autocommit()


@dataclass
class _Ran:
    """Labels of the actions that ran, in order; :meth:`seen` waits for one."""

    labels: list[str] = field(default_factory=list)
    _waiting: dict[str, asyncio.Event] = field(default_factory=dict)

    def action(self, label: str) -> Callable[[], None]:
        def run() -> None:
            self.labels.append(label)
            self.seen(label).set()

        return run

    def seen(self, label: str) -> asyncio.Event:
        return self._waiting.setdefault(label, asyncio.Event())

    def queue(self, store: str) -> None:
        """Queue one action per lane for the running execution's store."""
        channel = store_channel()
        assert channel is not None
        channel.enqueue_priority(self.action(f"{store}.priority"))
        add_pending_publish(self.action(f"{store}.publish"))
        add_pending_dispatch(self.action(f"{store}.plain"))


def _all_lanes(*stores: str) -> list[str]:
    """Every lane of ``stores``: priority lanes first, then publish, then plain."""
    return [f"{store}.{lane}" for lane in ("priority", "publish", "plain") for store in stores]


class _Invocation(UseCase[_Order, None]):
    """Primary store ``default``: writes an order, then runs ``steps``."""

    def __init__(self, orders: RepoFor[_Order], steps: tuple[Step, ...]) -> None:
        self.orders = orders
        self.steps = steps

    async def execute(self) -> None:
        await self.orders.create(_NewOrder(ref="o1"))
        for step in self.steps:
            await step()


class _QueueOnAudit(UseCase[_AuditEntry, None]):
    """Primary store ``audit``: writes an entry and queues audit's actions."""

    def __init__(self, entries: RepoFor[_AuditEntry], ran: _Ran) -> None:
        self.entries = entries
        self.ran = ran

    async def execute(self) -> None:
        await self.entries.create(_NewEntry(note="nested"))
        self.ran.queue("audit")


class _QueueOnEvents(UseCase[_Event, None]):
    """Primary store ``events``, which has no transactions: queues its actions."""

    def __init__(self, ran: _Ran) -> None:
        self.ran = ran

    async def execute(self) -> None:
        self.ran.queue("events")


class _DispatchReadOnly(UseCase[_Order, None]):
    """Run read-only: its frame names no store, so its dispatch is the enclosing store's."""

    def __init__(self, ran: _Ran) -> None:
        self.ran = ran

    async def execute(self) -> None:
        add_pending_dispatch(self.ran.action("read-only.plain"))


class _AuditFirst(UseCase[_Order, None]):
    """Primary store ``default``; writes to the joined audit store first."""

    def __init__(self, orders: RepoFor[_Order], entries: Joined[RepoFor[_AuditEntry]]) -> None:
        self.orders = orders
        self.entries = entries

    async def execute(self) -> None:
        await self.entries.create(_NewEntry(note="joined"))
        await self.orders.create(_NewOrder(ref="o1"))


def _manager(store: StoreKey) -> SessionManager:
    return SessionManager(
        "sqlite+aiosqlite:///:memory:",
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        store=store,
    )


@dataclass
class _Stores:
    """Two SQLite stores, a store without transactions, and an executor owning all three."""

    default: SessionManager
    audit: SessionManager
    orders: RepositorySQLAlchemy[_Order, int]
    entries: RepositorySQLAlchemy[_AuditEntry, int]
    ran: _Ran = field(default_factory=_Ran)
    failures: dict[StoreKey, BaseException] = field(default_factory=dict)
    executor: RuntimeExecutor = field(default_factory=lambda: RuntimeExecutor(UseCaseCompiler()))

    def __post_init__(self) -> None:
        # Until the composition builds it (PR 11), one coordinator per store set.
        self.executor._coordinator = OrderedCommits(
            {
                DEFAULT_STORE: _Factory(self.default, self.failures),
                AUDIT: _Factory(self.audit, self.failures),
                EVENTS: _AutocommitFactory(),
            }
        )

    async def run(self, *steps: Step) -> None:
        """Execute :class:`_Invocation`, which writes to ``default`` and then runs ``steps``."""
        await self.executor.execute(_Invocation(cast("RepoFor[_Order]", self.orders), steps))

    def queue(self, store: str = "default") -> Step:
        async def step() -> None:
            self.ran.queue(store)

        return step

    def nested(self, use_case: UseCase[Any, None], *, read_only: bool = False) -> Step:
        async def step() -> None:
            await self.executor.execute(use_case, read_only=read_only)

        return step

    def on_audit(self) -> Step:
        return self.nested(_QueueOnAudit(cast("RepoFor[_AuditEntry]", self.entries), self.ran))

    def on_events(self) -> Step:
        return self.nested(_QueueOnEvents(self.ran))

    def cached_entries(self, backend: CountingCacheBackend) -> RepoFor[_AuditEntry]:
        cached = CachedRepository(
            self.entries,
            config=CacheConfig(),
            cache=backend,
            dependency_resolver=GenerationalDependencyResolver(backend),
        )
        return cast("RepoFor[_AuditEntry]", cached)


@pytest.fixture
async def stores() -> AsyncIterator[_Stores]:
    compile_all(_Order, _AuditEntry)
    default, audit = _manager(DEFAULT_STORE), _manager(AUDIT)
    codec = CursorCodec(signing="ephemeral")
    for model, manager in ((_Order, default), (_AuditEntry, audit)):
        compiled = get_compiled(model)
        assert compiled is not None
        async with manager.engine.begin() as connection:
            await connection.run_sync(compiled.metadata.create_all)
    try:
        yield _Stores(
            default=default,
            audit=audit,
            orders=RepositorySQLAlchemy(default, _Order, cursor_codec=codec),
            entries=RepositorySQLAlchemy(audit, _AuditEntry, cursor_codec=codec),
        )
    finally:
        await default.dispose()
        await audit.dispose()


async def _fail() -> None:
    raise _UseCaseFailed


async def _poison_default() -> None:
    scope = current_scope()
    assert scope is not None
    scope.participants[DEFAULT_STORE].poison(_UseCaseFailed())


def _raises(error: type[BaseException] | None) -> AbstractContextManager[object]:
    return nullcontext() if error is None else pytest.raises(error)


class TestTwoTransactionalStores:
    @pytest.mark.parametrize(
        ("failures", "ran", "error"),
        [
            pytest.param({}, _all_lanes("default", "audit"), None, id="both-commit"),
            pytest.param(
                {AUDIT: _CommitRefused()},
                _all_lanes("default"),
                StoresNotCommitted,
                id="audit-rolls-back-after-default-committed",
            ),
            pytest.param(
                {AUDIT: CommitUncertain(ConnectionError("ack lost"))},
                ["default.priority", "audit.priority", "default.publish", "default.plain"],
                StoresNotCommitted,
                id="audit-uncertain-runs-its-priority-lane-only",
            ),
            pytest.param(
                {DEFAULT_STORE: _CommitRefused()}, [], _CommitRefused, id="default-rolls-back-first"
            ),
        ],
    )
    async def test_each_store_runs_what_its_outcome_lets_run(
        self,
        stores: _Stores,
        failures: dict[StoreKey, BaseException],
        ran: list[str],
        error: type[BaseException] | None,
    ) -> None:
        stores.failures.update(failures)
        steps = (stores.queue(), stores.on_audit())

        with _raises(error):
            await stores.run(*steps)

        assert stores.ran.labels == ran

    async def test_a_failed_use_case_runs_no_store_actions(self, stores: _Stores) -> None:
        steps = (stores.queue(), stores.on_audit(), _fail)

        with pytest.raises(_UseCaseFailed):
            await stores.run(*steps)

        assert stores.ran.labels == []

    async def test_a_read_only_nested_execution_queues_for_the_store_around_it(
        self, stores: _Stores
    ) -> None:
        stores.failures[AUDIT] = _CommitRefused()
        read_only = stores.nested(_DispatchReadOnly(stores.ran), read_only=True)
        steps = (read_only, stores.on_audit())

        with pytest.raises(StoresNotCommitted):
            await stores.run(*steps)

        assert stores.ran.labels == ["read-only.plain"]

    @pytest.mark.parametrize(
        ("failing", "bumped", "error"),
        [
            pytest.param(DEFAULT_STORE, True, StoresNotCommitted, id="audit-committed"),
            pytest.param(AUDIT, False, _CommitRefused, id="audit-rolled-back"),
        ],
    )
    async def test_a_cache_invalidation_follows_the_store_of_its_repository(
        self, stores: _Stores, failing: StoreKey, bumped: bool, error: type[BaseException]
    ) -> None:
        backend = CountingCacheBackend()
        stores.failures[failing] = _CommitRefused()
        use_case = _AuditFirst(
            cast("RepoFor[_Order]", stores.orders), stores.cached_entries(backend)
        )

        with pytest.raises(error):
            await stores.executor.execute(use_case)

        assert bool(backend.incr_keys) is bumped

    @pytest.mark.parametrize(
        ("failures", "ran", "error"),
        [
            pytest.param({}, ["gathered"], None, id="every-store-committed"),
            pytest.param(
                {AUDIT: _CommitRefused()}, [], StoresNotCommitted, id="a-store-did-not-commit"
            ),
        ],
    )
    async def test_an_action_no_store_holds_runs_only_if_every_store_committed(
        self,
        stores: _Stores,
        failures: dict[StoreKey, BaseException],
        ran: list[str],
        error: type[BaseException] | None,
    ) -> None:
        stores.failures.update(failures)

        async def gathered() -> None:  # a gather child does not own the scope
            add_pending_dispatch(stores.ran.action("gathered"))

        async def in_a_gather() -> None:
            await asyncio.gather(gathered())

        steps = (in_a_gather, stores.on_audit())

        with _raises(error):
            await stores.run(*steps)

        assert [label for label in stores.ran.labels if label == "gathered"] == ran


class TestStoreWithoutTransactions:
    @pytest.mark.parametrize(
        ("failures", "then", "ran", "error"),
        [
            pytest.param(
                {DEFAULT_STORE: _CommitRefused()},
                (),
                _all_lanes("events"),
                _CommitRefused,
                id="default-rolls-back",
            ),
            pytest.param({}, (_fail,), ["events.priority"], _UseCaseFailed, id="use-case-fails"),
            pytest.param(
                {},
                (_poison_default,),
                ["events.priority"],
                _UseCaseFailed,
                id="invocation-poisoned",
            ),
        ],
    )
    async def test_it_runs_every_lane_unless_the_invocation_failed(
        self,
        stores: _Stores,
        failures: dict[StoreKey, BaseException],
        then: tuple[Step, ...],
        ran: list[str],
        error: type[BaseException],
    ) -> None:
        stores.failures.update(failures)
        steps = (stores.queue(), stores.on_events(), *then)

        with pytest.raises(error):
            await stores.run(*steps)

        assert stores.ran.labels == ran


class _Gate:
    """A priority action that parks until released."""

    def __init__(self) -> None:
        self.started, self.release = asyncio.Event(), asyncio.Event()

    async def __call__(self) -> None:
        self.started.set()
        await self.release.wait()


async def test_a_cancellation_after_the_commit_still_runs_every_stores_shielded_lanes(
    stores: _Stores,
) -> None:
    gate = _Gate()

    async def park_default() -> None:
        channel = store_channel()
        assert channel is not None
        channel.enqueue_priority(gate)

    task = asyncio.create_task(stores.run(park_default, stores.queue(), stores.on_audit()))
    await gate.started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    gate.release.set()
    await stores.ran.seen("audit.publish").wait()

    assert stores.ran.labels == [
        "default.priority",
        "audit.priority",
        "default.publish",
        "audit.publish",
    ]


class _AuditService:
    """A ``@transactional`` service writing through a cached audit repository."""

    def __init__(self, session_manager: SessionManager, entries: RepoFor[_AuditEntry]) -> None:
        self.session_manager = session_manager
        self.entries = entries
        self.bumped_inside: bool | None = None

    @transactional
    async def write(self, backend: CountingCacheBackend) -> None:
        await self.entries.create(_NewEntry(note="service"))
        self.bumped_inside = bool(backend.incr_keys)


class TestOutsideTheExecutor:
    async def test_a_unit_of_work_entered_by_hand_bumps_inline(self, stores: _Stores) -> None:
        backend = CountingCacheBackend()
        entries = stores.cached_entries(backend)

        async with SQLAlchemyUnitOfWork(stores.audit):
            await entries.create(_NewEntry(note="by hand"))
            bumped_inside = bool(backend.incr_keys)

        assert bumped_inside

    async def test_transactional_bumps_after_its_commit(self, stores: _Stores) -> None:
        backend = CountingCacheBackend()
        service = _AuditService(stores.audit, stores.cached_entries(backend))

        await service.write(backend)

        assert (service.bumped_inside, bool(backend.incr_keys)) == (False, True)
