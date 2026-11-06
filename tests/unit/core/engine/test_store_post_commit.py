"""Post-commit actions run by the outcome of the stores they depend on (ADR 0008, T049, D4).

A job dispatch depends on every store of the execution that queued it, a
cache invalidation on its repository's store. The executor owns an
invocation of two SQLite stores and a store without transactions, the way the
composition builds it; a store's commit fails where a test sets ``FAILURES``.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractContextManager, nullcontext
from typing import Any, ClassVar, cast

import pytest

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.cache.abc.config import CacheConfig
from loom.core.cache.dependency import GenerationalDependencyResolver
from loom.core.cache.repository import CachedRepository
from loom.core.command import Command
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.events import EventKind, RuntimeEvent
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.post_commit import store_channel
from loom.core.errors import StoresNotCommitted
from loom.core.job.context import (
    add_pending_dispatch,
    add_pending_publish,
    clear_pending_dispatches,
)
from loom.core.model import BaseModel, ColumnField
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey
from loom.core.repository.abc import CursorCodec, RepoFor
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWork
from loom.core.uow.abc import UnitOfWork
from loom.core.uow.coordinator import CommitUncertain, OrderedCommits
from loom.core.uow.scope import current_scope, unscoped
from loom.core.use_case import Joined, UseCase
from tests.helpers.jobs import SQS_URL, EmailJob, SQSClient, sqs_service
from tests.unit.core.cache._doubles import CountingCacheBackend

pytestmark = pytest.mark.timeout(30)

AUDIT, EVENTS = StoreKey("audit"), StoreKey("events")
_NEVER_RAN = "PostCommitShieldedActionNeverRan"
Step = Callable[[], Awaitable[object]]

FAILURES: dict[StoreKey, BaseException] = {}
"""The commit error of each store a test sets; the ``stores`` fixture clears it."""


class _Order(BaseModel):
    __tablename__ = "owned_orders"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    ref: str = ColumnField(length=16)


class _Entry(BaseModel):
    __tablename__ = "owned_entries"
    __store__ = "audit"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    ref: str = ColumnField(length=16)


class _Event(BaseModel):
    __tablename__ = "owned_events"
    __store__ = "events"

    id: int = ColumnField(primary_key=True, autoincrement=True)


class _New(Command, frozen=True):
    ref: str


class _Refused(Exception):
    pass


class _Failed(Exception):
    pass


class _CommitFails(SQLAlchemyUnitOfWork):
    async def commit(self) -> None:
        raise FAILURES[self.owner.store]


class _Autocommit:
    """A protocol-only unit of work of a store without transactions."""

    transactional: ClassVar[bool] = False

    async def begin(self) -> None: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...

    async def __aenter__(self) -> _Autocommit:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: object) -> None: ...


class _Sql:
    def __init__(self, manager: SessionManager) -> None:
        self.manager = manager

    def create(self) -> UnitOfWork:
        failing = self.manager.store in FAILURES
        return (_CommitFails if failing else SQLAlchemyUnitOfWork)(self.manager)


class _NoTransactions:
    def create(self) -> UnitOfWork:
        return _Autocommit()


class _Steps:
    steps: tuple[Step, ...]

    async def execute(self) -> None:
        for step in self.steps:
            await step()


class _OnDefault(_Steps, UseCase[_Order, None]):
    def __init__(self, *steps: Step) -> None:
        self.steps = steps


class _OnAudit(_Steps, UseCase[_Entry, None]):
    def __init__(self, *steps: Step) -> None:
        self.steps = steps


class _OnEvents(_Steps, UseCase[_Event, None]):
    def __init__(self, *steps: Step) -> None:
        self.steps = steps


class _JoinsAudit(_Steps, UseCase[_Order, None]):
    def __init__(self, entries: Joined[RepoFor[_Entry]], *steps: Step) -> None:
        self.entries = entries
        self.steps = steps


def _manager(store: StoreKey) -> SessionManager:
    return SessionManager(
        "sqlite+aiosqlite:///:memory:",
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        store=store,
    )


class _Stores:
    """Two SQLite stores, a store without transactions, an executor owning all three."""

    def __init__(self, default: SessionManager, audit: SessionManager) -> None:
        codec = CursorCodec(signing="ephemeral")
        self.default, self.audit = default, audit
        self.orders = RepositorySQLAlchemy(default, _Order, cursor_codec=codec)
        self.entries = RepositorySQLAlchemy(audit, _Entry, cursor_codec=codec)
        self.ran: list[str] = []
        coordinator = OrderedCommits(
            {DEFAULT_STORE: _Sql(default), AUDIT: _Sql(audit), EVENTS: _NoTransactions()}
        )
        self.executor = RuntimeExecutor(UseCaseCompiler(), coordinator=coordinator)

    def mark(self, label: str) -> Callable[[], None]:
        return lambda: self.ran.append(label)

    def queue(self, label: str) -> Step:
        """Queue one action per lane for the running execution."""

        async def step() -> None:
            channel = store_channel()
            assert channel is not None
            channel.enqueue_priority(self.mark(f"{label}.priority"))
            add_pending_publish(self.mark(f"{label}.publish"))
            add_pending_dispatch(self.mark(f"{label}.plain"))

        return step

    def dispatch(self, label: str) -> Step:
        async def step() -> None:
            add_pending_dispatch(self.mark(label))

        return step

    def gathered(self) -> Step:
        """Queue a dispatch from a gather child, which does not own the scope."""
        return lambda: asyncio.gather(self.dispatch("gathered")())

    def run(self, use_case: UseCase[Any, Any], *, read_only: bool = False) -> Step:
        return lambda: self.executor.execute(use_case, read_only=read_only)

    def write_order(self) -> Step:
        return lambda: self.orders.create(_New(ref="o"))

    def write_entry(self, entries: Any = None) -> Step:
        return lambda: (entries or self.entries).create(_New(ref="e"))

    def cached_entries(self, backend: CountingCacheBackend) -> Any:
        resolver = GenerationalDependencyResolver(backend)
        return CachedRepository(
            self.entries, config=CacheConfig(), cache=backend, dependency_resolver=resolver
        )


@pytest.fixture
async def stores() -> AsyncIterator[_Stores]:
    compile_all(_Order, _Entry)
    default, audit = _manager(DEFAULT_STORE), _manager(AUDIT)
    for model, manager in ((_Order, default), (_Entry, audit)):
        compiled = get_compiled(model)
        assert compiled is not None
        async with manager.engine.begin() as connection:
            await connection.run_sync(compiled.metadata.create_all)
    try:
        yield _Stores(default, audit)
    finally:
        FAILURES.clear()
        await default.dispose()
        await audit.dispose()


def _raises(error: type[BaseException] | None) -> AbstractContextManager[object]:
    return nullcontext() if error is None else pytest.raises(error)


@pytest.mark.parametrize(
    ("failures", "ran", "error"),
    [
        pytest.param(
            {},
            ["default.priority", "audit.priority", "events.priority"]
            + ["default.publish", "audit.publish", "events.publish"]
            + ["gathered", "default.plain", "audit.plain", "events.plain"],
            None,
            id="every-store-commits",
        ),
        pytest.param(
            {AUDIT: _Refused()},
            ["default.priority", "events.priority", "default.publish", "events.publish"]
            + ["default.plain", "events.plain"],
            StoresNotCommitted,
            id="audit-rolls-back-after-default-committed",
        ),
        pytest.param(
            {AUDIT: CommitUncertain(ConnectionError("ack lost"))},
            ["default.priority", "audit.priority", "events.priority"]
            + ["default.publish", "events.publish", "default.plain", "events.plain"],
            StoresNotCommitted,
            id="audit-uncertain-runs-its-priority-lane-only",
        ),
        pytest.param(
            {DEFAULT_STORE: _Refused()},
            ["events.priority", "events.publish", "events.plain"],
            _Refused,
            id="default-rolls-back-first",
        ),
    ],
)
async def test_each_store_runs_what_its_outcome_lets_run(
    stores: _Stores,
    failures: dict[StoreKey, BaseException],
    ran: list[str],
    error: type[BaseException] | None,
) -> None:
    FAILURES.update(failures)
    use_case = _OnDefault(
        stores.write_order(),
        stores.queue("default"),
        stores.gathered(),
        stores.run(_OnAudit(stores.write_entry(), stores.queue("audit"))),
        stores.run(_OnEvents(stores.queue("events"))),
    )

    with _raises(error):
        await stores.executor.execute(use_case)

    assert stores.ran == ran


@pytest.mark.parametrize(
    ("failures", "ran", "error"),
    [
        pytest.param({}, ["job.priority", "job.publish", "job.plain"], None, id="both-commit"),
        pytest.param({AUDIT: _Refused()}, [], StoresNotCommitted, id="joined-rolls-back"),
    ],
)
async def test_a_dispatch_runs_only_if_every_store_of_its_execution_committed(
    stores: _Stores,
    failures: dict[StoreKey, BaseException],
    ran: list[str],
    error: type[BaseException] | None,
) -> None:
    FAILURES.update(failures)
    entries = cast("RepoFor[_Entry]", stores.entries)
    use_case = _JoinsAudit(entries, stores.write_order(), stores.write_entry(), stores.queue("job"))

    with _raises(error):
        await stores.executor.execute(use_case)

    assert stores.ran == ran


@pytest.mark.parametrize(
    ("failing", "bumped", "error"),
    [
        pytest.param(DEFAULT_STORE, True, StoresNotCommitted, id="audit-committed"),
        pytest.param(AUDIT, False, _Refused, id="audit-rolled-back"),
    ],
)
async def test_a_cache_invalidation_follows_the_store_of_its_repository(
    stores: _Stores, failing: StoreKey, bumped: bool, error: type[BaseException]
) -> None:
    FAILURES[failing] = _Refused()
    backend = CountingCacheBackend()
    cached = stores.cached_entries(backend)
    use_case = _JoinsAudit(cached, stores.write_entry(cached), stores.write_order())

    with pytest.raises(error):
        await stores.executor.execute(use_case)

    assert bool(backend.incr_keys) is bumped


async def test_a_unit_of_work_entered_by_hand_bumps_once_it_committed(stores: _Stores) -> None:
    backend = CountingCacheBackend()
    cached = stores.cached_entries(backend)

    async with SQLAlchemyUnitOfWork(stores.audit):
        await cached.create(_New(ref="by hand"))
        bumped_inside = bool(backend.incr_keys)

    assert (bumped_inside, bool(backend.incr_keys)) == (False, True)


async def test_a_durable_write_bumps_even_if_its_invocation_fails(stores: _Stores) -> None:
    """T099 P4: a write outside the invocation commits at once; its bump must not wait."""
    backend = CountingCacheBackend()
    cached = stores.cached_entries(backend)

    async def durable() -> None:
        with unscoped():
            await cached.create(_New(ref="durable"))

    async def fail() -> None:
        raise _BodyFails

    use_case = _OnDefault(stores.write_order(), durable, fail)

    with pytest.raises(_BodyFails):
        await stores.executor.execute(use_case)

    assert backend.incr_keys


class _BodyFails(Exception):
    pass


@pytest.mark.parametrize(
    ("failure", "ran", "error"),
    [
        pytest.param(
            None, ["default.priority", "default.publish", "default.plain"], None, id="commits"
        ),
        pytest.param(_BodyFails(), [], _BodyFails, id="body-raises"),
        pytest.param(_Refused(), [], _Refused, id="rolls-back"),
        pytest.param(
            CommitUncertain(ConnectionError("ack lost")),
            ["default.priority"],
            StoresNotCommitted,
            id="uncertain-runs-its-priority-lane-only",
        ),
    ],
)
async def test_a_use_case_run_inside_a_unit_of_work_entered_by_hand_waits_for_its_outcome(
    stores: _Stores, failure: Exception | None, ran: list[str], error: type[Exception] | None
) -> None:
    """T099 P1/P2: never drained before the outer commit, and by its outcome (D4)."""
    if failure is not None and not isinstance(failure, _BodyFails):
        FAILURES[DEFAULT_STORE] = failure
    uow = cast(SQLAlchemyUnitOfWork, _Sql(stores.default).create())
    ran_inside: list[str] = []

    async def by_hand() -> None:
        async with uow:
            await stores.executor.execute(_OnDefault(stores.queue("default")))
            ran_inside.extend(stores.ran)
            if isinstance(failure, _BodyFails):
                raise failure

    with _raises(error):
        await by_hand()

    assert (ran_inside, stores.ran) == ([], ran)


async def test_clearing_the_dispatches_keeps_every_cache_invalidation(stores: _Stores) -> None:
    async def clear() -> None:
        clear_pending_dispatches()

    use_case = _OnDefault(
        stores.write_order(),
        stores.queue("default"),
        stores.gathered(),
        stores.run(_OnAudit(stores.write_entry(), stores.queue("audit"))),
        clear,
    )

    await stores.executor.execute(use_case)

    assert stores.ran == ["default.priority", "audit.priority"]


async def test_an_action_queued_after_the_settlement_never_runs(stores: _Stores) -> None:
    FAILURES[AUDIT] = _Refused()
    settled, queued = asyncio.Event(), asyncio.Event()
    left_running: list[asyncio.Task[None]] = []

    async def late() -> None:  # outlives the use case, sharing its context
        await settled.wait()
        add_pending_dispatch(stores.mark("late"))
        queued.set()

    async def wait_for_late() -> None:  # runs in the drain, after the settlement
        settled.set()
        await queued.wait()

    async def spawn() -> None:
        channel = store_channel()
        assert channel is not None
        channel.enqueue_priority(wait_for_late)
        left_running.append(asyncio.create_task(late()))

    use_case = _OnDefault(stores.write_order(), spawn, stores.run(_OnAudit(stores.write_entry())))

    with pytest.raises(StoresNotCommitted):
        await stores.executor.execute(use_case)
    await left_running[0]

    assert stores.ran == []


async def test_a_cancellation_after_the_commit_still_runs_every_stores_shielded_lanes(
    stores: _Stores,
) -> None:
    started, release, published = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def gate() -> None:
        started.set()
        await release.wait()

    async def park() -> None:
        channel = store_channel()
        assert channel is not None
        channel.enqueue_priority(gate)

    async def publish() -> None:
        add_pending_publish(published.set)

    use_case = _OnDefault(
        stores.write_order(),
        park,
        stores.queue("default"),
        stores.run(_OnAudit(stores.write_entry(), publish)),
    )
    task = asyncio.create_task(stores.executor.execute(use_case))
    await started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    release.set()
    await published.wait()

    assert stores.ran == ["default.priority", "default.publish"]


async def test_sqs_jobs_leave_in_one_batch_per_store(stores: _Stores) -> None:
    client = SQSClient()
    service = sqs_service(client)
    ids: list[str] = []

    async def dispatch() -> None:
        ids.append(service.dispatch(EmailJob).job_id)

    use_case = _OnDefault(
        stores.write_order(), dispatch, stores.run(_OnAudit(stores.write_entry(), dispatch))
    )

    await stores.executor.execute(use_case)

    assert client.batches == [(SQS_URL, [ids[0]]), (SQS_URL, [ids[1]])]


class _FailsWhenDone:
    def on_event(self, event: RuntimeEvent) -> None:
        if event.kind is EventKind.EXEC_DONE:
            raise _Failed


async def test_a_failing_metrics_adapter_does_not_skip_the_actions(stores: _Stores) -> None:
    executor = RuntimeExecutor(
        UseCaseCompiler(),
        coordinator=OrderedCommits({DEFAULT_STORE: _Sql(stores.default)}),
        metrics=_FailsWhenDone(),
    )
    use_case = _OnDefault(stores.write_order(), stores.queue("default"))

    with pytest.raises(_Failed):
        await executor.execute(use_case)

    assert stores.ran == ["default.priority", "default.publish", "default.plain"]


async def _poison() -> None:
    scope = current_scope()
    assert scope is not None
    scope.participants[DEFAULT_STORE].poison(_Failed())


async def _fail() -> None:
    raise _Failed


@pytest.mark.parametrize("failure", [_fail, _poison], ids=["use-case-fails", "poisoned"])
async def test_one_store_without_transactions_in_a_failed_invocation_runs_its_priority_lane(
    stores: _Stores, failure: Step
) -> None:
    executor = RuntimeExecutor(
        UseCaseCompiler(), coordinator=OrderedCommits({DEFAULT_STORE: _NoTransactions()})
    )
    use_case = _OnDefault(stores.queue("default"), stores.gathered(), failure)

    with pytest.raises(_Failed):
        await executor.execute(use_case)

    assert stores.ran == ["default.priority"]


class _Send:
    """A broker send naming its job and task for the logs, never its payload."""

    def __call__(self) -> None:
        raise AssertionError("an uncertain store's send must not run")

    def log_fields(self) -> list[dict[str, object]]:
        return [{"job_name": "EmailJob", "task_id": "task-1"}]


@pytest.mark.parametrize(
    "failure",
    [
        pytest.param(CommitUncertain(ConnectionError("ack lost")), id="uncertain"),
        pytest.param(_Refused(), id="rolled-back"),
    ],
)
async def test_the_dispatches_a_store_drops_are_logged_by_job_and_task(
    stores: _Stores, caplog: pytest.LogCaptureFixture, failure: BaseException
) -> None:
    FAILURES[AUDIT] = failure

    async def send() -> None:
        add_pending_publish(_Send())

    use_case = _OnDefault(stores.write_order(), stores.run(_OnAudit(stores.write_entry(), send)))

    with pytest.raises(StoresNotCommitted):
        await stores.executor.execute(use_case)

    (fields,) = [
        record.msg
        for record in caplog.records
        if isinstance(record.msg, dict) and record.msg.get("event") == _NEVER_RAN
    ]
    assert (fields["job_name"], fields["task_id"]) == ("EmailJob", "task-1")
