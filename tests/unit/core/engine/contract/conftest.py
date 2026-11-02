"""Lifecycle contract suite fixtures (US1 s5, FR-014).

Every adapter runs the same scenarios through the real executor: the
SQLAlchemy unit of work over an in-memory sqlite ``SessionManager`` wrapped
by a counting one, the Mongo unit of work over the in-memory fake client
(transactional and no-op) and the DynamoDB no-op unit of work.  A scenario
that needs a real commit or rollback skips by the adapter's declared
``transactional`` ClassVar, read from a created unit of work.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Any, ClassVar, Protocol, cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.repository.dynamodb.uow import DynamoUnitOfWorkFactory
from loom.core.repository.mongo.uow import MongoUnitOfWorkFactory
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory
from loom.core.uow.abc import UnitOfWork, UnitOfWorkFactory

from ...repository.mongo._fake import FakeMongoClient
from .._lifecycle_doubles import Broker, Log, Metrics, start_hanging

# ---------------------------------------------------------------------------
# SQLAlchemy seams: a counting session manager over a real sqlite one
# ---------------------------------------------------------------------------


class _SessionProxy:
    """The real ``AsyncSession`` with a failing ``commit`` or a gated ``rollback``."""

    def __init__(self, session: AsyncSession, manager: CountingSessionManager) -> None:
        self._session = session
        self._manager = manager

    def __getattr__(self, name: str) -> Any:
        return getattr(self._session, name)

    async def commit(self) -> None:
        if self._manager.commit_error is not None:
            raise self._manager.commit_error
        await self._session.commit()

    async def rollback(self) -> None:
        if self._manager.rollback_gate is not None:
            await self._manager.rollback_gate.wait()
        await self._session.rollback()
        self._manager.rollbacks += 1


class CountingSessionManager:
    """Session manager that counts opened and closed session scopes (SC-002).

    Args:
        inner: Real session manager whose scopes are counted.
    """

    def __init__(self, inner: SessionManager) -> None:
        self.inner = inner
        self.opened = 0
        self.closed = 0
        self.rollbacks = 0
        self.begin_error: Exception | None = None
        self.commit_error: Exception | None = None
        self.rollback_gate: asyncio.Event | None = None

    @property
    def engine(self) -> Any:
        """The real manager's engine: the unit of work names its database from it."""
        return self.inner.engine

    @asynccontextmanager
    async def session(self) -> AsyncIterator[_SessionProxy]:
        if self.begin_error is not None:
            raise self.begin_error
        async with self.inner.session() as session:
            self.opened += 1
            try:
                yield _SessionProxy(session, self)
            finally:
                self.closed += 1

    def as_session_manager(self) -> SessionManager:
        """The single typing escape: this wrapper stands in for the real manager."""
        return cast(SessionManager, self)


@asynccontextmanager
async def sqlite_session_manager() -> AsyncIterator[CountingSessionManager]:
    """Yield a counting session manager over an in-memory sqlite engine."""
    # ``None`` pool options: SQLAlchemy picks StaticPool for ``:memory:``.
    manager = SessionManager(
        "sqlite+aiosqlite:///:memory:",
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        connect_args={},
    )
    try:
        yield CountingSessionManager(manager)
    finally:
        await manager.dispose()


# ---------------------------------------------------------------------------
# Adapter seams
# ---------------------------------------------------------------------------


class SessionProbe(Protocol):
    """Observation of the adapter's own session, outside the unit of work API."""

    def sessions_opened(self) -> int: ...

    def sessions_closed(self) -> int: ...

    def rollbacks(self) -> int: ...


class FailureSeams(Protocol):
    """Failure injection through the adapter's own driver seam."""

    def fail_begin(self, error: Exception) -> None: ...

    def fail_commit(self, error: Exception) -> None: ...

    def gate_rollback(self, gate: asyncio.Event) -> None:
        """Make the driver rollback wait on ``gate`` before completing."""
        ...


class SqlAlchemySeams:
    def __init__(self, manager: CountingSessionManager) -> None:
        self._manager = manager

    def sessions_opened(self) -> int:
        return self._manager.opened

    def sessions_closed(self) -> int:
        return self._manager.closed

    def rollbacks(self) -> int:
        return self._manager.rollbacks

    def fail_begin(self, error: Exception) -> None:
        self._manager.begin_error = error

    def fail_commit(self, error: Exception) -> None:
        self._manager.commit_error = error

    def gate_rollback(self, gate: asyncio.Event) -> None:
        self._manager.rollback_gate = gate


class MongoSeams:
    def __init__(self, client: FakeMongoClient) -> None:
        self._client = client

    def sessions_opened(self) -> int:
        return len(self._client.sessions)

    def sessions_closed(self) -> int:
        return sum(1 for session in self._client.sessions if session.ended)

    def rollbacks(self) -> int:
        return self._client.aborted

    def fail_begin(self, error: Exception) -> None:
        self._client.start_transaction_error = error

    def fail_commit(self, error: Exception) -> None:
        self._client.commit_error = error

    def gate_rollback(self, gate: asyncio.Event) -> None:
        self._client.abort_gate = gate


class NoSessionProbe:
    """Probe of an adapter that opens no session: every count is zero."""

    def sessions_opened(self) -> int:
        return 0

    def sessions_closed(self) -> int:
        return 0

    def rollbacks(self) -> int:
        return 0


# ---------------------------------------------------------------------------
# Lifecycle probe: counts the context-manager protocol and any direct call
# ---------------------------------------------------------------------------


class LifecycleProbe:
    """Wraps a real unit of work and records how the executor drives it.

    The adapter's own ``__aexit__`` calls ``commit``/``rollback`` on the
    wrapped instance, so ``direct_calls`` only records calls the executor
    made outside the context-manager protocol.  ``transactional`` is the
    wrapped adapter's value, forwarded by :func:`probe_class_for`.
    """

    transactional: ClassVar[bool]

    def __init__(self, inner: UnitOfWork, log: Log) -> None:
        self.inner = inner
        self._log = log
        self.entered = 0
        self.exits: list[type[BaseException] | None] = []
        self.direct_calls: list[str] = []

    async def begin(self) -> None:
        self.direct_calls.append("begin")
        await self.inner.begin()

    async def commit(self) -> None:
        self.direct_calls.append("commit")
        await self.inner.commit()

    async def rollback(self) -> None:
        self.direct_calls.append("rollback")
        await self.inner.rollback()

    async def __aenter__(self) -> LifecycleProbe:
        self.entered += 1
        self._log("uow.enter")
        await self.inner.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        try:
            await self.inner.__aexit__(exc_type, exc_val, exc_tb)
        finally:
            self.exits.append(exc_type)
            self._log("uow.exit")


def probe_class_for(adapter: type[UnitOfWork]) -> type[LifecycleProbe]:
    """Return a probe class declaring the same ``transactional`` as ``adapter``."""
    return type(
        f"{adapter.__name__}Probe", (LifecycleProbe,), {"transactional": adapter.transactional}
    )


class RecordingFactory:
    """Factory that hands the executor a :class:`LifecycleProbe` per created unit of work."""

    def __init__(self, inner: UnitOfWorkFactory, log: Log) -> None:
        self._inner = inner
        self._log = log
        self._probe_classes: dict[type[UnitOfWork], type[LifecycleProbe]] = {}
        self.created: list[LifecycleProbe] = []

    def create(self) -> LifecycleProbe:
        inner = self._inner.create()
        probe_class = self._probe_classes.setdefault(type(inner), probe_class_for(type(inner)))
        probe = probe_class(inner, self._log)
        self.created.append(probe)
        return probe

    @property
    def entered(self) -> int:
        return sum(probe.entered for probe in self.created)

    @property
    def exited(self) -> int:
        return sum(len(probe.exits) for probe in self.created)

    @property
    def direct_calls(self) -> list[str]:
        return [call for probe in self.created for call in probe.direct_calls]


# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LifecycleCase:
    """One adapter under the lifecycle contract suite.

    Args:
        name: Adapter name, used in test ids and skip reasons.
        factory: Recording factory the executor is built with.
        transactional: The ``transactional`` ClassVar of a created unit of work.
        probe: Observation of the adapter's session.
        failures: Failure injection; ``None`` when the adapter has no driver seam.
        sessions_expected: Driver sessions one execution opens on this adapter.
        rollbacks_after_failed_commit: Driver rollbacks the adapter issues when
            ``commit`` fails (pymongo marks the transaction itself: none).
    """

    name: str
    factory: RecordingFactory
    transactional: bool
    probe: SessionProbe
    failures: FailureSeams | None
    sessions_expected: int
    rollbacks_after_failed_commit: int


def _transactional_case(
    name: str,
    factory: UnitOfWorkFactory,
    log: Log,
    seams: SqlAlchemySeams | MongoSeams,
    rollbacks_after_failed_commit: int,
) -> LifecycleCase:
    return LifecycleCase(
        name=name,
        factory=RecordingFactory(factory, log),
        transactional=type(factory.create()).transactional,
        probe=seams,
        failures=seams,
        sessions_expected=1,
        rollbacks_after_failed_commit=rollbacks_after_failed_commit,
    )


def _no_op_case(name: str, factory: UnitOfWorkFactory, log: Log) -> LifecycleCase:
    return LifecycleCase(
        name=name,
        factory=RecordingFactory(factory, log),
        transactional=type(factory.create()).transactional,
        probe=NoSessionProbe(),
        failures=None,
        sessions_expected=0,
        rollbacks_after_failed_commit=0,
    )


@asynccontextmanager
async def _sqlalchemy_case(log: Log) -> AsyncIterator[LifecycleCase]:
    async with sqlite_session_manager() as manager:
        factory = SQLAlchemyUnitOfWorkFactory(manager.as_session_manager())
        yield _transactional_case("sqlalchemy", factory, log, SqlAlchemySeams(manager), 1)


@asynccontextmanager
async def _mongo_transactional_case(log: Log) -> AsyncIterator[LifecycleCase]:
    client = FakeMongoClient()
    factory = MongoUnitOfWorkFactory.transactional(cast(Any, client))
    yield _transactional_case("mongo-transactional", factory, log, MongoSeams(client), 0)


@asynccontextmanager
async def _mongo_noop_case(log: Log) -> AsyncIterator[LifecycleCase]:
    yield _no_op_case("mongo-noop", MongoUnitOfWorkFactory.without_transactions(), log)


@asynccontextmanager
async def _dynamodb_case(log: Log) -> AsyncIterator[LifecycleCase]:
    yield _no_op_case("dynamodb", DynamoUnitOfWorkFactory(), log)


CaseBuilder = Callable[[Log], AbstractAsyncContextManager[LifecycleCase]]

CASES: tuple[CaseBuilder, ...] = (
    _sqlalchemy_case,
    _mongo_transactional_case,
    _mongo_noop_case,
    _dynamodb_case,
)
CASE_IDS = ("sqlalchemy", "mongo-transactional", "mongo-noop", "dynamodb")


def require_transactional(case: LifecycleCase) -> FailureSeams:
    """Skip unless the adapter declares ``transactional``; return its failure seams."""
    if not case.transactional:
        pytest.skip(f"{case.name} declares transactional=False: nothing to commit or roll back")
    assert case.failures is not None, f"{case.name} declares transactional but has no seam"
    return case.failures


async def cancel_twice_during_close(executor: RuntimeExecutor) -> None:
    """Cancel a hanging execution, then again while its rollback is in flight."""
    task, observed = start_hanging(executor)
    await asyncio.sleep(0.01)
    task.cancel()  # cuts the body: the rollback starts and blocks on the gate
    await asyncio.sleep(0)
    task.cancel()  # arrives while the rollback is in flight
    with pytest.raises(asyncio.CancelledError):
        await task
    assert observed == [True]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def log() -> Log:
    return Log()


@pytest.fixture(params=CASES, ids=CASE_IDS)
async def case(request: pytest.FixtureRequest, log: Log) -> AsyncIterator[LifecycleCase]:
    builder: CaseBuilder = request.param
    async with builder(log) as built:
        yield built


@pytest.fixture
def metrics(log: Log) -> Metrics:
    return Metrics(log)


@pytest.fixture
def broker(log: Log) -> Broker:
    return Broker(log)


@pytest.fixture
def executor(case: LifecycleCase, metrics: Metrics) -> RuntimeExecutor:
    return RuntimeExecutor(UseCaseCompiler(), uow_factory=case.factory, metrics=metrics)
