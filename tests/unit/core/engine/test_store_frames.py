"""An execution writes only to the stores it declares (ADR 0008, T046).

A use case's primary store is the ``__store__`` of its main model, and
``Joined[...]`` repositories declare the further stores it writes to. Every
use case here runs through ``RuntimeExecutor.execute`` inside an invocation
of two SQLite stores the test opens, as the composition will once several
stores compose (PR 11). A refused write must never reach the driver: a
listener on the audit store's engine sees every statement sent to it.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, cast

import pytest
from sqlalchemy import event

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.command import Command
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.compiler import CompilationError, UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.model import BaseModel, ColumnField
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey
from loom.core.repository.abc import CursorCodec, RepoFor
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy, with_session_scope
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory
from loom.core.uow.coordinator import Committed, InvocationOutcome, OrderedCommits
from loom.core.uow.scope import UndeclaredStoreWrite, invocation
from loom.core.use_case import Joined, UseCase
from loom.core.use_case.factory import UseCaseFactory

pytestmark = pytest.mark.timeout(30)

AUDIT = StoreKey("audit")


class _Order(BaseModel):
    __tablename__ = "frame_orders"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    ref: str = ColumnField(length=16)


class _AuditEntry(BaseModel):
    __tablename__ = "frame_audit_entries"
    __store__ = "audit"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    note: str = ColumnField(length=16)


class _NewOrder(Command, frozen=True):
    ref: str


class _NewEntry(Command, frozen=True):
    note: str


Orders = RepositorySQLAlchemy[_Order, int]
Entries = RepositorySQLAlchemy[_AuditEntry, int]


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
    """Two SQLite stores, a repository on each, and the statements sent to audit."""

    default: SessionManager
    audit: SessionManager
    orders: Orders
    entries: Entries
    audit_statements: list[str] = field(default_factory=list)

    def coordinator(self) -> OrderedCommits:
        return OrderedCommits(
            {
                DEFAULT_STORE: SQLAlchemyUnitOfWorkFactory(self.default),
                AUDIT: SQLAlchemyUnitOfWorkFactory(self.audit),
            }
        )

    async def run(self, use_case: UseCase[Any, Any]) -> InvocationOutcome:
        """Execute ``use_case`` in an invocation of both stores and end it."""
        async with invocation(self.coordinator()) as scope:
            await RuntimeExecutor(UseCaseCompiler()).execute(use_case)
            return await scope.coordinator.finish(scope, None)

    @property
    def order_port(self) -> RepoFor[_Order]:
        """The orders repository as a use case injects it."""
        return cast("RepoFor[_Order]", self.orders)

    @property
    def entry_port(self) -> RepoFor[_AuditEntry]:
        """The audit repository as a use case injects it."""
        return cast("RepoFor[_AuditEntry]", self.entries)

    def audit_writes(self) -> list[str]:
        return [s for s in self.audit_statements if not s.lstrip().upper().startswith("SELECT")]


@pytest.fixture
async def stores() -> AsyncIterator[_Stores]:
    compile_all(_Order, _AuditEntry)
    default, audit = _manager(DEFAULT_STORE), _manager(AUDIT)
    codec = CursorCodec(signing="ephemeral")
    pair = _Stores(
        default=default,
        audit=audit,
        orders=RepositorySQLAlchemy(default, _Order, cursor_codec=codec),
        entries=RepositorySQLAlchemy(audit, _AuditEntry, cursor_codec=codec),
    )
    for model, manager in ((_Order, default), (_AuditEntry, audit)):
        compiled = get_compiled(model)
        assert compiled is not None
        async with manager.engine.begin() as connection:
            await connection.run_sync(compiled.metadata.create_all)
    await pair.entries.create(_NewEntry(note="seed"))

    def record(_conn: object, _cursor: object, statement: str, *_: object) -> None:
        pair.audit_statements.append(statement)

    event.listen(audit.engine.sync_engine, "before_cursor_execute", record)
    try:
        yield pair
    finally:
        await default.dispose()
        await audit.dispose()


EntryWrite = Callable[[Any], Awaitable[object]]

_ENTRY_WRITES: dict[str, EntryWrite] = {
    "create": lambda entries: entries.create(_NewEntry(note="x")),
    "create_many": lambda entries: entries.create_many([_NewEntry(note="x")]),
    "update": lambda entries: entries.update(1, _NewEntry(note="x")),
    "delete": lambda entries: entries.delete(1),
}


class _WriteAuditPlainly(UseCase[_Order, None]):
    """Primary store ``default``; writes to audit through a plain repository."""

    def __init__(self, entries: RepoFor[_AuditEntry], write: EntryWrite) -> None:
        self.entries = entries
        self.write = write

    async def execute(self) -> None:
        await self.write(self.entries)


class _WriteBothStores(UseCase[_Order, None]):
    """Primary store ``default``; audit joined."""

    def __init__(
        self, orders: RepoFor[_Order], entries: Joined[RepoFor[_AuditEntry]], fail: bool = False
    ) -> None:
        self.orders = orders
        self.entries = entries
        self.fail = fail

    async def execute(self) -> None:
        await self.orders.create(_NewOrder(ref="o1"))
        await self.entries.create(_NewEntry(note="joined"))
        if self.fail:
            raise _UseCaseFailed


class _UseCaseFailed(Exception):
    pass


class _WriteAudit(UseCase[_AuditEntry, None]):
    """Primary store ``audit``, from its main model's ``__store__``."""

    def __init__(self, entries: RepoFor[_AuditEntry]) -> None:
        self.entries = entries

    async def execute(self) -> None:
        await self.entries.create(_NewEntry(note="nested"))


class _WriteAuditAfterNesting(UseCase[_Order, None]):
    """Runs a use case on audit, then writes to audit itself, which it did not declare."""

    def __init__(self, executor: RuntimeExecutor, entries: RepoFor[_AuditEntry]) -> None:
        self.executor = executor
        self.entries = entries

    async def execute(self) -> None:
        await self.executor.execute(_WriteAudit(self.entries))
        await self.entries.create(_NewEntry(note="after"))


class _ReadAuditWriteOrder(UseCase[_Order, int]):
    """Primary store ``default``; reads audit without declaring it."""

    def __init__(self, orders: RepoFor[_Order], entries: RepoFor[_AuditEntry]) -> None:
        self.orders = orders
        self.entries = entries

    async def execute(self) -> int:
        await self.orders.create(_NewOrder(ref="o1"))
        return await self.entries.count()


class _MarkingEntries(RepositorySQLAlchemy[_AuditEntry, int]):
    """An audit repository with a custom write; records each run of its body."""

    ran: list[object]

    @with_session_scope(write=True)
    async def mark(self, session: object) -> None:
        self.ran.append(session)


class _WriteAuditInItsOwnSession(UseCase[_Order, None]):
    """Primary store ``default``; hands an audit session to a custom audit write."""

    def __init__(self, stores: _Stores, entries: _MarkingEntries) -> None:
        self.stores = stores
        self.entries = entries

    async def execute(self) -> None:
        async with self.stores.audit.session() as session:
            await self.entries.mark(session=session)


class _WriteOrder(UseCase[_Order, None]):
    def __init__(self, orders: RepoFor[_Order]) -> None:
        self.orders = orders

    async def execute(self) -> None:
        await self.orders.create(_NewOrder(ref="nested"))


class _NestReadOnlyWriter(UseCase[_Order, None]):
    """Runs a writing use case as read-only inside its own execution."""

    def __init__(self, executor: RuntimeExecutor, orders: RepoFor[_Order]) -> None:
        self.executor = executor
        self.orders = orders

    async def execute(self) -> None:
        await self.executor.execute(_WriteOrder(self.orders), read_only=True)


class TestUndeclaredWrite:
    @pytest.mark.parametrize("operation", sorted(_ENTRY_WRITES))
    async def test_a_plain_repository_of_another_store_is_refused_before_the_driver(
        self, stores: _Stores, operation: str
    ) -> None:
        use_case = _WriteAuditPlainly(stores.entry_port, _ENTRY_WRITES[operation])

        refusal = "store 'audit'.*declares only store 'default'"
        with pytest.raises(UndeclaredStoreWrite, match=refusal):
            await stores.run(use_case)

        assert stores.audit_statements == []

    async def test_after_a_nested_execution_on_another_store_the_outer_one_may_not_write_to_it(
        self, stores: _Stores
    ) -> None:
        use_case = _WriteAuditAfterNesting(RuntimeExecutor(UseCaseCompiler()), stores.entry_port)

        with pytest.raises(UndeclaredStoreWrite, match="store 'audit'"):
            await stores.run(use_case)

        assert len(stores.audit_writes()) == 1, "only the nested execution's insert is sent"
        assert await stores.entries.count() == 1, "the invocation rolled back the nested write"

    async def test_a_session_handed_to_a_custom_write_is_checked_too(self, stores: _Stores) -> None:
        entries = _MarkingEntries(
            stores.audit, _AuditEntry, cursor_codec=CursorCodec(signing="ephemeral")
        )
        entries.ran = []
        use_case = _WriteAuditInItsOwnSession(stores, entries)

        with pytest.raises(UndeclaredStoreWrite, match="store 'audit'"):
            await stores.run(use_case)

        assert entries.ran == []

    async def test_a_read_only_nested_execution_may_not_write(self, stores: _Stores) -> None:
        use_case = _NestReadOnlyWriter(RuntimeExecutor(UseCaseCompiler()), stores.order_port)

        with pytest.raises(UndeclaredStoreWrite, match="is read-only"):
            await stores.run(use_case)

        assert await stores.orders.count() == 0

    async def test_a_read_of_another_store_is_not_refused(self, stores: _Stores) -> None:
        outcome = await stores.run(_ReadAuditWriteOrder(stores.order_port, stores.entry_port))

        assert outcome.stores == (Committed(DEFAULT_STORE),)
        assert stores.audit_writes() == []


class TestJoinedWrite:
    async def test_a_joined_repository_commits_with_the_invocation(self, stores: _Stores) -> None:
        outcome = await stores.run(_WriteBothStores(stores.order_port, stores.entry_port))

        assert outcome.succeeded
        assert {store.key for store in outcome.stores} == {DEFAULT_STORE, AUDIT}
        assert (await stores.orders.count(), await stores.entries.count()) == (1, 2)

    async def test_a_joined_write_rolls_back_when_the_use_case_fails(self, stores: _Stores) -> None:
        use_case = _WriteBothStores(stores.order_port, stores.entry_port, fail=True)

        with pytest.raises(_UseCaseFailed):
            await stores.run(use_case)

        assert (await stores.orders.count(), await stores.entries.count()) == (0, 1)

    async def test_a_nested_execution_writes_to_its_own_primary_store(
        self, stores: _Stores
    ) -> None:
        outcome = await stores.run(_WriteAudit(stores.entry_port))

        assert outcome.stores == (Committed(AUDIT),)
        assert await stores.entries.count() == 2


class _NoMainModel(UseCase[Any, None]):
    async def execute(self) -> None:
        return None


class _JoinsItsOwnStore(UseCase[_Order, None]):
    def __init__(self, orders: Joined[RepoFor[_Order]]) -> None:
        self.orders = orders

    async def execute(self) -> None:
        return None


class _JoinsAConcreteRepository(UseCase[_Order, None]):
    def __init__(self, entries: Joined[Entries]) -> None:
        self.entries = entries

    async def execute(self) -> None:
        return None


class _JoinsANonModel(UseCase[_Order, None]):
    def __init__(self, names: Joined[list[str]]) -> None:
        self.names = names

    async def execute(self) -> None:
        return None


class _JoinsAListOfModels(UseCase[_Order, None]):
    def __init__(self, entries: Joined[list[_AuditEntry]]) -> None:
        self.entries = entries

    async def execute(self) -> None:
        return None


class TestCompiledStores:
    @pytest.mark.parametrize(
        ("use_case", "primary", "joined"),
        [
            pytest.param(_WriteAudit, AUDIT, frozenset(), id="primary-from-__store__"),
            pytest.param(_WriteAuditPlainly, DEFAULT_STORE, frozenset(), id="no-__store__"),
            pytest.param(_NoMainModel, DEFAULT_STORE, frozenset(), id="no-main-model"),
            pytest.param(_WriteBothStores, DEFAULT_STORE, frozenset({AUDIT}), id="joined"),
            pytest.param(_JoinsItsOwnStore, DEFAULT_STORE, frozenset(), id="joined-primary"),
        ],
    )
    def test_the_plan_names_the_primary_and_the_joined_stores(
        self, use_case: type[UseCase[Any, Any]], primary: StoreKey, joined: frozenset[StoreKey]
    ) -> None:
        plan = UseCaseCompiler().compile(use_case)

        assert (plan.primary, plan.joined) == (primary, joined)

    @pytest.mark.parametrize(
        "use_case",
        [
            pytest.param(_JoinsAConcreteRepository, id="concrete-repository"),
            pytest.param(_JoinsANonModel, id="not-a-model"),
            pytest.param(_JoinsAListOfModels, id="not-a-repository"),
        ],
    )
    def test_joined_without_a_model_does_not_compile(
        self, use_case: type[UseCase[Any, Any]]
    ) -> None:
        compiler = UseCaseCompiler()

        with pytest.raises(CompilationError, match=r"__init__: \w+: Joined\[.*naming its model"):
            compiler.compile(use_case)

    def test_a_joined_repository_is_injected_like_a_plain_one(self) -> None:
        orders, entries = object(), object()
        container = LoomContainer()
        container.register(Orders, lambda: orders, scope=Scope.APPLICATION)
        container.register(Entries, lambda: entries, scope=Scope.APPLICATION)
        container.register_repo(_Order, Orders)
        container.register_repo(_AuditEntry, Entries)

        built = UseCaseFactory(container).build(_JoinsItsOwnStore)

        assert built.orders is orders
