"""Backend contract suite fixtures (FR-014).

Every backend runs the same tests over the same neutral ``Order`` model; a
test that needs a capability the backend does not declare skips with the
capability name. Expected query results come from :func:`oracle`, a pure
Python evaluation of the seed data, so one ``QuerySpec`` must yield the same
rows on every backend.
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

import pytest

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.command import Command
from loom.core.model import BaseModel, ColumnField, DateTime, String
from loom.core.repository.abc import FilterGroup, FilterOp, FilterSpec, SortSpec
from loom.core.repository.dynamodb.repository import RepositoryDynamoDB
from loom.core.repository.dynamodb.uow import DynamoUnitOfWork
from loom.core.repository.mongo.repository import RepositoryMongo
from loom.core.repository.mongo.uow import NoOpMongoUnitOfWork
from loom.core.repository.registration import capabilities_of
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWork
from loom.core.uow.abc import UnitOfWork

from ..dynamodb._fake import FakeClient
from ..mongo._fake import FakeMongoClient

_TABLE = "contract_orders"


class OrderStatus(StrEnum):
    PENDING = "pending"
    PAID = "paid"
    CANCELLED = "cancelled"


class Order(BaseModel):
    __tablename__ = _TABLE

    id: int = ColumnField(primary_key=True, autoincrement=True)
    customer: str = ColumnField(length=32)
    amount: int = ColumnField()
    status: OrderStatus = ColumnField(String(16))
    # Naive: sqlite stores no offset, so an aware value would not round-trip.
    created_at: datetime = ColumnField(DateTime(tz=False))
    note: str | None = ColumnField(nullable=True, default=None)


class CreateOrder(Command, frozen=True):
    customer: str
    amount: int
    status: OrderStatus
    created_at: datetime
    note: str | None = None
    id: int | None = None


class UpdateOrder(Command, frozen=True):
    status: OrderStatus | None = None
    amount: int | None = None
    note: str | None = None


def _day(day: int) -> datetime:
    return datetime(2026, 1, day, 12, 0)


# Ids are explicit so backends without id generation seed the same rows; the
# duplicate (amount, status, created_at) values make sort ties only the id breaks.
# Customers are lowercase: LIKE case-sensitivity is not pinned (sqlite folds ASCII).
SEED: tuple[CreateOrder, ...] = (
    CreateOrder(id=1, customer="alice", amount=30, status=OrderStatus.PAID, created_at=_day(3)),
    CreateOrder(id=2, customer="anna", amount=10, status=OrderStatus.PENDING, created_at=_day(1)),
    CreateOrder(
        id=3, customer="bob", amount=30, status=OrderStatus.PAID, created_at=_day(5), note="rush"
    ),
    CreateOrder(
        id=4, customer="carol", amount=20, status=OrderStatus.CANCELLED, created_at=_day(2)
    ),
    CreateOrder(
        id=5,
        customer="dave",
        amount=30,
        status=OrderStatus.PENDING,
        created_at=_day(4),
        note="gift",
    ),
    CreateOrder(id=6, customer="erin", amount=20, status=OrderStatus.PAID, created_at=_day(6)),
    CreateOrder(id=7, customer="frank", amount=10, status=OrderStatus.PAID, created_at=_day(3)),
    CreateOrder(
        id=8,
        customer="anne",
        amount=30,
        status=OrderStatus.CANCELLED,
        created_at=_day(1),
        note="rush",
    ),
)


def seed_rows() -> list[dict[str, Any]]:
    """Return the seed as plain field dicts, the input of :func:`oracle`."""
    return [
        {
            "id": row.id,
            "customer": row.customer,
            "amount": row.amount,
            "status": row.status,
            "created_at": row.created_at,
            "note": row.note,
        }
        for row in SEED
    ]


def _like_matches(value: str, pattern: str, flags: re.RegexFlag) -> bool:
    regex = "".join(
        ".*" if char == "%" else "." if char == "_" else re.escape(char) for char in pattern
    )
    return re.fullmatch(regex, value, flags) is not None


_PREDICATES: dict[FilterOp, Callable[[Any, Any], bool]] = {
    FilterOp.EQ: lambda value, expected: value == expected,
    FilterOp.NE: lambda value, expected: value != expected,
    FilterOp.GT: lambda value, expected: value > expected,
    FilterOp.GTE: lambda value, expected: value >= expected,
    FilterOp.LT: lambda value, expected: value < expected,
    FilterOp.LTE: lambda value, expected: value <= expected,
    FilterOp.IN: lambda value, expected: value in expected,
    FilterOp.LIKE: lambda value, expected: _like_matches(value, expected, re.NOFLAG),
    FilterOp.ILIKE: lambda value, expected: _like_matches(value, expected, re.IGNORECASE),
}


def _holds(row: dict[str, Any], spec: FilterSpec) -> bool:
    """SQL three-valued logic: a comparison against NULL is never true."""
    value = row[spec.field]
    if spec.op is FilterOp.IS_NULL:
        return value is None
    return value is not None and _PREDICATES[spec.op](value, spec.value)


def _matches(row: dict[str, Any], group: FilterGroup) -> bool:
    verdicts = [_holds(row, spec) for spec in group.filters]
    return any(verdicts) if group.op == "OR" else all(verdicts)


def oracle(
    rows: list[dict[str, Any]],
    filters: FilterGroup | None = None,
    sort: tuple[SortSpec, ...] = (),
) -> list[int]:
    """Return the ids *filters* select from *rows*, ordered by *sort* then id."""
    selected = [row for row in rows if filters is None or _matches(row, filters)]
    selected.sort(key=lambda row: row["id"])
    for spec in reversed(sort):
        selected.sort(key=lambda row: row[spec.field], reverse=spec.direction == "DESC")
    return [row["id"] for row in selected]


@dataclass(frozen=True)
class BackendCase:
    """One backend under the contract suite.

    Args:
        name: Backend name, used in test ids and skip reasons.
        repository_factory: Opens a fresh repository over a fresh empty store.
        model: The output model the repository is bound to.
        capabilities: Standard protocols the repository class declares.
        transactions: Whether ``unit_of_work`` really rolls back writes.
        unit_of_work: Builds a unit of work bound to the repository's store.
    """

    name: str
    repository_factory: Callable[[], AbstractAsyncContextManager[Any]]
    model: type[Order]
    capabilities: frozenset[type]
    transactions: bool
    unit_of_work: Callable[[Any], UnitOfWork]


@asynccontextmanager
async def _sqlalchemy_repository() -> AsyncIterator[RepositorySQLAlchemy[Order, int]]:
    compile_all(Order)
    # ``None`` pool options: SQLAlchemy picks StaticPool for ``:memory:``.
    manager = SessionManager(
        "sqlite+aiosqlite:///:memory:",
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        connect_args={},
    )
    compiled = get_compiled(Order)
    assert compiled is not None
    try:
        async with manager.engine.begin() as connection:
            await connection.run_sync(compiled.metadata.create_all)
        yield RepositorySQLAlchemy(session_manager=manager, model=Order)
    finally:
        await manager.dispose()


@asynccontextmanager
async def _dynamodb_repository() -> AsyncIterator[RepositoryDynamoDB[Order, int]]:
    client = FakeClient(key_name="id", table_names=(_TABLE,))
    yield RepositoryDynamoDB(client=client, table_name=_TABLE, model=Order)


@asynccontextmanager
async def _mongo_repository() -> AsyncIterator[RepositoryMongo[Order, int]]:
    # ``Order.id`` is autoincrement, which the mongo backend refuses at boot;
    # the repository itself only maps the key, and the seed ids are explicit.
    collection = FakeMongoClient().collection(_TABLE)
    yield RepositoryMongo(Order, collection)


def _sqlalchemy_case() -> BackendCase:
    return BackendCase(
        name="sqlalchemy",
        repository_factory=_sqlalchemy_repository,
        model=Order,
        capabilities=frozenset(capabilities_of(RepositorySQLAlchemy)),
        transactions=True,
        unit_of_work=lambda repository: SQLAlchemyUnitOfWork(repository.session_manager),
    )


def _dynamodb_case() -> BackendCase:
    return BackendCase(
        name="dynamodb",
        repository_factory=_dynamodb_repository,
        model=Order,
        capabilities=frozenset(capabilities_of(RepositoryDynamoDB)),
        transactions=False,
        unit_of_work=lambda _repository: DynamoUnitOfWork(),
    )


def _mongo_case() -> BackendCase:
    return BackendCase(
        name="mongo",
        repository_factory=_mongo_repository,
        model=Order,
        capabilities=frozenset(capabilities_of(RepositoryMongo)),
        transactions=False,
        unit_of_work=lambda _repository: NoOpMongoUnitOfWork(),
    )


CASES: tuple[BackendCase, ...] = (_sqlalchemy_case(), _dynamodb_case(), _mongo_case())


def require(case: BackendCase, *capabilities: type) -> None:
    """Skip the current test unless *case* declares every capability."""
    missing = [proto.__name__ for proto in capabilities if proto not in case.capabilities]
    if missing:
        pytest.skip(f"{case.name} does not declare {', '.join(missing)}")


@pytest.fixture(params=CASES, ids=[case.name for case in CASES])
def case(request: pytest.FixtureRequest) -> BackendCase:
    return request.param


@pytest.fixture
async def repository(case: BackendCase) -> AsyncIterator[Any]:
    async with case.repository_factory() as repo:
        yield repo


@pytest.fixture
async def seeded(case: BackendCase, repository: Any) -> Any:
    """The repository with :data:`SEED` persisted, one ``create`` per row."""
    for row in SEED:
        await repository.create(row)
    return repository
