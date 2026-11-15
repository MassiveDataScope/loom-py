from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest

from loom.core.errors import Conflict
from loom.core.repository.abc import (
    Creatable,
    Deletable,
    FilterGroup,
    FilterOp,
    FilterSpec,
    Listable,
    QuerySpec,
    Readable,
    Updatable,
)

from .conftest import SEED, BackendCase, CreateOrder, OrderStatus, UpdateOrder, require

_FIRST = SEED[0]


class TestContractWrite:
    async def test_create_returns_output_with_an_id(
        self, case: BackendCase, repository: Any
    ) -> None:
        require(case, Creatable, Readable)

        created = await repository.create(_FIRST)

        assert isinstance(created, case.model)
        assert created.id is not None
        assert created.customer == _FIRST.customer
        assert type(created.status) is OrderStatus
        assert isinstance(created.created_at, datetime)
        assert await repository.get_by_id(created.id) == created

    async def test_create_stores_the_fields_left_at_their_default(
        self, case: BackendCase, repository: Any
    ) -> None:
        require(case, Creatable, Readable)

        created = await repository.create(_FIRST)

        stored = await repository.get_by_id(created.id)
        assert (stored.tags, stored.priority) == ([], 0)

    async def test_create_many_stores_the_fields_left_at_their_default(
        self, case: BackendCase, repository: Any
    ) -> None:
        require(case, Creatable, Readable)

        created = await repository.create_many(SEED[:2])

        stored = [await repository.get_by_id(row.id) for row in created]
        assert [(row.tags, row.priority) for row in stored] == [([], 0), ([], 0)]

    async def test_a_field_left_at_its_default_is_found_by_a_filter_on_it(
        self, case: BackendCase, repository: Any
    ) -> None:
        require(case, Creatable, Listable)
        await repository.create(_FIRST)
        spec = QuerySpec(filters=FilterGroup(filters=(FilterSpec("priority", FilterOp.EQ, 0),)))

        found = await repository.list_with_query(spec)

        assert [row.id for row in found.items] == [_FIRST.id]

    async def test_update_keeps_the_fields_the_caller_did_not_send(
        self, case: BackendCase, repository: Any
    ) -> None:
        require(case, Creatable, Updatable, Readable)
        sent = CreateOrder(
            id=9,
            customer="gina",
            amount=5,
            status=OrderStatus.PAID,
            created_at=_FIRST.created_at,
            note="gift",
            tags=["a"],
            priority=4,
        )
        await repository.create(sent)

        updated = await repository.update(9, UpdateOrder(status=OrderStatus.CANCELLED))

        assert updated is not None
        assert (updated.note, updated.tags, updated.priority) == ("gift", ["a"], 4)

    async def test_create_duplicate_key_raises_conflict(
        self, case: BackendCase, repository: Any
    ) -> None:
        require(case, Creatable)
        await repository.create(_FIRST)

        with pytest.raises(Conflict):
            await repository.create(_FIRST)

    async def test_update_changes_only_the_given_fields(
        self, case: BackendCase, seeded: Any
    ) -> None:
        require(case, Updatable, Readable)

        updated = await seeded.update(_FIRST.id, UpdateOrder(status=OrderStatus.CANCELLED))

        assert updated is not None
        assert updated.status == OrderStatus.CANCELLED
        assert type(updated.status) is OrderStatus
        assert isinstance(updated.created_at, datetime)
        assert (updated.customer, updated.amount, updated.note) == (
            _FIRST.customer,
            _FIRST.amount,
            _FIRST.note,
        )
        assert await seeded.get_by_id(_FIRST.id) == updated

    async def test_update_missing_returns_none(self, case: BackendCase, seeded: Any) -> None:
        require(case, Updatable)

        assert await seeded.update(404, UpdateOrder(amount=1)) is None

    async def test_delete_reports_whether_the_row_existed(
        self, case: BackendCase, seeded: Any
    ) -> None:
        require(case, Deletable, Readable)

        assert await seeded.delete(_FIRST.id) is True
        assert await seeded.get_by_id(_FIRST.id) is None
        assert await seeded.delete(_FIRST.id) is False
