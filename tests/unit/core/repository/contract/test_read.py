from __future__ import annotations

from datetime import datetime
from typing import Any

from loom.core.stores.repository import Countable, FieldReadable, Readable

from .conftest import SEED, BackendCase, OrderStatus, require

_FIRST = SEED[0]


class TestContractRead:
    async def test_get_by_id_returns_the_persisted_row(
        self, case: BackendCase, seeded: Any
    ) -> None:
        require(case, Readable)

        found = await seeded.get_by_id(_FIRST.id)

        assert found is not None
        assert (found.id, found.customer, found.amount) == (
            _FIRST.id,
            _FIRST.customer,
            _FIRST.amount,
        )
        assert found.status == _FIRST.status
        assert type(found.status) is OrderStatus
        assert isinstance(found.created_at, datetime)
        assert found.created_at == _FIRST.created_at

    async def test_get_by_id_miss_returns_none(self, case: BackendCase, seeded: Any) -> None:
        require(case, Readable)

        assert await seeded.get_by_id(404) is None

    async def test_get_by_key_returns_the_persisted_row(
        self, case: BackendCase, seeded: Any
    ) -> None:
        require(case, FieldReadable)

        found = await seeded.get_by("id", _FIRST.id)

        assert found is not None
        assert found.customer == _FIRST.customer

    async def test_get_by_key_miss_returns_none(self, case: BackendCase, seeded: Any) -> None:
        require(case, FieldReadable)

        assert await seeded.get_by("id", 404) is None

    async def test_exists_by_key(self, case: BackendCase, seeded: Any) -> None:
        require(case, Countable)

        assert await seeded.exists_by("id", _FIRST.id) is True
        assert await seeded.exists_by("id", 404) is False

    async def test_count_matches_seed(self, case: BackendCase, seeded: Any) -> None:
        require(case, Countable)

        assert await seeded.count() == len(SEED)
