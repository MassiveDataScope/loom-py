from __future__ import annotations

from typing import Any

import pytest

from loom.core.errors import Conflict
from loom.core.repository.abc import BulkCreatable, Countable, Readable

from .conftest import SEED, BackendCase, require


class TestContractBulkCreate:
    async def test_create_many_returns_outputs_in_input_order(
        self, case: BackendCase, repository: Any
    ) -> None:
        require(case, BulkCreatable, Readable)

        created = await repository.create_many(list(reversed(SEED)))

        assert [row.id for row in created] == [row.id for row in reversed(SEED)]
        assert [row.customer for row in created] == [row.customer for row in reversed(SEED)]
        assert all(isinstance(row, case.model) for row in created)
        assert await repository.get_by_id(SEED[-1].id) == created[0]

    async def test_create_many_with_empty_input_returns_nothing(
        self, case: BackendCase, repository: Any
    ) -> None:
        require(case, BulkCreatable, Countable)

        assert await repository.create_many([]) == ()
        assert await repository.count() == 0

    async def test_create_many_with_a_duplicate_persists_nothing(
        self, case: BackendCase, repository: Any
    ) -> None:
        require(case, BulkCreatable, Countable)

        with pytest.raises(Conflict):
            await repository.create_many([SEED[0], SEED[1], SEED[0]])

        assert await repository.count() == 0
