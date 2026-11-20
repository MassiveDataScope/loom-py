from __future__ import annotations

from typing import Any

import msgspec
import pytest

from loom.core.errors import Conflict
from loom.core.stores.repository import BulkCreatable, Countable, OnConflict, Readable

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

    @pytest.mark.parametrize("on_conflict", list(OnConflict))
    async def test_create_many_with_empty_input_returns_nothing(
        self, case: BackendCase, repository: Any, on_conflict: OnConflict
    ) -> None:
        require(case, BulkCreatable)

        assert await repository.create_many([], on_conflict=on_conflict) == ()

    async def test_create_many_with_a_duplicate_persists_nothing(
        self, case: BackendCase, repository: Any
    ) -> None:
        require(case, BulkCreatable, Countable)

        with pytest.raises(Conflict):
            await repository.create_many([SEED[0], SEED[1], SEED[0]])

        assert await repository.count() == 0

    async def test_ignore_skips_existing_and_repeated_keys_in_input_order(
        self, case: BackendCase, repository: Any
    ) -> None:
        require(case, BulkCreatable, Readable)
        existing = await repository.create(SEED[1])

        created = await repository.create_many(
            [
                SEED[2],
                msgspec.structs.replace(SEED[1], customer="zed"),
                SEED[0],
                msgspec.structs.replace(SEED[2], customer="zed"),
            ],
            on_conflict=OnConflict.IGNORE,
        )

        assert [(row.id, row.customer) for row in created] == [
            (SEED[2].id, SEED[2].customer),
            (SEED[0].id, SEED[0].customer),
        ]
        assert await repository.get_by_id(SEED[1].id) == existing

    async def test_ignore_with_every_key_present_returns_nothing(
        self, case: BackendCase, repository: Any
    ) -> None:
        require(case, BulkCreatable)
        await repository.create(SEED[0])

        assert await repository.create_many([SEED[0]], on_conflict=OnConflict.IGNORE) == ()

    async def test_ignore_inside_a_unit_of_work_skips_existing_keys(
        self, case: BackendCase, repository: Any
    ) -> None:
        require(case, BulkCreatable, Readable)
        await repository.create(SEED[1])

        async with case.open_unit_of_work(repository):
            created = await repository.create_many(
                [SEED[1], SEED[2]], on_conflict=OnConflict.IGNORE
            )

        assert [row.id for row in created] == [SEED[2].id]
        assert await repository.get_by_id(SEED[2].id) == created[0]
