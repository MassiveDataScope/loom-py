"""US2 s4 on a real replica set: a failing second write leaves nothing behind."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.errors import Conflict
from tests.unit.core.repository.contract.conftest import SEED, BackendCase

pytestmark = [pytest.mark.integration, pytest.mark.mongo]


class TestTransactions:
    async def test_failing_second_write_discards_the_first(
        self, case: BackendCase, repository: Any
    ) -> None:
        with pytest.raises(Conflict):
            async with case.unit_of_work(repository):
                await repository.create(SEED[0])
                await repository.create(SEED[0])

        assert await repository.get_by_id(SEED[0].id) is None
        assert await repository.count() == 0

    async def test_writes_of_a_committed_unit_of_work_are_visible_outside_it(
        self, case: BackendCase, repository: Any
    ) -> None:
        async with case.unit_of_work(repository):
            await repository.create(SEED[0])
            await repository.create(SEED[1])

        assert await repository.count() == 2
