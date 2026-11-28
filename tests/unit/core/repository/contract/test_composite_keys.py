from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Any

import pytest

from loom.core.errors import Conflict
from loom.core.stores.repository import (
    BulkCreatable,
    Creatable,
    Deletable,
    OnConflict,
    Readable,
    Updatable,
)

from .conftest import AcmeContractLine, BackendCase, CreateLine, UpdateLine, require

OpenRepository = Callable[..., AbstractAsyncContextManager[Any]]

_FIRST = CreateLine(order_id=7, line_no=1, quantity=2)
_SECOND = CreateLine(order_id=7, line_no=2, quantity=5)
_OTHER_ORDER = CreateLine(order_id=8, line_no=1, quantity=9)


def _require_composite(case: BackendCase) -> None:
    if not case.composite_keys:
        pytest.skip(f"{case.name} does not hold a composite primary key")


class TestContractCompositeKey:
    """A composite primary key is the tuple of its values, in key order."""

    async def test_create_then_get_by_the_key_tuple(
        self, case: BackendCase, open_repository: OpenRepository
    ) -> None:
        require(case, Creatable, Readable)
        _require_composite(case)

        async with open_repository(AcmeContractLine) as lines:
            created = await lines.create(_FIRST)

            assert await lines.get_by_id((7, 1)) == created
            assert await lines.get_by_id((1, 7)) is None

    async def test_rows_sharing_one_key_column_are_distinct(
        self, case: BackendCase, open_repository: OpenRepository
    ) -> None:
        require(case, Creatable, Readable)
        _require_composite(case)

        async with open_repository(AcmeContractLine) as lines:
            await lines.create(_FIRST)
            other = await lines.create(_OTHER_ORDER)

            assert await lines.get_by_id((8, 1)) == other

    async def test_create_conflicts_on_the_same_key_tuple(
        self, case: BackendCase, open_repository: OpenRepository
    ) -> None:
        require(case, Creatable, Readable)
        _require_composite(case)

        async with open_repository(AcmeContractLine) as lines:
            stored = await lines.create(_FIRST)

            with pytest.raises(Conflict):
                await lines.create(CreateLine(order_id=7, line_no=1, quantity=99))

            assert await lines.get_by_id((7, 1)) == stored

    async def test_ignore_skips_a_stored_key_tuple(
        self, case: BackendCase, open_repository: OpenRepository
    ) -> None:
        require(case, BulkCreatable, Creatable, Readable)
        _require_composite(case)

        async with open_repository(AcmeContractLine) as lines:
            stored = await lines.create(_FIRST)

            created = await lines.create_many(
                [CreateLine(order_id=7, line_no=1, quantity=99), _SECOND, _SECOND],
                on_conflict=OnConflict.IGNORE,
            )

            assert [(row.order_id, row.line_no) for row in created] == [(7, 2)]
            assert await lines.get_by_id((7, 1)) == stored

    async def test_create_many_conflicts_on_a_repeated_key_tuple_and_stores_nothing(
        self, case: BackendCase, open_repository: OpenRepository
    ) -> None:
        require(case, BulkCreatable, Readable)
        _require_composite(case)

        async with open_repository(AcmeContractLine) as lines:
            with pytest.raises(Conflict):
                await lines.create_many([_FIRST, _SECOND, _FIRST])

            assert await lines.get_by_id((7, 2)) is None

    async def test_update_and_delete_take_the_key_tuple(
        self, case: BackendCase, open_repository: OpenRepository
    ) -> None:
        require(case, Creatable, Updatable, Deletable, Readable)
        _require_composite(case)

        async with open_repository(AcmeContractLine) as lines:
            await lines.create(_FIRST)
            await lines.create(_OTHER_ORDER)

            updated = await lines.update((7, 1), UpdateLine(quantity=3))

            assert updated is not None
            assert (updated.order_id, updated.line_no, updated.quantity) == (7, 1, 3)
            assert await lines.delete((7, 1)) is True
            assert await lines.delete((7, 1)) is False
            assert await lines.get_by_id((8, 1)) is not None

    async def test_a_scalar_id_is_refused_for_a_composite_key(
        self, case: BackendCase, open_repository: OpenRepository
    ) -> None:
        require(case, Readable)
        _require_composite(case)

        async with open_repository(AcmeContractLine) as lines:
            with pytest.raises(ValueError, match=r"composite primary key \(order_id, line_no\)"):
                await lines.get_by_id(7)
