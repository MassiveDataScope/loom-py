"""``ServerStamps`` stamps the fields a store fills with the current time."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from loom.core.model import BaseModel, ColumnField, CreatedAtField, UpdatedAtField
from loom.core.stores.repository.record import ServerStamps

_NOW = datetime(2026, 1, 2, tzinfo=UTC)
_LONG_AGO = datetime(2020, 1, 1, tzinfo=UTC)


class _Note(BaseModel):
    __tablename__ = "server_stamps_notes"

    key: str = ColumnField(primary_key=True, length=16)
    body: str = ColumnField(length=16)
    created_at: datetime | None = CreatedAtField()
    updated_at: datetime | None = UpdatedAtField()


class TestServerStamps:
    @pytest.fixture
    def stamps(self) -> ServerStamps:
        return ServerStamps.of(_Note)

    @pytest.mark.parametrize(
        ("values", "expected"),
        [
            ({"body": "b"}, {"body": "b", "created_at": _NOW, "updated_at": _NOW}),
            (
                {"body": "b", "created_at": None},
                {"body": "b", "created_at": _NOW, "updated_at": _NOW},
            ),
            (
                {"body": "b", "created_at": _LONG_AGO},
                {"body": "b", "created_at": _LONG_AGO, "updated_at": _NOW},
            ),
        ],
        ids=["absent", "none", "supplied"],
    )
    def test_create_stamps_fill_only_the_empty_fields(
        self, stamps: ServerStamps, values: dict[str, Any], expected: dict[str, Any]
    ) -> None:
        assert stamps.with_create_stamps(values, _NOW) == expected

    def test_update_stamps_overwrite_what_the_changes_send(self, stamps: ServerStamps) -> None:
        changes = {"body": "b", "updated_at": _LONG_AGO}

        assert stamps.with_update_stamps(changes, _NOW) == {"body": "b", "updated_at": _NOW}

    def test_stamping_returns_a_copy(self, stamps: ServerStamps) -> None:
        values = {"body": "b"}

        stamps.with_create_stamps(values, _NOW)
        stamps.with_update_stamps(values, _NOW)

        assert values == {"body": "b"}
