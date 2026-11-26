"""Every column kind has a SQLAlchemy translation."""

from __future__ import annotations

import pytest
from sqlalchemy import SmallInteger

from loom.core.model.types import ColumnKind, ColumnType, TinyInteger
from loom.core.model.types import SmallInteger as SmallIntegerColumn
from loom.stores.sqlalchemy.schema import _SA_TYPE_MAP, _build_sa_column_type


class TestColumnKindTranslation:
    @pytest.mark.parametrize("kind", list(ColumnKind), ids=lambda kind: kind.name)
    def test_every_kind_has_a_row_in_the_translator(self, kind: ColumnKind) -> None:
        assert kind in _SA_TYPE_MAP

    @pytest.mark.parametrize(
        "column_type", [TinyInteger, SmallIntegerColumn], ids=["tiny", "small"]
    )
    def test_the_small_integers_are_a_smallint(self, column_type: ColumnType) -> None:
        assert isinstance(_build_sa_column_type(column_type), SmallInteger)
