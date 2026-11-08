"""The ``Audited`` marker, without a database driver."""

from __future__ import annotations

import pytest

from loom.core.model import Audited, BaseModel, ColumnField
from loom.core.model.audited import audited_table
from loom.core.model.types import Integer, Text


class Note(BaseModel, Audited):
    __tablename__ = "notes"
    id: int = ColumnField(Integer, primary_key=True)
    body: str = ColumnField(Text)


def test_an_audited_table_without_a_primary_key_is_refused() -> None:
    with pytest.raises(ValueError, match=r"events.*no primary key"):
        audited_table(Note, schema=None, name="events", columns=("body",), key_columns=())
