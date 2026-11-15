"""The ``Audited`` marker, without a database driver."""

from __future__ import annotations

import pytest

from loom.core.model.audited import audited_table
from tests.unit.core.doubles import Note


class TestAuditedWithoutPrimaryKey:
    def test_an_audited_table_without_a_primary_key_is_refused(self) -> None:
        with pytest.raises(ValueError, match=r"events.*no primary key"):
            audited_table(Note, schema=None, name="events", columns=("body",), key_columns=())
