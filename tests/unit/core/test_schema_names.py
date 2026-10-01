from __future__ import annotations

import pytest
from sqlalchemy.dialects.postgresql.base import RESERVED_WORDS

from loom.core.schema_names import (
    MAX_SCHEMA_LENGTH,
    POSTGRES_RESERVED_WORDS,
    SchemaNames,
    schema_identifier,
    sql_identifier,
)


def test_the_reserved_words_cover_every_word_postgres_reserves() -> None:
    assert RESERVED_WORDS <= POSTGRES_RESERVED_WORDS


def test_derived_names_follow_the_convention() -> None:
    assert SchemaNames.derived("notes") == SchemaNames(
        guard="loom_guard_notes",
        readers="notes_readers",
        writers="notes_writers",
        version_table="alembic_version",
        data_version_table="alembic_version_data",
    )


@pytest.mark.parametrize(
    "name",
    ["Notes", "1notes", "no-tes", "select", "public", "current_user", "pg_notes", "a" * 64],
)
def test_names_postgres_would_change_or_resolve_elsewhere_are_refused(name: str) -> None:
    with pytest.raises(ValueError, match="not a usable SQL identifier"):
        sql_identifier(name)


def test_a_schema_name_leaves_room_for_every_guard_object_name() -> None:
    name = "a" * (MAX_SCHEMA_LENGTH + 1)

    with pytest.raises(ValueError, match=name):
        schema_identifier(name)


@pytest.mark.parametrize("bad", ["my-schema", "1st", "Notes", "select", "s" * 48])
def test_deriving_from_an_invalid_schema_is_refused(bad: str) -> None:
    with pytest.raises(ValueError, match="identifier"):
        SchemaNames.derived(bad)
