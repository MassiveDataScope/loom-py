"""The audit log's pinned shape, the revision an audited application needs (FR-018), the reader."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection

from loom.core.repository.sqlalchemy.rls.audit import (
    EXPIRED_AUDIT_PARTITIONS,
    OLDEST_EXPIRED_AUDIT_PARTITION,
)
from loom.core.repository.sqlalchemy.rls.guard_manifest import (
    AUDIT_LOG_SHAPE,
    READ_AUDIT_LOG_ARGUMENTS,
)
from loom.core.repository.sqlalchemy.rls.integrity import (
    Problem,
    audit_problems,
    audit_shape_problems,
    reader_problems,
)


def test_the_pinned_shape_has_no_problem() -> None:
    assert audit_shape_problems(AUDIT_LOG_SHAPE) == []


def test_a_missing_log_names_the_log() -> None:
    [problem] = audit_shape_problems(())

    assert (problem.check, problem.subject, problem.actual) == (
        "audit.shape",
        "audit_log",
        "missing",
    )


def test_a_changed_column_names_the_column() -> None:
    changed = tuple(
        "subject text" if line.startswith("subject ") else line for line in AUDIT_LOG_SHAPE
    )

    [problem] = audit_shape_problems(changed)

    assert problem.check == "audit.shape"
    assert "subject text not null" in problem.expected
    assert "subject text" in problem.actual


def test_an_added_column_is_a_problem() -> None:
    assert audit_shape_problems((*AUDIT_LOG_SHAPE, "note text")) != []


def test_a_dropped_index_names_the_index() -> None:
    dropped = tuple(line for line in AUDIT_LOG_SHAPE if "audit_log_trace_idx" not in line)

    [problem] = audit_shape_problems(dropped)

    assert "audit_log_trace_idx" in problem.expected
    assert problem.actual == "none"


def test_a_changed_primary_key_is_a_problem() -> None:
    changed = tuple(
        "constraint PRIMARY KEY (id)" if line == "constraint PRIMARY KEY (at, id)" else line
        for line in AUDIT_LOG_SHAPE
    )

    [problem] = audit_shape_problems(changed)

    assert problem.expected == "constraint PRIMARY KEY (at, id)"
    assert problem.actual == "constraint PRIMARY KEY (id)"


class _NoLog:
    """A connection whose catalogue holds no audit log: every query returns no row."""

    async def execute(self, *_: Any) -> list[Any]:
        return []


async def test_reading_without_the_log_is_a_problem_even_without_audited_tables() -> None:
    """N13: a reading store needs the log; without it the first read fails, so startup does."""
    connection = cast(AsyncConnection, _NoLog())

    unread = await audit_problems(connection, "g", "s1", audited=False, reading=False)
    [problem] = await audit_problems(connection, "g", "s1", audited=False, reading=True)

    assert unread == []
    assert (problem.check, problem.subject, problem.actual) == (
        "audit.shape",
        "audit_log",
        "missing",
    )


def test_drop_expired_takes_the_oldest_of_the_partitions_verify_lists() -> None:
    assert OLDEST_EXPIRED_AUDIT_PARTITION == EXPIRED_AUDIT_PARTITIONS + " LIMIT 1"


# --- The audit reader at startup (spec 009, US1 #3, T12) -------------------------------------

SOURCE = "-- the released reader body"
# The result type ``guard/0004.sql`` pins, as ``pg_get_function_result`` renders it.
RESULT = (
    "TABLE(id bigint, at timestamp with time zone, op text, schema_name text, table_name text, "
    "row_key text, changed jsonb, subject text, mechanism text, on_behalf_of text, "
    "trace_id text, elevated text[], origin text)"
)


@dataclass(frozen=True, slots=True)
class _Reader:
    """One ``<schema>.read_audit_log`` as the startup query reads it."""

    arguments: str = READ_AUDIT_LOG_ARGUMENTS
    owned: bool = True
    definer: bool = True
    config: tuple[str, ...] = ("search_path=pg_catalog, s1, pg_temp",)
    grants: tuple[str, ...] = ("s1_readers:EXECUTE",)
    executable: bool = True
    search_path: str = "search_path=pg_catalog, s1, pg_temp"
    source: str = SOURCE
    volatility: str = "s"
    language: str = "plpgsql"
    result: str = RESULT


def _checks(problems: list[Problem]) -> list[str]:
    return [problem.check for problem in problems]


def test_a_released_reader_the_user_may_execute_has_no_problem() -> None:
    assert reader_problems([_Reader()], SOURCE, reading=True) == []


def test_reading_without_the_reader_names_the_missing_function() -> None:
    [problem] = reader_problems([], SOURCE, reading=True)

    assert (problem.check, problem.subject, problem.expected, problem.actual) == (
        "audit.reader",
        "read_audit_log",
        "installed",
        "missing",
    )


def test_reading_without_execute_is_a_grant_problem() -> None:
    reader = _Reader(grants=(), executable=False)

    assert _checks(reader_problems([reader], SOURCE, reading=True)) == ["audit.reader_grant"]


def test_a_grant_implies_the_flag() -> None:
    [problem] = reader_problems([_Reader()], SOURCE, reading=False)

    assert (problem.check, problem.subject, problem.actual) == (
        "audit.reader_grant",
        "read_audit_log",
        "EXECUTE",
    )


def test_reading_off_without_execute_or_without_a_reader_has_no_problem() -> None:
    locked = _Reader(grants=(), executable=False)

    assert reader_problems([locked], SOURCE, reading=False) == []
    assert reader_problems([], SOURCE, reading=False) == []


@pytest.mark.parametrize(
    "reader",
    [
        _Reader(arguments="p_all boolean"),
        _Reader(owned=False),
        _Reader(definer=False),
        _Reader(config=()),
        _Reader(config=("search_path=public",)),
        _Reader(grants=("PUBLIC:EXECUTE", "s1_readers:EXECUTE")),
        _Reader(volatility="v"),
        _Reader(language="sql"),
        _Reader(result="SETOF text"),
    ],
    ids=[
        "arguments",
        "owner",
        "invoker",
        "no-search-path",
        "other-search-path",
        "public",
        "volatile",
        "language",
        "result",
    ],
)
def test_a_reader_that_is_not_the_released_shape_is_a_shape_problem(reader: _Reader) -> None:
    assert _checks(reader_problems([reader], SOURCE, reading=True)) == ["audit.reader_shape"]


def test_an_overloaded_reader_is_a_shape_problem() -> None:
    [problem] = reader_problems([_Reader(), _Reader(arguments="")], SOURCE, reading=True)

    assert (problem.check, problem.actual) == ("audit.reader_shape", "2 functions")


def test_a_schema_postgres_quotes_matches_the_setting_it_renders() -> None:
    """Review F4: the expected ``search_path`` is the one Postgres renders, quotes included."""
    quoted = 'search_path=pg_catalog, "Odd", pg_temp'
    reader = _Reader(config=(quoted,), search_path=quoted)

    assert reader_problems([reader], SOURCE, reading=True) == []


@pytest.mark.parametrize(
    "grants",
    [
        ("s1_readers:EXECUTE", "s1_ops:EXECUTE"),
        ("s1_readers:EXECUTE (grantable)",),
        (),
    ],
    ids=["extra-grantee", "grantable", "no-grantee"],
)
def test_reading_needs_execute_granted_to_one_role_without_grant_option(
    grants: tuple[str, ...],
) -> None:
    """Review F1: one non-PUBLIC grantee, the readers group, and never a grant option."""
    reader = _Reader(grants=grants)

    assert _checks(reader_problems([reader], SOURCE, reading=True)) == ["audit.reader_grant"]


def test_a_grant_the_user_cannot_use_still_implies_the_flag() -> None:
    """Review F2: a NOINHERIT member sees no privilege, yet the reader's ACL names a grantee."""
    [problem] = reader_problems([_Reader(executable=False)], SOURCE, reading=False)

    assert (problem.check, problem.actual) == ("audit.reader_grant", "s1_readers:EXECUTE")


def test_a_changed_body_is_a_source_problem() -> None:
    """Review F1: startup compares the body with the released one, as ``verify`` does."""
    changed = _Reader(source=SOURCE + " -- and more")

    assert _checks(reader_problems([changed], SOURCE, reading=True)) == ["audit.reader_source"]
