"""The SQLAlchemy audit-log reader and its binding (spec 009, T13).

The reader calls ``<schema>.read_audit_log`` with one Core statement. These
tests compile that statement with the PostgreSQL dialects, and drive the
reader through a recording session that stands in for Postgres; the guard
function itself is exercised against real Postgres by the RLS integration
suite.
"""

from __future__ import annotations

import logging
import re
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from sqlalchemy import Column, MetaData, Table, select
from sqlalchemy import Text as SAText
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg
from sqlalchemy.exc import CompileError, DBAPIError, StatementError

from loom.core.audit import (
    ALL_SUBJECTS,
    OWN,
    AuditActors,
    AuditEntry,
    AuditLogReader,
    AuditReadFailed,
    AuditSubjectsInvalid,
    audit_entry_for,
)
from loom.core.config import ConfigContext, ConfigError
from loom.core.di.container import LoomContainer
from loom.core.errors import UnsupportedQuery
from loom.core.persistence import PersistenceWiring
from loom.core.persistence.stores import StoreKey, store_binding
from loom.core.repository.abc import (
    CursorCodec,
    CursorResult,
    FilterGroup,
    FilterOp,
    FilterSpec,
    PaginationMode,
    QuerySpec,
    SortSpec,
)
from loom.core.repository.sqlalchemy.audit.reader import (
    READ_AUDIT_LOG_CALL,
    SQLAlchemyAuditLogReader,
)
from loom.core.repository.sqlalchemy.backend import SQLAlchemyBackend
from loom.core.repository.sqlalchemy.rls.guard_manifest import READ_AUDIT_LOG_ARGUMENTS
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.rest.errors import HttpErrorMapper
from tests.helpers.persistence import build_backend

_LEDGER = StoreKey("ledger")
_SCOPE = {"loom.scope.holder": "h-1", "loom.actor.subject": "alice"}
_GUIDANCE = "model the team as a scope"
_T0 = datetime(2026, 1, 1, tzinfo=UTC)
_TEAM = Table(
    "team_member",
    MetaData(),
    Column("team_id", SAText),
    Column("member", SAText),
    Column("role", SAText),
)


def _row(at: datetime, id_: int) -> dict[str, Any]:
    return {
        "id": id_,
        "at": at,
        "op": "U",
        "schema_name": "ledger",
        "table_name": "entries",
        "row_key": '{"id":1}',
        "changed": {"note": {"old": "a", "new": "b"}},
        "subject": "alice",
        "mechanism": "jwt",
        "on_behalf_of": "",
        "trace_id": "t-1",
        "elevated": ["holder"],
        "origin": "app",
    }


class _Result:
    def __init__(self, rows: Sequence[dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> _Result:
        return self

    def all(self) -> Sequence[dict[str, Any]]:
        return self._rows


class _Session:
    """Records what the reader sends to Postgres, in order."""

    def __init__(self, rows: Sequence[dict[str, Any]], error: Exception | None) -> None:
        self._rows = rows
        self._error = error
        self.events: list[str] = []
        self.statements: list[Any] = []
        self._begun = False

    def _autobegin(self) -> None:
        if not self._begun:
            self.events.append("begin")
            self._begun = True

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[None]:
        self._autobegin()
        yield
        await self.commit()  # what SQLAlchemy does on a clean exit

    async def commit(self) -> None:
        self.events.append("commit")
        self._begun = False

    async def rollback(self) -> None:
        self.events.append("rollback")
        self._begun = False

    async def connection(self) -> _Session:
        self._autobegin()
        return self

    async def exec_driver_sql(self, statement: str) -> None:
        self.events.append(statement)

    async def execute(self, statement: Any) -> _Result:
        self.events.append("call")
        self.statements.append(statement)
        if self._error is not None:
            raise self._error
        return _Result(self._rows)


class _SessionManager:
    store = _LEDGER

    def __init__(self, rows: Sequence[dict[str, Any]] = (), error: Exception | None = None) -> None:
        self.settings: dict[str, str] = dict(_SCOPE)
        self._rows = rows
        self._error = error
        self.sessions: list[_Session] = []

    @asynccontextmanager
    async def session(self) -> AsyncIterator[_Session]:
        session = _Session(self._rows, self._error)
        self.sessions.append(session)
        yield session

    def current_settings(self) -> dict[str, str]:
        return self.settings

    @property
    def statement(self) -> Any:
        return self.sessions[-1].statements[-1]


def _codec() -> CursorCodec:
    return CursorCodec(signing="ephemeral")


def _reader(
    manager: _SessionManager,
    *,
    codec: CursorCodec | None = None,
    schema: str = "ledger",
    store: StoreKey = _LEDGER,
    max_subjects: int = 3,
) -> SQLAlchemyAuditLogReader:
    return SQLAlchemyAuditLogReader(
        cast(SessionManager, manager),
        codec or _codec(),
        schema=schema,
        store=store,
        max_subjects=max_subjects,
    )


def _query(*filters: FilterSpec, limit: int = 2, cursor: str | None = None) -> QuerySpec:
    return QuerySpec(
        filters=FilterGroup(filters) if filters else None,
        sort=(SortSpec("at", "DESC"),),
        pagination=PaginationMode.CURSOR,
        limit=limit,
        cursor=cursor,
    )


def _dbapi_error(sqlstate: str) -> DBAPIError:
    original = Exception("ERROR: secret SELECT * FROM ledger.audit_log WHERE subject = 'bob'")
    original.sqlstate = sqlstate  # type: ignore[attr-defined]
    return DBAPIError("SELECT ledger.read_audit_log($1)", ("bob",), original)


async def _read(
    reader: SQLAlchemyAuditLogReader, actors: object, query: QuerySpec | None = None
) -> CursorResult[AuditEntry]:
    return await reader.read(query or _query(), cast(AuditActors, actors))


def _arguments_of(signature: str) -> list[tuple[str, str]]:
    return [cast(tuple[str, str], tuple(a.split(" ", 1))) for a in signature.split(", ")]


# --- The call -------------------------------------------------------------------------------


def test_the_call_matches_the_guard_signature_by_name_type_and_order() -> None:
    dialect = postgresql.dialect()
    call = [(name, type_.compile(dialect=dialect).lower()) for name, type_ in READ_AUDIT_LOG_CALL]

    assert call == _arguments_of(READ_AUDIT_LOG_ARGUMENTS)


async def test_the_reader_implements_the_port() -> None:
    assert isinstance(_reader(_SessionManager()), AuditLogReader)


async def test_the_call_is_qualified_by_a_schema_postgres_stores_as_written() -> None:
    manager = _SessionManager()

    await _read(_reader(manager, schema="ledger_"), frozenset({"alice"}))

    sql = str(manager.statement.compile(dialect=PGDialect_asyncpg()))
    assert "ledger_.read_audit_log(" in sql


def test_a_schema_postgres_would_not_store_as_written_is_refused() -> None:
    with pytest.raises(ValueError, match="identifier"):
        _reader(_SessionManager(), schema="Ledger")


async def test_every_argument_is_a_typed_bind_and_the_row_key_a_jsonb_cast() -> None:
    manager = _SessionManager()
    query = _query(
        FilterSpec("row_key", FilterOp.EQ, '{"id":1}'),
        FilterSpec("table", FilterOp.IN, ("entries",)),
    )

    await _read(_reader(manager), frozenset({"alice"}), query)

    sql = str(manager.statement.compile(dialect=PGDialect_asyncpg()))
    arguments = sql[sql.index("read_audit_log(") :]
    binds = re.findall(r"\$\d+(::[A-Z ]+(?:\[\])?)?", arguments)
    assert len(binds) == len(READ_AUDIT_LOG_CALL)
    assert all(binds), arguments
    assert re.search(r"CAST\(\$9::VARCHAR AS JSONB\)", arguments), arguments


async def test_filters_window_sort_and_page_size_reach_their_arguments() -> None:
    manager = _SessionManager()
    query = QuerySpec(
        filters=FilterGroup(
            (
                FilterSpec("at", FilterOp.GTE, _T0),
                FilterSpec("at", FilterOp.LT, _T0 + timedelta(days=1)),
                FilterSpec("subject", FilterOp.EQ, "bob"),
                FilterSpec("table", FilterOp.EQ, "entries"),
                FilterSpec("trace_id", FilterOp.EQ, "t-9"),
                FilterSpec("op", FilterOp.EQ, "D"),
            )
        ),
        sort=(SortSpec("at", "ASC"),),
        pagination=PaginationMode.CURSOR,
        limit=7,
    )

    await _read(_reader(manager), frozenset({"bob", "alice"}), query)

    params = list(manager.statement.compile(dialect=postgresql.dialect()).params.values())
    assert params == [
        False,
        ["alice", "bob"],
        3,
        _T0,
        _T0 + timedelta(days=1),
        "bob",
        "entries",
        None,
        None,
        "t-9",
        "D",
        None,
        None,
        True,
        8,
    ]


async def test_a_filter_the_reader_cannot_send_is_refused_rather_than_dropped() -> None:
    manager = _SessionManager()

    with pytest.raises(UnsupportedQuery):
        await _read(
            _reader(manager), frozenset({"alice"}), _query(FilterSpec("origin", FilterOp.EQ, "x"))
        )

    assert manager.sessions == []


_SUBJECT = FilterSpec("subject", FilterOp.EQ, "bob")


@pytest.mark.parametrize(
    "query",
    [
        _query(_SUBJECT, FilterSpec("subject", FilterOp.EQ, "carla")),
        QuerySpec(
            filters=FilterGroup((_SUBJECT,), op="OR"),
            sort=(SortSpec("at", "DESC"),),
            pagination=PaginationMode.CURSOR,
            limit=2,
        ),
        QuerySpec(sort=(SortSpec("id", "DESC"),), pagination=PaginationMode.CURSOR, limit=2),
        QuerySpec(sort=(), pagination=PaginationMode.CURSOR, limit=2),
        _query(limit=0),
        _query(limit=101),
        QuerySpec(sort=(SortSpec("at", "DESC"),), pagination=PaginationMode.OFFSET, limit=2),
    ],
    ids=["repeated-filter", "or-group", "sort-by-id", "no-sort", "limit-0", "limit-101", "offset"],
)
async def test_a_query_the_function_cannot_answer_as_asked_is_refused_before_postgres(
    query: QuerySpec,
) -> None:
    manager = _SessionManager()

    with pytest.raises(UnsupportedQuery):
        await _read(_reader(manager), frozenset({"alice"}), query)

    assert manager.sessions == []


async def test_a_page_of_one_hundred_is_the_largest_sent() -> None:
    manager = _SessionManager()

    await _read(_reader(manager), frozenset({"alice"}), _query(limit=100))

    params = list(manager.statement.compile(dialect=postgresql.dialect()).params.values())
    assert params[-1] == 101


# --- Actors ---------------------------------------------------------------------------------


async def test_all_subjects_sends_p_all_and_a_null_set() -> None:
    manager = _SessionManager()

    await _read(_reader(manager), ALL_SUBJECTS)

    params = list(manager.statement.compile(dialect=postgresql.dialect()).params.values())
    assert params[:3] == [True, None, 3]


async def test_an_empty_set_is_an_empty_page_without_postgres() -> None:
    manager = _SessionManager()

    result = await _read(_reader(manager), frozenset())

    assert result == CursorResult(items=(), next_cursor=None, has_next=False)
    assert manager.sessions == []


@pytest.mark.parametrize("actors", [OWN, 42, "alice", None], ids=["own", "int", "str", "none"])
async def test_anything_but_a_set_a_select_or_all_subjects_is_invalid(actors: object) -> None:
    manager = _SessionManager()

    with pytest.raises(AuditSubjectsInvalid):
        await _read(_reader(manager), actors)

    assert manager.sessions == []


async def test_a_select_of_two_columns_is_a_500_before_postgres() -> None:
    manager = _SessionManager()

    with pytest.raises(AuditSubjectsInvalid) as raised:
        await _read(_reader(manager), select(_TEAM.c.member, _TEAM.c.role))

    assert HttpErrorMapper().to_http(raised.value).status_code == 500
    assert manager.sessions == []


async def test_a_product_select_stays_bound_wrapped_filtered_and_limited_outside() -> None:
    manager = _SessionManager()
    product = select(_TEAM.c.member).where(_TEAM.c.team_id == "team-42").limit(5)

    await _read(_reader(manager), product)

    compiled = manager.statement.compile(dialect=postgresql.dialect())
    sql = " ".join(str(compiled).split())
    assert "team-42" in compiled.params.values()
    assert "team-42" not in sql
    assert "array((SELECT" in sql
    inner = sql[sql.index("array((SELECT") :]
    wrapped = re.search(
        r"FROM \((SELECT .*?)\) AS s WHERE (.*?) LIMIT %\((\w+)\)s(?:::\w+)?\)\)", inner
    )
    assert wrapped, inner
    assert "LIMIT" in wrapped.group(1)  # the product's own limit stays inside
    assert "IS NOT NULL" in wrapped.group(2)
    assert "!=" in wrapped.group(2)
    assert compiled.params[wrapped.group(3)] == 4  # cap + 1
    assert "" in compiled.params.values()


async def test_a_set_over_the_cap_is_refused_before_postgres_and_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    manager = _SessionManager()

    with caplog.at_level(logging.WARNING), pytest.raises(AuditSubjectsInvalid) as raised:
        await _read(_reader(manager, max_subjects=3), frozenset({"a", "b", "c", "d"}))

    assert manager.sessions == []
    _assert_cap_logged_not_answered(caplog, raised.value)


async def test_a_select_over_the_cap_lg004_is_invalid_subjects_and_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    manager = _SessionManager(error=_dbapi_error("LG004"))

    with caplog.at_level(logging.WARNING), pytest.raises(AuditSubjectsInvalid) as raised:
        await _read(_reader(manager, max_subjects=3), select(_TEAM.c.member))

    _assert_cap_logged_not_answered(caplog, raised.value)


def _assert_cap_logged_not_answered(
    caplog: pytest.LogCaptureFixture, error: AuditSubjectsInvalid
) -> None:
    messages = [record.getMessage() for record in caplog.records]
    assert any("3" in message and _GUIDANCE in message for message in messages), messages
    body = str(cast(Any, HttpErrorMapper().to_http(error)).detail)
    assert _GUIDANCE not in body
    assert "3" not in body.replace(str(error.code), "")


# --- Transaction and errors (FR-013) --------------------------------------------------------


async def test_the_read_runs_read_only_under_a_statement_timeout_and_rolls_back() -> None:
    manager = _SessionManager()

    await _read(_reader(manager), frozenset({"alice"}))

    assert len(manager.sessions) == 1
    assert manager.sessions[0].events == [
        "begin",
        "SET LOCAL statement_timeout = '5s'",
        "SET TRANSACTION READ ONLY",
        "call",
        "rollback",
    ]


def _assert_no_sql_leaks(error: Exception, caplog: pytest.LogCaptureFixture) -> None:
    """Neither the error, its 500 body nor the log carry SQL, driver text or bound values."""
    assert error.__cause__ is None
    assert error.__context__ is None
    mapped = HttpErrorMapper().to_http(cast(Any, error))
    assert mapped.status_code == 500
    for leaked in ("SELECT", "secret", "bob"):
        for where in (str(error), repr(error.args), str(mapped.detail), caplog.text):
            assert leaked not in where, (leaked, where)


@pytest.mark.parametrize("sqlstate", ["42501", "42883", "57014", "LG005", "XX000"])
async def test_a_database_error_is_a_fixed_500_echoing_no_sql(
    sqlstate: str, caplog: pytest.LogCaptureFixture
) -> None:
    manager = _SessionManager(error=_dbapi_error(sqlstate))

    with caplog.at_level(logging.DEBUG), pytest.raises(AuditReadFailed) as raised:
        await _read(_reader(manager), frozenset({"alice"}))

    _assert_no_sql_leaks(raised.value, caplog)
    assert sqlstate in caplog.text


@pytest.mark.parametrize(
    "error",
    [
        StatementError(
            "(builtins.TypeError) bob is secret",
            "SELECT secret FROM ledger.read_audit_log($1)",
            {"param_1": "bob"},
            TypeError("bob is secret"),
        ),
        CompileError("SELECT secret bob"),
    ],
    ids=["statement", "compile"],
)
async def test_any_other_sqlalchemy_error_is_a_fixed_500_logging_its_type_only(
    error: Exception, caplog: pytest.LogCaptureFixture
) -> None:
    manager = _SessionManager(error=error)

    with caplog.at_level(logging.DEBUG), pytest.raises(AuditReadFailed) as raised:
        await _read(_reader(manager), frozenset({"alice"}))

    _assert_no_sql_leaks(raised.value, caplog)
    assert type(error).__name__ in caplog.text


async def test_a_timeout_is_logged_as_a_timeout(caplog: pytest.LogCaptureFixture) -> None:
    manager = _SessionManager(error=_dbapi_error("57014"))

    with caplog.at_level(logging.WARNING), pytest.raises(AuditReadFailed):
        await _read(_reader(manager), frozenset({"alice"}))

    assert "AuditReadTimeout" in caplog.text


# --- Rows and cursor (N3, N25) --------------------------------------------------------------


async def test_rows_become_store_entries_and_the_lookahead_sets_has_next() -> None:
    rows = [_row(_T0, 3), _row(_T0 - timedelta(seconds=1), 2), _row(_T0, 1)]
    manager = _SessionManager(rows)

    result = await _read(_reader(manager), frozenset({"alice"}), _query(limit=2))

    assert result.has_next is True
    assert result.next_cursor is not None
    assert [type(item) for item in result.items] == [audit_entry_for("ledger")] * 2
    first = result.items[0]
    assert (first.id, first.at, first.table, first.row_key) == (3, _T0, "entries", '{"id":1}')
    assert first.elevated == ("holder",)


async def test_the_last_page_has_no_cursor() -> None:
    manager = _SessionManager([_row(_T0, 1)])

    result = await _read(_reader(manager), frozenset({"alice"}), _query(limit=2))

    assert (result.has_next, result.next_cursor) == (False, None)


async def _next_cursor(reader: SQLAlchemyAuditLogReader, query: QuerySpec) -> str:
    result = await _read(reader, frozenset({"alice"}), query)
    assert result.next_cursor is not None
    return result.next_cursor


async def test_the_next_cursor_resumes_after_the_last_row() -> None:
    last = _T0 - timedelta(seconds=1)
    codec = _codec()
    manager = _SessionManager([_row(_T0, 3), _row(last, 2), _row(last, 1)])
    reader = _reader(manager, codec=codec)
    token = await _next_cursor(reader, _query(limit=2))

    await _read(reader, frozenset({"alice"}), _query(limit=2, cursor=token))

    params = list(manager.statement.compile(dialect=postgresql.dialect()).params.values())
    assert params[11:13] == [last, 2]


async def test_the_cursor_is_bound_to_the_store_tables_and_scope_settings() -> None:
    codec = _codec()
    manager = _SessionManager([_row(_T0, 2), _row(_T0, 1)])
    tables = FilterSpec("table", FilterOp.IN, ("entries",))
    token = await _next_cursor(_reader(manager, codec=codec), _query(tables, limit=1))

    scope = '[["loom.scope.holder","h-1"]]'
    model = "loom.audit.ledger:entries"
    assert codec.decode(
        token,
        backend="sqlalchemy",
        model=model,
        query=_query(tables, limit=1),
        scope=scope,
        columns={"at": datetime, "id": int},
        primary_key=("at", "id"),
    ).tie_breaker == (_T0, 2)


async def test_a_cursor_replayed_under_another_scope_is_refused() -> None:
    codec = _codec()
    manager = _SessionManager([_row(_T0, 2), _row(_T0, 1)])
    reader = _reader(manager, codec=codec)
    token = await _next_cursor(reader, _query(limit=1))
    manager.settings = {**_SCOPE, "loom.scope.holder": "h-2"}

    with pytest.raises(UnsupportedQuery):
        await _read(reader, frozenset({"alice"}), _query(limit=1, cursor=token))


async def test_a_cursor_replayed_on_another_store_is_refused() -> None:
    codec = _codec()
    manager = _SessionManager([_row(_T0, 2), _row(_T0, 1)])
    token = await _next_cursor(_reader(manager, codec=codec), _query(limit=1))
    other = _reader(manager, codec=codec, store=StoreKey("other"))

    with pytest.raises(UnsupportedQuery):
        await _read(other, frozenset({"alice"}), _query(limit=1, cursor=token))


async def test_a_cursor_replayed_with_other_tables_is_refused() -> None:
    codec = _codec()
    manager = _SessionManager([_row(_T0, 2), _row(_T0, 1)])
    reader = _reader(manager, codec=codec)
    token = await _next_cursor(reader, _query(FilterSpec("table", FilterOp.IN, ("a",)), limit=1))

    with pytest.raises(UnsupportedQuery):
        await _read(
            reader,
            frozenset({"alice"}),
            _query(FilterSpec("table", FilterOp.IN, ("b",)), limit=1, cursor=token),
        )


async def test_a_cursor_replayed_with_another_filter_value_is_refused() -> None:
    codec = _codec()
    manager = _SessionManager([_row(_T0, 2), _row(_T0, 1)])
    reader = _reader(manager, codec=codec)
    token = await _next_cursor(reader, _query(FilterSpec("op", FilterOp.EQ, "U"), limit=1))

    with pytest.raises(UnsupportedQuery):
        await _read(
            reader,
            frozenset({"alice"}),
            _query(FilterSpec("op", FilterOp.EQ, "D"), limit=1, cursor=token),
        )


async def test_a_cursor_replayed_with_the_other_sort_direction_is_refused() -> None:
    codec = _codec()
    manager = _SessionManager([_row(_T0, 2), _row(_T0, 1)])
    reader = _reader(manager, codec=codec)
    token = await _next_cursor(reader, _query(limit=1))
    ascending = QuerySpec(
        sort=(SortSpec("at", "ASC"),), pagination=PaginationMode.CURSOR, limit=1, cursor=token
    )

    with pytest.raises(UnsupportedQuery):
        await _read(reader, frozenset({"alice"}), ascending)


# --- Binding in SQLAlchemyBackend.build (M10, r2 M2, N6, Option C) --------------------------

_POSTGRES = "postgresql+asyncpg://app:secret@db.invalid/app"


_READING_ON: dict[str, Any] = {"audit": {"reading": {"enabled": True}}}


def _guarded(**schema: Any) -> dict[str, Any]:
    return {
        "backend": "sqlalchemy",
        "url": _POSTGRES,
        "schema": {"mode": "external", "name": "ledger", "guard": "ledger_guard", **schema},
    }


def _wiring(section: dict[str, Any], *models: type[Any]) -> PersistenceWiring:
    ctx = ConfigContext.from_dict(
        {"stores": {"ledger": section}, "persistence": {"cursors": {"signing": "ephemeral"}}}
    )
    return build_backend(SQLAlchemyBackend(), ctx, models, store=_LEDGER)


def _container(wiring: PersistenceWiring, *before: Any) -> LoomContainer:
    container = LoomContainer()
    for binding in before:
        container.register_instance(store_binding(AuditLogReader, _LEDGER), binding)
    wiring.repo_registration_module(container)
    return container


def test_a_guarded_store_of_only_the_audit_model_binds_the_reader_and_no_repository() -> None:
    model = audit_entry_for("ledger")
    container = _container(_wiring(_guarded(**_READING_ON), model))

    reader = container.resolve(store_binding(AuditLogReader, _LEDGER))
    assert isinstance(reader, SQLAlchemyAuditLogReader)
    assert container.resolve(store_binding(AuditLogReader, _LEDGER)) is reader
    assert not container.has_repo_mapping(model)
    assert (reader.schema, reader.store, reader.max_subjects) == ("ledger", _LEDGER, 1000)


def test_max_subjects_comes_from_reading() -> None:
    section = _guarded(audit={"reading": {"enabled": True, "max_subjects": 25}})

    reader = _container(_wiring(section)).resolve(store_binding(AuditLogReader, _LEDGER))

    assert reader.max_subjects == 25


def test_a_binding_made_before_the_backend_module_is_kept() -> None:
    own = object()

    container = _container(_wiring(_guarded(**_READING_ON)), own)

    assert container.resolve(store_binding(AuditLogReader, _LEDGER)) is own


def test_a_reading_store_without_a_schema_name_fails_at_build() -> None:
    section = _guarded(**_READING_ON)
    del section["schema"]["name"]

    with pytest.raises(ConfigError, match=r"stores\.ledger.*schema\.name"):
        _wiring(section)


@pytest.mark.parametrize(
    "schema",
    [{}, {"audit": {"reading": {}}}, {"audit": {"reading": {"enabled": False}}}],
    ids=["no-reading", "reading-default", "reading-disabled"],
)
def test_a_guarded_store_that_does_not_read_its_log_binds_no_reader(
    schema: dict[str, Any],
) -> None:
    """Fails closed (security review S1): no reader to inject without reading enabled."""
    container = _container(_wiring(_guarded(**schema)))

    assert not container.is_registered(store_binding(AuditLogReader, _LEDGER))


@pytest.mark.parametrize(
    "section",
    [
        {
            "backend": "sqlalchemy",
            "url": _POSTGRES,
            "schema": {"mode": "external", "name": "ledger"},
        },
        {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///"},
    ],
    ids=["unguarded-external", "sqlite"],
)
def test_a_store_without_a_guard_has_no_reader(section: dict[str, Any]) -> None:
    container = _container(_wiring(section))

    assert not container.is_registered(store_binding(AuditLogReader, _LEDGER))


def test_a_connection_only_build_binds_no_reader() -> None:
    ctx = ConfigContext.from_dict(
        {"stores": {"ledger": _guarded()}, "persistence": {"cursors": {"signing": "ephemeral"}}}
    )
    wiring = build_backend(SQLAlchemyBackend(), ctx, connection_only=True, store=_LEDGER)

    assert not _container(wiring).is_registered(store_binding(AuditLogReader, _LEDGER))
