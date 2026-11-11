"""ReadAuditLog contract at the core boundary."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any, cast, get_args, get_type_hints

import pytest

from loom.core.audit import (
    ALL_SUBJECTS,
    OWN,
    AuditActors,
    AuditEntry,
    AuditLogReader,
    AuditSubjects,
    ReadAuditLog,
    audit_entry_for,
)
from loom.core.audit.errors import AuditSubjectsInvalid
from loom.core.di.container import LoomContainer
from loom.core.discovery._utils import infer_model_from_use_case
from loom.core.engine.compiler import _primary_store
from loom.core.errors import Forbidden, NotFound, Unauthenticated, UnsupportedQuery
from loom.core.identity import Identity, reset_identity, set_identity
from loom.core.persistence.stores import InStore, StoreKey, store_binding
from loom.core.repository.abc import (
    CursorResult,
    FilterGroup,
    FilterOp,
    FilterSpec,
    PaginationMode,
    QuerySpec,
    SortSpec,
)
from loom.core.use_case.factory import UseCaseFactory


class Reader:
    def __init__(self) -> None:
        self.calls: list[tuple[QuerySpec, object]] = []

    async def read(self, query: QuerySpec, actors: object) -> CursorResult[AuditEntry]:
        self.calls.append((query, actors))
        return CursorResult(items=(), next_cursor=None, has_next=False)


class Hook:
    def __init__(self, value: object = OWN, error: Exception | None = None) -> None:
        self.value = value
        self.error = error
        self.calls = 0

    async def audit_subjects(self, identity: Identity, store: StoreKey) -> AuditActors:
        self.calls += 1
        if self.error is not None:
            raise self.error
        return cast(AuditActors, self.value)


def invoke(
    query: QuerySpec,
    *,
    hook: Hook | None = None,
    subject: str | None = "alice",
    tables: tuple[str, ...] = (),
    reader: Reader | None = None,
) -> tuple[Reader, CursorResult[AuditEntry]]:
    reader = reader or Reader()
    use_case = ReadAuditLog(reader, hook or Hook(), StoreKey("ledger"), tables)
    token = set_identity(Identity(subject=subject)) if subject is not None else None
    try:
        result = asyncio.run(use_case.execute(query))
    finally:
        if token is not None:
            reset_identity(token)
    return reader, result


def test_for_store_caches_and_exposes_ledger_model_and_primary_store() -> None:
    use_case = ReadAuditLog.for_store("ledger")
    assert use_case is ReadAuditLog.for_store(StoreKey("ledger"))
    assert use_case is not ReadAuditLog.for_store("ledger", tables=("orders",))
    model = infer_model_from_use_case(cast(Any, use_case))
    assert model is not None and model.__name__ == "AuditEntry_ledger"
    assert _primary_store(use_case) == StoreKey("ledger")


def test_for_store_normalizes_tables_and_names_the_bound_class() -> None:
    first = ReadAuditLog.for_store("ledger", ("b", "a"))
    assert first is ReadAuditLog.for_store("ledger", ("a", "b", "a"))
    assert first.__name__ == "ReadAuditLog_ledger__a__b"
    assert ReadAuditLog.for_store("ledger", ("a_b",)).__name__ == "ReadAuditLog_ledger__a_b"


def _row_key_query(value: str) -> QuerySpec:
    return QuerySpec(
        pagination=PaginationMode.CURSOR,
        filters=FilterGroup((FilterSpec("row_key", FilterOp.EQ, value),)),
    )


@pytest.mark.parametrize(
    "value",
    ["[" * 100_000 + "]" * 100_000, '"' + "k" * 4095 + '"'],
    ids=["deeply-nested", "over-4096-characters"],
)
def test_a_row_key_over_4096_characters_is_an_unsupported_query(value: str) -> None:
    """Security review S6: refused as 400 before it is parsed."""
    reader = Reader()

    with pytest.raises(UnsupportedQuery, match="row_key"):
        invoke(_row_key_query(value), reader=reader)

    assert reader.calls == []


def test_a_row_key_that_exhausts_the_parser_stack_is_an_unsupported_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Security review S6: a stack limit lower than the cap allows is a 400, not a 500."""

    def too_deep(_: str) -> object:
        raise RecursionError("maximum recursion depth exceeded while decoding a JSON array")

    monkeypatch.setattr(json, "loads", too_deep)

    with pytest.raises(UnsupportedQuery, match="row_key"):
        invoke(_row_key_query("[[1]]"))


def test_a_row_key_of_4096_characters_is_accepted() -> None:
    reader, _ = invoke(_row_key_query('"' + "k" * 4094 + '"'))

    assert len(reader.calls) == 1


def test_each_store_publishes_its_own_result_model() -> None:
    """Site 8: the compiler reads ``execute``'s return annotation; it names the store's model."""
    for key in ("ledger", "books"):
        returned = get_type_hints(ReadAuditLog.for_store(key).execute)["return"]
        assert returned == CursorResult[audit_entry_for(key)]
    assert get_type_hints(ReadAuditLog.execute)["return"] == CursorResult[AuditEntry]


def test_the_generated_execute_runs_the_base_contract() -> None:
    reader = Reader()
    use_case = cast(Any, ReadAuditLog.for_store("ledger"))(reader, Hook())
    token = set_identity(Identity(subject="alice"))
    try:
        result = asyncio.run(use_case.execute(QuerySpec(pagination=PaginationMode.CURSOR)))
    finally:
        reset_identity(token)
    assert result.items == ()
    assert reader.calls[0][1] == frozenset({"alice"})


@pytest.mark.parametrize(
    "tables", [("",), ("a", 3), (None,), ("bad-name",), ("a" * 64,), ("1table",), ("has space",)]
)
def test_for_store_rejects_invalid_tables(tables: tuple[str, ...]) -> None:
    with pytest.raises(ValueError):
        ReadAuditLog.for_store("ledger", cast(Any, tables))


def test_generated_constructor_has_real_annotated_reader_and_factory_resolves_it() -> None:
    use_case = ReadAuditLog.for_store("ledger")
    annotation = get_type_hints(use_case.__init__, include_extras=True)["reader"]
    assert get_args(annotation)[0] is AuditLogReader
    assert get_args(annotation)[1] == InStore(StoreKey("ledger"))
    reader = Reader()
    container = LoomContainer()
    container.register_instance(store_binding(AuditLogReader, StoreKey("ledger")), reader)
    container.register_instance(AuditSubjects, Hook())
    factory = UseCaseFactory(container)
    factory.register(use_case)
    factory.verify()
    assert factory.build(use_case).reader is reader


def test_anonymous_caller_is_refused_before_reader() -> None:
    reader = Reader()
    with pytest.raises(Unauthenticated):
        invoke(QuerySpec(pagination=PaginationMode.CURSOR), subject=None, reader=reader)
    assert reader.calls == []


@pytest.mark.parametrize(
    "query",
    [
        QuerySpec(pagination=PaginationMode.OFFSET),
        QuerySpec(pagination=PaginationMode.CURSOR, filters=FilterGroup((), op="OR")),
        QuerySpec(
            pagination=PaginationMode.CURSOR,
            filters=FilterGroup((FilterSpec("id", FilterOp.EQ, 1),)),
        ),
        QuerySpec(
            pagination=PaginationMode.CURSOR,
            filters=FilterGroup((FilterSpec("subject", FilterOp.LIKE, "a"),)),
        ),
        QuerySpec(
            pagination=PaginationMode.CURSOR,
            filters=FilterGroup((FilterSpec("op", FilterOp.EQ, "X"),)),
        ),
        QuerySpec(
            pagination=PaginationMode.CURSOR,
            filters=FilterGroup((FilterSpec("row_key", FilterOp.EQ, "{"),)),
        ),
        QuerySpec(
            pagination=PaginationMode.CURSOR,
            filters=FilterGroup(
                (FilterSpec("subject", FilterOp.EQ, "a"), FilterSpec("subject", FilterOp.EQ, "b"))
            ),
        ),
        QuerySpec(pagination=PaginationMode.CURSOR, sort=(SortSpec("id"),)),
        QuerySpec(
            pagination=PaginationMode.CURSOR,
            filters=FilterGroup(
                (
                    FilterSpec("at", FilterOp.GTE, datetime(2024, 1, 1, tzinfo=UTC)),
                    FilterSpec("at", FilterOp.LT, datetime(2024, 2, 2, tzinfo=UTC)),
                )
            ),
        ),
        QuerySpec(
            pagination=PaginationMode.CURSOR,
            filters=FilterGroup(
                (
                    FilterSpec("at", FilterOp.GTE, datetime(2024, 1, 1, tzinfo=UTC)),
                    FilterSpec("at", FilterOp.LT, datetime(2024, 1, 1, tzinfo=UTC)),
                )
            ),
        ),
        QuerySpec(
            pagination=PaginationMode.CURSOR,
            filters=FilterGroup(
                (
                    FilterSpec("at", FilterOp.GTE, datetime(2024, 1, 2, tzinfo=UTC)),
                    FilterSpec("at", FilterOp.LT, datetime(2024, 1, 1, tzinfo=UTC)),
                )
            ),
        ),
    ],
)
def test_unsupported_queries_are_refused_before_reader(query: QuerySpec) -> None:
    reader = Reader()
    with pytest.raises(UnsupportedQuery):
        invoke(query, reader=reader)
    assert reader.calls == []


def test_default_sort_limit_and_tables_are_forwarded_as_server_filters() -> None:
    reader, _ = invoke(QuerySpec(pagination=PaginationMode.CURSOR, limit=500), tables=("orders",))
    query, actors = reader.calls[0]
    assert query.sort == (SortSpec("at", "DESC"),)
    assert query.limit == 100
    assert query.filters is not None
    assert query.filters.filters[-1] == FilterSpec("table", FilterOp.IN, ("orders",))
    assert actors == frozenset({"alice"})


def test_client_table_filter_cannot_remove_server_table_restriction() -> None:
    client_filter = FilterSpec("table", FilterOp.EQ, "payments")
    reader, _ = invoke(
        QuerySpec(
            pagination=PaginationMode.CURSOR,
            filters=FilterGroup((client_filter,)),
        ),
        tables=("orders",),
    )
    forwarded, _ = reader.calls[0]
    assert forwarded.filters is not None
    assert forwarded.filters.op == "AND"
    assert forwarded.filters.filters[0] == client_filter
    assert forwarded.filters.filters[-1] == FilterSpec("table", FilterOp.IN, ("orders",))


_NOW = datetime(2024, 2, 1, tzinfo=UTC)


def _at_bounds(reader: Reader) -> dict[FilterOp, datetime]:
    query, _ = reader.calls[0]
    return {
        item.op: item.value
        for item in (query.filters.filters if query.filters is not None else ())
        if item.field == "at"
    }


def test_no_at_bound_is_forwarded_without_at_filters() -> None:
    reader, _ = invoke(QuerySpec(pagination=PaginationMode.CURSOR))
    assert _at_bounds(reader) == {}


def test_lt_only_is_forwarded() -> None:
    lt = _NOW - timedelta(days=2)
    reader, _ = invoke(
        QuerySpec(
            pagination=PaginationMode.CURSOR,
            filters=FilterGroup((FilterSpec("at", FilterOp.LT, lt),)),
        )
    )
    assert _at_bounds(reader) == {FilterOp.LT: lt}


def test_gte_only_is_forwarded() -> None:
    gte = _NOW - timedelta(days=2)
    reader, _ = invoke(
        QuerySpec(
            pagination=PaginationMode.CURSOR,
            filters=FilterGroup((FilterSpec("at", FilterOp.GTE, gte),)),
        )
    )
    assert _at_bounds(reader) == {FilterOp.GTE: gte}


def test_gte_only_older_than_31_days_is_forwarded() -> None:
    query = QuerySpec(
        pagination=PaginationMode.CURSOR,
        filters=FilterGroup((FilterSpec("at", FilterOp.GTE, _NOW - timedelta(days=32)),)),
    )
    reader, _ = invoke(query)
    assert _at_bounds(reader) == {FilterOp.GTE: _NOW - timedelta(days=32)}


@pytest.mark.parametrize("op", [FilterOp.GTE, FilterOp.LT])
def test_naive_at_bound_is_refused(op: FilterOp) -> None:
    query = QuerySpec(
        pagination=PaginationMode.CURSOR,
        filters=FilterGroup((FilterSpec("at", op, datetime(2024, 1, 1)),)),
    )
    with pytest.raises(UnsupportedQuery, match="timezone-aware"):
        invoke(query)


@pytest.mark.parametrize("value", ["alice", frozenset({""}), {"alice"}, None])
def test_invalid_hook_result_is_500_before_reader(value: object) -> None:
    reader = Reader()
    with pytest.raises(AuditSubjectsInvalid):
        invoke(QuerySpec(pagination=PaginationMode.CURSOR), hook=Hook(value), reader=reader)
    assert reader.calls == []


@pytest.mark.parametrize("error", [RuntimeError("secret"), ValueError("secret")])
def test_hook_exception_is_500_before_reader(error: Exception) -> None:
    reader = Reader()
    with pytest.raises(AuditSubjectsInvalid):
        invoke(
            QuerySpec(pagination=PaginationMode.CURSOR),
            hook=Hook(error=error),
            reader=reader,
        )
    assert reader.calls == []


def test_hook_forbidden_propagates_before_reader() -> None:
    reader = Reader()
    error = Forbidden()
    with pytest.raises(Forbidden) as raised:
        invoke(QuerySpec(pagination=PaginationMode.CURSOR), hook=Hook(error=error), reader=reader)
    assert raised.value is error
    assert reader.calls == []


def test_hook_unauthenticated_propagates_before_reader() -> None:
    reader = Reader()
    error = Unauthenticated()
    with pytest.raises(Unauthenticated) as raised:
        invoke(QuerySpec(pagination=PaginationMode.CURSOR), hook=Hook(error=error), reader=reader)
    assert raised.value is error
    assert reader.calls == []


def test_hook_other_loom_error_is_500_before_reader() -> None:
    reader = Reader()
    with pytest.raises(AuditSubjectsInvalid):
        invoke(
            QuerySpec(pagination=PaginationMode.CURSOR),
            hook=Hook(error=NotFound("AuditEntry", id=1)),
            reader=reader,
        )
    assert reader.calls == []


def test_empty_subject_with_own_is_401() -> None:
    reader = Reader()
    hook = Hook(OWN)
    with pytest.raises(Unauthenticated):
        invoke(QuerySpec(pagination=PaginationMode.CURSOR), hook=hook, subject="", reader=reader)
    assert hook.calls == 0
    assert reader.calls == []


def test_unauthenticated_with_an_invalid_query_is_401_not_400() -> None:
    reader = Reader()
    query = QuerySpec(pagination=PaginationMode.OFFSET)
    with pytest.raises(Unauthenticated):
        invoke(query, subject="", reader=reader)
    assert reader.calls == []


@pytest.mark.parametrize("actors", [ALL_SUBJECTS, frozenset({"alice"})])
def test_empty_subject_is_401_before_hook_and_reader(actors: object) -> None:
    reader = Reader()
    hook = Hook(actors)
    with pytest.raises(Unauthenticated):
        invoke(QuerySpec(pagination=PaginationMode.CURSOR), hook=hook, subject="", reader=reader)
    assert hook.calls == 0
    assert reader.calls == []


def test_empty_frozenset_returns_empty_page_without_reader() -> None:
    reader, result = invoke(QuerySpec(pagination=PaginationMode.CURSOR), hook=Hook(frozenset()))
    assert result.items == ()
    assert reader.calls == []


def test_at_iso_string_is_refused_before_reader() -> None:
    reader = Reader()
    query = QuerySpec(
        pagination=PaginationMode.CURSOR,
        filters=FilterGroup((FilterSpec("at", FilterOp.GTE, "2024-01-01T00:00:00+00:00"),)),
    )
    with pytest.raises(UnsupportedQuery, match="at bounds must be datetimes"):
        invoke(query, reader=reader)
    assert reader.calls == []


def test_mixed_timezone_at_bounds_are_refused_before_reader() -> None:
    reader = Reader()
    query = QuerySpec(
        pagination=PaginationMode.CURSOR,
        filters=FilterGroup(
            (
                FilterSpec("at", FilterOp.GTE, datetime(2024, 1, 1)),
                FilterSpec("at", FilterOp.LT, datetime(2024, 1, 15, tzinfo=UTC)),
            )
        ),
    )
    with pytest.raises(UnsupportedQuery, match="timezone-aware"):
        invoke(query, reader=reader)
    assert reader.calls == []


def test_duplicate_at_gte_bound_is_refused_before_reader() -> None:
    reader = Reader()
    query = QuerySpec(
        pagination=PaginationMode.CURSOR,
        filters=FilterGroup(
            (
                FilterSpec("at", FilterOp.GTE, datetime(2024, 1, 1, tzinfo=UTC)),
                FilterSpec("at", FilterOp.GTE, datetime(2024, 1, 2, tzinfo=UTC)),
            )
        ),
    )
    with pytest.raises(UnsupportedQuery, match="duplicate at bound"):
        invoke(query, reader=reader)
    assert reader.calls == []


def test_zero_limit_is_refused_before_reader() -> None:
    reader = Reader()
    with pytest.raises(UnsupportedQuery, match="limit must be positive"):
        invoke(QuerySpec(pagination=PaginationMode.CURSOR, limit=0), reader=reader)
    assert reader.calls == []


def test_all_subjects_hook_is_forwarded_to_the_reader() -> None:
    reader, _ = invoke(QuerySpec(pagination=PaginationMode.CURSOR), hook=Hook(ALL_SUBJECTS))
    assert len(reader.calls) == 1
    _, actors = reader.calls[0]
    assert actors is ALL_SUBJECTS
