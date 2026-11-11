"""Validated, store-bound audit log reads."""

# pyright: strict

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta
from functools import cache
from typing import Annotated, Any, cast

from msgspec.structs import replace

from loom.core.audit.entry import AuditEntry, audit_entry_for
from loom.core.audit.errors import AuditSubjectsInvalid
from loom.core.audit.ports import AuditLogReader
from loom.core.audit.subjects import ALL_SUBJECTS, OWN, AuditActors, AuditSubjects
from loom.core.errors import Forbidden, Unauthenticated, UnsupportedQuery
from loom.core.identity import current_identity
from loom.core.persistence.stores import InStore, StoreKey, store_key
from loom.core.repository.abc import (
    CursorResult,
    FilterGroup,
    FilterOp,
    FilterSpec,
    PaginationMode,
    QuerySpec,
    SortSpec,
)
from loom.core.use_case.use_case import UseCase

_LOG = logging.getLogger(__name__)
_EQ_FIELDS = frozenset({"subject", "table", "row_key", "trace_id", "op"})
_MAX_WINDOW = timedelta(days=31)
_MAX_LIMIT = 100
_MAX_ROW_KEY = 4096
_VALID_OPS = frozenset({"I", "U", "D"})
_TABLE_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _unsupported(reason: str) -> UnsupportedQuery:
    return UnsupportedQuery(reason, model="AuditEntry")


def _at_value(value: object) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise _unsupported("at bounds must be timezone-aware")
        return value
    raise _unsupported("at bounds must be datetimes")


def _validated_eq_filter(item: FilterSpec, seen: set[tuple[str, FilterOp]]) -> FilterSpec:
    if item.op is not FilterOp.EQ or (item.field, item.op) in seen:
        raise _unsupported(f"unsupported filter on {item.field}")
    if item.field == "op" and item.value not in _VALID_OPS:
        raise _unsupported("op must be I, U or D")
    if item.field == "row_key":
        if isinstance(item.value, str) and len(item.value) > _MAX_ROW_KEY:
            raise _unsupported(f"row_key must be at most {_MAX_ROW_KEY} characters")
        try:
            parsed = json.loads(item.value)
        except (TypeError, ValueError, RecursionError) as exc:
            raise _unsupported("row_key must be JSON") from exc
        return replace(item, value=json.dumps(parsed, sort_keys=True, separators=(",", ":")))
    return item


def _validated_window(bounds: dict[FilterOp, datetime]) -> None:
    if FilterOp.LT not in bounds or FilterOp.GTE not in bounds:
        return
    span = bounds[FilterOp.LT] - bounds[FilterOp.GTE]
    if span <= timedelta(0) or span > _MAX_WINDOW:
        raise _unsupported("at range must be no more than 31 days")


def _validated_item(
    item: FilterSpec, seen: set[tuple[str, FilterOp]], bounds: dict[FilterOp, datetime]
) -> FilterSpec:
    if item.field in _EQ_FIELDS:
        return _validated_eq_filter(item, seen)
    if item.field != "at" or item.op not in (FilterOp.GTE, FilterOp.LT):
        raise _unsupported(f"unsupported filter on {item.field}")
    if (item.field, item.op) in seen:
        raise _unsupported("duplicate at bound")
    bounds[item.op] = _at_value(item.value)
    return item


def _validated_filters(group: FilterGroup | None) -> list[FilterSpec]:
    if group is not None and group.op != "AND":
        raise _unsupported("audit filters must use AND")
    seen: set[tuple[str, FilterOp]] = set()
    bounds: dict[FilterOp, datetime] = {}
    filters: list[FilterSpec] = []
    for item in group.filters if group is not None else ():
        filters.append(_validated_item(item, seen, bounds))
        seen.add((item.field, item.op))
    _validated_window(bounds)
    return filters


def _validated_sort(query: QuerySpec) -> tuple[SortSpec, ...]:
    if len(query.sort) > 1 or any(
        item.field != "at" or item.direction not in ("ASC", "DESC") for item in query.sort
    ):
        raise _unsupported("audit sort supports at only")
    return query.sort or (SortSpec("at", "DESC"),)


def _validated(query: QuerySpec, tables: tuple[str, ...]) -> QuerySpec:
    if query.pagination is not PaginationMode.CURSOR:
        raise _unsupported("audit reads require cursor pagination")
    if query.limit < 1:
        raise _unsupported("limit must be positive")
    filters = _validated_filters(query.filters)
    sort = _validated_sort(query)
    if tables:
        filters.append(FilterSpec("table", FilterOp.IN, tables))
    return replace(
        query,
        filters=FilterGroup(tuple(filters)) if filters else None,
        sort=sort,
        limit=min(query.limit, _MAX_LIMIT),
    )


def _actors(value: object, subject: str) -> AuditActors:
    if value is OWN:
        return frozenset({subject})
    if value is ALL_SUBJECTS:
        return ALL_SUBJECTS
    if isinstance(value, frozenset):
        items = cast(frozenset[object], value)
        if any(not isinstance(item, str) or not item for item in items):
            raise AuditSubjectsInvalid()
        return cast(frozenset[str], items)
    if isinstance(value, (str, set, list, tuple, dict)) or value is None:
        raise AuditSubjectsInvalid()
    return cast(AuditActors, value)


class ReadAuditLog(UseCase[Any, CursorResult[AuditEntry]]):
    """Read visible audit entries after validating the portable query contract."""

    read_only = True

    def __init__(
        self,
        reader: AuditLogReader,
        subjects: AuditSubjects,
        store: StoreKey,
        tables: tuple[str, ...] = (),
    ) -> None:
        self.reader = reader
        self.subjects = subjects
        self.store = store
        self.tables = tables

    async def execute(self, query: QuerySpec) -> CursorResult[AuditEntry]:
        identity = current_identity()
        if not identity.is_authenticated:
            raise Unauthenticated()
        validated = _validated(query, self.tables)
        try:
            result = await self.subjects.audit_subjects(identity, self.store)
        except (Forbidden, Unauthenticated):
            raise
        except Exception as exc:
            _LOG.exception("AuditSubjects hook raised for store %s", self.store)
            raise AuditSubjectsInvalid() from exc
        actors = _actors(result, identity.subject)
        if isinstance(actors, frozenset) and not actors:
            return CursorResult(items=(), next_cursor=None, has_next=False)
        return await self.reader.read(validated, actors)

    @staticmethod
    def for_store(key: str | StoreKey, tables: tuple[str, ...] = ()) -> type[ReadAuditLog]:
        """Return the cached use case for a store and optional table restriction."""
        if any(not isinstance(cast(object, table), str) for table in tables):
            raise ValueError("audit tables must be SQL identifiers of at most 63 characters")
        return _for_store(store_key(key), tuple(sorted(set(tables))))


@cache
def _for_store(key: StoreKey, tables: tuple[str, ...]) -> type[ReadAuditLog]:
    if any(len(table) > 63 or not _TABLE_PATTERN.fullmatch(table) for table in tables):
        raise ValueError("audit tables must be SQL identifiers of at most 63 characters")
    model = audit_entry_for(key)

    def __init__(self: ReadAuditLog, reader: AuditLogReader, subjects: AuditSubjects) -> None:
        ReadAuditLog.__init__(self, reader, subjects, key, tables)

    __init__.__annotations__["reader"] = Annotated[AuditLogReader, InStore(key)]

    async def execute(self: ReadAuditLog, query: QuerySpec) -> CursorResult[AuditEntry]:
        return await ReadAuditLog.execute(self, query)

    # The REST compiler publishes the result named here: one schema per store.
    result = CursorResult[model]  # type: ignore[valid-type]
    execute.__annotations__.update({"query": QuerySpec, "return": result})
    generated = cast(
        type[ReadAuditLog],
        type(
            f"ReadAuditLog_{key}" + (f"__{'__'.join(tables)}" if tables else ""),
            (ReadAuditLog,),
            {
                "__init__": __init__,
                "execute": execute,
                # Inherited by a product subclass, unlike the main model.
                "__audit_store__": key,
                "__module__": __name__,
            },
        ),
    )
    # UseCase.__init_subclass__ initializes the metadata as the class is built.
    # Set the generated store model after that hook has run.
    generated.__loom_main_model__ = model
    return generated
