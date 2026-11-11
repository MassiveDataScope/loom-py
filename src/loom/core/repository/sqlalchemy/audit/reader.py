"""Read the audit log through the guard's ``<schema>.read_audit_log`` (spec 009, FR-008, FR-013).

Postgres decides what a caller sees; this adapter only turns a validated
:class:`~loom.core.repository.abc.QuerySpec` and the resolved actors into one
SQLAlchemy Core call of that function, in a read-only transaction of its own
under a statement timeout that is rolled back, and turns the rows into a
cursor page. The product's subjects select runs inside that statement, so a
select that would write or lock rows fails instead.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any, Final, cast

import msgspec
from sqlalchemy import (
    BigInteger,
    Boolean,
    ColumnElement,
    DateTime,
    Integer,
    RowMapping,
    Select,
    Text,
    bindparam,
    func,
    literal_column,
    select,
)
from sqlalchemy import cast as sql_cast
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.exc import DBAPIError, SQLAlchemyError
from sqlalchemy.sql.functions import Function
from sqlalchemy.types import TypeEngine

from loom.core.audit.entry import AuditEntry, audit_entry_for
from loom.core.audit.errors import AuditReadFailed, AuditSubjectsInvalid
from loom.core.audit.subjects import ALL_SUBJECTS, AuditActors
from loom.core.errors import UnsupportedQuery
from loom.core.persistence.stores import StoreKey
from loom.core.repository.abc import (
    Cursor,
    CursorCodec,
    CursorResult,
    FilterOp,
    PaginationMode,
    QuerySpec,
)
from loom.core.repository.sqlalchemy.rls.provider import SCOPE_PREFIX
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.schema_names import schema_identifier

_logger = logging.getLogger(__name__)

_BACKEND: Final = "sqlalchemy"
_FUNCTION: Final = "read_audit_log"
_TIMESTAMP: Final = DateTime(timezone=True)
_TEXTS: Final = ARRAY(Text())
READ_AUDIT_LOG_CALL: Final[tuple[tuple[str, TypeEngine[Any]], ...]] = (
    ("p_all", Boolean()),
    ("p_subjects", _TEXTS),
    ("p_cap", Integer()),
    ("p_from", _TIMESTAMP),
    ("p_to", _TIMESTAMP),
    ("p_subject", Text()),
    ("p_table", Text()),
    ("p_tables", _TEXTS),
    ("p_row_key", JSONB()),
    ("p_trace_id", Text()),
    ("p_op", Text()),
    ("p_after_at", _TIMESTAMP),
    ("p_after_id", BigInteger()),
    ("p_ascending", Boolean()),
    ("p_limit", Integer()),
)
"""The positional arguments of ``read_audit_log``, in order, with the SQL type each binds as."""

STATEMENT_TIMEOUT: Final = "SET LOCAL statement_timeout = '5s'"
READ_ONLY: Final = "SET TRANSACTION READ ONLY"
_MAX_LIMIT: Final = 100
_CAP_EXCEEDED: Final = "LG004"
_TIMEOUT: Final = "57014"
_GUIDANCE: Final = "model the team as a scope"
_FILTER_ARGUMENTS: Final[Mapping[tuple[str, FilterOp], str]] = {
    ("subject", FilterOp.EQ): "p_subject",
    ("table", FilterOp.EQ): "p_table",
    ("table", FilterOp.IN): "p_tables",
    ("row_key", FilterOp.EQ): "p_row_key",
    ("trace_id", FilterOp.EQ): "p_trace_id",
    ("op", FilterOp.EQ): "p_op",
    ("at", FilterOp.GTE): "p_from",
    ("at", FilterOp.LT): "p_to",
}
_CURSOR_COLUMNS: Final[Mapping[str, Any]] = {"at": datetime, "id": int}
_PRIMARY_KEY: Final = ("at", "id")


class SQLAlchemyAuditLogReader:
    """The :class:`~loom.core.audit.AuditLogReader` of one guarded Postgres store.

    Args:
        session_manager: The store's session manager; every read opens its own
            session on it, never the invocation's.
        codec: The application's cursor codec.
        schema: The store's ``schema.name``, which qualifies the call.
        store: The store's key; cursors are bound to it.
        max_subjects: The largest actor set a read may send (``p_cap``).

    Raises:
        ValueError: When *schema* is not a name Postgres stores as written.
    """

    def __init__(
        self,
        session_manager: SessionManager,
        codec: CursorCodec,
        *,
        schema: str,
        store: StoreKey,
        max_subjects: int,
    ) -> None:
        self.schema = schema_identifier(schema)
        self.store = store
        self.max_subjects = max_subjects
        self._session_manager = session_manager
        self._codec = codec

    async def read(self, query: QuerySpec, actors: AuditActors) -> CursorResult[AuditEntry]:
        """Read one cursor page of the entries *actors* wrote that Postgres lets the caller see.

        Raises:
            AuditSubjectsInvalid: When *actors* is not ``ALL_SUBJECTS``, a
                ``frozenset`` of at most ``max_subjects`` subjects or a
                one-column select, or when Postgres finds the select over the cap.
            UnsupportedQuery: When the query is not one ``ReadAuditLog``
                validates (filters joined by ``AND``, each at most once and
                one the function takes; sort by ``at`` alone; a limit of 1 to
                100), or its cursor was issued for another binding.
            AuditReadFailed: On any other database or SQLAlchemy error.
        """
        _check_shape(query)
        subjects = self._subjects(actors)
        if subjects is None:
            return CursorResult(items=(), next_cursor=None, has_next=False)
        arguments = _filter_arguments(query)
        binding = f"loom.audit.{self.store}" + _tables_suffix(arguments.get("p_tables"))
        scope = self._cursor_scope()
        if query.cursor is not None:
            after = self._codec.decode(
                query.cursor,
                backend=_BACKEND,
                model=binding,
                query=query,
                scope=scope,
                columns=_CURSOR_COLUMNS,
                primary_key=_PRIMARY_KEY,
            ).tie_breaker
            arguments["p_after_at"], arguments["p_after_id"] = cast(tuple[datetime, int], after)
        arguments |= subjects
        arguments["p_cap"] = self.max_subjects
        arguments["p_ascending"] = query.sort[0].direction == "ASC"
        arguments["p_limit"] = query.limit + 1
        rows = await self._fetch(self._statement(arguments))
        return self._page(rows, query, binding, scope)

    def _subjects(self, actors: object) -> dict[str, Any] | None:
        """``p_all`` and ``p_subjects`` for *actors*; ``None`` for an empty set."""
        if actors is ALL_SUBJECTS:
            return {"p_all": True, "p_subjects": None}
        if isinstance(actors, frozenset):
            subjects = cast(frozenset[str], actors)
            if len(subjects) > self.max_subjects:
                self._log_cap(f"actor set of {len(subjects)} subjects")
                raise AuditSubjectsInvalid()
            return {"p_all": False, "p_subjects": sorted(subjects)} if subjects else None
        if isinstance(actors, Select):
            return {"p_all": False, "p_subjects": self._selected(cast(Select[Any], actors))}
        _logger.warning("AuditSubjectsInvalid: %s reached the reader", type(actors).__name__)
        raise AuditSubjectsInvalid()

    def _selected(self, product: Select[Any]) -> ColumnElement[Any]:
        """The product's one-column select as an ``ARRAY(subquery)`` of non-empty subjects.

        The product's select is wrapped, so its own ``LIMIT``, ``DISTINCT`` or
        compound parts keep their meaning; the filter and the limit apply outside.
        """
        if len(product.selected_columns) != 1:
            _logger.warning("AuditSubjectsInvalid: the subjects select has more than one column")
            raise AuditSubjectsInvalid()
        wrapped = product.subquery("s")
        subject = sql_cast(wrapped.c[0], Text())
        members = (
            select(subject)
            .select_from(wrapped)
            .where(wrapped.c[0].is_not(None), subject != "")
            .limit(self.max_subjects + 1)
        )
        return func.array(members.scalar_subquery(), type_=_TEXTS)

    def _statement(self, arguments: Mapping[str, Any]) -> Select[Any]:
        """``SELECT * FROM <schema>.read_audit_log(...)`` with every argument typed."""
        call: Function[Any] = Function(
            _FUNCTION,
            *(_argument(arguments.get(name), type_) for name, type_ in READ_AUDIT_LOG_CALL),
            packagenames=(self.schema,),
        )
        return select(literal_column("*")).select_from(call)

    async def _fetch(self, statement: Select[Any]) -> Sequence[RowMapping]:
        """Run *statement* read-only under :data:`STATEMENT_TIMEOUT`, then roll back.

        The error raised carries no cause or context: a SQLAlchemy error's
        text holds the SQL and its bound values.
        """
        failure: AuditSubjectsInvalid | AuditReadFailed
        try:
            async with self._session_manager.session() as session:
                connection = await session.connection()
                await connection.exec_driver_sql(STATEMENT_TIMEOUT)
                await connection.exec_driver_sql(READ_ONLY)
                rows: Sequence[RowMapping] = (await session.execute(statement)).mappings().all()
                await session.rollback()
                return rows
        except SQLAlchemyError as exc:
            failure = self._read_error(exc)
        raise failure

    def _read_error(self, exc: SQLAlchemyError) -> AuditSubjectsInvalid | AuditReadFailed:
        """Log *exc* by type and SQLSTATE only; its message may hold SQL and bound values."""
        sqlstate = _sqlstate(exc) if isinstance(exc, DBAPIError) else None
        if sqlstate == _CAP_EXCEEDED:
            self._log_cap(f"subjects select (SQLSTATE {sqlstate})")
            return AuditSubjectsInvalid()
        kind = "AuditReadTimeout" if sqlstate == _TIMEOUT else "AuditReadFailed"
        _logger.error(
            "%s: store %s, %s, SQLSTATE %s", kind, self.store, type(exc).__name__, sqlstate
        )
        return AuditReadFailed()

    def _log_cap(self, what: str) -> None:
        _logger.warning(
            "AuditSubjectsInvalid: store %s: %s over max_subjects %d; %s",
            self.store,
            what,
            self.max_subjects,
            _GUIDANCE,
        )

    def _cursor_scope(self) -> str:
        """The ``loom.scope.*`` settings a transaction opened now starts with, canonically."""
        settings = self._session_manager.current_settings()
        scoped = sorted((k, v) for k, v in settings.items() if k.startswith(SCOPE_PREFIX))
        return msgspec.json.encode(scoped).decode() if scoped else ""

    def _page(
        self, rows: Sequence[RowMapping], query: QuerySpec, binding: str, scope: str
    ) -> CursorResult[AuditEntry]:
        model = audit_entry_for(self.store)
        items = tuple(_entry(model, row) for row in rows[: query.limit])
        if len(rows) <= query.limit:
            return CursorResult(items=items, next_cursor=None, has_next=False)
        last = items[-1]
        position = Cursor(backend=_BACKEND, keys=(last.at,), tie_breaker=(last.at, last.id))
        token = self._codec.encode(position, model=binding, query=query, scope=scope)
        return CursorResult(items=items, next_cursor=token, has_next=True)


def _unsupported(reason: str) -> UnsupportedQuery:
    return UnsupportedQuery(reason, model="AuditEntry")


def _check_shape(query: QuerySpec) -> None:
    """Refuse a query the function cannot answer as asked; the port is public in DI.

    Raises:
        UnsupportedQuery: For pagination other than cursor, filters not
            joined by ``AND``, a sort other than ``at`` alone, or a limit
            outside 1 to 100.
    """
    if query.pagination is not PaginationMode.CURSOR:
        raise _unsupported("audit reads require cursor pagination")
    if query.filters is not None and query.filters.op != "AND":
        raise _unsupported("audit filters must use AND")
    if len(query.sort) != 1 or query.sort[0].field != "at":
        raise _unsupported("audit sort supports at only")
    if not 1 <= query.limit <= _MAX_LIMIT:
        raise _unsupported(f"limit must be between 1 and {_MAX_LIMIT}")


def _filter_arguments(query: QuerySpec) -> dict[str, Any]:
    """The arguments the query's filters set.

    Raises:
        UnsupportedQuery: For a filter the function has no argument for, or
            one given twice; dropping either would change the read.
    """
    arguments: dict[str, Any] = {}
    for item in query.filters.filters if query.filters is not None else ():
        name = _FILTER_ARGUMENTS.get((item.field, item.op))
        if name is None or name in arguments:
            raise _unsupported(f"unsupported filter on {item.field}")
        arguments[name] = sorted(item.value) if item.op is FilterOp.IN else item.value
    return arguments


def _tables_suffix(tables: Sequence[str] | None) -> str:
    return f":{','.join(tables)}" if tables else ""


def _argument(value: Any, type_: TypeEngine[Any]) -> ColumnElement[Any]:
    """*value* as a bind parameter of *type_*, NULL included; ``jsonb`` binds text and casts it."""
    if isinstance(value, ColumnElement):
        return value
    if isinstance(type_, JSONB):
        return sql_cast(bindparam(None, value, type_=Text()), type_)
    return bindparam(None, value, type_=type_)


def _sqlstate(exc: DBAPIError) -> str | None:
    original = exc.orig
    return getattr(original, "sqlstate", None) or getattr(original, "pgcode", None)


def _entry(model: type[AuditEntry], row: RowMapping) -> AuditEntry:
    return model(
        at=row["at"],
        id=row["id"],
        op=row["op"],
        schema_name=row["schema_name"],
        table=row["table_name"],
        row_key=row["row_key"],
        changed=row["changed"],
        subject=row["subject"],
        mechanism=row["mechanism"],
        on_behalf_of=row["on_behalf_of"],
        trace_id=row["trace_id"],
        elevated=tuple(row["elevated"]),
        origin=row["origin"],
    )


__all__ = ["READ_AUDIT_LOG_CALL", "READ_ONLY", "STATEMENT_TIMEOUT", "SQLAlchemyAuditLogReader"]
