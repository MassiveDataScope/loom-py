"""DynamoDB source builder — FromDynamoDb."""

from __future__ import annotations

import re
from typing import Any

from loom.core.expr.nodes import AndExpr, ExprNode
from loom.etl.declarative.source._specs import DynamoDbSourceSpec, ExtraFieldsMode
from loom.etl.schema._contract import SchemaContract, resolve_schema
from loom.etl.schema._schema import ColumnSchema

_TABLE_RE = re.compile(r"^[a-zA-Z0-9_.\-]{3,255}$")


class FromDynamoDb:
    """Declare a DynamoDB table as an ETL source.

    Supports full-table scans with server-side filter and projection
    pushdown, plus id-based lookups via ``where(col(...).isin(SourceRef(...)))``
    — the same pattern as :class:`~loom.etl.declarative.source.FromMongo`.

    Args:
        table: DynamoDB table name.  Must match ``^[a-zA-Z0-9_.\\-]{3,255}$``.

    Examples::

        # Full snapshot with param-driven filter
        FromDynamoDb("orders").where(col("status") == "active")

        # Id-based lookup from another step's output
        FromDynamoDb("orders").where(col("pk").isin(SourceRef(FromTemp("ids"), col="id")))

        # Snapshot with schema — same contract as FromTable / FromMongo
        FromDynamoDb("orders").with_schema((
            ColumnSchema("order_id", LoomDtype.UTF8, nullable=False),
            ColumnSchema("status",   LoomDtype.UTF8),
            ColumnSchema("amount",   LoomDtype.FLOAT64),
        ))

        # Large tables — parallel segmented scan
        FromDynamoDb("orders").parallel_scan(8)
    """

    __slots__ = (
        "_table",
        "_filter",
        "_projection",
        "_schema",
        "_extra_fields_mode",
        "_batch_size",
        "_limit",
        "_consistent_read",
        "_total_segments",
    )

    def __init__(self, table: str) -> None:
        if not _TABLE_RE.match(table):
            raise ValueError(
                f"Invalid DynamoDB table name {table!r}. "
                "Only letters, digits, underscores, dots and hyphens are allowed "
                "(3-255 characters)."
            )
        self._table: str = table
        self._filter: ExprNode | None = None
        self._projection: tuple[str, ...] | None = None
        self._schema: tuple[ColumnSchema, ...] = ()
        self._extra_fields_mode = ExtraFieldsMode.IGNORE
        self._batch_size: int = 1_000
        self._limit: int | None = None
        self._consistent_read: bool = False
        self._total_segments: int | None = None

    def where(self, filter: ExprNode) -> FromDynamoDb:
        """Filter items using the col()/params DSL — pushed down as a ``FilterExpression``.

        Multiple ``.where()`` calls are AND-ed together.
        Values may reference params (``params.run_date``) or another step's
        output (``SourceRef(FromTemp("ids"), col="id")``).
        """
        combined = AndExpr(left=self._filter, right=filter) if self._filter is not None else filter
        return self._clone(_filter=combined)

    def project(self, *fields: str) -> FromDynamoDb:
        """Server-side projection — include only these attributes.

        Pushed down as a ``ProjectionExpression`` with aliased attribute
        names, so reserved words are safe.

        Args:
            fields: Attribute names to include.  Empty names are rejected.
        """
        for f in fields:
            if not f.strip():
                raise ValueError("Projection field names must be non-empty.")
        return self._clone(_projection=fields)

    def with_schema(self, schema: SchemaContract) -> FromDynamoDb:
        """Attach a Loom schema contract — same form as ``FromTable.with_schema()``.

        Fields declared as :attr:`~loom.etl.schema.LoomDtype.UTF8` are
        pre-serialized to a JSON string when the DynamoDB value is a complex
        type (map / list).  All other declared types are enforced via
        ``schema_overrides`` before Polars builds the DataFrame.

        Args:
            schema: Either a ``tuple[ColumnSchema, ...]`` or an annotated class
                    (plain Python, ``dataclass``, or ``msgspec.Struct``) whose
                    fields define the item contract.
        """
        return self._clone(_schema=resolve_schema(schema))

    def on_extra_fields(self, mode: ExtraFieldsMode | str) -> FromDynamoDb:
        """Control how item attributes absent from the schema are handled."""
        return self._clone(_extra_fields_mode=ExtraFieldsMode(mode))

    def batch_size(self, n: int) -> FromDynamoDb:
        """Override the DynamoDB scan page size (``Limit`` per page, 1–10 000)."""
        if not 1 <= n <= 10_000:
            raise ValueError(f"batch_size must be between 1 and 10000, got {n}")
        return self._clone(_batch_size=n)

    def limit(self, n: int) -> FromDynamoDb:
        """Limit the number of items returned — for dev/CI only."""
        if n < 1:
            raise ValueError(f"limit must be >= 1, got {n}")
        return self._clone(_limit=n)

    def consistent_read(self, enabled: bool = True) -> FromDynamoDb:  # noqa: FBT001, FBT002
        """Opt in to strongly consistent scan reads.

        Off by default: eventually consistent reads cost half as much and are
        sufficient for snapshot ETL.  Enable only when the pipeline must see
        writes committed immediately before the scan.
        """
        return self._clone(_consistent_read=enabled)

    def parallel_scan(self, total_segments: int) -> FromDynamoDb:
        """Enable DynamoDB parallel ``Scan`` with ``TotalSegments`` segments (2–64).

        Each segment is paged by a dedicated worker thread, multiplying scan
        throughput on large tables.  Item ordering is not guaranteed — but a
        DynamoDB ``Scan`` never guarantees ordering anyway, so combining with
        ``.limit()`` only affects *which* items are returned, never
        correctness: the global limit is still enforced across all segments.
        """
        if not 2 <= total_segments <= 64:
            raise ValueError(
                f"parallel_scan total_segments must be between 2 and 64, got {total_segments}"
            )
        return self._clone(_total_segments=total_segments)

    def _to_spec(self, alias: str) -> DynamoDbSourceSpec:
        return DynamoDbSourceSpec(
            alias=alias,
            table=self._table,
            filter=self._filter,
            projection=self._projection,
            schema=self._schema,
            extra_fields_mode=self._extra_fields_mode,
            batch_size=self._batch_size,
            limit=self._limit,
            consistent_read=self._consistent_read,
            total_segments=self._total_segments,
        )

    def _clone(self, **overrides: Any) -> FromDynamoDb:
        new = object.__new__(FromDynamoDb)
        for slot in self.__slots__:
            object.__setattr__(new, slot, overrides.get(slot, getattr(self, slot)))
        return new

    def __repr__(self) -> str:
        return f"FromDynamoDb({self._table!r})"


__all__ = ["FromDynamoDb"]
