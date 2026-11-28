"""MongoDB source builder — FromMongo and SourceRef."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from loom.core.errors import InvalidValue
from loom.core.expr.nodes import AndExpr, ExprNode
from loom.etl.declarative.source._specs import ExtraFieldsMode, MongoSourceSpec
from loom.etl.schema._model import SchemaContract, resolve_schema
from loom.etl.schema._schema import ColumnSchema

_COLLECTION_RE = re.compile(r"^[a-zA-Z0-9_\-]+$")


@dataclass(frozen=True)
class SourceRef:
    """Reference to a column in another step's output, used inside ``isin()``.

    When used as ``InExpr.values``, the executor resolves this to a concrete
    tuple of values via ``materialize_filter()`` before ``MongoSourceReader.read()``
    is called — the reader never sees a ``SourceRef`` in the filter.

    Args:
        source: A builder (e.g. ``FromTemp``) or compiled spec that produces
                the DataFrame containing the values.
        col:    Column name to extract from the resolved DataFrame.

    Example::

        FromMongo("orders").where(col("_id").isin(SourceRef(FromTemp("ids"), col="order_id")))
    """

    source: Any
    col: str


class FromMongo:
    """Declare a MongoDB collection as an ETL source.

    Supports full-collection snapshots and id-based lookups — the lookup
    pattern is simply a ``where(col("_id").isin(SourceRef(...)))`` filter.

    Args:
        collection: MongoDB collection name.  Must match ``^[a-zA-Z0-9_\\-]+$``.

    Examples::

        # Full snapshot with param-driven filter
        FromMongo("orders").where(col("status") == "active")

        # Id-based lookup from another step's output
        FromMongo("orders").where(col("_id").isin(SourceRef(FromTemp("order_ids"), col="id")))

        # Snapshot with schema — same contract as FromTable / FromFile
        FromMongo("orders").with_schema((
            ColumnSchema("order_id", LoomDtype.UTF8, nullable=False),
            ColumnSchema("status",   LoomDtype.UTF8),
            ColumnSchema("amount",   LoomDtype.FLOAT64),
        ))

        # Or pass an annotated class (plain Python, dataclass, msgspec.Struct)
        class OrderDoc:
            order_id: str
            status: str

        FromMongo("orders").with_schema(OrderDoc)
    """

    __slots__ = (
        "_collection",
        "_filter",
        "_projection",
        "_schema",
        "_extra_fields_mode",
        "_normalize_extended_json",
        "_batch_size",
        "_limit",
    )

    def __init__(self, collection: str) -> None:
        if not _COLLECTION_RE.match(collection):
            raise InvalidValue(
                "Invalid MongoDB collection name. "
                "Only letters, digits, underscores and hyphens are allowed."
            )
        self._collection: str = collection
        self._filter: ExprNode | None = None
        self._projection: tuple[str, ...] | None = None
        self._schema: tuple[ColumnSchema, ...] = ()
        self._extra_fields_mode = ExtraFieldsMode.IGNORE
        self._normalize_extended_json: bool = False
        self._batch_size: int = 2_000
        self._limit: int | None = None

    def where(self, filter: ExprNode) -> FromMongo:
        """Filter documents using the col()/params DSL.

        Multiple ``.where()`` calls are AND-ed together.
        Values may reference params (``params.run_date``) or another step's
        output (``SourceRef(FromTemp("ids"), col="id")``).
        """
        combined = AndExpr(left=self._filter, right=filter) if self._filter is not None else filter
        return self._clone(_filter=combined)

    def project(self, *fields: str) -> FromMongo:
        """Server-side projection — include only these fields.

        Args:
            fields: Field names to include.  ``$`` operator expressions are not allowed.
        """
        for f in fields:
            if "$" in f:
                raise InvalidValue(
                    "Projection fields must not contain '$' — operator expressions "
                    "are not allowed in projection."
                )
        return self._clone(_projection=fields)

    def with_schema(self, schema: SchemaContract) -> FromMongo:
        """Attach a Loom schema contract — same form as ``FromTable.with_schema()``.

        Fields declared as :attr:`~loom.etl.schema.LoomDtype.UTF8` are
        pre-serialized to a JSON string when the MongoDB value is a complex
        type (dict / list).  All other declared types are enforced via
        ``schema_overrides`` before Polars builds the DataFrame.

        Args:
            schema: Either a ``tuple[ColumnSchema, ...]`` or an annotated class
                    (plain Python, ``dataclass``, or ``msgspec.Struct``) whose
                    fields define the document contract.
        """
        return self._clone(_schema=resolve_schema(schema))

    def normalize_extended_json(self, enabled: bool = True) -> FromMongo:  # noqa: FBT001, FBT002
        """Flatten literal Extended JSON wrappers stored as data (``{"$date": ...}``).

        Opt-in and off by default: reinterpreting stored data is a consumer
        decision, and MongoDB 5.0+ permits ``$``-prefixed field names in stored
        documents, so a legitimate single-key ``$date``/``$oid`` subdocument
        would be reinterpreted. ``$numberDecimal`` flattens to float64
        (~15-17 significant digits) — declare a schema and cast for
        high-precision monetary columns.
        """
        return self._clone(_normalize_extended_json=enabled)

    def on_extra_fields(self, mode: ExtraFieldsMode | str) -> FromMongo:
        """Control how document fields absent from the schema are handled."""
        return self._clone(_extra_fields_mode=ExtraFieldsMode(mode))

    def batch_size(self, n: int) -> FromMongo:
        """Override the pymongo cursor batch size (1–50 000)."""
        if not 1 <= n <= 50_000:
            raise InvalidValue("batch_size must be between 1 and 50000")
        return self._clone(_batch_size=n)

    def limit(self, n: int) -> FromMongo:
        """Limit the number of documents returned — for dev/CI only."""
        if n < 1:
            raise InvalidValue("limit must be >= 1")
        return self._clone(_limit=n)

    def _to_spec(self, alias: str) -> MongoSourceSpec:
        return MongoSourceSpec(
            alias=alias,
            collection=self._collection,
            filter=self._filter,
            projection=self._projection,
            schema=self._schema,
            extra_fields_mode=self._extra_fields_mode,
            normalize_extended_json=self._normalize_extended_json,
            batch_size=self._batch_size,
            limit=self._limit,
        )

    def _clone(self, **overrides: Any) -> FromMongo:
        new = object.__new__(FromMongo)
        for slot in self.__slots__:
            object.__setattr__(new, slot, overrides.get(slot, getattr(self, slot)))
        return new

    def __repr__(self) -> str:
        return f"FromMongo({self._collection!r})"


__all__ = ["FromMongo", "SourceRef"]
