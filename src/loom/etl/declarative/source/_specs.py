"""Normalized source spec types — one frozen dataclass per source kind.

Internal module — import from :mod:`loom.etl.declarative.source`.

Each spec carries exactly the fields required by the compiler and backends.
Specs are produced by the builder ``_to_spec()`` methods and consumed by the
compiler and executor; they are never constructed directly in user code.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from loom.core.errors import InvalidValue
from loom.core.model import BaseModel
from loom.etl.declarative._format import Format
from loom.etl.declarative._read_options import ReadOptions
from loom.etl.declarative.expr._refs import TableRef, columns_in
from loom.etl.schema import ModelTable
from loom.etl.schema._schema import ColumnSchema, FrameType

if TYPE_CHECKING:
    from loom.core.expr.nodes import ExprNode


class ExtraFieldsMode(StrEnum):
    """How a source handles fields its declared schema does not cover."""

    IGNORE = "ignore"
    WARN = "warn"
    CAPTURE = "capture"
    ERROR = "error"

    @classmethod
    def _missing_(cls, value: object) -> ExtraFieldsMode:
        """Refuse a value that names no mode.

        Raises:
            InvalidValue: Always, listing the valid modes.
        """
        raise InvalidValue(
            "on_extra_fields mode is not valid. "
            f"Choose one of: {sorted(mode.value for mode in cls)}"
        )


class SourceKind(StrEnum):
    """Physical kind of an ETL source."""

    TABLE = "table"
    FILE = "file"
    TEMP = "temp"
    CLICKHOUSE = "clickhouse"
    MONGO = "mongo"
    DYNAMODB = "dynamodb"


class JsonMismatch(StrEnum):
    """What a JSON column does with a document that does not match its declared type.

    ``FAIL`` decodes natively and fails the run. ``NULL`` tolerates the
    mismatch: malformed JSON, or an object where a list is declared (and the
    reverse), becomes null, and an object or array under a ``str`` field is kept
    as JSON text. On Polars, ``NULL`` runs Python per row on that column only.
    """

    FAIL = "fail"
    NULL = "null"


@dataclass(frozen=True)
class JsonColumnSpec:
    """Declares that a string column contains JSON to be decoded at read time.

    Produced by :meth:`~loom.etl.declarative.source.FromTable.parse_json` and
    :meth:`~loom.etl.declarative.source.FromFile.parse_json`.
    Consumed by backend readers — never constructed directly in user code.

    Args:
        column:    Name of the string column that holds the JSON payload.
        frame_type: Target :data:`~loom.etl.schema.FrameType` to decode into.
        on_mismatch: Whether a document that does not match fails or becomes null.
    """

    column: str
    frame_type: FrameType
    on_mismatch: JsonMismatch = JsonMismatch.FAIL


@dataclass(frozen=True)
class TableSourceSpec:
    """Normalized internal representation of a Delta table ETL source.

    Produced by :meth:`~loom.etl.declarative.source.FromTable._to_spec`.
    Consumed by the compiler and executor — never exposed in user code.

    Args:
        alias:        Name matching the ``execute()`` parameter.
        table_ref:    Logical table reference.
        predicates:   Compiled predicate nodes from ``.where()``.
        columns:      Column names to project at scan time.  When non-empty,
                      only these columns are read from storage — all other
                      columns are discarded before the frame reaches
                      ``execute()``.  The projection is pushed down to the
                      Parquet row-group scanner, reducing I/O.
        schema:       Optional user-declared schema applied at read time via
                      ``with_columns(cast(...))``; casts each declared column to
                      its :class:`~loom.etl.schema.LoomDtype`.  Extra columns
                      in the source pass through untouched.
        json_columns: JSON decode specs applied at read time.
        model:        The model the source was declared with, ``None`` otherwise.
                      It gives the read schema; the compiler translates it and
                      checks the columns the source names against it.
    """

    alias: str
    table_ref: TableRef
    predicates: tuple[Any, ...] = field(default_factory=tuple)
    columns: tuple[str, ...] = field(default_factory=tuple)
    schema: tuple[ColumnSchema, ...] = field(default_factory=tuple)
    json_columns: tuple[JsonColumnSpec, ...] = field(default_factory=tuple)
    model: type[BaseModel] | None = None

    @property
    def read_schema(self) -> tuple[ColumnSchema, ...]:
        """The schema applied at read time: the model's lake schema, else the declared one."""
        return self.schema if self.model is None else ModelTable.from_model(self.model).schema

    @property
    def declared_columns(self) -> Mapping[str, tuple[str, ...]]:
        """The model columns this source names, by the call that names them."""
        return {
            "columns": self.columns,
            "where": columns_in(self.predicates),
            "parse_json": tuple(spec.column for spec in self.json_columns),
        }

    @property
    def kind(self) -> SourceKind:
        """Physical kind — always :attr:`SourceKind.TABLE`."""
        return SourceKind.TABLE

    @property
    def format(self) -> Format:
        """I/O format — always :attr:`Format.DELTA` for table sources."""
        return Format.DELTA


@dataclass(frozen=True)
class FileSourceSpec:
    """Normalized internal representation of a file-based ETL source.

    Produced by :meth:`~loom.etl.declarative.source.FromFile._to_spec`.
    Consumed by the compiler and executor — never exposed in user code.

    Args:
        alias:        Name matching the ``execute()`` parameter.
        path:         Literal file path/template, or logical alias when
                      ``is_alias=True``.
        format:       I/O format (CSV, JSON, XLSX, Parquet).
        is_alias:     When ``True``, *path* is a logical alias resolved via
                      :class:`~loom.etl.storage.FileLocator` at runtime.
                      Set automatically by :meth:`~loom.etl.FromFile.alias`.
        read_options: Format-specific read options set via ``.with_options()``.
        columns:      Column names to project at scan time.  When non-empty,
                      only these columns are loaded from the file.
        schema:       Optional user-declared schema applied at read time via
                      column-level casts.
        json_columns: JSON decode specs applied at read time.
    """

    alias: str
    path: str
    format: Format
    is_alias: bool = False
    read_options: ReadOptions | None = None
    columns: tuple[str, ...] = field(default_factory=tuple)
    schema: tuple[ColumnSchema, ...] = field(default_factory=tuple)
    json_columns: tuple[JsonColumnSpec, ...] = field(default_factory=tuple)

    @property
    def read_schema(self) -> tuple[ColumnSchema, ...]:
        """The schema applied at read time: the declared one."""
        return self.schema

    @property
    def kind(self) -> SourceKind:
        """Physical kind — always :attr:`SourceKind.FILE`."""
        return SourceKind.FILE


@dataclass(frozen=True)
class TempSourceSpec:
    """Normalized internal representation of an intermediate (temp) ETL source.

    Produced by :meth:`~loom.etl.declarative.source.FromTemp._to_spec`.
    Consumed by the executor to retrieve data from
    :class:`~loom.etl.checkpoint.CheckpointStore`.

    Args:
        alias:     Name matching the ``execute()`` parameter.
        temp_name: Logical intermediate name matching :class:`~loom.etl.IntoTemp`.
    """

    alias: str
    temp_name: str

    @property
    def kind(self) -> SourceKind:
        """Physical kind — always :attr:`SourceKind.TEMP`."""
        return SourceKind.TEMP


@dataclass(frozen=True)
class ClickHouseSourceSpec:
    """Normalized internal representation of a ClickHouse ETL source.

    Produced by :meth:`~loom.etl.declarative.source.FromClickHouse._to_spec`.
    Consumed by the executor and ClickHouse reader.
    """

    alias: str
    table_ref: TableRef
    predicates: tuple[Any, ...] = field(default_factory=tuple)
    columns: tuple[str, ...] = field(default_factory=tuple)
    schema: tuple[ColumnSchema, ...] = field(default_factory=tuple)
    distinct: bool = False
    allow_full_scan: bool = False

    @property
    def kind(self) -> SourceKind:
        """Physical kind — always :attr:`SourceKind.CLICKHOUSE`."""
        return SourceKind.CLICKHOUSE


@dataclass(frozen=True)
class MongoSourceSpec:
    """Normalized internal representation of a MongoDB ETL source.

    Produced by :meth:`~loom.etl.io.sources.FromMongo._to_spec`.
    Consumed by the executor and :class:`~loom.etl.io.sources.MongoSourceReader`.

    Args:
        alias:             Name matching the ``execute()`` parameter.
        collection:        MongoDB collection name.
        filter:            Optional predicate built with the col()/params DSL.
                           May contain :class:`~loom.etl.io.sources.SourceRef`
                           nodes inside ``InPred.values`` — these are resolved
                           by the executor before ``read()`` is called.
        projection:        Tuple of field names to include (server-side projection).
        schema:            Optional column schema — same contract as
                           :class:`~loom.etl.declarative.source.FromTable`.
                           Fields declared as :attr:`~loom.etl.schema.LoomDtype.UTF8`
                           are pre-serialized to JSON string when the MongoDB value
                           is a complex type (dict / list).
        extra_fields_mode: How to handle fields not covered by the schema.
        normalize_extended_json: Flatten literal Extended JSON wrappers stored
                           as data into plain values during normalization.
        batch_size:        Pymongo cursor batch size.
        limit:             Hard limit on documents returned (dev/CI only).
    """

    alias: str
    collection: str
    filter: ExprNode | None = None
    projection: tuple[str, ...] | None = None
    schema: tuple[ColumnSchema, ...] = field(default_factory=tuple)
    extra_fields_mode: ExtraFieldsMode = ExtraFieldsMode.IGNORE
    normalize_extended_json: bool = False
    batch_size: int = 2_000
    limit: int | None = None

    def __post_init__(self) -> None:
        if not 1 <= self.batch_size <= 50_000:
            raise InvalidValue("MongoSourceSpec.batch_size must be between 1 and 50000")

    @property
    def kind(self) -> SourceKind:
        """Physical kind — always :attr:`SourceKind.MONGO`."""
        return SourceKind.MONGO


@dataclass(frozen=True)
class DynamoDBSourceSpec:
    """Normalized internal representation of a DynamoDB ETL source.

    Produced by :meth:`~loom.etl.declarative.source.FromDynamoDB._to_spec`.
    Consumed by the executor and
    :class:`~loom.etl.io.sources.DynamoDBSourceReader`.

    Args:
        alias:             Name matching the ``execute()`` parameter.
        table:             DynamoDB table name.
        filter:            Optional predicate built with the col()/params DSL,
                           pushed down as a ``FilterExpression``.  May contain
                           :class:`~loom.etl.io.sources.SourceRef` nodes inside
                           ``InExpr.values`` — these are resolved by the
                           executor before ``read()`` is called.
        projection:        Tuple of attribute names to include (server-side
                           ``ProjectionExpression``).
        schema:            Optional column schema — same contract as
                           :class:`~loom.etl.declarative.source.FromTable`.
                           Fields declared as :attr:`~loom.etl.schema.LoomDtype.UTF8`
                           are pre-serialized to JSON string when the DynamoDB
                           value is a complex type (map / list).
        extra_fields_mode: How to handle attributes not covered by the schema.
        batch_size:        Scan page size (DynamoDB ``Limit`` per page).
        limit:             Hard limit on items returned (dev/CI only),
                           enforced globally across scan segments.
        consistent_read:   Use strongly consistent scan reads.
        total_segments:    When set, enables parallel ``Scan`` with this many
                           ``TotalSegments`` (one worker thread per segment).

    Raises:
        InvalidValue: When ``batch_size`` is outside 1..10 000 or
            ``total_segments`` is set outside 2..64.
    """

    alias: str
    table: str
    filter: ExprNode | None = None
    projection: tuple[str, ...] | None = None
    schema: tuple[ColumnSchema, ...] = field(default_factory=tuple)
    extra_fields_mode: ExtraFieldsMode = ExtraFieldsMode.IGNORE
    batch_size: int = 1_000
    limit: int | None = None
    consistent_read: bool = False
    total_segments: int | None = None

    def __post_init__(self) -> None:
        if not 1 <= self.batch_size <= 10_000:
            raise InvalidValue("DynamoDBSourceSpec.batch_size must be between 1 and 10000")
        if self.total_segments is not None and not 2 <= self.total_segments <= 64:
            raise InvalidValue("DynamoDBSourceSpec.total_segments must be between 2 and 64")
        if self.limit is not None and self.limit < 1:
            raise InvalidValue("DynamoDBSourceSpec.limit must be >= 1")

    @property
    def kind(self) -> SourceKind:
        """Physical kind — always :attr:`SourceKind.DYNAMODB`."""
        return SourceKind.DYNAMODB


SourceSpec = (
    TableSourceSpec
    | FileSourceSpec
    | TempSourceSpec
    | MongoSourceSpec
    | ClickHouseSourceSpec
    | DynamoDBSourceSpec
)
