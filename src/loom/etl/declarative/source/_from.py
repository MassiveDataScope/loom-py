"""Source builder types — FromTable, FromFile, FromTemp, Sources, SourceSet.

Public API — import from :mod:`loom.etl.declarative.source` or :mod:`loom.etl.declarative`.

Three equivalent authoring forms are supported:

* **Form 1 — inline attributes**::

      class MyStep(ETLStep[P]):
          orders = FromTable("raw.orders").where(...)
          customers = FromTable("raw.customers")

* **Form 2 — grouped** ``Sources``::

      class MyStep(ETLStep[P]):
          sources = Sources(
              orders=FromTable("raw.orders").where(...),
              customers=FromTable("raw.customers"),
          )

* **Form 3 — reusable** ``SourceSet``::

      class OrderSources(SourceSet[P]):
          orders = FromTable("raw.orders").where(...)

      class MyStep(ETLStep[P]):
          sources = OrderSources.extended(
              customers=FromTable("raw.customers"),
          )

All three normalize to ``tuple[SourceSpec, ...]`` during compilation.
"""

from __future__ import annotations

from typing import Any, Generic, TypeVar

from loom.core.routing import LogicalRef
from loom.etl.declarative._format import Format
from loom.etl.declarative._read_options import ReadOptions
from loom.etl.declarative._utils import _clone_slots
from loom.etl.declarative.expr._predicate import PredicateNode
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.source._from_clickhouse import FromClickHouse
from loom.etl.declarative.source._from_mongo import FromMongo
from loom.etl.declarative.source._specs import (
    FileSourceSpec,
    JsonColumnSpec,
    SourceSpec,
    TableSourceSpec,
    TempSourceSpec,
)
from loom.etl.schema._contract import (
    JsonContract,
    SchemaContract,
    resolve_json_type,
    resolve_schema,
)
from loom.etl.schema._schema import ColumnSchema

ParamsT = TypeVar("ParamsT")

_SourceEntry = "FromTable | FromFile | FromTemp | FromMongo | FromClickHouse"


class FromTable:
    """Declare a Delta table as an ETL source.

    Accepts a dotted logical reference or an explicit :class:`~loom.etl.TableRef`.
    Predicates are added via :meth:`where` using the standard operator DSL.

    Args:
        ref: Logical table reference — ``str`` or :class:`~loom.etl.TableRef`.

    Example::

        orders = FromTable("raw.orders").where(
            (col("year")  == params.run_date.year)
            & (col("month") == params.run_date.month),
        )
    """

    __slots__ = ("_ref", "_predicates", "_schema", "_columns", "_json_columns")

    def __init__(self, ref: str | LogicalRef | TableRef) -> None:
        self._ref: TableRef = ref if isinstance(ref, TableRef) else TableRef(ref)
        self._predicates: tuple[PredicateNode, ...] = ()
        self._schema: tuple[ColumnSchema, ...] = ()
        self._columns: tuple[str, ...] = ()
        self._json_columns: tuple[JsonColumnSpec, ...] = ()

    @property
    def table_ref(self) -> TableRef:
        """The logical table reference."""
        return self._ref

    @property
    def predicates(self) -> tuple[PredicateNode, ...]:
        """Declared filter predicates."""
        return self._predicates

    @property
    def json_columns(self) -> tuple[JsonColumnSpec, ...]:
        """JSON columns declared with :meth:`parse_json`, in declaration order."""
        return self._json_columns

    def with_schema(self, schema: SchemaContract) -> FromTable:
        """Return a new ``FromTable`` with a user-declared source schema.

        The schema is applied at read time via column-level casts — each
        declared column is cast to its declared type.  Extra columns present
        in the table but absent from *schema* pass through unchanged.

        Args:
            schema: Either a ``tuple[ColumnSchema, ...]`` or an annotated class
                    (``msgspec.Struct``, ``dataclass``, or plain Python class)
                    whose fields define the column contract.

        Returns:
            New ``FromTable`` with the schema applied at read time.

        Example::

            orders = FromTable("raw.orders").with_schema((
                ColumnSchema("id", LoomDtype.INT64, nullable=False),
                ColumnSchema("amount", LoomDtype.FLOAT64),
            ))

            # Equivalent with an annotated class:
            class OrderRow(msgspec.Struct):
                id: int
                amount: float

            orders = FromTable("raw.orders").with_schema(OrderRow)
        """
        new = _clone_slots(self, FromTable, FromTable.__slots__)
        object.__setattr__(new, "_schema", resolve_schema(schema))
        return new

    def parse_json(self, column: str, contract: JsonContract) -> FromTable:
        """Return a new ``FromTable`` that decodes *column* from JSON at read time.

        The string column *column* is decoded into a structured type using the
        Polars ``str.json_decode`` expression (or Spark ``from_json``).

        Args:
            column:   Name of the string column that contains the JSON payload.
            contract: Target type for the decoded column.  Accepted forms:

                      * Any :data:`~loom.etl._schema.LoomType` instance
                        (e.g. ``StructType(...)``, ``ListType(...)``).
                      * An annotated class (``msgspec.Struct``, ``dataclass``,
                        or plain Python class) — converted to
                        :class:`~loom.etl._schema.StructType`.
                      * ``list[SomeClass]`` — converted to
                        :class:`~loom.etl._schema.ListType`.

        Returns:
            New ``FromTable`` with the JSON decode applied at read time.

        Example::

            class Payload(msgspec.Struct):
                event_type: str
                user_id: int

            events = FromTable("raw.events").parse_json("payload", Payload)
            events = FromTable("raw.events").parse_json("tags", list[str])
        """
        loom_type = resolve_json_type(contract)
        spec = JsonColumnSpec(column=column, loom_type=loom_type)
        new = _clone_slots(self, FromTable, FromTable.__slots__)
        object.__setattr__(new, "_json_columns", self._json_columns + (spec,))
        return new

    def where(self, *predicates: Any) -> FromTable:
        """Return a new ``FromTable`` with the given predicates added.

        Args:
            *predicates: One or more :data:`~loom.etl._predicate.PredicateNode`
                expressions built with the column / param operator DSL.

        Returns:
            New ``FromTable`` instance (original is unchanged).
        """
        new = _clone_slots(self, FromTable, FromTable.__slots__)
        object.__setattr__(new, "_predicates", predicates)
        return new

    def columns(self, *cols: str) -> FromTable:
        """Return a new ``FromTable`` projecting only the listed columns.

        The projection is pushed down to the Parquet row-group scanner —
        only the declared columns are read from storage, reducing I/O and
        memory pressure on wide tables.

        Columns used in ``.where()`` predicates that are absent from *cols*
        are still applied at the filter level; the optimizer drops them from
        the output automatically.

        Args:
            *cols: Column names to project.  Must be non-empty.

        Returns:
            New ``FromTable`` with column projection applied at scan time.

        Raises:
            ValueError: When called with no column names.

        Example::

            orders = FromTable("raw.orders") \\
                .where(col("year") == params.run_date.year) \\
                .columns("id", "amount", "status")
        """
        if not cols:
            raise ValueError("FromTable.columns() requires at least one column name.")
        new = _clone_slots(self, FromTable, FromTable.__slots__)
        object.__setattr__(new, "_columns", cols)
        return new

    def _to_spec(self, alias: str) -> TableSourceSpec:
        return TableSourceSpec(
            alias=alias,
            table_ref=self._ref,
            predicates=self._predicates,
            columns=self._columns,
            schema=self._schema,
            json_columns=self._json_columns,
        )

    def __repr__(self) -> str:
        return f"FromTable({self._ref.ref!r})"


class FromFile:
    """Declare a file-based source (CSV, JSON, XLSX, Parquet).

    The ``path`` supports ``{field_name}`` template placeholders that the
    executor resolves from the concrete params at runtime.

    Args:
        path:   File path or template, e.g. ``"s3://raw/orders_{run_date}.csv"``.
        format: :class:`~loom.etl.Format` of the file.

    Example::

        report = FromFile("s3://raw/report_{run_date}.xlsx", format=Format.XLSX)
    """

    __slots__ = (
        "_path",
        "_format",
        "_schema",
        "_read_options",
        "_columns",
        "_json_columns",
        "_is_alias",
    )

    def __init__(self, path: str, *, format: Format) -> None:
        self._path = path
        self._format = format
        self._schema: tuple[ColumnSchema, ...] = ()
        self._read_options: ReadOptions | None = None
        self._columns: tuple[str, ...] = ()
        self._json_columns: tuple[JsonColumnSpec, ...] = ()
        self._is_alias = False

    @classmethod
    def alias(cls, name: str, *, format: Format) -> FromFile:
        """Declare a file source by logical alias resolved from storage config.

        The physical URI is looked up in ``storage.files`` at runtime via the
        injected :class:`~loom.etl.storage.FileLocator`.  Use this form when
        the path is environment-specific and should not be hard-coded in the
        pipeline.

        Args:
            name:   Logical file alias matching a ``storage.files[].name``
                    entry in the config YAML.
            format: :class:`~loom.etl.Format` of the file.

        Returns:
            New ``FromFile`` backed by a logical alias.

        Example::

            events = FromFile.alias("events_raw", format=Format.CSV)
        """
        instance = cls.__new__(cls)
        instance._path = name
        instance._format = format
        instance._schema = ()
        instance._read_options = None
        instance._columns = ()
        instance._json_columns = ()
        instance._is_alias = True
        return instance

    @property
    def path(self) -> str:
        """File path template."""
        return self._path

    @property
    def json_columns(self) -> tuple[JsonColumnSpec, ...]:
        """JSON columns declared with :meth:`parse_json`, in declaration order."""
        return self._json_columns

    @property
    def format(self) -> Format:
        """I/O format."""
        return self._format

    def with_schema(self, schema: SchemaContract) -> FromFile:
        """Return a new ``FromFile`` with a user-declared source schema.

        The schema is applied at read time — each declared column is cast to
        its declared type.  Extra columns in the file not declared in *schema*
        pass through unchanged.

        Args:
            schema: Either a ``tuple[ColumnSchema, ...]`` or an annotated class
                    (``msgspec.Struct``, ``dataclass``, or plain Python class)
                    whose fields define the column contract.

        Returns:
            New ``FromFile`` with the schema applied at read time.

        Example::

            class EventRow(msgspec.Struct):
                id: int
                ts: datetime.datetime

            events = (
                FromFile("s3://raw/events.json", format=Format.JSON)
                .with_options(JsonReadOptions(infer_schema_length=None))
                .with_schema(EventRow)
            )
        """
        new = _clone_slots(self, FromFile, FromFile.__slots__)
        object.__setattr__(new, "_schema", resolve_schema(schema))
        return new

    def parse_json(self, column: str, contract: JsonContract) -> FromFile:
        """Return a new ``FromFile`` that decodes *column* from JSON at read time.

        The string column *column* is decoded into a structured type using the
        Polars ``str.json_decode`` expression (or Spark ``from_json``).

        Args:
            column:   Name of the string column that contains the JSON payload.
            contract: Target type for the decoded column.  Accepted forms:

                      * Any :data:`~loom.etl._schema.LoomType` instance.
                      * An annotated class — converted to
                        :class:`~loom.etl._schema.StructType`.
                      * ``list[SomeClass]`` — converted to
                        :class:`~loom.etl._schema.ListType`.

        Returns:
            New ``FromFile`` with the JSON decode applied at read time.
        """
        loom_type = resolve_json_type(contract)
        spec = JsonColumnSpec(column=column, loom_type=loom_type)
        new = _clone_slots(self, FromFile, FromFile.__slots__)
        object.__setattr__(new, "_json_columns", self._json_columns + (spec,))
        return new

    def with_options(self, options: ReadOptions) -> FromFile:
        """Return a new ``FromFile`` with format-specific read options.

        Args:
            options: Format-specific read options — use
                     :class:`~loom.etl.CsvReadOptions`,
                     :class:`~loom.etl.JsonReadOptions`,
                     :class:`~loom.etl.ExcelReadOptions`, or
                     :class:`~loom.etl.ParquetReadOptions`.

        Returns:
            New ``FromFile`` with the options applied at read time.

        Example::

            report = FromFile("s3://erp/export.csv", format=Format.CSV)
                .with_options(CsvReadOptions(separator=";", has_header=False))
        """
        new = _clone_slots(self, FromFile, FromFile.__slots__)
        object.__setattr__(new, "_read_options", options)
        return new

    def columns(self, *cols: str) -> FromFile:
        """Return a new ``FromFile`` projecting only the listed columns.

        Only the declared columns are loaded from the file — useful for
        wide CSVs or Parquet files where most columns are not needed.

        Args:
            *cols: Column names to project.  Must be non-empty.

        Returns:
            New ``FromFile`` with column projection applied at scan time.

        Raises:
            ValueError: When called with no column names.

        Example::

            report = FromFile("s3://raw/report.parquet", format=Format.PARQUET) \\
                .columns("order_id", "amount", "currency")
        """
        if not cols:
            raise ValueError("FromFile.columns() requires at least one column name.")
        new = _clone_slots(self, FromFile, FromFile.__slots__)
        object.__setattr__(new, "_columns", cols)
        return new

    def _to_spec(self, alias: str) -> FileSourceSpec:
        return FileSourceSpec(
            alias=alias,
            path=self._path,
            format=self._format,
            is_alias=self._is_alias,
            schema=self._schema,
            read_options=self._read_options,
            columns=self._columns,
            json_columns=self._json_columns,
        )

    def __repr__(self) -> str:
        return f"FromFile({self._path!r}, format={self._format!r})"


class FromTemp:
    """Declare an intermediate result as an ETL source.

    The *name* must match the :attr:`~loom.etl.IntoTemp.temp_name` of an
    :class:`~loom.etl.IntoTemp` target that appears **before** this step in
    the pipeline execution order.  The compiler validates this forward-
    reference at compile time.

    The physical format is resolved automatically by
    :class:`~loom.etl.checkpoint.CheckpointStore` — Polars steps receive a
    lazy :class:`polars.LazyFrame` (Arrow IPC), Spark steps receive a
    ``pyspark.sql.DataFrame`` (Parquet).

    Args:
        name: Logical name of the intermediate to consume — must match the
              corresponding :class:`~loom.etl.IntoTemp`.

    Example::

        normalized = FromTemp("normalized_orders")
    """

    __slots__ = ("_name",)

    def __init__(self, name: str) -> None:
        self._name = name

    @property
    def temp_name(self) -> str:
        """Logical name of the intermediate to consume."""
        return self._name

    def _to_spec(self, alias: str) -> TempSourceSpec:
        return TempSourceSpec(
            alias=alias,
            temp_name=self._name,
        )

    def __repr__(self) -> str:
        return f"FromTemp({self._name!r})"


_SourceEntryType = FromTable | FromFile | FromTemp | FromMongo | FromClickHouse


class Sources:
    """Group multiple source declarations under a single ``sources`` attribute.

    Keyword arguments become the source aliases used in ``execute()``
    parameter names.

    Args:
        **sources: Mapping of alias → source declaration.

    Example::

        sources = Sources(
            orders=FromTable("raw.orders").where(...),
            customers=FromTable("raw.customers"),
        )
    """

    __slots__ = ("_sources",)

    def __init__(self, **sources: _SourceEntryType) -> None:
        self._sources: dict[str, _SourceEntryType] = dict(sources)

    @property
    def aliases(self) -> tuple[str, ...]:
        """Declared source aliases in insertion order."""
        return tuple(self._sources)

    def _to_specs(self) -> tuple[SourceSpec, ...]:
        return tuple(src._to_spec(alias) for alias, src in self._sources.items())

    def __repr__(self) -> str:
        return f"Sources({', '.join(self._sources)})"


class SourceSet(Generic[ParamsT]):
    """Reusable named group of sources that can be extended per step.

    Declare shared reads once and compose them into individual steps via
    :meth:`extended`.

    Args:
        **sources: Base source declarations.

    Example::

        class OrderSources(SourceSet[DailyOrdersParams]):
            orders = FromTable("raw.orders").where(...)
            customers = FromTable("raw.customers")

        class MyStep(ETLStep[DailyOrdersParams]):
            sources = OrderSources.extended(
                brands=FromTable("erp.brands"),
            )
    """

    # Class-level sources populated by __init_subclass__
    _sources: dict[str, _SourceEntryType]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        for base in cls.__mro__[1:]:
            if base is SourceSet:
                break
            if issubclass(base, SourceSet) and base is not SourceSet:
                raise TypeError(
                    f"{cls.__name__} subclasses {base.__name__} which is already a concrete "
                    "SourceSet. Subclassing a concrete SourceSet is not supported — use "
                    f"{base.__name__}.extended(...) to add sources."
                )
        cls._sources = {name: val for name, val in cls.__dict__.items() if hasattr(val, "_to_spec")}

    @classmethod
    def extended(cls, **extra: _SourceEntryType) -> SourceSet[ParamsT]:
        """Return a new :class:`SourceSet` with additional sources merged in.

        Args:
            **extra: Additional source declarations to add.

        Returns:
            New ``SourceSet`` with the combined sources.

        Raises:
            ValueError: If any key in ``extra`` conflicts with an existing alias.
        """
        conflicts = set(cls._sources) & set(extra)
        if conflicts:
            raise ValueError(f"SourceSet.extended(): conflicting source names: {sorted(conflicts)}")
        merged: dict[str, _SourceEntryType] = {**cls._sources, **extra}
        instance = object.__new__(SourceSet)
        instance._sources = merged
        return instance

    def _to_specs(self) -> tuple[SourceSpec, ...]:
        return tuple(src._to_spec(alias) for alias, src in self._sources.items())

    def __repr__(self) -> str:
        names = ", ".join(self._sources)
        return f"SourceSet({names})"
