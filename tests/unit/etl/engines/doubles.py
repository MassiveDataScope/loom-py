"""Shared doubles for backend tests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Protocol

import polars as pl

from loom.etl import ETLParams, FromTable
from loom.etl.declarative.target import SchemaMode
from loom.etl.engines.polars import PolarsSourceReader, PolarsTargetWriter, polars_to_frame_type
from loom.etl.ports.contracts import SourceReader, TargetWriter
from loom.etl.schema import FrameType, StructType
from loom.etl.storage._config import AuditConfig, MissingTablePolicy


@dataclass
class RunDateParams:
    run_date: date


class NoParams(ETLParams):
    pass


class PassthroughSchema:
    """A physical schema that leaves every frame as it is."""

    def align(self, frame: Any, mode: SchemaMode) -> Any:
        """Return *frame* untouched."""
        return frame


class PassthroughSchemaWriter:
    """Mixin of a stub write policy whose physical schemas align nothing."""

    def _physical_of(self, schema: StructType) -> PassthroughSchema:
        """Ignore *schema*: nothing is aligned to it."""
        return PassthroughSchema()


class ModelEngine(Protocol):
    """What a test needs of a backend: a reader, a writer, frames, and what a table stores."""

    reader: SourceReader
    writer: TargetWriter

    def frame(self, columns: dict[str, list[Any]]) -> Any: ...

    def stored(self, ref: str) -> list[dict[str, Any]]: ...

    def stored_types(self, ref: str) -> dict[str, FrameType]: ...

    def read_columns(self, source: FromTable) -> list[str]: ...


def table_path(root: Path, ref: str) -> str:
    """The Delta path of table *ref* under *root*."""
    return str(root.joinpath(*ref.split(".")))


class PolarsEngine:
    """A Polars reader and writer on one Delta root, with the helpers a model test reads it by."""

    def __init__(self, root: Path, audit: AuditConfig | None = None) -> None:
        self._root = root
        self.reader = PolarsSourceReader(str(root))
        self.writer = PolarsTargetWriter(
            str(root), missing_table_policy=MissingTablePolicy.CREATE, audit_config=audit
        )

    def frame(self, columns: dict[str, list[Any]]) -> pl.LazyFrame:
        """A lazy frame of *columns*."""
        return pl.LazyFrame(columns)

    def stored(self, ref: str) -> list[dict[str, Any]]:
        """The rows of table *ref*."""
        return pl.read_delta(table_path(self._root, ref)).to_dicts()

    def read_columns(self, source: FromTable) -> list[str]:
        """The column names *source* reads."""
        return self.reader.read(source._to_spec("orders"), None).collect_schema().names()

    def stored_types(self, ref: str) -> dict[str, FrameType]:
        """The lake type of each column of table *ref*."""
        schema = pl.scan_delta(table_path(self._root, ref)).collect_schema()
        return {name: polars_to_frame_type(dtype) for name, dtype in schema.items()}


class SparkEngine:
    """A Spark reader and writer on one Delta root, with the helpers a model test reads it by."""

    def __init__(self, spark: Any, root: Path) -> None:
        from loom.etl.engines.spark import SparkSourceReader, SparkTargetWriter

        self.spark = spark
        self._root = root
        self.reader = SparkSourceReader(spark, str(root))
        self.writer = SparkTargetWriter(spark, root, missing_table_policy=MissingTablePolicy.CREATE)

    def frame(self, columns: dict[str, list[Any]]) -> Any:
        """A DataFrame of *columns*."""
        return self.spark.createDataFrame(list(zip(*columns.values(), strict=True)), list(columns))

    def stored(self, ref: str) -> list[dict[str, Any]]:
        """The rows of table *ref*."""
        return [row.asDict(recursive=True) for row in self._stored(ref).collect()]

    def read_columns(self, source: FromTable) -> list[str]:
        """The column names *source* reads."""
        return self.reader.read(source._to_spec("orders"), None).columns

    def stored_types(self, ref: str) -> dict[str, FrameType]:
        """The lake type of each column of table *ref*."""
        from loom.etl.engines.spark._dtype import spark_to_frame_type

        return {f.name: spark_to_frame_type(f.dataType) for f in self._stored(ref).schema.fields}

    def _stored(self, ref: str) -> Any:
        """The Delta table *ref* as a DataFrame."""
        return self.spark.read.format("delta").load(table_path(self._root, ref))
