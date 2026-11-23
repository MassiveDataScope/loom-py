"""Spark Parquet backend for CheckpointStore.

Internal module — not part of the public API.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

from loom.core.errors import InvalidType
from loom.etl.checkpoint._cleaners import _join_path
from loom.etl.event import EtlEvent, EtlKey

_log = logging.getLogger(__name__)


class _SparkCheckpointIO:
    """PySpark Parquet backend for :class:`~loom.etl.checkpoint.CheckpointStore`.

    Writes Spark DataFrames as Parquet directories via ``df.write.parquet()``
    and reads them back via ``spark.read.parquet()``.  Cuts the lineage DAG;
    Photon-optimised.  Avoids competing with the shuffle memory pool that
    ``df.cache()`` would enter.

    Args:
        spark: Active :class:`pyspark.sql.SparkSession`.
    """

    def __init__(self, spark: Any) -> None:
        self._spark = spark
        self._schemas: dict[str, Any] = {}
        self._schema_lock = threading.Lock()

    def probe(self, name: str, base: str) -> Any | None:
        """Return a ``DataFrame`` reading *name* under *base*, or ``None`` if absent.

        Args:
            name: Logical intermediate name.
            base: Scope-level base directory path.

        Returns:
            Spark DataFrame when found; ``None`` when the path does not exist.
        """
        return _probe_spark(self._spark, _join_path(base, name))

    def write(self, name: str, base: str, data: Any, *, append: bool) -> None:
        """Write *data* as a Parquet directory under *base*.

        Args:
            name:   Logical intermediate name.
            base:   Scope-level base directory path.
            data:   ``pyspark.sql.DataFrame`` to materialise.
            append: When ``True``, appends to any existing Parquet data for
                    this name (fan-in).  When ``False``, overwrites.

        Raises:
            InvalidType: When *data* is not a ``pyspark.sql.DataFrame``.
        """
        if not _is_spark_dataframe(data):
            raise InvalidType(
                f"Spark backend expects pyspark.sql.DataFrame, got {type(data).__qualname__!r}. "
                "Verify that CheckpointStore was constructed with a SparkSession."
            )
        path = _join_path(base, name)
        frame = data
        schema_key = f"{base}/{name}"

        with self._schema_lock:
            existing_schema = self._schemas.get(schema_key)
            if existing_schema is None:
                self._schemas[schema_key] = frame.schema

        if append and existing_schema is not None:
            frame = _align_spark_to_existing(frame, existing_schema)

        _log.debug(EtlEvent.CHECKPOINT_WRITTEN, extra={EtlKey.PATH: path, EtlKey.APPEND: append})
        frame.write.mode("append" if append else "overwrite").parquet(path)


def _is_spark_dataframe(obj: Any) -> bool:
    """Return ``True`` when *obj* is a ``pyspark.sql.DataFrame``."""
    t = type(obj)
    return "pyspark" in t.__module__ and t.__name__ == "DataFrame"


def _probe_spark(spark: Any, path: str) -> Any | None:
    """Read a Parquet path via *spark*; return ``None`` when the path does not exist.

    Catches ``pyspark.sql.utils.AnalysisException`` only — the specific Spark
    exception for missing paths and unresolvable table references.  Checked by
    module prefix and class name to avoid importing pyspark at module level
    (optional dependency).  Any other exception is re-raised.

    Args:
        spark: Active :class:`pyspark.sql.SparkSession`.
        path:  Parquet directory path to read.

    Returns:
        Spark DataFrame when the path exists; ``None`` on ``AnalysisException``.
    """
    try:
        return spark.read.parquet(path)
    except Exception as exc:
        t = type(exc)
        if t.__module__.startswith("pyspark") and t.__name__ == "AnalysisException":
            return None
        raise


def _align_spark_to_existing(frame: Any, schema: Any) -> Any:
    """Cast/reorder *frame* to *schema* and drop extra columns."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    source_cols = set(frame.columns)
    projected: list[Any] = []
    for field in schema.fields:
        if field.name in source_cols:
            projected.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            projected.append(F.lit(None).cast(field.dataType).alias(field.name))
    return frame.select(*projected)
