"""MongoDB source reader — orchestrator."""

from __future__ import annotations

import logging
import time
from typing import Any

import polars as pl

from loom.core.errors import InvalidUse
from loom.core.log_names import LogField
from loom.core.observability.span import elapsed_ms
from loom.etl.declarative.source._from_mongo import FromMongo, SourceRef
from loom.etl.declarative.source._specs import ExtraFieldsMode, MongoSourceSpec
from loom.etl.engines.polars._dtype import frame_type_to_polars
from loom.etl.event import EtlEvent, EtlKey
from loom.etl.io.sources._mongo_batch import (
    BatchProcessorProtocol,
    MongoBatchProcessor,
    align_to_schema,
    apply_declared_schema,
)
from loom.etl.io.sources._mongo_bson import normalize_bson_doc
from loom.etl.io.sources._mongo_predicate import predicate_to_mongo
from loom.etl.io.sources._serialization import _serialization_stats
from loom.etl.schema._schema import LoomDtype

_log = logging.getLogger(__name__)


class MongoSourceReader:
    """Read a :class:`~loom.etl.declarative.source.MongoSourceSpec` into a Polars LazyFrame.

    MongoDB I/O is deferred until ``.collect()`` is called. With ``schema``
    declared, zero queries are issued during ``read()`` — the cursor opens lazily
    inside the scan plugin. Without ``schema``, up to ``min(batch_size, 1024)``
    documents are sampled eagerly to infer the registered schema; the full scan
    opens a fresh cursor on each ``.collect()``.

    Args:
        client:   pymongo ``MongoClient`` (or compatible).
        database: MongoDB database name.
    """

    def __init__(
        self,
        client: Any = None,
        database: str = "",
    ) -> None:
        self._client = client
        self._database = database

    def read(self, spec: Any, params_instance: Any, /) -> pl.LazyFrame:
        from polars.io.plugins import register_io_source

        if self._client is None:
            raise InvalidUse(
                "MongoSourceReader: no client configured. "
                "Pass a pymongo MongoClient and database name at construction time."
            )

        collection = self._client[self._database][spec.collection]
        filter_dict = predicate_to_mongo(spec.filter, params_instance) if spec.filter else {}
        projection = dict.fromkeys(spec.projection, 1) if spec.projection else None

        declared_schema, schema_str_fields = self._resolve_declared_schema(spec)
        batch_processor: BatchProcessorProtocol = MongoBatchProcessor(
            schema_str_fields=schema_str_fields,
            declared_schema=declared_schema or None,
            table=spec.collection,
        )

        registered_schema = self._prepare_scan(
            spec,
            collection,
            filter_dict,
            projection,
            declared_schema,
            batch_processor,
        )

        def _io_source(
            with_columns: list[str] | None,
            predicate: pl.Expr | None,
            n_rows: int | None,
            batch_size_hint: int | None,
        ) -> Any:
            yield from _scan(
                collection,
                filter_dict,
                projection,
                spec,
                batch_processor,
                declared_schema,
                registered_schema,
                with_columns,
                n_rows,
            )

        return register_io_source(
            _io_source,
            schema=registered_schema,
            validate_schema=False,
            is_pure=False,
        )

    def _resolve_declared_schema(self, spec: Any) -> tuple[dict[str, pl.DataType], frozenset[str]]:
        if not spec.schema:
            return {}, frozenset()
        field_dtypes = {col.name: frame_type_to_polars(col.dtype) for col in spec.schema}
        str_field_names = frozenset(col.name for col in spec.schema if col.dtype == LoomDtype.UTF8)
        return (field_dtypes, str_field_names)

    def _prepare_scan(
        self,
        spec: Any,
        collection: Any,
        filter_dict: dict[str, Any],
        projection: dict[str, Any] | None,
        declared_schema: dict[str, pl.DataType],
        batch_processor: BatchProcessorProtocol,
    ) -> dict[str, pl.DataType]:
        if declared_schema:
            schema = dict(declared_schema)
            if spec.extra_fields_mode == ExtraFieldsMode.CAPTURE:
                schema["_extra"] = pl.String()
            return schema
        return _infer_schema_from_first_batch(
            collection, filter_dict, projection, spec, batch_processor
        )


def _normalize_null_dtypes(schema: dict[str, pl.DataType]) -> dict[str, pl.DataType]:
    """Promote Null → String and List(Null) → List(String) in schemaless inference."""
    result: dict[str, pl.DataType] = {}
    for name, dtype in schema.items():
        if dtype == pl.Null:
            result[name] = pl.String()
        elif dtype == pl.List(pl.Null):
            result[name] = pl.List(pl.String())
        else:
            result[name] = dtype
    return result


def _infer_schema_from_first_batch(
    collection: Any,
    filter_dict: dict[str, Any],
    projection: dict[str, Any] | None,
    spec: Any,
    batch_processor: BatchProcessorProtocol,
) -> dict[str, pl.DataType]:
    cursor = _open_cursor(collection, filter_dict, projection, spec, None)
    inference_size = min(spec.batch_size, 1024)
    docs: list[dict[str, Any]] = []
    try:
        for doc in cursor:
            docs.append(normalize_bson_doc(doc, extended_json=spec.normalize_extended_json))
            if len(docs) >= inference_size:
                break
    finally:
        cursor.close()
    if not docs:
        _log.info(
            EtlEvent.SOURCE_SCHEMA_INFERRED, extra={EtlKey.TABLE: collection.name, EtlKey.ROWS: 0}
        )
        return {}
    frame = batch_processor.build_frame(docs)
    schema = _normalize_null_dtypes(dict(zip(frame.columns, frame.dtypes, strict=True)))
    _log.info(
        EtlEvent.SOURCE_SCHEMA_INFERRED,
        extra={EtlKey.TABLE: collection.name, EtlKey.ROWS: len(docs)},
    )
    return schema  # noqa: RET504


def _open_cursor(
    collection: Any,
    filter_dict: dict[str, Any],
    projection: dict[str, Any] | None,
    spec: Any,
    n_rows: int | None,
) -> Any:
    cursor = collection.find(filter_dict, projection, batch_size=spec.batch_size)
    effective_limit = spec.limit
    if n_rows is not None:
        effective_limit = min(effective_limit, n_rows) if effective_limit is not None else n_rows
    if effective_limit is not None:
        cursor = cursor.limit(effective_limit)
    _log.info(
        EtlEvent.SOURCE_QUERY_STARTED,
        extra={
            EtlKey.TABLE: collection.name,
            EtlKey.COLUMNS: list(projection) if projection else None,
        },
    )
    return cursor


def _finalize_batch(
    frame: pl.DataFrame,
    declared_schema: dict[str, pl.DataType],
    registered_schema: dict[str, pl.DataType],
    extra_fields_mode: ExtraFieldsMode,
    schema_name: str,
    with_columns: list[str] | None,
    n_rows: int | None,
) -> pl.DataFrame:
    if declared_schema:
        frame = apply_declared_schema(frame, declared_schema, extra_fields_mode, schema_name)
    else:
        frame = align_to_schema(frame, registered_schema, table=schema_name)
    if with_columns:
        present = [c for c in with_columns if c in frame.columns]
        frame = frame.select(present) if present else frame.head(0)
    if n_rows is not None:
        frame = frame.head(n_rows)
    return frame


def _scan(
    collection: Any,
    filter_dict: dict[str, Any],
    projection: dict[str, Any] | None,
    spec: Any,
    batch_processor: BatchProcessorProtocol,
    declared_schema: dict[str, pl.DataType],
    registered_schema: dict[str, pl.DataType],
    with_columns: list[str] | None,
    n_rows: int | None,
) -> Any:
    schema_name = spec.collection
    schema_overrides = declared_schema or registered_schema or None
    remaining = n_rows
    cursor = _open_cursor(collection, filter_dict, projection, spec, n_rows)
    batch: list[dict[str, Any]] = []
    batches_yielded = 0
    total_rows = 0
    started = time.perf_counter()
    serialization_failures_mark = _serialization_stats.failures
    try:
        for doc in cursor:
            batch.append(normalize_bson_doc(doc, extended_json=spec.normalize_extended_json))
            if len(batch) >= spec.batch_size:
                frame = batch_processor.build_frame(batch, schema_overrides=schema_overrides)
                frame = _finalize_batch(
                    frame,
                    declared_schema,
                    registered_schema,
                    spec.extra_fields_mode,
                    schema_name,
                    with_columns,
                    remaining,
                )
                if remaining is not None:
                    remaining -= len(frame)
                rows = len(frame)
                total_rows += rows
                batches_yielded += 1
                _log_batch(
                    collection.name,
                    batches_yielded,
                    rows,
                    total_rows,
                    elapsed_ms(started),
                )
                yield frame
                batch = []
                if remaining is not None and remaining <= 0:
                    return
        if batch:
            frame = batch_processor.build_frame(batch, schema_overrides=schema_overrides)
            frame = _finalize_batch(
                frame,
                declared_schema,
                registered_schema,
                spec.extra_fields_mode,
                schema_name,
                with_columns,
                remaining,
            )
            rows = len(frame)
            total_rows += rows
            batches_yielded += 1
            _log_batch(
                collection.name,
                batches_yielded,
                rows,
                total_rows,
                elapsed_ms(started),
            )
            yield frame
    finally:
        cursor.close()
        serialization_failures = _serialization_stats.failures - serialization_failures_mark
        if serialization_failures:
            _log.warning(
                EtlEvent.SOURCE_READ_DEGRADED,
                extra={
                    EtlKey.TABLE: collection.name,
                    EtlKey.SERIALIZATION_FAILURES: serialization_failures,
                },
            )
        _log.info(
            EtlEvent.SOURCE_READ_COMPLETED,
            extra={
                EtlKey.TABLE: collection.name,
                EtlKey.ROWS: total_rows,
                EtlKey.BATCHES: batches_yielded,
                EtlKey.SERIALIZATION_FAILURES: serialization_failures,
                LogField.DURATION_MS: elapsed_ms(started),
            },
        )


def _log_batch(
    collection: str, batch_index: int, rows: int, total_rows: int, duration_ms: float
) -> None:
    payload: dict[str, object] = {
        EtlKey.TABLE: collection,
        EtlKey.BATCHES: batch_index,
        EtlKey.ROWS: rows,
        LogField.DURATION_MS: duration_ms,
    }
    if batch_index == 1:
        _log.info(EtlEvent.SOURCE_BATCH_READ, extra=payload)
    else:
        _log.debug(EtlEvent.SOURCE_BATCH_READ, extra=payload)


__all__ = ["FromMongo", "MongoSourceReader", "MongoSourceSpec", "SourceRef"]
