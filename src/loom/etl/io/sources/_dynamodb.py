"""DynamoDB source reader — orchestrator."""

from __future__ import annotations

import contextlib
import logging
import queue
import threading
import time
from collections.abc import Generator, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

import polars as pl

from loom.core.errors import InvalidUse
from loom.core.log_names import LogField
from loom.core.observability.span import elapsed_ms
from loom.etl.declarative.source._from_dynamodb import FromDynamoDB
from loom.etl.declarative.source._specs import DynamoDBSourceSpec, ExtraFieldsMode
from loom.etl.engines.polars._dtype import loom_type_to_polars
from loom.etl.event import EtlEvent, EtlKey
from loom.etl.io.sources._dynamodb_items import normalize_dynamo_item
from loom.etl.io.sources._dynamodb_predicate import predicate_to_dynamo
from loom.etl.io.sources._mongo_batch import (
    BatchProcessorProtocol,
    MongoBatchProcessor,
    align_to_schema,
    apply_declared_schema,
)
from loom.etl.io.sources._serialization import _serialization_stats
from loom.etl.schema._schema import LoomDtype

_log = logging.getLogger(__name__)

_QUEUE_POLL_S = 0.1


class DynamoDBSourceReader:
    """Read a :class:`~loom.etl.declarative.source.DynamoDBSourceSpec` into a Polars LazyFrame.

    DynamoDB I/O is deferred until ``.collect()`` is called. With ``schema``
    declared, zero requests are issued during ``read()`` — the scan starts
    lazily inside the scan plugin. Without ``schema``, up to
    ``min(batch_size, 1024)`` items are sampled eagerly to infer the
    registered schema; the full scan restarts on each ``.collect()``.

    Filter and projection are pushed down server-side (``FilterExpression`` /
    ``ProjectionExpression``); ``spec.total_segments`` enables a parallel
    segmented scan with one worker thread per segment.

    Args:
        client: boto3 low-level ``dynamodb`` client (or compatible).
    """

    def __init__(self, client: Any = None) -> None:
        self._client = client

    def read(self, spec: Any, params_instance: Any, /) -> pl.LazyFrame:
        from polars.io.plugins import register_io_source

        if self._client is None:
            raise InvalidUse(
                "DynamoDBSourceReader: no client configured. "
                "Pass a boto3 dynamodb client at construction time."
            )

        client = self._client
        scan_kwargs = _build_scan_kwargs(spec, params_instance)
        declared_schema, schema_str_fields = self._resolve_declared_schema(spec)
        batch_processor: BatchProcessorProtocol = MongoBatchProcessor(
            schema_str_fields=schema_str_fields,
            declared_schema=declared_schema or None,
            table=spec.table,
        )
        registered_schema = self._prepare_scan(
            spec, client, scan_kwargs, declared_schema, batch_processor
        )

        def _io_source(
            with_columns: list[str] | None,
            predicate: pl.Expr | None,
            n_rows: int | None,
            batch_size_hint: int | None,
        ) -> Any:
            yield from _scan(
                client,
                scan_kwargs,
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

    def _resolve_declared_schema(
        self, spec: DynamoDBSourceSpec
    ) -> tuple[dict[str, pl.DataType], frozenset[str]]:
        if not spec.schema:
            return {}, frozenset()
        field_dtypes = {col.name: loom_type_to_polars(col.dtype) for col in spec.schema}
        str_field_names = frozenset(col.name for col in spec.schema if col.dtype == LoomDtype.UTF8)
        return (field_dtypes, str_field_names)

    def _prepare_scan(
        self,
        spec: DynamoDBSourceSpec,
        client: Any,
        scan_kwargs: dict[str, Any],
        declared_schema: dict[str, pl.DataType],
        batch_processor: BatchProcessorProtocol,
    ) -> dict[str, pl.DataType]:
        if declared_schema:
            schema = dict(declared_schema)
            if spec.extra_fields_mode == ExtraFieldsMode.CAPTURE:
                schema["_extra"] = pl.String()
            return schema
        return _infer_schema_from_sample(client, scan_kwargs, spec, batch_processor)


def _build_scan_kwargs(spec: DynamoDBSourceSpec, params_instance: Any) -> dict[str, Any]:
    """Compile the spec into the base ``client.scan()`` keyword arguments."""
    kwargs: dict[str, Any] = {"TableName": spec.table, "Limit": spec.batch_size}
    if spec.consistent_read:
        kwargs["ConsistentRead"] = True
    names: dict[str, str] = {}
    values: dict[str, dict[str, Any]] = {}
    if spec.filter is not None:
        compiled = predicate_to_dynamo(spec.filter, params_instance)
        kwargs["FilterExpression"] = compiled.expression
        names.update(compiled.attribute_names)
        values.update(compiled.attribute_values)
    if spec.projection:
        aliases = {f"#p{i}": name for i, name in enumerate(spec.projection)}
        kwargs["ProjectionExpression"] = ", ".join(aliases)
        names.update(aliases)
    if names:
        kwargs["ExpressionAttributeNames"] = names
    if values:
        kwargs["ExpressionAttributeValues"] = values
    return kwargs


def _sequential_items(
    client: Any, scan_kwargs: dict[str, Any], limit: int | None = None
) -> Generator[dict[str, Any], None, None]:
    """Page the table sequentially, yielding each item normalized exactly once.

    When *limit* is set, the per-request ``Limit`` is capped at the remaining
    item budget and paging stops as soon as the budget is exhausted — no items
    beyond the limit are fetched or normalized.
    """
    kwargs = dict(scan_kwargs)
    remaining = limit
    while True:
        if remaining is not None:
            if remaining <= 0:
                return
            kwargs["Limit"] = min(kwargs["Limit"], remaining)
        response = client.scan(**kwargs)
        items = response.get("Items", ())
        for item in items:
            yield normalize_dynamo_item(item)
        if remaining is not None:
            remaining -= len(items)
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            return
        kwargs["ExclusiveStartKey"] = last_key


class _ParallelSegmentScanner:
    """Parallel segmented Scan — one producer thread per segment.

    Producers page their segment and push normalized-item chunks into a
    bounded queue (``maxsize = total_segments * 2``) to cap memory. The
    consumer (:meth:`__iter__`) drains chunks until every segment posts its
    ``None`` sentinel; each sentinel triggers a fail-fast check so a failed
    segment aborts the whole scan immediately instead of letting the other
    segments finish paging the table. :meth:`close` stops producers between
    pages via a shared event, drains the queue to unblock them, joins the
    pool, and logs any segment error that was never surfaced (a legitimate
    case when the consumer stopped early on a satisfied limit).
    """

    def __init__(self, client: Any, scan_kwargs: dict[str, Any], total_segments: int) -> None:
        self._total = total_segments
        self._queue: queue.Queue[list[dict[str, Any]] | None] = queue.Queue(
            maxsize=total_segments * 2
        )
        self._stop = threading.Event()
        self._error_raised = False
        self._executor = ThreadPoolExecutor(
            max_workers=total_segments, thread_name_prefix="loom-dynamodb-scan"
        )
        self._futures: list[Future[None]] = [
            self._executor.submit(self._produce, client, scan_kwargs, segment)
            for segment in range(total_segments)
        ]

    def _produce(self, client: Any, scan_kwargs: dict[str, Any], segment: int) -> None:
        kwargs = dict(scan_kwargs)
        kwargs["Segment"] = segment
        kwargs["TotalSegments"] = self._total
        try:
            while not self._stop.is_set():
                response = client.scan(**kwargs)
                items = [normalize_dynamo_item(item) for item in response.get("Items", ())]
                if items:
                    self._put(items)
                last_key = response.get("LastEvaluatedKey")
                if not last_key:
                    return
                kwargs["ExclusiveStartKey"] = last_key
        finally:
            self._put(None)

    def _put(self, chunk: list[dict[str, Any]] | None) -> None:
        """Bounded put that gives up when the consumer has closed the scan."""
        while True:
            if self._stop.is_set() and chunk is not None:
                return
            try:
                self._queue.put(chunk, timeout=_QUEUE_POLL_S)
                return
            except queue.Full:
                if self._stop.is_set():
                    return

    def __iter__(self) -> Iterator[dict[str, Any]]:
        finished = 0
        while finished < self._total:
            chunk = self._queue.get()
            if chunk is None:
                finished += 1
                self._raise_producer_errors()
                continue
            yield from chunk
        self._raise_final_errors()

    def _raise_producer_errors(self) -> None:
        """Fail fast: abort the scan on the first segment that finished with an error."""
        for future in self._futures:
            if not future.done() or future.cancelled():
                continue
            exc = future.exception()
            if exc is not None:
                self._abort(exc)

    def _raise_final_errors(self) -> None:
        """Blocking error check once every sentinel arrived — closes the sentinel/done race."""
        for future in self._futures:
            if future.cancelled():
                continue
            exc = future.exception()
            if exc is not None:
                self._abort(exc)

    def _abort(self, exc: BaseException) -> None:
        self._error_raised = True
        self._stop.set()
        raise exc

    def _log_unraised_errors(self) -> None:
        if self._error_raised:
            return
        for segment, future in enumerate(self._futures):
            if not future.done() or future.cancelled():
                continue
            exc = future.exception()
            if exc is not None:
                _log.warning(
                    EtlEvent.DYNAMODB_SEGMENT_FAILED,
                    extra={EtlKey.SEGMENT: segment},
                    exc_info=exc,
                )

    def close(self) -> None:
        """Stop producers, drain the queue, join the pool, and log unraised segment errors."""
        self._stop.set()
        with contextlib.suppress(queue.Empty):
            while True:
                self._queue.get_nowait()
        self._executor.shutdown(wait=True, cancel_futures=True)
        self._log_unraised_errors()


def _open_item_source(
    client: Any,
    scan_kwargs: dict[str, Any],
    spec: DynamoDBSourceSpec,
    effective_limit: int | None,
) -> Generator[dict[str, Any], None, None] | _ParallelSegmentScanner:
    _log.info(EtlEvent.SOURCE_QUERY_STARTED, extra={EtlKey.TABLE: spec.table})
    if spec.total_segments is not None:
        return _ParallelSegmentScanner(client, scan_kwargs, spec.total_segments)
    return _sequential_items(client, scan_kwargs, effective_limit)


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


def _infer_schema_from_sample(
    client: Any,
    scan_kwargs: dict[str, Any],
    spec: DynamoDBSourceSpec,
    batch_processor: BatchProcessorProtocol,
) -> dict[str, pl.DataType]:
    inference_size = min(spec.batch_size, 1024)
    items: list[dict[str, Any]] = []
    sample_kwargs = {**scan_kwargs, "Limit": inference_size}
    source = _sequential_items(client, sample_kwargs, inference_size)
    try:
        for item in source:
            items.append(item)
            if len(items) >= inference_size:
                break
    finally:
        source.close()
    if not items:
        _log.info(EtlEvent.SOURCE_SCHEMA_INFERRED, extra={EtlKey.TABLE: spec.table, EtlKey.ROWS: 0})
        return {}
    frame = batch_processor.build_frame(items)
    schema = _normalize_null_dtypes(dict(zip(frame.columns, frame.dtypes, strict=True)))
    _log.info(
        EtlEvent.SOURCE_SCHEMA_INFERRED,
        extra={EtlKey.TABLE: spec.table, EtlKey.ROWS: len(items)},
    )
    return schema  # noqa: RET504


def _effective_limit(spec_limit: int | None, n_rows: int | None) -> int | None:
    if spec_limit is None:
        return n_rows
    return min(spec_limit, n_rows) if n_rows is not None else spec_limit


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


class _ScanTracker:
    """Accumulates per-scan counters and emits structured progress logs."""

    def __init__(self, table: str) -> None:
        self._table = table
        self._batches = 0
        self._rows = 0
        self._started = time.perf_counter()
        self._failures_mark = _serialization_stats.failures

    def record(self, frame: pl.DataFrame, remaining: int | None) -> int | None:
        rows = len(frame)
        self._rows += rows
        self._batches += 1
        payload: dict[str, object] = {
            EtlKey.TABLE: self._table,
            EtlKey.BATCHES: self._batches,
            EtlKey.ROWS: rows,
            LogField.DURATION_MS: elapsed_ms(self._started),
        }
        if self._batches == 1:
            _log.info(EtlEvent.SOURCE_BATCH_READ, extra=payload)
        else:
            _log.debug(EtlEvent.SOURCE_BATCH_READ, extra=payload)
        return remaining - rows if remaining is not None else None

    def log_complete(self) -> None:
        serialization_failures = _serialization_stats.failures - self._failures_mark
        if serialization_failures:
            _log.warning(
                EtlEvent.SOURCE_READ_DEGRADED,
                extra={
                    EtlKey.TABLE: self._table,
                    EtlKey.SERIALIZATION_FAILURES: serialization_failures,
                },
            )
        _log.info(
            EtlEvent.SOURCE_READ_COMPLETED,
            extra={
                EtlKey.TABLE: self._table,
                EtlKey.ROWS: self._rows,
                EtlKey.BATCHES: self._batches,
                EtlKey.SERIALIZATION_FAILURES: serialization_failures,
                LogField.DURATION_MS: elapsed_ms(self._started),
            },
        )


def _frames_from_items(
    items: Generator[dict[str, Any], None, None] | _ParallelSegmentScanner,
    spec: DynamoDBSourceSpec,
    batch_processor: BatchProcessorProtocol,
    declared_schema: dict[str, pl.DataType],
    registered_schema: dict[str, pl.DataType],
    with_columns: list[str] | None,
    remaining: int | None,
    tracker: _ScanTracker,
) -> Iterator[pl.DataFrame]:
    schema_overrides = declared_schema or registered_schema or None

    def _emit(batch: list[dict[str, Any]], head: int | None) -> pl.DataFrame:
        frame = batch_processor.build_frame(batch, schema_overrides=schema_overrides)
        return _finalize_batch(
            frame,
            declared_schema,
            registered_schema,
            spec.extra_fields_mode,
            spec.table,
            with_columns,
            head,
        )

    batch: list[dict[str, Any]] = []
    for item in items:
        batch.append(item)
        if len(batch) < spec.batch_size:
            continue
        frame = _emit(batch, remaining)
        batch = []
        remaining = tracker.record(frame, remaining)
        yield frame
        if remaining is not None and remaining <= 0:
            return
    if batch:
        frame = _emit(batch, remaining)
        tracker.record(frame, remaining)
        yield frame


def _scan(
    client: Any,
    scan_kwargs: dict[str, Any],
    spec: DynamoDBSourceSpec,
    batch_processor: BatchProcessorProtocol,
    declared_schema: dict[str, pl.DataType],
    registered_schema: dict[str, pl.DataType],
    with_columns: list[str] | None,
    n_rows: int | None,
) -> Iterator[pl.DataFrame]:
    remaining = _effective_limit(spec.limit, n_rows)
    source = _open_item_source(client, scan_kwargs, spec, remaining)
    tracker = _ScanTracker(spec.table)
    try:
        yield from _frames_from_items(
            source,
            spec,
            batch_processor,
            declared_schema,
            registered_schema,
            with_columns,
            remaining,
            tracker,
        )
    finally:
        source.close()
        tracker.log_complete()


__all__ = ["DynamoDBSourceReader", "DynamoDBSourceSpec", "FromDynamoDB"]
