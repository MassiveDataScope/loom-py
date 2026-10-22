"""ClickHouse source reader."""

from __future__ import annotations

import tempfile
import threading
import time
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

try:
    import pyarrow as _pa
except ImportError:
    _pa = None  # type: ignore[assignment]

from loom.core.connections import ClickHouseConnection
from loom.core.logger import get_logger
from loom.etl.backends.polars._dtype import loom_type_to_polars
from loom.etl.declarative.expr._predicate_dialect import PredicateDialect, fold_predicate
from loom.etl.declarative.source._specs import ClickHouseSourceSpec
from loom.etl.io._clickhouse_connection import open_client, resolve_connection
from loom.etl.runtime.contracts import ClientCommandExecutor, SourceReader
from loom.etl.schema._schema import ColumnSchema

_log = get_logger(__name__)


class _ClickHousePredicateDialect(PredicateDialect[str]):
    """Render predicate nodes as ClickHouse SQL fragments."""

    def column(self, name: str) -> str:
        return name

    def literal(self, value: Any) -> str:
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, datetime):
            value_utc = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
            timestamp = value_utc.strftime("%Y-%m-%d %H:%M:%S")
            return f"toDateTime64('{timestamp}', 3, 'UTC')"
        if isinstance(value, date):
            return f"toDate('{value.isoformat()}')"
        if isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        return str(value)

    def eq(self, left: str, right: str) -> str:
        return f"{left} = {right}"

    def ne(self, left: str, right: str) -> str:
        return f"{left} != {right}"

    def gt(self, left: str, right: str) -> str:
        return f"{left} > {right}"

    def ge(self, left: str, right: str) -> str:
        return f"{left} >= {right}"

    def lt(self, left: str, right: str) -> str:
        return f"{left} < {right}"

    def le(self, left: str, right: str) -> str:
        return f"{left} <= {right}"

    def in_(self, ref: str, values: tuple[Any, ...]) -> str:
        formatted = ", ".join(self.literal(value) for value in values)
        return f"{ref} IN ({formatted})"

    def and_(self, left: str, right: str) -> str:
        return f"({left}) AND ({right})"

    def or_(self, left: str, right: str) -> str:
        return f"({left}) OR ({right})"

    def not_(self, operand: str) -> str:
        return f"NOT ({operand})"


_CLICKHOUSE_PREDICATE_DIALECT = _ClickHousePredicateDialect()


class ClickHouseSourceReader(SourceReader, ClientCommandExecutor):
    """Read ClickHouse sources and execute client commands via clickhouse-connect.

    Implements both :class:`~loom.etl.runtime.contracts.SourceReader` (DataFrame
    reads) and :class:`~loom.etl.runtime.contracts.ClientCommandExecutor`
    (side-effect commands), so one instance can serve as both ``reader`` and
    ``client_executor`` in :class:`~loom.etl.ETLRunner` — reusing a single
    connection for both roles.

    Args:
        url:        ClickHouse DSN, e.g. ``clickhouse://host:8123/default``; a
                    shortcut for ``connection=ClickHouseConnection(url=url)``.
        connection: The ``storage.clickhouse`` connection: DSN, credentials and
                    bounds.  Mutually exclusive with *url*.
        client:     Pre-built ``clickhouse_connect`` client.  Takes precedence
                    over *url* and *connection*.  Use for testing or when the
                    client is managed externally.

    Raises:
        ValueError: When both *url* and *connection* are given.
    """

    def __init__(
        self,
        url: str | None = None,
        *,
        connection: ClickHouseConnection | None = None,
        client: Any | None = None,
    ) -> None:
        self._connection = resolve_connection(url, connection, owner="ClickHouseSourceReader")
        self._client = client
        self._client_lock = threading.Lock()

    def read(self, spec: Any, params_instance: Any, /) -> pl.LazyFrame:
        """Execute the ClickHouse source query and return a lazy Polars frame.

        Materializes the full result via ``query_arrow()`` (or ``query_df()``
        as a pandas fallback). Use :meth:`read_streaming` for very large
        result sets that would not fit in memory.

        Args:
            spec: Compiled ClickHouse source specification.
            params_instance: Concrete params for current run.

        Returns:
            Lazy Polars frame with the query result.
        """
        client = self._client or self._get_client()
        query = self._build_query(spec, params_instance)
        _log_query_start(spec, query, streaming=False)
        started = time.monotonic()
        frame, rows = self._query_to_frame(client, query)
        _log.info(
            "clickhouse read complete",
            table=spec.table_ref.ref,
            rows=rows,
            duration_s=round(time.monotonic() - started, 3),
        )
        if spec.schema:
            frame = self._apply_source_schema(frame, spec.schema)
        return frame

    def read_streaming(self, spec: Any, params_instance: Any, /) -> pl.LazyFrame:
        """Stream the ClickHouse query result through a temporary IPC file.

        Uses ``clickhouse_connect``'s ``query_arrow_stream()`` to pull the
        result batch-by-batch. Each Arrow batch is appended to a temporary
        IPC file, and the returned ``pl.LazyFrame`` is a ``pl.scan_ipc(...)``
        over that file. Downstream ``collect(engine="streaming")`` then
        consumes the file incrementally, capping peak RAM at roughly one
        Arrow batch. Required for sources that emit millions of rows where
        a full ``query_arrow()`` would exhaust memory.

        Args:
            spec: Compiled ClickHouse source specification.
            params_instance: Concrete params for current run.

        Returns:
            Lazy Polars frame backed by the spooled IPC file.

        Raises:
            TypeError: When the underlying client does not expose
                ``query_arrow_stream()``.
        """
        client = self._client or self._get_client()
        query = self._build_query(spec, params_instance)
        _log_query_start(spec, query, streaming=True)
        started = time.monotonic()
        frame, rows, batches = self._stream_query_to_frame(client, query)
        _log.info(
            "clickhouse stream spooled",
            table=spec.table_ref.ref,
            rows=rows,
            batches=batches,
            duration_s=round(time.monotonic() - started, 3),
        )
        if spec.schema:
            frame = self._apply_source_schema(frame, spec.schema)
        return frame

    def command(self, fn: Callable[[Any], None]) -> None:
        """Obtain the ClickHouse client and invoke *fn* with it.

        Implements :class:`~loom.etl.runtime.contracts.ClientCommandExecutor`
        so the reader can be passed as ``client_executor`` to avoid opening
        a second connection when the runner already holds one.

        Args:
            fn: Callable that receives the raw ``clickhouse_connect`` client.

        Raises:
            ValueError: If neither a connection nor a pre-built client was provided.
            ImportError: If ``clickhouse-connect`` is not installed.
        """
        fn(self._client or self._get_client())

    def _get_client(self) -> Any:
        with self._client_lock:
            if self._client is not None:
                return self._client
            if self._connection is None:
                raise ValueError(
                    "ClickHouseSourceReader requires storage.clickhouse to be configured "
                    "or an explicit client injected at construction time."
                )
            self._client = open_client(self._connection, owner="ClickHouseSourceReader")
            return self._client

    def _build_query(self, spec: ClickHouseSourceSpec, params_instance: Any) -> str:
        columns = (
            ", ".join(self._quote_identifier(col) for col in spec.columns) if spec.columns else "*"
        )
        distinct = "DISTINCT " if spec.distinct else ""
        query = f"SELECT {distinct}{columns} FROM {self._quote_identifier(spec.table_ref.ref)}"

        predicates = tuple(
            fold_predicate(pred, params_instance, _CLICKHOUSE_PREDICATE_DIALECT)
            for pred in spec.predicates
        )
        if predicates:
            query += " WHERE " + " AND ".join(f"({pred})" for pred in predicates)
        return query

    @staticmethod
    def _quote_identifier(name: str) -> str:
        parts = [part for part in name.split(".") if part]
        if not parts:
            raise ValueError("ClickHouse identifiers must not be empty.")
        return ".".join(f"`{part.replace('`', '``')}`" for part in parts)

    @staticmethod
    def _query_to_frame(client: Any, query: str) -> tuple[pl.LazyFrame, int]:
        if hasattr(client, "query_arrow"):
            result = pl.from_arrow(client.query_arrow(query))
            if not isinstance(result, pl.DataFrame):
                raise TypeError("Expected a PyArrow Table from query_arrow(), not a scalar array.")
            return result.lazy(), result.height
        if hasattr(client, "query_df"):
            df = pl.from_pandas(client.query_df(query))
            return df.lazy(), df.height
        raise TypeError(
            "ClickHouse client must expose query_arrow() or query_df() to read sources."
        )

    @staticmethod
    def _stream_query_to_frame(client: Any, query: str) -> tuple[pl.LazyFrame, int, int]:
        if not hasattr(client, "query_arrow_stream"):
            raise TypeError(
                "ClickHouse client must expose query_arrow_stream() to read in streaming mode."
            )
        if _pa is None:
            raise ImportError(
                "read_streaming requires 'pyarrow'. "
                "Install it with: pip install 'loom-py[clickhouse]'"
            )
        # Spool the streaming Arrow result to a temp IPC file so downstream
        # ``collect(engine="streaming")`` can consume it lazily. Caps peak
        # RAM at one batch instead of the full result set.
        # clickhouse-connect StreamContext yields _pa.Table per block — peek
        # the first chunk to obtain schema before opening the IPC writer.
        with client.query_arrow_stream(query) as stream:
            iterator = iter(stream)
            try:
                first_chunk = next(iterator)
            except StopIteration:
                frame, rows = ClickHouseSourceReader._query_to_frame(client, query)
                return frame, rows, 0
            tmp = tempfile.NamedTemporaryFile(  # noqa: SIM115
                prefix="loom-clickhouse-", suffix=".arrow", delete=False
            )
            rows = 0
            batches = 0
            try:
                _ipc_opts = _pa.ipc.IpcWriteOptions(compression="lz4")
                with _pa.ipc.new_file(tmp, first_chunk.schema, options=_ipc_opts) as writer:
                    rows += ClickHouseSourceReader._write_arrow_chunk(writer, first_chunk)
                    batches += 1
                    for chunk in iterator:
                        rows += ClickHouseSourceReader._write_arrow_chunk(writer, chunk)
                        batches += 1
            finally:
                tmp.close()
        return pl.scan_ipc(tmp.name, memory_map=False), rows, batches

    @staticmethod
    def _write_arrow_chunk(writer: Any, chunk: Any) -> int:
        assert _pa is not None
        if isinstance(chunk, _pa.Table):
            rows = 0
            for batch in chunk.to_batches():
                writer.write_batch(batch)
                rows += batch.num_rows
            return rows
        writer.write_batch(chunk)
        return int(getattr(chunk, "num_rows", 0))

    @staticmethod
    def _apply_source_schema(frame: pl.LazyFrame, schema: tuple[ColumnSchema, ...]) -> pl.LazyFrame:
        if not schema:
            return frame
        exprs = [pl.col(col.name).cast(loom_type_to_polars(col.dtype)) for col in schema]
        return frame.with_columns(exprs)


def _log_query_start(spec: ClickHouseSourceSpec, query: str, *, streaming: bool) -> None:
    _log.info(
        "clickhouse query",
        table=spec.table_ref.ref,
        columns=list(spec.columns) if spec.columns else None,
        predicates=len(spec.predicates),
        sql=query,
        streaming=streaming,
    )


__all__ = ["ClickHouseSourceReader"]
