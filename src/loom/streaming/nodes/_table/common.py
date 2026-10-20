"""IntoTable streaming sink node — writes typed events to Delta or SQLAlchemy tables.

IntoTable is a frozen dataclass that implements IntoSink structurally.
The compiler detects it via isinstance(node, IntoSink) without any
registration or inheritance.

Supported backends
------------------
- ``Backend.SQLALCHEMY``: bulk-inserts rows using SQLAlchemy async sessions.
  Requires an async bridge and a shared SQLAlchemy session manager from the
  Bytewax runtime.
  Install: ``pip install sqlalchemy``.
- ``Backend.DELTA``: stages rows as parquet parts in temporary files and
  appends them to Delta via delta-rs.
  Requires a ``uri`` key in the resolved config section.
  Install: ``pip install deltalake polars``.

Config example (streaming.sinks.<name>)::

    database:
      warehouse:
        url: postgresql+asyncpg://user:pass@host/db
        pool_pre_ping: true
        pool_size: 10

    streaming:
      sinks:
        orders_sink:
          database: warehouse
          table: orders
          chunk_size: 500

        events_sink:
          uri: s3://data-lake/events
          mode: append            # delta write mode (default: append)

SQLAlchemy example::

    from loom.core.config import ConfigContext
    from loom.core.repository.sqlalchemy.session_manager import SessionManager
    from loom.streaming.nodes._table import SqlAlchemyDatabaseConfig, SqlAlchemySinkConfig

    ctx = ConfigContext.from_yaml("config.yaml")
    sink_cfg = SqlAlchemySinkConfig.from_config(
        ctx.section("streaming.sinks.orders_sink", dict),
        default_table="orders",
    )
    db_cfg = SqlAlchemyDatabaseConfig.from_config(
        ctx.section(f"database.{sink_cfg.database}", dict),
    )
    session_manager = SessionManager.from_config(db_cfg.to_session_manager_config())

Delta example::

    from loom.core.config import ConfigContext
    from loom.streaming.nodes._table import DeltaSinkConfig

    ctx = ConfigContext.from_yaml("config.yaml")
    sink_cfg = DeltaSinkConfig.from_config(
        ctx.section("streaming.sinks.events_sink", dict),
        default_table="events",
    )
"""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping, Sequence
from contextlib import AbstractAsyncContextManager, suppress
from dataclasses import dataclass, field
from enum import StrEnum
from tempfile import SpooledTemporaryFile
from time import monotonic
from typing import (
    Any,
    ClassVar,
    Generic,
    Literal,
    Protocol,
    Self,
    TypeVar,
    cast,
)
from uuid import uuid4

import msgspec

from loom.core.async_bridge import AsyncBridge
from loom.core.errors.codes import ErrorCode
from loom.core.model import BoundaryValidationError
from loom.core.model.convert import to_column_value
from loom.core.model.introspection import get_column_fields
from loom.streaming.core._message import StreamPayload
from loom.streaming.nodes._sink import SinkPartition

# ── optional SQLAlchemy dependency ────────────────────────────────────────────
# Imported once at module level. _SA_AVAILABLE stays False when sqlalchemy is
# missing; an import error naming another module (a broken dependency) is raised.
try:
    from sqlalchemy import insert as _sa_insert

    from loom.core.backend.sqlalchemy import sa_table_for
    from loom.core.repository.sqlalchemy.values import to_column_row

    _SA_AVAILABLE = True
except ImportError as exc:
    if exc.name is not None and exc.name.partition(".")[0] != "sqlalchemy":
        raise
    _sa_insert = cast(Any, None)
    sa_table_for = cast(Any, None)
    to_column_row = cast(Any, None)
    _SA_AVAILABLE = False

# ── optional Delta dependency ─────────────────────────────────────────────────
# Fallback assignments in the except branch ensure both names are always bound,
# removing pyright's "possibly unbound" diagnostic on write_batch call sites.
try:
    import polars as pl
    import pyarrow as _pa
    import pyarrow.ipc as _pa_ipc
    from deltalake.writer import WriterProperties, write_deltalake

    _writer_properties_factory = WriterProperties
    _DELTA_AVAILABLE = True
except ImportError:
    pl = cast(Any, None)
    _pa = cast(Any, None)
    _pa_ipc = cast(Any, None)
    _writer_properties_factory = cast(Any, None)
    write_deltalake = cast(Any, None)
    _DELTA_AVAILABLE = False

# ── optional ClickHouse dependency ────────────────────────────────────────────
try:
    import clickhouse_connect as _cc  # type: ignore[import-untyped]

    _CC_AVAILABLE = True
except ImportError:
    _cc = cast(Any, None)
    _CC_AVAILABLE = False

EventT = TypeVar("EventT", bound=StreamPayload)


class _SessionManagerLike(Protocol):
    """Minimal async session manager contract required by IntoTable."""

    def session(self) -> AbstractAsyncContextManager[Any]:
        """Return an async context manager yielding one SQLAlchemy session."""
        ...


_DELTA_MODES: frozenset[str] = frozenset({"error", "append", "ignore"})
_DEFAULT_BUFFER_MAX_RECORDS = 50_000
_DEFAULT_BUFFER_MAX_BYTES = 128 * 1024 * 1024
_DEFAULT_BUFFER_MAX_AGE_S = 60.0
_DEFAULT_FLUSH_ON_SHUTDOWN = True


class Backend(StrEnum):
    """Storage backend used by :class:`IntoTable` to write epoch batches.

    Args:
        SQLALCHEMY:  Bulk-insert rows via SQLAlchemy Core. Transport-agnostic —
            works with any SA-supported database.
        DELTA:       Stage parquet parts and append them to a Delta Lake table.
            Requires ``deltalake`` and ``polars`` to be installed.
        CLICKHOUSE:  Bulk-insert rows via ``clickhouse-connect`` (native HTTP/2
            protocol, no SQLAlchemy). Requires ``clickhouse-connect`` to be
            installed. Supports ``Array``, ``Nullable`` and all ClickHouse-native
            types without JSON serialisation workarounds.
    """

    SQLALCHEMY = "sqlalchemy"
    DELTA = "delta"
    CLICKHOUSE = "clickhouse"


@dataclass(frozen=True, slots=True)
class _TableBufferPolicy:
    """Common buffering policy shared by all IntoTable backends."""

    max_records: int = _DEFAULT_BUFFER_MAX_RECORDS
    max_bytes: int = _DEFAULT_BUFFER_MAX_BYTES
    max_age_s: float = _DEFAULT_BUFFER_MAX_AGE_S
    flush_on_shutdown: bool = _DEFAULT_FLUSH_ON_SHUTDOWN

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> _TableBufferPolicy:
        """Build a buffering policy from a resolved sink config."""
        return cls(
            max_records=_optional_int(
                config.get("buffer_max_records"),
                _DEFAULT_BUFFER_MAX_RECORDS,
            ),
            max_bytes=_optional_int(config.get("buffer_max_bytes"), _DEFAULT_BUFFER_MAX_BYTES),
            max_age_s=float(config.get("buffer_max_age_s", _DEFAULT_BUFFER_MAX_AGE_S)),
            flush_on_shutdown=bool(config.get("flush_on_shutdown", _DEFAULT_FLUSH_ON_SHUTDOWN)),
        )


@dataclass(frozen=True)
class IntoTable(Generic[EventT]):
    """Streaming terminal node that writes typed events to a storage table.

    Implements :class:`~loom.streaming.nodes.IntoSink` structurally — no
    base class or registration required.  The compiler detects it via
    structural ``isinstance(node, IntoSink)`` and resolves its config from
    ``streaming.sinks.<name>``.

    Args:
        payload: Concrete event type written by this sink.
        table:   Target table name.  May be overridden by the resolved config
                 section or left empty when the config supplies it.
        backend: Storage backend.  Defaults to :attr:`Backend.SQLALCHEMY`.
        name:    Config section key (``streaming.sinks.<name>``).  Required
                 when the sink is resolved from ``ConfigContext``.
        router_branch_safe: Always ``True`` — IntoTable may appear inside
                 Router branches.

    Example::

        Process(
            Explode(StoreEventExpander),
            Router.by(
                PayloadType(),
                routes={
                    StoreRow: Process(
                        IntoTable(payload=StoreRow, table="store", name="store_sink")
                    ),
                    LanguageRow: Process(
                        IntoTable(payload=LanguageRow, table="language", name="lang_sink")
                    ),
                },
            ),
        )

    ``PayloadType`` is a ``Selector`` returning ``type(message.payload)``, as in
    the :class:`~loom.streaming.nodes._decompose.Explode` example.
    """

    payload: type[EventT]
    table: str = ""
    backend: Backend = Backend.SQLALCHEMY
    name: str = ""
    router_branch_safe: ClassVar[bool] = True

    def build_partition(
        self,
        config: SqlAlchemySinkConfig | DeltaSinkConfig | ClickHouseSinkConfig,
        worker_index: int,
        worker_count: int,
        bridge: AsyncBridge | None = None,
        session_manager: _SessionManagerLike | None = None,
        logger: Any = None,
    ) -> SinkPartition[EventT]:
        """Build the per-worker partition for this sink.

        Args:
            config:       Resolved ``streaming.sinks.<name>`` config section.
            worker_index: Zero-based index of the calling worker (unused).
            worker_count: Total number of workers (unused).
            bridge:       Optional adapter-owned async bridge for SQLAlchemy sinks.
            session_manager: Optional adapter-owned SQLAlchemy session manager.

        Returns:
            A :class:`~loom.streaming.nodes.SinkPartition` ready to receive
            ``write_batch`` and ``close`` calls.

        Raises:
            TypeError: If the provided config does not match the selected backend.
            RuntimeError: If required runtime resources are missing.
            ValueError: If the resolved backend is not supported, or a Delta
                payload type has no fields.
        """
        del worker_index, worker_count
        if self.backend is Backend.SQLALCHEMY:
            if not isinstance(config, SqlAlchemySinkConfig):
                raise TypeError("IntoTable backend=sqlalchemy requires a SqlAlchemySinkConfig.")
            if bridge is None:
                raise RuntimeError(
                    "IntoTable backend=sqlalchemy requires an AsyncBridge from the Bytewax runtime."
                )
            if session_manager is None:
                raise RuntimeError(
                    "IntoTable backend=sqlalchemy requires a shared "
                    "SessionManager from the Bytewax runtime."
                )
            return cast(
                SinkPartition[EventT],
                _SQLAlchemyTablePartition(
                    config,
                    self.payload,
                    bridge,
                    session_manager,
                    logger=logger,
                ),
            )
        if self.backend is Backend.DELTA:
            if not isinstance(config, DeltaSinkConfig):
                raise TypeError("IntoTable backend=delta requires a DeltaSinkConfig.")
            if not _column_names(self.payload):
                raise ValueError(
                    f"IntoTable backend=delta requires a payload type with fields; "
                    f"{self.payload.__name__} has none, so it has no row to write."
                )
            return cast(
                SinkPartition[EventT],
                _DeltaTablePartition(config, self.payload, logger=logger),
            )
        if self.backend is Backend.CLICKHOUSE:
            if not isinstance(config, ClickHouseSinkConfig):
                raise TypeError("IntoTable backend=clickhouse requires a ClickHouseSinkConfig.")
            return cast(
                SinkPartition[EventT],
                _ClickHouseTablePartition(config, self.payload, logger=logger),
            )
        raise ValueError(f"Unsupported backend: {self.backend!r}")


# ── SQLAlchemy helpers ────────────────────────────────────────────────────────
# The SQLAlchemy partition uses the names bound by the optional-import block
# above; its __init__ raises ImportError first when _SA_AVAILABLE is False.


def _optional_int(value: Any, default: int) -> int:
    if value is None:
        return default
    return int(value)


@dataclass(frozen=True, slots=True)
class SqlAlchemyDatabaseConfig:
    """Resolved SQLAlchemy connection settings for a shared database entry.

    Args:
        url: SQLAlchemy async URL.
        echo: Enable SQL echo logging.
        pool_pre_ping: Check pooled connections before checkout.
        pool_size: Base connection pool size.
        max_overflow: Additional overflow connections.
        pool_timeout: Seconds to wait for a pooled connection.
        pool_recycle: Seconds before recycling pooled connections.
        connect_args: Extra arguments passed to the DBAPI connect call.
    """

    url: str
    echo: bool = False
    pool_pre_ping: bool = True
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30
    pool_recycle: int = 1800
    connect_args: dict[str, object] = field(default_factory=dict)

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> Self:
        """Build a SQLAlchemy connection config from a resolved YAML section."""
        url = str(config.get("url") or "").strip()
        if not url:
            raise ValueError("SQLAlchemy database config requires a non-empty 'url'.")
        return cls(
            url=url,
            echo=bool(config.get("echo", False)),
            pool_pre_ping=bool(config.get("pool_pre_ping", True)),
            pool_size=_optional_int(config.get("pool_size"), 10),
            max_overflow=_optional_int(config.get("max_overflow"), 20),
            pool_timeout=_optional_int(config.get("pool_timeout"), 30),
            pool_recycle=_optional_int(config.get("pool_recycle"), 1800),
            connect_args=dict(config.get("connect_args") or {}),
        )

    def to_session_manager_config(self) -> dict[str, object]:
        """Convert the config to ``SessionManager.from_config`` keyword mapping."""
        return {
            "url": self.url,
            "echo": self.echo,
            "pool_pre_ping": self.pool_pre_ping,
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "pool_timeout": self.pool_timeout,
            "pool_recycle": self.pool_recycle,
            "connect_args": dict(self.connect_args),
        }


@dataclass(frozen=True, slots=True)
class SqlAlchemySinkConfig:
    """Resolved ``IntoTable`` settings for the SQLAlchemy backend.

    Args:
        database: Shared ``database.<name>`` reference. Empty when using
            inline ``url`` resolution.
        url: Inline SQLAlchemy URL fallback.
        table: Final target table name.
        chunk_size: Number of rows per bulk ``executemany`` call.
        buffer_policy: Common flush policy shared with other backends.
    """

    database: str
    url: str
    table: str
    chunk_size: int
    buffer_policy: _TableBufferPolicy

    @classmethod
    def from_config(cls, config: Mapping[str, Any], *, default_table: str) -> Self:
        """Build SQLAlchemy sink settings from a resolved sink config."""
        table = str(config.get("table") or default_table).strip()
        database = str(config.get("database") or "").strip()
        url = str(config.get("url") or "").strip()
        if not table:
            raise ValueError("SQLAlchemy sink config requires a non-empty table name.")
        return cls(
            database=database,
            url=url,
            table=table,
            chunk_size=_optional_int(config.get("chunk_size"), 500),
            buffer_policy=_TableBufferPolicy.from_config(config),
        )


@dataclass(frozen=True, slots=True)
class DeltaSinkConfig:
    """Resolved ``IntoTable`` settings for the Delta backend.

    Args:
        uri: Delta table URI.
        table: Final target table name.
        mode: delta-rs write mode.
        storage_options: Object-store credentials/options.
        partition_by: Column names used to partition the Delta table.
        target_file_size: Optional delta-rs target file size in bytes.
        staging_compression: Compression codec for the IPC staging stream.
        spool_max_bytes: Maximum in-memory bytes before spooling the staging file to disk.
        part_max_records: Backward-compatible row-group hint for the final Delta write.
        writer_properties: Final Delta writer properties passed to delta-rs.
        buffer_policy: Common flush policy shared with other backends.
    """

    uri: str
    table: str
    mode: Literal["error", "append", "ignore"]
    storage_options: dict[str, str]
    partition_by: tuple[str, ...]
    target_file_size: int | None
    staging_compression: Literal["uncompressed", "lz4", "zstd"]
    spool_max_bytes: int
    part_max_records: int
    writer_properties: Any | None
    buffer_policy: _TableBufferPolicy
    mini_batch_size: int = 500

    @classmethod
    def from_config(cls, config: Mapping[str, Any], *, default_table: str) -> Self:
        """Build Delta sink settings from a resolved sink config."""
        table = str(config.get("table") or default_table).strip()
        uri = str(config.get("uri") or "").strip()
        if not uri:
            raise ValueError("Delta sink config requires a non-empty 'uri'.")
        raw_partition_by = config.get("partition_by") or []
        staging_config = config.get("staging") if isinstance(config.get("staging"), Mapping) else {}
        staging_compression = _validate_ipc_compression(
            staging_config.get("compression")
            if isinstance(staging_config, Mapping)
            else config.get("staging_compression", "uncompressed")
        )
        writer_properties = _build_writer_properties(
            config.get("writer_properties")
            if isinstance(config.get("writer_properties"), Mapping)
            else {}
        )
        if writer_properties is None:
            writer_properties = _writer_properties_factory(
                max_row_group_size=_optional_int(config.get("part_max_records"), 10_000)
            )
        target_file_size_raw = config.get("target_file_size")
        return cls(
            uri=uri,
            table=table,
            mode=_validate_delta_mode(config.get("mode", "append")),
            storage_options={k: str(v) for k, v in (config.get("storage_options") or {}).items()},
            partition_by=tuple(str(c) for c in raw_partition_by),
            target_file_size=int(target_file_size_raw)
            if target_file_size_raw is not None
            else None,
            staging_compression=staging_compression,
            spool_max_bytes=_optional_int(config.get("spool_max_bytes"), 32 * 1024 * 1024),
            part_max_records=_optional_int(config.get("part_max_records"), 10_000),
            writer_properties=writer_properties,
            buffer_policy=_TableBufferPolicy.from_config(config),
            mini_batch_size=_optional_int(
                staging_config.get("mini_batch_size")
                if isinstance(staging_config, Mapping)
                else config.get("mini_batch_size"),
                500,
            ),
        )


@dataclass(frozen=True, slots=True)
class ClickHouseSinkConfig:
    """Resolved ``IntoTable`` settings for the ClickHouse backend.

    Connection is established via ``clickhouse-connect`` using a standard
    ``clickhouse://`` DSN derived from the ``database.<name>.url`` entry in
    ``config.yaml``.  The ``clickhouse+asynch://`` scheme used by
    ``clickhouse-sqlalchemy`` is normalised automatically — no config changes
    are required.

    Args:
        url:            Normalised ``clickhouse://`` DSN for ``clickhouse-connect``.
        table:          Target table name.
        buffer_policy:  Common flush policy (max_records, max_bytes, max_age_s).
        insert_timeout: Seconds before a single insert call is aborted.
            ``None`` disables the timeout (default).
    """

    url: str
    table: str
    buffer_policy: _TableBufferPolicy
    insert_timeout: float | None = None

    @classmethod
    def from_config(cls, config: Mapping[str, Any], *, default_table: str) -> Self:
        """Build ClickHouse sink settings from a resolved sink + database config."""
        table = str(config.get("table") or default_table).strip()
        url = str(config.get("url") or "").strip()
        if not table:
            raise ValueError("ClickHouse sink config requires a non-empty table name.")
        if not url:
            raise ValueError("ClickHouse sink config requires a 'url' in the database section.")
        timeout_raw = config.get("insert_timeout")
        return cls(
            url=url,
            table=table,
            buffer_policy=_TableBufferPolicy.from_config(config),
            insert_timeout=float(timeout_raw) if timeout_raw is not None else None,
        )


def _validate_ipc_compression(value: object) -> Literal["uncompressed", "lz4", "zstd"]:
    compression = str(value or "uncompressed").strip().lower()
    if compression not in {"uncompressed", "lz4", "zstd"}:
        raise ValueError(
            "Invalid IPC compression "
            f"{compression!r}. Must be one of: ['lz4', 'uncompressed', 'zstd']"
        )
    return cast(Literal["uncompressed", "lz4", "zstd"], compression)


def _build_writer_properties(config: Mapping[str, Any] | None) -> Any | None:
    if not config:
        return None
    kwargs = dict(config)
    compression = kwargs.get("compression")
    if isinstance(compression, str):
        kwargs["compression"] = compression.upper()
    return _writer_properties_factory(**kwargs)


def _row_size_bytes(row: Mapping[str, Any]) -> int:
    """Estimate the serialized size of one row for buffer accounting."""
    return len(msgspec.json.encode(dict(row)))


class _SQLAlchemyTablePartition:
    """Bulk-insert epoch batches into a relational table via SQLAlchemy async sessions.

    The partition reuses a shared SessionManager owned by the Bytewax adapter
    and uses the Bytewax async bridge to execute batch inserts without exposing
    event-loop management to callers.
    Columns and their types are those core compiles for the payload's fields,
    and each value takes the form core's repository writes; no ORM mapping or
    reflection is required.

    Args:
        settings:        Resolved SQLAlchemy sink config.
        payload_type:    Struct type whose fields map to table columns.
        bridge:          Bytewax async bridge used to run async writes.
        session_manager: Shared SQLAlchemy session manager for this DB config.

    Raises:
        ImportError: If ``sqlalchemy`` is not installed.
    """

    def __init__(
        self,
        settings: SqlAlchemySinkConfig,
        payload_type: type[Any],
        bridge: AsyncBridge,
        session_manager: _SessionManagerLike,
        logger: Any = None,
    ) -> None:
        if not _SA_AVAILABLE:
            raise ImportError(
                "SQLAlchemy backend requires 'sqlalchemy'. Install it with: pip install sqlalchemy"
            )
        self._bridge = bridge
        self._session_manager = session_manager
        self._logger = logger
        self._table = sa_table_for(payload_type, settings.table)
        self._chunk_size = settings.chunk_size
        self._buffer_policy = settings.buffer_policy
        self._buffer: deque[tuple[dict[str, Any], int]] = deque()
        self._buffer_records = 0
        self._buffer_bytes = 0
        self._buffer_started_at: float | None = None
        self._closed = False

    def write_batch(self, items: Sequence[Any]) -> int:
        """Buffer one epoch batch and bulk-insert it when a threshold is reached.

        Args:
            items: Struct instances to insert.  An empty batch still checks
                the age threshold; Bytewax 0.21.1 calls ``write_batch`` only
                when items arrive, so it does not send one on idle epochs.

        Returns:
            How many buffered items this call committed to the table.
        """
        if not items:
            if self._buffer and self._should_flush():
                return self._bridge.run(self._flush_async())
            return 0
        durable = 0
        for item in items:
            row = to_column_row(item, self._table.columns)
            row_bytes = _row_size_bytes(row)
            if not self._buffer:
                self._buffer_started_at = monotonic()
            self._buffer.append((row, row_bytes))
            self._buffer_records += 1
            self._buffer_bytes += row_bytes
            if self._should_flush():
                durable += self._bridge.run(self._flush_async())
        return durable

    def close(self) -> int:
        """Release partition resources.

        The shared session manager is owned by the adapter runtime and disposed
        centrally when the Bytewax worker shuts down.

        Returns:
            How many buffered items the final flush committed to the table.
        """
        if self._closed:
            return 0
        self._closed = True
        if self._buffer and self._buffer_policy.flush_on_shutdown:
            return self._bridge.run(self._flush_async())
        return 0

    def _should_flush(self) -> bool:
        if self._closed or not self._buffer:
            return False
        if self._buffer_records >= self._buffer_policy.max_records:
            return True
        if self._buffer_bytes >= self._buffer_policy.max_bytes:
            return True
        if self._buffer_started_at is None:
            return False
        return monotonic() - self._buffer_started_at >= self._buffer_policy.max_age_s

    async def _flush_async(self) -> int:
        statement = _sa_insert(self._table)
        trace_id = uuid4().hex
        correlation_id = uuid4().hex
        async with self._session_manager.session() as session:
            try:
                n = 0
                t0 = monotonic()
                while self._buffer:
                    rows: list[dict[str, Any]] = []
                    while self._buffer and len(rows) < self._chunk_size:
                        row, row_bytes = self._buffer.popleft()
                        rows.append(row)
                        self._buffer_records -= 1
                        self._buffer_bytes -= row_bytes
                    for same_columns in _rows_by_column_set(rows):
                        await session.execute(statement, same_columns)
                    n += len(rows)
                await session.commit()
                if self._logger is not None:
                    self._logger.info(
                        "sink_flush",
                        trace_id=trace_id,
                        correlation_id=correlation_id,
                        backend="sqlalchemy",
                        table=self._table.name,
                        records=n,
                        duration_ms=round((monotonic() - t0) * 1000, 1),
                    )
            except Exception:
                await session.rollback()
                raise
        self._buffer_started_at = None
        return n


def _column_names(payload_type: type[Any]) -> list[str]:
    """Return the column fields of *payload_type* in declaration order.

    Relation and projection fields are not columns.
    """
    return list(get_column_fields(payload_type))


def _set_column_values(item: Any, column_names: Sequence[str], target: str) -> list[Any]:
    """Return the column values of *item*'s attributes named by *column_names*, in that order.

    Each value is converted by :func:`~loom.core.model.convert.to_column_value`,
    as ``Backend.SQLALCHEMY`` converts it: an ``Enum`` becomes its value and a
    struct a dict, while ``bytes``, ``datetime``, ``date``, ``time``,
    ``Decimal`` and ``UUID`` are kept.

    Neither partition that calls it can leave a column out for one row: Delta
    has no per-insert column defaults, and the ClickHouse insert names every
    column for the whole buffer. An ``UNSET`` value is therefore refused rather
    than written.

    Args:
        item: A struct with one attribute per column.
        column_names: The columns to read.
        target: The table, as named in the error.

    Raises:
        BoundaryValidationError: ``boundary_validation``, with one violation per
            ``UNSET`` value, in column order.
    """
    values = [getattr(item, name) for name in column_names]
    violations = tuple(
        (name, f"UNSET has no column value in {target}")
        for name, value in zip(column_names, values, strict=True)
        if value is msgspec.UNSET
    )
    if violations:
        message = "; ".join(f"{name}: {reason}" for name, reason in violations)
        raise BoundaryValidationError(message, violations, code=ErrorCode.BOUNDARY_VALIDATION)
    return [to_column_value(value) for value in values]


def _rows_by_column_set(rows: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Group *rows* by the columns they set, keeping their order within a group.

    One executemany INSERT takes its columns from the first row: a later row
    missing one fails, and a column only a later row sets is dropped.
    """
    groups: dict[frozenset[str], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(frozenset(row), []).append(row)
    return list(groups.values())


# ── Delta helpers ─────────────────────────────────────────────────────────────


def _validate_delta_mode(value: object) -> Literal["error", "append", "ignore"]:
    mode = str(value)
    if mode not in _DELTA_MODES:
        raise ValueError(
            f"Invalid Delta write mode {mode!r}. Must be one of: {sorted(_DELTA_MODES)}"
        )
    return mode  # type: ignore[return-value]


class _DeltaTablePartition:
    """Stage rows as a PyArrow IPC stream and append them to Delta.

    Rows accumulate in a plain ``list[dict]``; every ``mini_batch_size`` rows
    they are converted to a single ``pa.RecordBatch`` and written to a
    ``SpooledTemporaryFile``-backed IPC stream.  At flush the stream is closed,
    rewound, and passed directly to ``write_deltalake`` as a
    ``RecordBatchReader`` — delta-rs iterates the batches without collecting
    them into a single in-memory frame.

    Args:
        settings:     Resolved Delta sink config.
        payload_type: Struct type whose column fields map to table columns.

    Raises:
        ImportError: If ``deltalake``, ``polars``, or ``pyarrow`` are not installed.
    """

    def __init__(
        self,
        settings: DeltaSinkConfig,
        payload_type: type[Any],
        logger: Any = None,
    ) -> None:
        if not _DELTA_AVAILABLE:
            raise ImportError(
                "Delta backend requires 'deltalake', 'polars' and 'pyarrow'. "
                "Install them with: pip install deltalake polars pyarrow"
            )
        self._logger = logger
        self._column_names = _column_names(payload_type)
        self._target = f"Delta table {settings.table!r}"
        self._uri = settings.uri
        self._mode = settings.mode
        self._storage_options = dict(settings.storage_options)
        self._partition_by = settings.partition_by
        self._buffer_policy = settings.buffer_policy
        self._spool_max_bytes = settings.spool_max_bytes
        self._target_file_size = settings.target_file_size
        self._staging_compression = settings.staging_compression
        self._writer_properties = settings.writer_properties
        self._mini_batch_size = settings.mini_batch_size
        self._mini_batch: list[dict[str, Any]] = []
        self._ipc_writer: Any = None  # pa.ipc.RecordBatchStreamWriter | None
        self._spool: Any = self._new_spool()
        self._staged_records = 0
        self._staged_bytes = 0
        self._buffer_started_at: float | None = None
        self._closed = False

    def _new_spool(self) -> Any:
        return SpooledTemporaryFile(max_size=self._spool_max_bytes)  # noqa: SIM115

    def _ipc_compression(self) -> Any:
        if self._staging_compression == "uncompressed":
            return None
        return self._staging_compression  # "lz4" | "zstd"

    def write_batch(self, items: Sequence[Any]) -> int:
        """Accumulate items into mini-batches and flush when a threshold is reached.

        Args:
            items: Struct instances to write.  An empty batch still checks
                the age threshold; Bytewax 0.21.1 calls ``write_batch`` only
                when items arrive, so it does not send one on idle epochs.

        Returns:
            How many buffered items this call wrote to the Delta table.
        """
        if not items:
            if self._staged_records and self._should_flush():
                return self._flush()
            return 0
        durable = 0
        for item in items:
            if self._staged_records == 0:
                self._buffer_started_at = monotonic()
            self._stage_item(item)
            if self._should_flush():
                durable += self._flush()
        return durable

    def _stage_item(self, item: Any) -> None:
        values = _set_column_values(item, self._column_names, self._target)
        self._mini_batch.append(dict(zip(self._column_names, values, strict=True)))
        self._staged_records += 1
        if len(self._mini_batch) >= self._mini_batch_size:
            self._commit_mini_batch()

    def _commit_mini_batch(self) -> None:
        if not self._mini_batch:
            return
        batch = _pa.RecordBatch.from_pylist(self._mini_batch)
        if self._ipc_writer is None:
            options = _pa_ipc.IpcWriteOptions(compression=self._ipc_compression())
            self._ipc_writer = _pa_ipc.new_stream(self._spool, batch.schema, options=options)
            if self._logger is not None:
                self._logger.debug(
                    "delta_staging_stream_opened",
                    uri=self._uri,
                    schema_fields=len(batch.schema),
                    compression=self._staging_compression,
                )
        self._ipc_writer.write_batch(batch)
        pos = self._spool.tell()
        self._staged_bytes = pos
        if self._logger is not None:
            self._logger.debug(
                "delta_mini_batch_written",
                uri=self._uri,
                batch_rows=len(self._mini_batch),
                spool_bytes=pos,
            )
        self._mini_batch.clear()

    def _should_flush(self) -> bool:
        if self._closed or not self._staged_records:
            return False
        if self._staged_records >= self._buffer_policy.max_records:
            return True
        if self._staged_bytes >= self._buffer_policy.max_bytes:
            if self._logger is not None:
                self._logger.warning(
                    "delta_staging_max_bytes_reached",
                    uri=self._uri,
                    staged_bytes=self._staged_bytes,
                    max_bytes=self._buffer_policy.max_bytes,
                )
            return True
        if self._buffer_started_at is None:
            return False
        return monotonic() - self._buffer_started_at >= self._buffer_policy.max_age_s

    def _flush(self) -> int:
        self._commit_mini_batch()
        writer = self._ipc_writer
        if writer is None:
            return 0
        n = self._staged_records
        spool = self._spool
        self._spool = self._new_spool()
        self._ipc_writer = None
        self._staged_records = 0
        self._staged_bytes = 0
        self._buffer_started_at = None
        trace_id = uuid4().hex
        correlation_id = uuid4().hex
        t0 = monotonic()
        try:
            writer.close()
            spool.seek(0)
            reader = _pa_ipc.open_stream(spool)
            self._write_to_delta(reader, n, trace_id, correlation_id, t0)
        except Exception:
            if self._logger is not None:
                self._logger.error(
                    "sink_flush_error",
                    backend="delta",
                    uri=self._uri,
                    records=n,
                )
            raise
        finally:
            spool.close()
        return n

    def _write_to_delta(
        self,
        reader: Any,
        n: int,
        trace_id: str,
        correlation_id: str,
        t0: float,
    ) -> None:
        kwargs: dict[str, Any] = {
            "mode": self._mode,
            "storage_options": self._storage_options or None,
        }
        if self._partition_by:
            kwargs["partition_by"] = list(self._partition_by)
        if self._target_file_size is not None:
            kwargs["target_file_size"] = self._target_file_size
        if self._writer_properties is not None:
            kwargs["writer_properties"] = self._writer_properties
        if self._logger is not None:
            self._logger.debug(
                "delta_write_start",
                uri=self._uri,
                records=n,
                mode=self._mode,
            )
        write_deltalake(self._uri, reader, **kwargs)
        if self._logger is not None:
            self._logger.info(
                "sink_flush",
                trace_id=trace_id,
                correlation_id=correlation_id,
                backend="delta",
                uri=self._uri,
                records=n,
                duration_ms=round((monotonic() - t0) * 1000, 1),
            )

    def close(self) -> int:
        """Flush staged records if configured and release all resources.

        Returns:
            How many buffered items the final flush wrote to the Delta table.
        """
        if self._closed:
            return 0
        self._closed = True
        try:
            if self._staged_records and self._buffer_policy.flush_on_shutdown:
                return self._flush()
            if self._staged_records and self._logger is not None:
                self._logger.warning(
                    "delta_staged_records_discarded_on_close",
                    uri=self._uri,
                    records=self._staged_records,
                )
            return 0
        finally:
            if self._ipc_writer is not None:
                with suppress(Exception):
                    self._ipc_writer.close()
            self._spool.close()


# ── ClickHouse helpers ────────────────────────────────────────────────────────


def _to_ch_row(item: Any, column_names: list[str], target: str) -> list[Any]:
    """Return *item*'s values for *column_names*, in order, for a ``clickhouse-connect`` insert.

    The values come from :func:`_set_column_values`, which refuses an ``UNSET``
    value and converts the others as core converts a column value. They then
    get the minimal coercions ClickHouse native types require:

    - ``bool``      → ``int`` (ClickHouse ``UInt8`` expects 0/1, not ``True``/``False``)
    - ``dict``      → JSON text from ``msgspec.json``, keys sorted (no universal
      Map type in CH); a struct arrives here as a dict, and a ``datetime`` or
      ``Decimal`` inside it is encoded as an ISO-8601 or decimal string

    All other values (``str``, ``int``, ``float``, ``datetime``, ``date``,
    ``Decimal``, ``UUID``, ``list``, ``None``) are passed through unchanged, a
    ``list`` as it arrives, its structs already turned into dicts;
    ``clickhouse-connect`` writes a ``Date`` column from ``date`` objects only.
    """
    result: list[Any] = []
    for value in _set_column_values(item, column_names, target):
        if isinstance(value, bool):
            result.append(int(value))
        elif isinstance(value, dict):
            result.append(msgspec.json.encode(value, order="sorted").decode())
        else:
            result.append(value)
    return result


class _ClickHouseTablePartition:
    """Bulk-insert epoch batches into ClickHouse via ``clickhouse-connect``.

    Uses the native HTTP/2 client — no SQLAlchemy, no async bridge required.
    The buffer and flush policy mirror :class:`_DeltaTablePartition`: data
    accumulates in memory and is flushed as a single ``INSERT`` when any
    of the configured thresholds is reached.

    **Column-oriented buffer layout** — the internal buffer is a list of
    *column* lists rather than a list of *row* lists::

        buffer = [
            [col0_row0, col0_row1, ...],   # one list per column
            [col1_row0, col1_row1, ...],
            ...
        ]

    This matches the ``column_oriented=True`` path in ``clickhouse-connect``,
    which skips the internal ``pivot()`` transposition and sends the data
    directly when the batch fits in a single network block (the common case).

    ``list[str]`` and other collection fields are handled by the native
    ClickHouse protocol without JSON serialisation workarounds.

    Args:
        settings:     Resolved ClickHouse sink config.
        payload_type: Struct type whose fields define the column layout.

    Raises:
        ImportError: If ``clickhouse-connect`` is not installed.
    """

    def __init__(
        self,
        settings: ClickHouseSinkConfig,
        payload_type: type[Any],
        logger: Any = None,
    ) -> None:
        if not _CC_AVAILABLE:
            raise ImportError(
                "ClickHouse backend requires 'clickhouse-connect'. "
                "Install it with: pip install clickhouse-connect"
            )
        kwargs: dict[str, Any] = {}
        if settings.insert_timeout is not None:
            t = int(settings.insert_timeout)
            kwargs = {"connect_timeout": t, "send_receive_timeout": t}
        self._client = _cc.get_client(dsn=settings.url, **kwargs)
        self._logger = logger
        self._table = settings.table
        self._target = f"ClickHouse table {settings.table!r}"
        self._column_names = _column_names(payload_type)
        self._buffer_policy = settings.buffer_policy
        # Column-oriented: one inner list per column, grows with each row.
        self._buffer: list[list[Any]] = [[] for _ in self._column_names]
        self._buffer_records = 0
        self._buffer_bytes = 0
        self._buffer_started_at: float | None = None
        self._closed = False

    def write_batch(self, items: Sequence[Any]) -> int:
        """Buffer items and flush to ClickHouse when a threshold is reached.

        Args:
            items: Struct instances to write.  An empty batch still checks
                the age threshold; Bytewax 0.21.1 calls ``write_batch`` only
                when items arrive, so it does not send one on idle epochs.

        Returns:
            How many buffered items this call inserted into ClickHouse.
        """
        if not items:
            if self._buffer_records and self._should_flush():
                return self._flush()
            return 0
        durable = 0
        for item in items:
            if not self._buffer_records:
                self._buffer_started_at = monotonic()
            row = _to_ch_row(item, self._column_names, self._target)
            row_bytes = sum(len(str(v)) for v in row)
            for col_idx, value in enumerate(row):
                self._buffer[col_idx].append(value)
            self._buffer_records += 1
            self._buffer_bytes += row_bytes
            if self._should_flush():
                durable += self._flush()
        return durable

    def close(self) -> int:
        """Flush remaining buffer and close the ClickHouse client.

        Returns:
            How many buffered items the final flush inserted into ClickHouse.
        """
        if self._closed:
            return 0
        self._closed = True
        try:
            if self._buffer_records and self._buffer_policy.flush_on_shutdown:
                return self._flush()
            return 0
        finally:
            self._client.close()

    def _should_flush(self) -> bool:
        if self._closed or not self._buffer_records:
            return False
        if self._buffer_records >= self._buffer_policy.max_records:
            return True
        if self._buffer_bytes >= self._buffer_policy.max_bytes:
            return True
        if self._buffer_started_at is None:
            return False
        return monotonic() - self._buffer_started_at >= self._buffer_policy.max_age_s

    def _flush(self) -> int:
        if not self._buffer_records:
            return 0
        n = self._buffer_records
        columns = self._buffer
        trace_id = uuid4().hex
        correlation_id = uuid4().hex
        t0 = monotonic()
        try:
            self._client.insert(
                self._table,
                columns,
                column_names=self._column_names,
                column_oriented=True,
            )
        except Exception:
            if self._logger is not None:
                self._logger.error(
                    "sink_flush_error",
                    backend="clickhouse",
                    table=self._table,
                    records=n,
                )
            raise
        self._buffer = [[] for _ in self._column_names]
        self._buffer_records = 0
        self._buffer_bytes = 0
        self._buffer_started_at = None
        if self._logger is not None:
            self._logger.info(
                "sink_flush",
                trace_id=trace_id,
                correlation_id=correlation_id,
                backend="clickhouse",
                table=self._table,
                records=n,
                duration_ms=round((monotonic() - t0) * 1000, 1),
            )
        return n


__all__ = [
    "Backend",
    "ClickHouseSinkConfig",
    "DeltaSinkConfig",
    "IntoTable",
    "SqlAlchemyDatabaseConfig",
    "SqlAlchemySinkConfig",
]
