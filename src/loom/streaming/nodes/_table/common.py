"""IntoTable streaming sink node — writes typed events to SQLAlchemy, Delta or ClickHouse tables.

IntoTable is a frozen dataclass that implements IntoSink structurally.
The compiler detects it via isinstance(node, IntoSink) without any
registration or inheritance.

Supported backends
------------------
- ``Backend.SQLALCHEMY``: bulk-inserts rows using SQLAlchemy async sessions.
  Requires an async bridge and a shared SQLAlchemy session manager from the
  Bytewax runtime.
  Install: ``pip install sqlalchemy``.
- ``Backend.DELTA``: stages rows as a PyArrow IPC stream in a spooled
  temporary file and appends it to Delta via delta-rs.
  Requires a ``uri`` key in the resolved config section.
  Install: ``pip install deltalake polars``.
- ``Backend.CLICKHOUSE``: bulk-inserts rows via ``clickhouse-connect``.
  Requires a ``database`` reference whose ``database.<name>`` section is a
  ClickHouse connection (``url``, optional credentials and timeouts).
  Install: ``pip install clickhouse-connect``.

Config example (streaming.sinks.<name>)::

    database:
      warehouse:
        url: postgresql+asyncpg://user:pass@host/db
        pool:
          size: 2
          pre_ping: true

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
    from loom.core.persistence import STREAMING_PROFILE, load_database_config
    from loom.core.repository.sqlalchemy import build_session_manager
    from loom.streaming.nodes._table import SqlAlchemySinkConfig

    ctx = ConfigContext.from_yaml("config.yaml")
    sink_cfg = SqlAlchemySinkConfig.from_config(
        ctx.section("streaming.sinks.orders_sink", dict),
        default_table="orders",
    )
    db_cfg = load_database_config(ctx, f"database.{sink_cfg.database}")
    session_manager = build_session_manager(db_cfg, STREAMING_PROFILE)

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

from collections.abc import Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from enum import StrEnum
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

import msgspec

from loom.core.async_bridge import AsyncBridge
from loom.core.connections import ClickHouseConnection
from loom.core.errors.codes import ErrorCode
from loom.core.model import BoundaryValidationError
from loom.core.model.convert import to_column_value
from loom.core.model.introspection import get_column_fields
from loom.core.persistence.database import DatabaseConfig
from loom.core.plugins.optional import import_optional
from loom.streaming.core._message import StreamPayload
from loom.streaming.nodes._sink import SinkPartition

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
        DELTA:       Stage rows as a PyArrow IPC stream and append it to a Delta Lake table.
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
            sqlalchemy = cast(
                Any, import_optional("loom.streaming.nodes._table._sqlalchemy", extra="sqlalchemy")
            )
            return cast(
                SinkPartition[EventT],
                sqlalchemy._SQLAlchemyTablePartition(
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
            delta = cast(
                Any, import_optional("loom.streaming.nodes._table._delta", extra="etl-polars")
            )
            return cast(
                SinkPartition[EventT],
                delta._DeltaTablePartition(config, self.payload, logger=logger),
            )
        if self.backend is Backend.CLICKHOUSE:
            if not isinstance(config, ClickHouseSinkConfig):
                raise TypeError("IntoTable backend=clickhouse requires a ClickHouseSinkConfig.")
            clickhouse = cast(
                Any, import_optional("loom.streaming.nodes._table._clickhouse", extra="clickhouse")
            )
            return cast(
                SinkPartition[EventT],
                clickhouse._ClickHouseTablePartition(config, self.payload, logger=logger),
            )
        raise ValueError(f"Unsupported backend: {self.backend!r}")


def _optional_int(value: Any, default: int) -> int:
    if value is None:
        return default
    return int(value)


_BUFFER_KEYS: frozenset[str] = frozenset(
    {"buffer_max_records", "buffer_max_bytes", "buffer_max_age_s", "flush_on_shutdown"}
)
SQLALCHEMY_INLINE_DATABASE_KEYS: frozenset[str] = frozenset(DatabaseConfig.__struct_fields__) - {
    "schema"
}
"""Database keys an inline-``url`` sink takes: the ``database`` contract but ``schema``."""
_SQLALCHEMY_SINK_KEYS: frozenset[str] = _BUFFER_KEYS | {"table", "database", "url", "chunk_size"}
_DELTA_SINK_KEYS: frozenset[str] = _BUFFER_KEYS | {
    "table",
    "uri",
    "mode",
    "storage_options",
    "partition_by",
    "target_file_size",
    "spool_max_bytes",
    "staging",
    "writer_properties",
}
_DELTA_STAGING_KEYS: frozenset[str] = frozenset({"compression", "mini_batch_size"})
_CLICKHOUSE_SINK_KEYS: frozenset[str] = _BUFFER_KEYS | {
    "table",
    "insert_timeout",
}


def _mapping_section(config: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    """Return the nested section *key* of a Delta sink config, ``{}`` when unset.

    Raises:
        ValueError: If *key* is set to something other than a mapping.
    """
    value = config.get(key)
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(
            f"Delta sink config '{key}' must be a mapping, got {type(value).__name__}."
        )
    return value


def _refuse_unknown_keys(
    config: Mapping[str, Any], known: frozenset[str], *, sink: str, prefix: str = ""
) -> None:
    """Raise when *config* carries a key the sink does not read.

    Raises:
        ValueError: Naming every unknown key, sorted.
    """
    unknown = sorted(f"{prefix}{key}" for key in config if key not in known)
    if not unknown:
        return
    noun = "key" if len(unknown) == 1 else "keys"
    names = ", ".join(repr(key) for key in unknown)
    raise ValueError(f"{sink} sink config has unknown {noun} {names}.")


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
        known = (
            _SQLALCHEMY_SINK_KEYS
            if database
            else _SQLALCHEMY_SINK_KEYS | SQLALCHEMY_INLINE_DATABASE_KEYS
        )
        _refuse_unknown_keys(config, known, sink="SQLAlchemy")
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
    writer_properties: Any | None
    buffer_policy: _TableBufferPolicy
    mini_batch_size: int = 500

    @classmethod
    def from_config(cls, config: Mapping[str, Any], *, default_table: str) -> Self:
        """Build Delta sink settings from a resolved sink config.

        Without ``writer_properties`` the final write uses row groups of at
        most 10 000 rows.

        Raises:
            ValueError: If ``uri`` is empty, a value is invalid, or the config
                or its ``staging`` carries a key the sink does not read.
        """
        _refuse_unknown_keys(config, _DELTA_SINK_KEYS, sink="Delta")
        table = str(config.get("table") or default_table).strip()
        uri = str(config.get("uri") or "").strip()
        if not uri:
            raise ValueError("Delta sink config requires a non-empty 'uri'.")
        raw_partition_by = config.get("partition_by") or []
        staging_config = _mapping_section(config, "staging")
        _refuse_unknown_keys(staging_config, _DELTA_STAGING_KEYS, sink="Delta", prefix="staging.")
        staging_compression = _validate_ipc_compression(staging_config.get("compression"))
        delta = cast(Any, import_optional("loom.streaming.nodes._table._delta", extra="etl-polars"))
        writer_properties = delta.build_writer_properties(
            _mapping_section(config, "writer_properties")
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
            writer_properties=writer_properties,
            buffer_policy=_TableBufferPolicy.from_config(config),
            mini_batch_size=_optional_int(staging_config.get("mini_batch_size"), 500),
        )


@dataclass(frozen=True, slots=True)
class ClickHouseSinkConfig:
    """Resolved ``IntoTable`` settings for the ClickHouse backend.

    The connection is the ``database.<name>`` section the sink names, decoded
    as the :class:`~loom.core.connections.ClickHouseConnection` every loom
    context reading ClickHouse shares: DSN, credentials and bounds.

    Args:
        connection:     ClickHouse connection of the target database.
        table:          Target table name.
        buffer_policy:  Common flush policy (max_records, max_bytes, max_age_s).
        insert_timeout: Seconds before a single insert call is aborted, used
            only when the connection declares no timeout of its own.
            ``None`` disables the timeout (default).
    """

    connection: ClickHouseConnection
    table: str
    buffer_policy: _TableBufferPolicy
    insert_timeout: float | None = None

    @classmethod
    def from_config(
        cls,
        config: Mapping[str, Any],
        *,
        default_table: str,
        connection: ClickHouseConnection,
    ) -> Self:
        """Build ClickHouse sink settings from a resolved sink config.

        Args:
            config: The sink section.
            default_table: Table used when the section names none.
            connection: The ClickHouse connection of the sink's database.

        Raises:
            ValueError: If the table is empty or the config carries a key the
                sink does not read.
        """
        _refuse_unknown_keys(config, _CLICKHOUSE_SINK_KEYS, sink="ClickHouse")
        table = str(config.get("table") or default_table).strip()
        if not table:
            raise ValueError("ClickHouse sink config requires a non-empty table name.")
        timeout_raw = config.get("insert_timeout")
        return cls(
            connection=connection,
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


def _row_size_bytes(row: Mapping[str, Any]) -> int:
    """Estimate the serialized size of one row for buffer accounting."""
    return len(msgspec.json.encode(dict(row)))


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


def _validate_delta_mode(value: object) -> Literal["error", "append", "ignore"]:
    mode = str(value)
    if mode not in _DELTA_MODES:
        raise ValueError(
            f"Invalid Delta write mode {mode!r}. Must be one of: {sorted(_DELTA_MODES)}"
        )
    return mode  # type: ignore[return-value]


__all__ = [
    "SQLALCHEMY_INLINE_DATABASE_KEYS",
    "Backend",
    "ClickHouseSinkConfig",
    "DeltaSinkConfig",
    "IntoTable",
    "SqlAlchemySinkConfig",
]
