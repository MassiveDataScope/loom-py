"""ClickHouse target — IntoClickHouse builder, ClickHouseTableSpec, and ClickHouseClientExecutor."""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

import polars as pl

from loom.core.connections import ClickHouseConnection
from loom.core.logger import get_logger
from loom.etl.declarative.target import TargetSpec
from loom.etl.declarative.target._client import ClientSpec
from loom.etl.io._clickhouse_connection import open_client, resolve_connection

_log = get_logger(__name__)


@dataclass(frozen=True)
class ClickHouseTableSpec:
    """Compiled spec for a ClickHouse write target.

    Produced by :meth:`IntoClickHouse._to_spec`. Consumed by the executor
    and :class:`ClickHouseTargetWriter` — never constructed directly in user code.
    """

    table: str
    write_mode: Literal["replace", "append"] = "append"
    database: str | None = None

    @property
    def kind(self) -> str:
        """Target kind identifier."""
        return "clickhouse"


class IntoClickHouse:
    """Declare a ClickHouse table as the write target for an ETL step.

    Args:
        table:    Target table name.
        database: Optional database override.  When omitted, the executor
                  uses the database configured in the storage section.

    Example::

        target = IntoClickHouse("motos").replace()
        target = IntoClickHouse("moto_features", database="analytics").append()
    """

    __slots__ = ("_table", "_database", "_write_mode")

    def __init__(self, table: str, *, database: str | None = None) -> None:
        self._table: str = table
        self._database: str | None = database
        self._write_mode: Literal["replace", "append"] = "append"

    def replace(self) -> IntoClickHouse:
        """Set write mode to TRUNCATE + INSERT (idempotent full overwrite)."""
        return self._clone(_write_mode="replace")

    def append(self) -> IntoClickHouse:
        """Set write mode to INSERT without truncate."""
        return self._clone(_write_mode="append")

    def _to_spec(self) -> ClickHouseTableSpec:
        """Compile to a frozen spec for the executor."""
        return ClickHouseTableSpec(
            table=self._table,
            write_mode=self._write_mode,
            database=self._database,
        )

    def _clone(self, **overrides: Any) -> IntoClickHouse:
        new = object.__new__(IntoClickHouse)
        for slot in self.__slots__:
            object.__setattr__(new, slot, overrides.get(slot, getattr(self, slot)))
        return new

    def __repr__(self) -> str:
        return f"IntoClickHouse({self._table!r}, mode={self._write_mode!r})"


class ClickHouseTargetWriter:
    """Writes a Polars DataFrame to ClickHouse via clickhouse-connect.

    Uses the native HTTP/2 columnar insert protocol — no SQLAlchemy required.

    Args:
        url:        ClickHouse DSN, e.g. ``clickhouse://host:8123/default``; a
                    shortcut for ``connection=ClickHouseConnection(url=url)``.
        connection: The ``storage.clickhouse`` connection: DSN, credentials and
                    bounds.  Exactly one of *url* and *connection* is given.

    Raises:
        ImportError: If ``clickhouse-connect`` is not installed.
        ValueError: When neither or both of *url* and *connection* are given.
    """

    def __init__(
        self, url: str | None = None, *, connection: ClickHouseConnection | None = None
    ) -> None:
        resolved = resolve_connection(url, connection, owner="ClickHouseTargetWriter")
        if resolved is None:
            raise ValueError("ClickHouseTargetWriter requires 'url' or 'connection'")
        self._client = open_client(resolved)

    def write(
        self,
        frame: Any,
        spec: ClickHouseTableSpec | TargetSpec,
        params: Any,
        /,
        *,
        streaming: bool = False,
        write_ctx: Any = None,
    ) -> None:
        """Write *frame* to the ClickHouse table described by *spec*.

        For ``replace`` mode the table is truncated before the insert so the
        operation is idempotent (same semantics as ``dbt full_refresh``).
        The truncation window is typically 3–15 s; plan accordingly.

        Args:
            frame:     Polars ``LazyFrame`` or ``DataFrame``.
            spec:      Compiled ClickHouse target spec.
            params:    ETL step params (unused — reserved for future predicate
                       parametrization).
            streaming: Ignored — ClickHouse inserts are always materialised
                       before the network call.
            write_ctx: Execution context for audit-column injection (unused
                       in ClickHouse targets).
        """
        if isinstance(spec, ClientSpec):
            raise TypeError(
                "ClickHouseTargetWriter.write() does not handle ClientSpec. "
                "ClientStep execution is routed by ETLExecutor before write() is called. "
                "This is an internal error — ClientSpec should never reach write()."
            )
        if not isinstance(spec, ClickHouseTableSpec):
            raise TypeError(
                f"ClickHouseTargetWriter.write() does not handle {type(spec).__qualname__}. "
                "The writer registry dispatches a spec to this writer by its 'clickhouse' "
                "kind, so this is an internal error."
            )
        _ = write_ctx
        engine: Literal["streaming", "auto"] = "streaming" if streaming else "auto"
        df: pl.DataFrame = (
            frame.collect(engine=engine) if isinstance(frame, pl.LazyFrame) else frame
        )
        if df.is_empty():
            _log.warning("write_produced_no_rows", target=spec.table)
            return

        qualified = f"{spec.database}.{spec.table}" if spec.database else spec.table

        if spec.write_mode == "replace":
            _log.debug("clickhouse_truncate_table", table=qualified)
            self._client.command(f"TRUNCATE TABLE IF EXISTS {qualified}")

        column_names = df.columns
        data = [df[col].to_list() for col in column_names]
        _log.debug(
            "clickhouse_insert",
            rows=len(df),
            cols=len(column_names),
            table=qualified,
        )
        self._client.insert(
            qualified,
            data,
            column_names=column_names,
            column_oriented=True,
        )


class ClickHouseClientExecutor:
    """Provides a ClickHouse client to :class:`~loom.etl.ClientStep` instances.

    Implements :class:`~loom.etl.runtime.contracts.ClientCommandExecutor`.
    Inject into :class:`~loom.etl.executor.ETLExecutor` (or
    :class:`~loom.etl.ETLRunner`) to enable ``ClientStep`` execution against
    ClickHouse.

    Args:
        url:        ClickHouse DSN, e.g. ``clickhouse://host:8123/db``; a shortcut
                    for ``connection=ClickHouseConnection(url=url)``.
        connection: The ``storage.clickhouse`` connection: DSN, credentials and
                    bounds.  Mutually exclusive with *url*.
        client:     Pre-built ``clickhouse_connect`` client.  Use for testing or
                    when the client is managed externally.  Takes precedence
                    over *url* and *connection*.

    Raises:
        ImportError: If ``clickhouse-connect`` is not installed and *client*
            is not provided.
        ValueError: When both *url* and *connection* are given, or when none
            of the three is provided and :meth:`command` is called.

    Example::

        from loom.etl import ETLRunner
        from loom.etl.io import ClickHouseClientExecutor

        runner = ETLRunner(
            reader=ch_reader,
            writer=ch_writer,
            client_executor=ClickHouseClientExecutor(url="clickhouse://user:pass@host:8123/db"),
        )
    """

    def __init__(
        self,
        url: str | None = None,
        *,
        connection: ClickHouseConnection | None = None,
        client: Any | None = None,
    ) -> None:
        self._connection = resolve_connection(url, connection, owner="ClickHouseClientExecutor")
        self._client = client
        self._client_lock = threading.Lock()

    def command(self, fn: Callable[[Any], None]) -> None:
        """Resolve the ClickHouse client and invoke *fn* with it.

        Args:
            fn: Callable produced by the executor that accepts the raw
                ``clickhouse_connect`` client and executes the step logic.

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
                    "ClickHouseClientExecutor requires 'url', 'connection' or 'client' "
                    "at construction time."
                )
            self._client = open_client(self._connection)
            return self._client


__all__ = [
    "ClickHouseTableSpec",
    "ClickHouseTargetWriter",
    "IntoClickHouse",
    "ClickHouseClientExecutor",
]
