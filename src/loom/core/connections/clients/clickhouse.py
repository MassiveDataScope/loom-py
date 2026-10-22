"""The clickhouse-connect island: the one module of loom that builds a ClickHouse client.

Contexts call :func:`build_clickhouse_client`; the asynchronous SQL service
builds its options with :func:`async_client_options` and opens the client
with :func:`open_async_client`.

Loaded with ``import_optional("loom.core.connections.clients.clickhouse",
extra="clickhouse")``. The DSN goes to the driver as written; explicit
credentials and bounds win over it, as in the driver.
"""

from __future__ import annotations

import warnings
from typing import Any

import clickhouse_connect  # type: ignore[import-untyped]
from clickhouse_connect.driver import httputil  # type: ignore[import-untyped]

from loom.core.connections._base import merge_extra
from loom.core.connections.clickhouse import ClickHouseConnection

_RESERVED = ("dsn", "pool_mgr")

__all__ = ["async_client_options", "build_clickhouse_client", "open_async_client"]


def build_clickhouse_client(connection: ClickHouseConnection, **extra: Any) -> Any:
    """Build a synchronous ``clickhouse_connect`` client for *connection*.

    Args:
        connection: Address, credentials and bounds.
        **extra: Options of the caller's context.

    Returns:
        The driver client; typed ``Any`` because the driver ships no stubs.

    Raises:
        ValueError: When *extra* names an option *connection* decides.
    """
    return clickhouse_connect.get_client(**_client_options(connection, **extra))


def _client_options(connection: ClickHouseConnection, **extra: Any) -> dict[str, Any]:
    """Return the ``get_client`` options *connection* and the context's *extra* make.

    Raises:
        ValueError: When *extra* names an option *connection* decides.
    """
    return merge_extra(_connection_options(connection), extra, connection, reserved=_RESERVED)


def async_client_options(connection: ClickHouseConnection, **extra: Any) -> dict[str, Any]:
    """Return the ``get_async_client`` options *connection* and the context's *extra* make.

    Raises:
        ValueError: When *extra* names an option *connection* decides.
    """
    built = _connection_options(connection)
    if connection.executor_threads is not None:
        built["executor_threads"] = connection.executor_threads
    return merge_extra(built, extra, connection, reserved=_RESERVED)


async def open_async_client(options: dict[str, Any]) -> Any:
    """Open the driver ``AsyncClient`` from options built by :func:`async_client_options`.

    The two steps stay apart so the SQL service keeps a factory it can
    replace in tests, called with the options alone; the 0.15 client runs the
    synchronous one on a thread pool (``executor_threads``): close it on
    shutdown.
    """
    with warnings.catch_warnings():
        # The 0.15.x AsyncClient is a thread-pool wrapper and warns about the
        # upcoming native client; migration to 1.0 is tracked debt.
        warnings.simplefilter("ignore", FutureWarning)
        return await clickhouse_connect.get_async_client(**options)


def _pool_manager(maxsize: int) -> Any:
    """Build a urllib3 pool manager holding up to *maxsize* connections."""
    return httputil.get_pool_manager(maxsize=maxsize)


def _connection_options(connection: ClickHouseConnection) -> dict[str, Any]:
    built: dict[str, Any] = {"dsn": connection.url}
    for name in ("username", "password", "connect_timeout", "send_receive_timeout"):
        value = getattr(connection, name)
        if value is not None:
            built[name] = value
    if connection.pool_size is not None:
        built["pool_mgr"] = _pool_manager(connection.pool_size)
    return built
