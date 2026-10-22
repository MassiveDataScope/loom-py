"""How the ETL ClickHouse sources, targets and client steps open their client.

All three take the ``storage.clickhouse`` connection, or a bare DSN as a
shortcut, and open their client through the ClickHouse island of
:mod:`loom.core.connections.clients`.
"""

from __future__ import annotations

from typing import Any

from loom.core.connections import ClickHouseConnection
from loom.core.connections.clients import clickhouse_island
from loom.core.plugins.optional import MissingExtraError


def resolve_connection(
    url: str | None, connection: ClickHouseConnection | None, *, owner: str
) -> ClickHouseConnection | None:
    """Return the connection a component was given, building it from a bare DSN.

    Raises:
        ValueError: When both a DSN and a connection are given.
    """
    if url and connection is not None:
        raise ValueError(f"{owner} takes 'url' or 'connection', not both")
    if connection is not None:
        return connection
    return ClickHouseConnection(url=url) if url else None


def open_client(connection: ClickHouseConnection, *, owner: str) -> Any:
    """Open a synchronous ClickHouse client for *connection*.

    Raises:
        ImportError: When ``clickhouse-connect`` is not installed, naming *owner*
            and the extra.
    """
    try:
        island = clickhouse_island()
    except MissingExtraError as exc:
        raise ImportError(
            f"{owner} requires 'clickhouse-connect'. "
            "Install it with: pip install 'loom-kernel[clickhouse]'"
        ) from exc
    return island.build_clickhouse_client(connection)


__all__ = ["open_client", "resolve_connection"]
