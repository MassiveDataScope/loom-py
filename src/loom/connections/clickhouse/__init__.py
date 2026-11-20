"""ClickHouse: the loader of the island that builds the driver client.

Importing this package loads no SDK; the client is built by
:mod:`loom.connections.clickhouse.client`, loaded with :func:`clickhouse_island`.
"""

from __future__ import annotations

from loom.connections.clickhouse.loader import ClickHouseIsland, clickhouse_island

__all__ = ["ClickHouseIsland", "clickhouse_island"]
