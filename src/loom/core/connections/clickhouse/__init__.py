"""ClickHouse: the connection every context shares, and the typed loader of its island.

Importing this package loads no SDK; the client is built by
:mod:`loom.core.connections.clickhouse.client`, loaded with :func:`clickhouse_island`.
"""

from __future__ import annotations

from loom.core.connections.clickhouse.config import ClickHouseConnection
from loom.core.connections.clickhouse.loader import ClickHouseIsland, clickhouse_island

__all__ = ["ClickHouseConnection", "ClickHouseIsland", "clickhouse_island"]
