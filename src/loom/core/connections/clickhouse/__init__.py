"""ClickHouse: the connection every context shares.

The driver client is built by :mod:`loom.connections.clickhouse`; this package loads no SDK.
"""

from __future__ import annotations

from loom.core.connections.clickhouse.config import ClickHouseConnection

__all__ = ["ClickHouseConnection"]
