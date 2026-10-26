"""The typed way to load the clickhouse-connect island, and what the island publishes.

Importing this module loads no SDK: :func:`clickhouse_island` loads
:mod:`loom.core.connections.clickhouse.client` through
:func:`~loom.core.plugins.optional.import_optional`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, cast

from loom.core.plugins.optional import import_optional

if TYPE_CHECKING:
    from loom.core.connections.clickhouse.config import ClickHouseConnection

_ISLAND = "loom.core.connections.clickhouse.client"


class ClickHouseIsland(Protocol):
    """What :mod:`loom.core.connections.clickhouse.client` publishes.

    The clients are typed ``Any``: clickhouse-connect ships no stubs.
    """

    def build_clickhouse_client(self, connection: ClickHouseConnection, **extra: Any) -> Any: ...

    def async_client_options(
        self, connection: ClickHouseConnection, **extra: Any
    ) -> dict[str, Any]: ...

    async def open_async_client(self, options: dict[str, Any]) -> Any: ...


def clickhouse_island() -> ClickHouseIsland:
    """Load the clickhouse-connect island.

    Raises:
        MissingExtraError: When clickhouse-connect is not installed.
    """
    return cast(ClickHouseIsland, import_optional(_ISLAND, extra="clickhouse"))


__all__ = ["ClickHouseIsland", "clickhouse_island"]
