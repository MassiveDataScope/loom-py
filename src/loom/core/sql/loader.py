"""The typed way to load the ClickHouse SQL island, and what the island publishes.

Importing this module loads no SDK: :func:`sql_island` loads
:mod:`loom.core.sql.clickhouse` through
:func:`~loom.core.plugins.optional.import_optional`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, cast

from loom.core.plugins.optional import import_optional

if TYPE_CHECKING:
    from loom.core.sql.clickhouse import ClickHouseConnectionRegistry

_ISLAND = "loom.core.sql.clickhouse"


class SqlIsland(Protocol):
    """What :mod:`loom.core.sql.clickhouse` publishes to the composition."""

    @property
    def ClickHouseConnectionRegistry(self) -> type[ClickHouseConnectionRegistry]: ...  # noqa: N802


def sql_island() -> SqlIsland:
    """Load the ClickHouse SQL island.

    Raises:
        MissingExtraError: When clickhouse-connect is not installed.
    """
    return cast(SqlIsland, import_optional(_ISLAND, extra="clickhouse"))


__all__ = ["SqlIsland", "sql_island"]
