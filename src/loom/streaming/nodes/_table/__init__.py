"""Public table sink package for streaming nodes.

The user-facing API stays on ``loom.streaming.nodes._table``. Shared
contracts live in :mod:`loom.streaming.nodes._table.common`; each storage
backend has its own private adapter.
"""

from loom.streaming.nodes._table.common import (
    ClickHouseSinkConfig,
    DeltaSinkConfig,
    IntoTable,
    SqlAlchemySinkConfig,
    TableEngine,
)

__all__ = [
    "TableEngine",
    "ClickHouseSinkConfig",
    "DeltaSinkConfig",
    "IntoTable",
    "SqlAlchemySinkConfig",
]
