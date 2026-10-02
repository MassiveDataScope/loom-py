from __future__ import annotations

from loom.core.model.field import ColumnType


class _PostgresTypes:
    """Namespace for PostgreSQL-specific column types."""

    JSONB = ColumnType("Postgres.JSONB")
    UUID = ColumnType("Postgres.UUID")
    TSVECTOR = ColumnType("Postgres.TSVECTOR")
    INET = ColumnType("Postgres.INET")

    @staticmethod
    def ARRAY(item: ColumnType) -> ColumnType:
        return ColumnType("Postgres.ARRAY", args=(item,))


Postgres = _PostgresTypes()

__all__ = ["Postgres"]
