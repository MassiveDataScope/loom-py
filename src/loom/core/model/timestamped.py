"""Opt-in timestamped base model.

Provides :class:`TimestampedModel` for domain models that require audit
timestamps.  Timestamps are populated by the database server, not by
application code, avoiding clock-skew issues in distributed deployments.

Usage::

    from loom.core.model import TimestampedModel, ColumnField, DateTime

    class Order(TimestampedModel):
        __tablename__ = "orders"
        id: int = ColumnField(primary_key=True, autoincrement=True)
        total: float
"""

from __future__ import annotations

from datetime import datetime

from loom.core.model.base import BaseModel
from loom.core.model.enums import ServerDefault, ServerOnUpdate
from loom.core.model.field import ColumnField


class TimestampedModel(BaseModel):
    """Opt-in base for domain models that need audit timestamps.

    Adds ``created_at`` and ``updated_at`` as nullable server-default
    columns. Both fields are omitted from serialisation output when
    ``None``.

    **Design note:** ``trace_id`` is intentionally absent. Trace
    identifiers are an observability concern belonging to the transport
    layer, not to the domain model.

    Attributes:
        created_at: UTC timestamp set by the database at INSERT time.
            ``None`` until the first flush/commit.
        updated_at: UTC timestamp updated by the database on every UPDATE.
            ``None`` until the first flush/commit.
    """

    created_at: datetime | None = ColumnField(
        server_default=ServerDefault.NOW,
    )
    updated_at: datetime | None = ColumnField(
        server_default=ServerDefault.NOW,
        server_onupdate=ServerOnUpdate.NOW,
    )
