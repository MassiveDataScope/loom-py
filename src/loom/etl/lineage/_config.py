"""ETL lineage configuration extending the core observability config."""

from __future__ import annotations

from typing import Any

import msgspec

from loom.core.model import LoomFrozenStruct
from loom.core.observability.config import LogObservabilityConfig, ObservabilityConfig


class LineageConfig(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Persisted execution-lineage destination settings."""

    enabled: bool = False
    root: str = ""
    database: str = ""
    storage_options: dict[str, str] = msgspec.field(default_factory=dict)
    writer: dict[str, Any] = msgspec.field(default_factory=dict)
    delta_config: dict[str, str | None] = msgspec.field(default_factory=dict)
    commit: dict[str, Any] = msgspec.field(default_factory=dict)

    def validate(self) -> None:
        """Validate that exactly one destination mode is configured when enabled."""
        if not self.enabled:
            return
        has_root = bool(self.root.strip())
        has_database = bool(self.database.strip())
        if has_root == has_database:
            raise ValueError("lineage requires exactly one destination: root or database")


class ETLObservabilityConfig(ObservabilityConfig, frozen=True, kw_only=True):
    """ETL observability config extending the core runtime config with lineage."""

    log: LogObservabilityConfig = msgspec.field(
        default_factory=lambda: LogObservabilityConfig(enabled=False)
    )
    lineage: LineageConfig = msgspec.field(default_factory=LineageConfig)


__all__ = ["ETLObservabilityConfig", "LineageConfig"]
