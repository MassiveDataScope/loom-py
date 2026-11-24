"""The records configuration loading logs, and the fields they carry."""

from __future__ import annotations

from loom.core.log_names import LogEvent, LogKey, ValueKind


class ConfigEvent(LogEvent):
    """What loading and resolving configuration logs."""

    KEYS_OVERRIDDEN = "loom.config.keys_overridden"
    RESOLVER_DEFAULT_SKIPPED = "loom.config.resolver_default_skipped"
    RESOLVER_FETCHED = "loom.config.resolver_fetched"


class ConfigLogKey(LogKey):
    """The fields of a configuration record."""

    KEY = "loom.config.key"
    KEYS = "loom.config.keys", ValueKind.ARRAY
    RESOLVER = "loom.config.resolver"
