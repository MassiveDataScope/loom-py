"""The records configuration loading logs, and the fields they carry."""

from __future__ import annotations

from loom.core.log_names import LogEvent, LogKey, ValueKind


class ConfigEvent(LogEvent):
    """What loading and resolving configuration logs."""

    KEYS_OVERRIDDEN = "config.keys_overridden"
    RESOLVER_DEFAULT_SKIPPED = "config.resolver_default_skipped"
    RESOLVER_FETCHING = "config.resolver_fetching"


class ConfigLogKey(LogKey):
    """The fields of a configuration record."""

    KEY = "loom.config.key"
    KEYS = "loom.config.keys", ValueKind.ARRAY
    RESOLVER = "loom.config.resolver"
