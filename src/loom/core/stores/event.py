"""The fields the stores package logs; its events are :class:`UowEvent` and :class:`StoresEvent`."""

from __future__ import annotations

from loom.core.log_names import LogEvent, LogKey, ValueKind


class StoresEvent(LogEvent):
    """What the cursor codec and SQL role authorization log."""

    CURSOR_SIGNING_EPHEMERAL = "stores.cursor_signing_ephemeral"
    CURSOR_SIGNING_UNSIGNED = "stores.cursor_signing_unsigned"
    ROLE_AUTHORIZATION_DENIED = "stores.role_authorization_denied"


class StoresKey(LogKey):
    """The fields of a stores record."""

    STORE = "loom.stores.store"
    OUTCOMES = "loom.stores.outcomes", ValueKind.ARRAY
    CONNECTION = "loom.stores.connection"
    MECHANISM = "loom.stores.mechanism"
