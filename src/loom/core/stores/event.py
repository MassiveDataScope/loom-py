"""The fields the stores package logs; its events are :class:`UowEvent` and :class:`StoresEvent`."""

from __future__ import annotations

from loom.core.log_names import LogEvent, LogKey, ValueKind


class StoresEvent(LogEvent):
    """What the cursor codec and SQL role authorization log."""

    CURSOR_SIGNING_WEAKENED = "stores.cursor_signing_weakened"
    ROLE_AUTHORIZATION_DENIED = "stores.role_authorization_denied"


class StoresKey(LogKey):
    """The fields of a stores record."""

    STORE = "loom.stores.store"
    OUTCOMES = "loom.stores.outcomes", ValueKind.ARRAY
    CONNECTION = "loom.stores.connection"
    MECHANISM = "loom.stores.mechanism"
    CURSOR_SIGNING = "loom.stores.cursor_signing"
