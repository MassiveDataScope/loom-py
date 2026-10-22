"""Typed MongoDB CDC configuration contracts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal

import msgspec

from loom.core.connections import MongoConnection
from loom.core.model import LoomFrozenStruct
from loom.core.routing import DefaultingRouteResolver, LogicalRef


class MongoSourceConfig(MongoConnection, frozen=True, kw_only=True):
    """One MongoDB change-stream source: the Mongo connection plus how to watch it.

    Every key of :class:`~loom.core.connections.MongoConnection` applies, as
    in every other context reading MongoDB; the repr hides the URI's
    credentials.

    Args:
        watch_options: Options handed to ``watch()``.
        on_oplog_expired: What to do when the resume token fell off the oplog.
    """

    watch_options: Mapping[str, object] = msgspec.field(default_factory=dict)
    on_oplog_expired: Literal["fail", "restart_from_now"] = "fail"


class MongoConfig(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Top-level MongoDB settings loaded from the ``mongo`` config section."""

    source: MongoSourceConfig | None = None
    sources: dict[str, MongoSourceConfig] = msgspec.field(default_factory=dict)

    def source_for(self, ref: str | LogicalRef) -> MongoSourceConfig:
        """Resolve source settings by logical reference with default fallback."""
        resolver = DefaultingRouteResolver(
            default=self.source,
            overrides=self.sources,
            kind="Mongo source config",
        )
        return resolver.resolve(ref)


__all__ = ["MongoConfig", "MongoSourceConfig"]
