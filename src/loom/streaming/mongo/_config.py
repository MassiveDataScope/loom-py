"""Typed MongoDB CDC configuration contracts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal

import msgspec

from loom.core.config import ConfigContext, ConfigKey
from loom.core.connections._base import reject_retired_keys
from loom.core.connections.mongo import MongoConnection
from loom.core.model import LoomFrozenStruct
from loom.core.routing import DefaultingRouteResolver, LogicalRef


class MongoSourceConfig(MongoConnection, frozen=True, kw_only=True):
    """One MongoDB change-stream source: the Mongo connection plus how to watch it.

    Every key of :class:`~loom.core.connections.MongoConnection` applies, as
    in every other context reading MongoDB; the repr hides the URL's
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


def load_mongo_config(ctx: ConfigContext) -> MongoConfig:
    """Read the ``mongo`` section of *ctx*, refusing a retired key in any of its sources.

    Raises:
        ConfigError: When the section is absent or invalid, or a source holds
            a retired key, naming its replacement.
    """
    key = ConfigKey.MONGO.value
    raw = ctx.raw(key)
    if isinstance(raw, Mapping):
        reject_retired_keys(raw.get("source"), f"{key}.source", MongoSourceConfig)
        sources = raw.get("sources")
        if isinstance(sources, Mapping):
            for name, source in sources.items():
                reject_retired_keys(source, f"{key}.sources.{name}", MongoSourceConfig)
    return ctx.section(key, MongoConfig)


__all__ = ["MongoConfig", "MongoSourceConfig", "load_mongo_config"]
