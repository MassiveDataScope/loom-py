"""Typed MongoDB CDC configuration contracts."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Literal

import msgspec

from loom.core.model import LoomFrozenStruct
from loom.core.routing import DefaultingRouteResolver, LogicalRef

_URI_CREDENTIALS_RE = re.compile(r"://[^@]+@")


def _redact_uri(uri: str) -> str:
    return _URI_CREDENTIALS_RE.sub("://***@", uri)


class MongoSourceConfig(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Connection settings for one MongoDB CDC source."""

    uri: str
    database: str
    watch_options: Mapping[str, object] = msgspec.field(default_factory=dict)
    server_api_version: str | None = None
    on_oplog_expired: Literal["fail", "restart_from_now"] = "fail"

    def __repr__(self) -> str:
        return (
            f"MongoSourceConfig(uri={_redact_uri(self.uri)!r},"
            f" database={self.database!r},"
            f" server_api_version={self.server_api_version!r},"
            f" on_oplog_expired={self.on_oplog_expired!r})"
        )


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
