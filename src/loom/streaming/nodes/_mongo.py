"""MongoDB CDC input boundary."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Generic

import msgspec
from typing_extensions import TypeVar

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.core.routing import LogicalRef, as_logical_ref
from loom.streaming.mongo._event import MongoCDCEvent

MongoCDCPayloadT = TypeVar(
    "MongoCDCPayloadT",
    bound=LoomStruct | LoomFrozenStruct,
    default=MongoCDCEvent,
    covariant=True,
)


class FromMongoCDC(LoomFrozenStruct, Generic[MongoCDCPayloadT], frozen=True):
    """Declare a MongoDB CDC stream as a streaming input boundary.

    The type parameter ``MongoCDCPayloadT`` identifies the payload type
    produced by this source.  It defaults to ``MongoCDCEvent`` so callers
    that do not need to override the payload type can omit it.

    Args:
        name: Logical input reference used to resolve Mongo source config.
        collections: Collection names to watch. An empty tuple means a
            database-level watch.
        watch_options: Driver watch kwargs resolved later by compiler/runtime.
    """

    name: str
    collections: tuple[str, ...] = ()
    watch_options: Mapping[str, object] = msgspec.field(default_factory=dict)

    @property
    def logical_ref(self) -> LogicalRef:
        """Return the logical input reference."""
        return as_logical_ref(self.name)


__all__ = ["FromMongoCDC"]
