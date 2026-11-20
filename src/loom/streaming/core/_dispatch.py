"""How a heterogeneous source maps message types to payload types, whatever the transport."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from loom.core.model import LoomFrozenStruct, LoomStruct


@dataclass(frozen=True)
class DispatchTable:
    """Pre-built decode dispatch table for a source that carries several message types.

    Args:
        plain: Maps outer ``message_type`` strings to their payload types.
        error: Maps inner ``ErrorEnvelope.payload_type`` strings to the
            corresponding ``ErrorEnvelope[T]`` generic alias.
        wire: Maps outer wire-error ``message_type`` strings to their payload
            types.
    """

    plain: Mapping[str, type[LoomStruct | LoomFrozenStruct]]
    error: Mapping[str, Any]
    wire: Mapping[str, type[LoomStruct | LoomFrozenStruct]]
