"""Shared streaming exception types."""

from __future__ import annotations

from loom.core.errors import LoomError


class LoomStreamingError(LoomError):
    """Base class for explicit streaming-layer errors."""


class UnsupportedNodeError(LoomStreamingError):
    """Raised when a DSL node is not supported by the current runtime path."""


class MissingSinkError(LoomStreamingError):
    """Raised when a terminal sink is required but absent."""


class MissingBridgeError(LoomStreamingError):
    """Raised when async execution is required but no bridge exists."""


class DuplicateErrorSinkError(LoomStreamingError):
    """Raised when two registered sinks cover the same ErrorKind."""


__all__ = [
    "DuplicateErrorSinkError",
    "LoomStreamingError",
    "MissingBridgeError",
    "MissingSinkError",
    "UnsupportedNodeError",
]
