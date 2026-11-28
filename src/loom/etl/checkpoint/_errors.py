"""Errors raised while deleting checkpoint trees."""

from __future__ import annotations

from loom.core.errors import SystemError


class CheckpointCleanupError(SystemError):
    """Raised when a checkpoint tree cannot be deleted."""
