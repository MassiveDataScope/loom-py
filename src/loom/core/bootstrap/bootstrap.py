"""The error raised when building a kernel fails."""

from __future__ import annotations


class BootstrapError(Exception):
    """Raised when the application bootstrap pipeline fails.

    Args:
        message: Human-readable description of the failure.

    Example::

        raise BootstrapError("Failed to compile CreateOrderUseCase") from exc
    """
