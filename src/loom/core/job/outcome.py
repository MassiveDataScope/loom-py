"""Retry policy shared by job transports."""


def is_retryable(exc: Exception) -> bool:
    """Return whether repeating the job is safe according to its error."""
    return bool(getattr(exc, "retryable", True))
