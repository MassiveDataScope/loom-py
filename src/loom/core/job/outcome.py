"""Retry policy shared by job transports."""

from loom.core.engine.post_commit import PostCommitError


def is_retryable(exc: Exception) -> bool:
    """Return whether repeating the job is safe according to its error."""
    return bool(getattr(exc, "retryable", True))


def failure_log_fields(exc: Exception) -> dict[str, object]:
    """Return transport-neutral details for a failed job."""
    if isinstance(exc, PostCommitError):
        return {"committed": exc.committed, "failures": len(exc.failures)}
    return {}
