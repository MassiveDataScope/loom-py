"""Path helpers for checkpoint scope layout."""

from __future__ import annotations

from loom.core.errors import InternalError
from loom.etl.checkpoint._cleaners import _join_path
from loom.etl.checkpoint._scope import CheckpointScope


def run_scope_base(root: str, run_id: str) -> str:
    """Return base path for RUN-scope intermediates."""
    return _join_path(root, "runs", run_id)


def correlation_scope_base(root: str, correlation_id: str) -> str:
    """Return base path for CORRELATION-scope intermediates."""
    return _join_path(root, "correlations", correlation_id)


def scope_base(
    root: str,
    *,
    run_id: str,
    correlation_id: str | None,
    scope: CheckpointScope,
) -> str:
    """Return scope base path for one checkpoint write/read operation.

    Args:
        root: Checkpoint root configured in :class:`~loom.etl.storage.StorageConfig`.
        run_id: Current run identifier.
        correlation_id: Optional correlation identifier used for retry groups.
        scope: Requested checkpoint scope.

    Returns:
        Base directory where checkpoint artifacts for this scope are stored.

    Raises:
        InternalError: If ``scope`` is CORRELATION and ``correlation_id`` is missing.
    """
    if scope is CheckpointScope.CORRELATION:
        if correlation_id is None:
            raise InternalError(
                "scope_base called with scope=CORRELATION but correlation_id=None. "
                "This is a framework bug — please report it."
            )
        return correlation_scope_base(root, correlation_id)
    return run_scope_base(root, run_id)


def runs_root(root: str) -> str:
    """Return root directory containing run-scoped checkpoint directories."""
    return _join_path(root, "runs")
