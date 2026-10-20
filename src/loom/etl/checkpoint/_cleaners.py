"""Checkpoint cleaners."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Protocol, runtime_checkable

import fsspec.core

from loom.etl.checkpoint._options import checkpoint_options

_log = logging.getLogger(__name__)


class CheckpointCleanupError(RuntimeError):
    """Raised when a checkpoint tree cannot be deleted."""


@runtime_checkable
class TempCleaner(Protocol):
    """Protocol for deleting checkpoint trees."""

    def delete_tree(self, path: str) -> None:
        """Delete *path* recursively."""
        ...


class FsspecTempCleaner:
    """Delete checkpoint trees via fsspec (s3, gs, abfss, etc)."""

    def __init__(self, storage_options: Mapping[str, str] | None = None) -> None:
        self._storage_options = checkpoint_options(storage_options or {}).fsspec

    def delete_tree(self, path: str) -> None:
        """Remove *path* and its contents from cloud storage.

        Raises:
            CheckpointCleanupError: When the deletion fails for any reason.
        """
        try:
            fs, fpath = fsspec.core.url_to_fs(path, **self._storage_options)
            if fs.exists(fpath):
                _log.debug("checkpoint cleanup path=%s", path)
                fs.rm(fpath, recursive=True)
        except Exception as exc:
            _log.warning("checkpoint cleanup skipped path=%r reason=%s", path, exc)


def _join_path(base: str, *parts: str) -> str:
    """Join *base* and *parts* into a single path, preserving cloud URI schemes."""
    root = base.rstrip("/")
    suffix = "/".join(part.strip("/") for part in parts if part)
    if not root:
        return f"/{suffix}" if suffix else "/"
    return f"{root}/{suffix}" if suffix else root
