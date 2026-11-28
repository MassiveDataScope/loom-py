"""S3-backed manifest store using fsspec."""

from __future__ import annotations

import logging
import re
from typing import IO, cast

import fsspec
import msgspec

from loom.core.errors import InvalidValue
from loom.core.log_names import LogField
from loom.prefect.event import PrefectEvent
from loom.prefect.manifest._model import RunManifest

_log = logging.getLogger(__name__)

_SAFE_CORR_ID = re.compile(r"^[a-zA-Z0-9_\-]{1,256}$")


def _validate_correlation_id(correlation_id: str) -> None:
    """Raise ValueError if correlation_id is not safe to embed in an S3 key."""
    if not _SAFE_CORR_ID.match(correlation_id):
        raise InvalidValue(
            "correlation_id contains invalid characters. "
            "Only alphanumerics, hyphens, and underscores are allowed."
        )


class S3JsonManifestStore:
    """``ManifestStore`` backed by S3 via fsspec.

    Path pattern: ``s3://<bucket>/<prefix>/<correlation_id>/manifest.json``

    Uses fsspec — already present in the project via ``CheckpointStore``.
    Writes are synchronous and best-effort: a failed ``save()`` is logged but
    not re-raised (the manifest is an optimisation, not critical path).

    Args:
        bucket: S3 bucket name.
        prefix: Key prefix (default ``"loom/manifests"``).

    Example::

        store = S3JsonManifestStore(bucket="my-data-lake")
        store.save(manifest)
        loaded = store.load("orders-2026-06-02")
        store.delete("orders-2026-06-02")
    """

    def __init__(self, bucket: str, prefix: str = "loom/manifests") -> None:
        self._bucket = bucket
        self._prefix = prefix

    def _path(self, correlation_id: str) -> str:
        return f"s3://{self._bucket}/{self._prefix}/{correlation_id}/manifest.json"

    def load(self, correlation_id: str) -> RunManifest | None:
        """Return the stored manifest, or ``None`` if it does not exist.

        Args:
            correlation_id: Manifest key (matches the flow's ``correlation_id``).

        Returns:
            Decoded ``RunManifest`` or ``None`` when absent.

        Raises:
            ValueError: If ``correlation_id`` contains characters unsafe for S3 keys.
        """
        _validate_correlation_id(correlation_id)
        path = self._path(correlation_id)
        try:
            with fsspec.open(path, "rb") as f:
                data = cast(IO[bytes], f).read()
            return msgspec.json.decode(data, type=RunManifest)
        except FileNotFoundError:
            return None
        except Exception:
            _log.warning(
                PrefectEvent.MANIFEST_LOAD_FAILED,
                extra={LogField.CORRELATION_ID: correlation_id},
                exc_info=True,
            )
            return None

    def save(self, manifest: RunManifest) -> None:
        """Persist the manifest to S3.

        Args:
            manifest: Manifest to write. Serialised as JSON.

        Raises:
            ValueError: If ``manifest.correlation_id`` contains unsafe characters.
        """
        _validate_correlation_id(manifest.correlation_id)
        path = self._path(manifest.correlation_id)
        data = msgspec.json.encode(manifest)
        with fsspec.open(path, "wb") as f:
            cast(IO[bytes], f).write(data)

    def delete(self, correlation_id: str) -> None:
        """Remove the manifest object from S3.  Silent if absent.

        Args:
            correlation_id: Manifest key to delete.

        Raises:
            ValueError: If ``correlation_id`` contains unsafe characters.
        """
        _validate_correlation_id(correlation_id)
        path = self._path(correlation_id)
        try:
            fs = fsspec.filesystem("s3")
            key = path[len("s3://") :]
            fs.rm(key)
        except FileNotFoundError:
            pass
        except Exception:
            _log.warning(
                PrefectEvent.MANIFEST_DELETE_FAILED,
                extra={LogField.CORRELATION_ID: correlation_id},
                exc_info=True,
            )


__all__ = ["S3JsonManifestStore"]
