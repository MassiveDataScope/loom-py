"""Cloud URI schemes the ETL storage layer recognises.

Internal module — not part of the public API.
"""

from __future__ import annotations

CLOUD_SCHEMES = frozenset({"s3", "gs", "gcs", "abfss", "abfs", "az", "r2"})
"""URI schemes of the object stores loom reads and writes."""


def is_cloud_path(path: str) -> bool:
    """Return ``True`` when *path* uses a supported cloud URI scheme."""
    if "://" not in path:
        return False
    scheme, _ = path.split("://", 1)
    return scheme.lower() in CLOUD_SCHEMES


__all__ = ["CLOUD_SCHEMES", "is_cloud_path"]
