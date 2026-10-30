"""Polars Arrow IPC backend for CheckpointStore."""

from __future__ import annotations

import logging
import threading
import uuid
from typing import Any, TypeGuard

import fsspec.core
import polars as pl

from loom.etl._uri import is_cloud_path
from loom.etl.checkpoint._cleaners import _join_path
from loom.etl.checkpoint._options import CheckpointOptions, checkpoint_options

_log = logging.getLogger(__name__)
_WRITING = ".writing"


def _is_polars_lazy_frame(value: Any) -> TypeGuard[pl.LazyFrame]:
    """Return True when value is a Polars LazyFrame."""
    return isinstance(value, pl.LazyFrame)


def _require_polars_lazy_frame(data: Any) -> pl.LazyFrame:
    """Return *data* as a LazyFrame, or raise the error every Polars backend raises.

    Raises:
        TypeError: When *data* is not a :class:`polars.LazyFrame`.
    """
    if not _is_polars_lazy_frame(data):
        raise TypeError(
            f"Polars backend expects polars.LazyFrame, got {type(data).__qualname__!r}. "
            "Verify that CheckpointStore was constructed without a SparkSession."
        )
    return data


class _PolarsCheckpointBackend:
    """Polars Arrow IPC backend with atomic write-then-rename."""

    def __init__(self, storage_options: dict[str, str]) -> None:
        self._options = checkpoint_options(storage_options)
        self._schemas: dict[str, pl.Schema] = {}
        self._lock = threading.Lock()

    def probe(self, name: str, base: str) -> Any | None:
        """Return LazyFrame scanning *name* under *base*, or None if absent."""
        scan_path = self._find_arrow(name, base)
        if scan_path is None:
            _log.info("checkpoint READ MISS name=%s base=%s", name, base)
            return None
        kwargs: dict[str, Any] = {"memory_map": not is_cloud_path(base)}
        # The read declares no encryption: decryption is server-side.
        read_opts = self._options.object_store_read
        if read_opts:
            kwargs["storage_options"] = read_opts
        _log.info("checkpoint READ HIT  name=%s path=%s", name, scan_path)
        return pl.scan_ipc(scan_path, **kwargs)

    def write(self, name: str, base: str, data: Any, *, append: bool) -> None:
        """Sink *data* as Arrow IPC atomically (write-then-rename)."""
        frame = _require_polars_lazy_frame(data)
        key = _checkpoint_key(name, base)

        with self._lock:
            existing = self._schemas.get(key)
            if existing is None:
                self._schemas[key] = frame.collect_schema()

        if append and existing is not None:
            frame = _align_lazy_to_schema(frame, existing)

        final = _arrow_part(name, base) if append else _arrow_path(name, base)
        tmp = f"{final}{_WRITING}"

        _log.info("checkpoint WRITE     name=%s path=%s", name, final)
        _write_atomic(frame, tmp, final, self._options)

    def _find_arrow(self, name: str, base: str) -> str | None:
        """Return scan path for *name* under *base*, or None if absent."""
        single = _arrow_path(name, base)
        fs, bp = fsspec.core.url_to_fs(base, **self._options.fsspec)
        if fs.exists(_join_path(bp, f"{name}.arrow")):
            return single
        dp = _join_path(bp, name)
        if fs.exists(dp) and fs.glob(_join_path(dp, "*.arrow")):
            return _join_path(base, name, "*.arrow")
        return None


def _checkpoint_key(name: str, base: str) -> str:
    """Return the key identifying the intermediate *name* under *base*."""
    return f"{base}/{name}"


def _arrow_path(name: str, base: str) -> str:
    return _join_path(base, f"{name}.arrow")


def _arrow_part(name: str, base: str) -> str:
    return _join_path(base, name, f"{uuid.uuid4().hex}.arrow")


def _align_lazy_to_schema(frame: pl.LazyFrame, schema: pl.Schema) -> pl.LazyFrame:
    source = frame.collect_schema()
    exprs: list[pl.Expr] = []
    for n, dt in schema.items():
        exprs.append(
            pl.col(n).cast(dt, strict=False).alias(n)
            if n in source.names()
            else pl.lit(None).cast(dt).alias(n)
        )
    return frame.select(exprs)


def _write_atomic(
    frame: pl.LazyFrame,
    tmp: str,
    final: str,
    options: CheckpointOptions,
) -> None:
    """Write to tmp, then rename to final atomically.

    ``sink_ipc`` writes the temp object through object_store and the rename is
    a server-side copy through fsspec, so the declared encryption has to reach
    both: otherwise the surviving object lands with the bucket default key.
    """
    fs, p = fsspec.core.url_to_fs(tmp, **options.fsspec)
    fs.makedirs(fs._parent(p), exist_ok=True)
    kwargs: dict[str, Any] = (
        {"storage_options": options.object_store} if options.object_store else {}
    )
    frame.sink_ipc(tmp, **kwargs)
    fs.rename(p, p.removesuffix(_WRITING))
