"""In-memory Polars backend for CheckpointStore, used by the step test runners."""

from __future__ import annotations

from typing import Any

import polars as pl

from loom.etl.checkpoint._backends._polars import (
    _align_lazy_to_schema,
    _require_polars_lazy_frame,
)


class _InMemoryCheckpointBackend:
    """Polars checkpoint backend holding collected frames in a dict.

    Keeps the checks of the Arrow IPC backend: only a ``LazyFrame`` is accepted,
    and a fan-in write is aligned to the schema of the first write for that name.
    Frames are collected at write time, so errors surface at the write like the
    real materialisation does.  Not thread-safe: one instance serves one runner.
    """

    def __init__(self) -> None:
        self._parts: dict[str, list[pl.DataFrame]] = {}
        self._schemas: dict[str, pl.Schema] = {}

    def probe(self, name: str, base: str) -> pl.LazyFrame | None:
        """Return the concatenated parts stored for *name* under *base*, or ``None``."""
        parts = self._parts.get(_key(name, base))
        if not parts:
            return None
        return pl.concat(parts, how="vertical").lazy()

    def write(self, name: str, base: str, data: Any, *, append: bool) -> None:
        """Store *data* for *name* under *base*, appending a part in fan-in mode."""
        frame = _require_polars_lazy_frame(data)
        key = _key(name, base)
        existing = self._schemas.setdefault(key, frame.collect_schema())
        if append:
            frame = _align_lazy_to_schema(frame, existing)
            self._parts.setdefault(key, []).append(frame.collect())
        else:
            self._parts[key] = [frame.collect()]


def _key(name: str, base: str) -> str:
    return f"{base}/{name}"
