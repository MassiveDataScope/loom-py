"""In-memory Polars backend for CheckpointStore, used by the step test runners."""

from __future__ import annotations

from typing import Any

import polars as pl

from loom.etl.checkpoint._backends._polars import (
    _align_lazy_to_schema,
    _checkpoint_key,
    _require_polars_lazy_frame,
)


class _InMemoryCheckpointBackend:
    """Polars checkpoint backend holding collected frames in a dict.

    Keeps the checks of the Arrow IPC backend: only a ``LazyFrame`` is accepted,
    and a fan-in write is aligned to the schema of the first write for that name.
    Frames are collected at write time, so errors surface at the write like the
    real materialisation does.  It remembers the last intermediate written, so a
    runner can return what a step wrote without rebuilding scope paths.  Not
    thread-safe: one instance serves one runner.
    """

    def __init__(self) -> None:
        self._parts: dict[str, list[pl.DataFrame]] = {}
        self._schemas: dict[str, pl.Schema] = {}
        self._last_key: str | None = None

    def probe(self, name: str, base: str) -> pl.LazyFrame | None:
        """Return the concatenated parts stored for *name* under *base*, or ``None``."""
        return self._probe_key(_checkpoint_key(name, base))

    def probe_last(self) -> pl.LazyFrame | None:
        """Return the intermediate of the last :meth:`write`, or ``None`` before any."""
        return None if self._last_key is None else self._probe_key(self._last_key)

    def write(self, name: str, base: str, data: Any, *, append: bool) -> None:
        """Store *data* for *name* under *base*, appending a part in fan-in mode."""
        frame = _require_polars_lazy_frame(data)
        key = _checkpoint_key(name, base)
        existing = self._schemas.get(key)
        if existing is None:
            self._schemas[key] = frame.collect_schema()
        elif append:
            frame = _align_lazy_to_schema(frame, existing)
        collected = frame.collect()
        if append:
            self._parts.setdefault(key, []).append(collected)
        else:
            self._parts[key] = [collected]
        self._last_key = key

    def _probe_key(self, key: str) -> pl.LazyFrame | None:
        parts = self._parts.get(key)
        if not parts:
            return None
        return pl.concat(parts, how="vertical").lazy()
