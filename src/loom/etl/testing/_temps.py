"""In-memory temps for the step runners: a CheckpointStore that never touches storage."""

from __future__ import annotations

import threading
from typing import Any

import polars as pl

from loom.etl.checkpoint import CheckpointScope, CheckpointStore
from loom.etl.checkpoint._backends._polars import (
    _align_lazy_to_schema,
    _require_polars_lazy_frame,
)
from loom.etl.lineage._records import RunContext

_ROOT = "memory://loom-step-runner"


class _InMemoryCheckpointBackend:
    """Polars checkpoint backend holding collected frames in a dict.

    Keeps the checks of the Arrow IPC backend: only a ``LazyFrame`` is accepted,
    and a fan-in write is aligned to the schema of the first write for that name.
    Frames are collected at write time, so errors surface at the write like the
    real materialisation does.
    """

    def __init__(self) -> None:
        self._parts: dict[str, list[pl.DataFrame]] = {}
        self._schemas: dict[str, pl.Schema] = {}
        self._lock = threading.Lock()

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
        with self._lock:
            existing = self._schemas.get(key)
            if existing is None:
                self._schemas[key] = frame.collect_schema()
        if append and existing is not None:
            frame = _align_lazy_to_schema(frame, existing)
        collected = frame.collect()
        with self._lock:
            if append:
                self._parts.setdefault(key, []).append(collected)
            else:
                self._parts[key] = [collected]


class RunnerTemps:
    """Temps of one runner: an in-memory :class:`CheckpointStore` and a fixed run context.

    The context never changes, so a temp written by one ``run()`` is read by the
    next one, and ``IntoTemp(scope=CORRELATION)`` has a correlation id to use.
    """

    def __init__(self) -> None:
        self.store = CheckpointStore(_ROOT, backend=_InMemoryCheckpointBackend())
        self.context = RunContext(run_id="step-runner-run", correlation_id="step-runner")

    def put(self, name: str, frame: pl.LazyFrame) -> None:
        """Replace the RUN-scope temp *name* with *frame*."""
        self.store.put(
            name,
            run_id=self.context.run_id,
            correlation_id=self.context.correlation_id,
            scope=CheckpointScope.RUN,
            data=frame,
        )

    def get(self, name: str) -> pl.LazyFrame:
        """Return the temp *name*, looked up RUN scope first as in production."""
        frame: pl.LazyFrame = self.store.get(
            name,
            run_id=self.context.run_id,
            correlation_id=self.context.correlation_id,
        )
        return frame


def _key(name: str, base: str) -> str:
    return f"{base}/{name}"
