"""In-memory temps for the step runners: a CheckpointStore that never touches storage."""

from __future__ import annotations

import polars as pl

from loom.etl.checkpoint import CheckpointScope, CheckpointStore
from loom.etl.checkpoint._backends._memory import _InMemoryCheckpointBackend
from loom.etl.checkpoint._paths import scope_base
from loom.etl.lineage._records import RunContext

_ROOT = "memory://loom-step-runner"


class RunnerTemps:
    """Temps of one runner: an in-memory :class:`CheckpointStore` and a fixed run context.

    The context never changes, so a temp written by one ``run()`` is read by the
    next one, and ``IntoTemp(scope=CORRELATION)`` has a correlation id to use.
    """

    def __init__(self) -> None:
        self._backend = _InMemoryCheckpointBackend()
        self.store = CheckpointStore(_ROOT, backend=self._backend)
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

    def read(self, name: str, scope: CheckpointScope) -> pl.DataFrame:
        """Return the temp *name* stored in *scope* only, with no fallback.

        Raises:
            FileNotFoundError: When nothing is stored for *name* in *scope*.
        """
        base = scope_base(
            _ROOT,
            run_id=self.context.run_id,
            correlation_id=self.context.correlation_id,
            scope=scope,
        )
        frame = self._backend.probe(name, base)
        if frame is None:
            raise FileNotFoundError(f"Temp {name!r} was not written in scope {scope}.")
        return frame.collect()
