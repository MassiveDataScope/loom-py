"""Checkpoint store for ETL pipeline intermediates.

:class:`CheckpointStore` handles the physical read/write of intermediate
results materialised by :class:`~loom.etl.IntoTemp` targets and consumed by
:class:`~loom.etl.FromTemp` sources.

Physical formats
----------------
The backend is selected at construction time and injected via the *backend*
parameter:

* **Polars** :class:`polars.LazyFrame` — Arrow IPC written via ``sink_ipc()``
  (streaming, no in-memory collect) and read back via ``scan_ipc()`` (lazy,
  memory-mapped, predicate pushdown).
  Backend: :class:`~loom.etl.checkpoint._backends._polars._PolarsCheckpointBackend`.

* **Spark** ``pyspark.sql.DataFrame`` — Parquet directory written via
  ``df.write.parquet()`` and read via ``spark.read.parquet()``.  Cuts the
  lineage DAG; Photon-optimised.  Avoids competing with the shuffle memory
  pool that ``df.cache()`` would enter.
  Backend: :class:`~loom.etl.checkpoint._backends._spark._SparkCheckpointBackend`.

Path structure
--------------
RUN scope::

    {root}/runs/{run_id}/{name}.arrow   ← Polars
    {root}/runs/{run_id}/{name}/        ← Spark (Parquet dir)

CORRELATION scope::

    {root}/correlations/{correlation_id}/{name}.arrow
    {root}/correlations/{correlation_id}/{name}/

Cleanup
-------
* :meth:`cleanup_run` — removes the entire ``runs/{run_id}/`` tree.
  Called in the ``finally`` of every pipeline run.

* :meth:`cleanup_correlation` — removes ``correlations/{correlation_id}/``.
  Called by :class:`~loom.etl.ETLRunner` on successful last attempt, or
  manually via :meth:`~loom.etl.ETLRunner.cleanup_correlation`.
"""

from __future__ import annotations

import logging
from typing import Any, Protocol, runtime_checkable

from loom.etl.checkpoint._backends._polars import _PolarsCheckpointBackend
from loom.etl.checkpoint._cleaners import FsspecTempCleaner, TempCleaner
from loom.etl.checkpoint._paths import correlation_scope_base, run_scope_base, scope_base
from loom.etl.checkpoint._scope import CheckpointScope

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Backend Protocol — domain contract
# ---------------------------------------------------------------------------


@runtime_checkable
class _CheckpointBackend(Protocol):
    """Physical I/O contract for one DataFrame backend.

    Both :class:`~loom.etl.checkpoint._backends._polars._PolarsCheckpointBackend` and
    :class:`~loom.etl.checkpoint._backends._spark._SparkCheckpointBackend` satisfy this
    protocol.  ``CheckpointStore`` holds a single ``_CheckpointBackend`` chosen
    at construction time, keeping ``put`` / ``get`` free of backend branches.
    """

    def probe(self, name: str, base: str) -> Any | None:
        """Return the frame stored at (*name*, *base*), or ``None`` if absent."""
        ...

    def write(self, name: str, base: str, data: Any, *, append: bool) -> None:
        """Materialise *data* at (*name*, *base*)."""
        ...


# ---------------------------------------------------------------------------
# CheckpointStore
# ---------------------------------------------------------------------------


class CheckpointStore:
    """Physical store for intermediate ETL results.

    The backend (Polars or Spark) is injected at construction time via the
    *backend* parameter.  ``put`` and ``get`` are then backend-agnostic — no
    runtime type switching.

    Args:
        root: Root cloud URI where intermediates are stored.
        backend:  Physical I/O backend. Defaults to a Polars checkpoint backend
            with no storage options, which writes through the ambient credential
            chain and under the bucket default encryption key.  Declare
            ``storage_options`` — as the example does, or through
            ``storage.temp.storage_options`` in the config — to write with an
            explicit endpoint, explicit credentials or a chosen CMK.
        cleaner: Cleaner implementation used to delete checkpoint trees.

    Example::

        store = CheckpointStore(
            root="s3://my-bucket/loom-checkpoints",
            backend=_PolarsCheckpointBackend(
                storage_options={
                    "aws_server_side_encryption": "aws:kms",
                    "aws_sse_kms_key_id": "alias/loom-temp",
                }
            ),
        )
        store.put("orders", run_id="abc", correlation_id=None,
                  scope=CheckpointScope.RUN, data=polars_lazy_frame)
        lf = store.get("orders", run_id="abc", correlation_id=None)
    """

    def __init__(
        self,
        root: str,
        backend: _CheckpointBackend | None = None,
        cleaner: TempCleaner | None = None,
    ) -> None:
        self._root = root.rstrip("/")
        self._cleaner = cleaner if cleaner is not None else FsspecTempCleaner()
        self._backend = backend if backend is not None else _PolarsCheckpointBackend({})

    # ------------------------------------------------------------------
    # Public write / read
    # ------------------------------------------------------------------

    def put(
        self,
        name: str,
        *,
        run_id: str,
        correlation_id: str | None,
        scope: CheckpointScope,
        data: Any,
        append: bool = False,
    ) -> None:
        """Materialise *data* as a named intermediate.

        Args:
            name:           Logical name matching :class:`~loom.etl.IntoTemp`.
            run_id:         UUID of the current pipeline run.
            correlation_id: Logical job ID grouping retries.  Required when
                            *scope* is :attr:`~CheckpointScope.CORRELATION`.
            scope:          Whether to keep across retries.
            data:           ``polars.LazyFrame`` or ``pyspark.sql.DataFrame``.
            append:         When ``True``, writes a new partition file alongside
                            any previously written parts for this name (fan-in).
                            When ``False`` (default), the write is exclusive —
                            any prior content for this name is overwritten.

        Raises:
            ValueError: When *scope* is ``CORRELATION`` and *correlation_id*
                        is ``None``.
            TypeError:  When *data* type does not match the configured backend.
        """
        if scope is CheckpointScope.CORRELATION and not correlation_id:
            raise ValueError(
                f"IntoTemp({name!r}, scope=CORRELATION) requires a correlation_id. "
                "Pass correlation_id= to ETLRunner.run()."
            )
        _log.debug("checkpoint put name=%r scope=%s append=%s", name, scope, append)
        base = scope_base(
            self._root,
            run_id=run_id,
            correlation_id=correlation_id,
            scope=scope,
        )
        self._backend.write(name, base, data, append=append)

    def get(
        self,
        name: str,
        *,
        run_id: str,
        correlation_id: str | None,
    ) -> Any:
        """Load a previously materialised intermediate.

        Tries the RUN-scope path first, then falls back to the CORRELATION-scope
        path.  This allows :class:`~loom.etl.FromTemp` consumers to be
        scope-agnostic — they only declare the name.

        Returns a :class:`polars.LazyFrame` for Polars intermediates, or a
        ``pyspark.sql.DataFrame`` for Spark intermediates.

        Args:
            name:           Logical name matching :class:`~loom.etl.IntoTemp`.
            run_id:         UUID of the pipeline run.
            correlation_id: Logical job ID.  Checked when RUN path is absent.

        Raises:
            FileNotFoundError: When no intermediate exists at either path.
        """
        run_base = run_scope_base(self._root, run_id)
        result = self._backend.probe(name, run_base)
        if result is not None:
            _log.debug("checkpoint get name=%r scope=run", name)
            return result
        if correlation_id is not None:
            corr_base = correlation_scope_base(self._root, correlation_id)
            result = self._backend.probe(name, corr_base)
            if result is not None:
                _log.debug("checkpoint get name=%r scope=correlation", name)
                return result
        raise FileNotFoundError(
            f"Intermediate {name!r} not found. "
            f"Check that IntoTemp({name!r}) ran before FromTemp({name!r})."
        )

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def cleanup_run(self, run_id: str) -> None:
        """Remove all RUN-scope intermediates for *run_id*.

        Safe to call even when no intermediates were written.

        Args:
            run_id: UUID of the pipeline run to clean.
        """
        path = run_scope_base(self._root, run_id)
        _log.debug("checkpoint cleanup run path=%s", path)
        self._cleaner.delete_tree(path)

    def cleanup_correlation(self, correlation_id: str) -> None:
        """Remove all CORRELATION-scope intermediates for *correlation_id*.

        Call this after the final successful attempt, or to reclaim space
        after a permanently failed job.

        Args:
            correlation_id: Logical job ID whose intermediates to purge.
        """
        path = correlation_scope_base(self._root, correlation_id)
        _log.debug("checkpoint cleanup correlation path=%s", path)
        self._cleaner.delete_tree(path)
