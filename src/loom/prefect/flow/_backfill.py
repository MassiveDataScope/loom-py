"""Prefect flow factory for chunked ETL backfills — mirrors etl_flow()."""

from __future__ import annotations

import inspect
from datetime import datetime
from typing import Any

import msgspec

from loom.etl.compiler import ETLCompiler
from loom.etl.pipeline import ETLPipeline
from loom.prefect.flow._assemble import assemble_flow, load_flow_settings
from loom.prefect.flow._backfill_body import BackfillChunk, build_backfill_body
from loom.prefect.flow._signature import synthesise_flow_signature
from loom.prefect.flow._stages import known_stage_names, validate_stage_names
from loom.prefect.manifest import ManifestStore


def backfill_flow(
    *,
    name: str,
    pipeline: type[ETLPipeline[Any]],
    params_type: type[msgspec.Struct],
    config_path: str,
    source_file: str,
    per_chunk_processes: list[str],
    finalize_processes: list[str],
    window_start_field: str,
    window_end_field: str,
    chunk: BackfillChunk = "day",
    storage_config_path: str = "/app/config.yaml",
    manifest_store: ManifestStore | None = None,
) -> Any:
    """Build a Prefect flow that backfills a pipeline one chunk at a time.

    Unlike :func:`~loom.prefect.etl_flow`, which runs a pipeline once over the
    whole parameter window, ``backfill_flow`` slices the window
    ``[window_start_field, window_end_field)`` into ``chunk``-aligned partitions
    (``[C, C+1chunk)``) and runs ``per_chunk_processes`` once per chunk, oldest
    first. After all chunks it runs ``finalize_processes`` once — the finalize
    run's ``window_end_field`` is pinned to the start of the *current* chunk
    (not ``now``) so downstream batch-sequenced refreshes get a stable boundary.

    The flow is registered without Prefect retries on purpose: a failed
    backfill should be resumed from the failed chunk via ``start_from`` rather
    than replayed from the beginning. Naive datetimes (window fields and
    ``start_from``) are assumed to be UTC.

    Args:
        name: Logical flow name shown in the Prefect UI.
        pipeline: ``ETLPipeline`` subclass to execute.
        params_type: ``msgspec.Struct`` whose fields become typed flow kwargs.
            Must carry the two window fields named below.
        config_path: Path to the per-flow YAML (schedule, params, tags, …).
        source_file: ``__file__`` of the calling module (needed by Prefect
            ``from_source`` to locate the flow on disk).
        per_chunk_processes: Process/step names run once for each chunk window.
            Validated against the compiled pipeline at build time.
        finalize_processes: Process/step names run once after all chunks.
            Validated against the compiled pipeline at build time.
        window_start_field: Name of the datetime field holding the window start.
        window_end_field: Name of the datetime field holding the (exclusive)
            window end; also the field pinned on the finalize run.
        chunk: Partition granularity used to slice the window.  At
            ``"year"`` the floor/finalize edges operate at year scale —
            see :data:`~loom.prefect.BackfillChunk`.
        storage_config_path: Path to the loom storage YAML read at runtime.
            Overridable via ``LOOM_STORAGE_CONFIG_PATH``.
        manifest_store: Optional manifest backend, for observability only.
            Each chunk gets its own correlation id so manifests never leak
            across chunks. Completed steps are NOT skipped on re-runs — the
            only resume mechanism is ``start_from``, at chunk granularity.

    Returns:
        A ``@prefect.flow``-decorated callable with ``__loom_etl_meta__``
        attached for the deployer. It accepts ``start_from`` to resume the
        backfill from a given chunk (earlier chunks are skipped). Its
        published annotation is ``datetime | None``, which is what the
        Prefect form renders as a date picker; because Prefect does not
        validate run parameters, the body also accepts a ``date``, an ISO
        string and a placeholder. Plus ``env``, as for
        :func:`~loom.prefect.etl_flow`: it confirms the worker's
        ``LOOM_ENV`` and a run whose ``env`` differs fails.

    Raises:
        TypeError: When a process list is not ``list[str]``.
        ValueError: When a process/step name does not exist in *pipeline*, and
            at run time when ``start_from`` resolves to neither a datetime nor
            a date.
    """
    settings = load_flow_settings(config_path)

    known = known_stage_names(ETLCompiler().compile(pipeline))
    validate_stage_names(per_chunk_processes, known, field="per_chunk_processes")
    validate_stage_names(finalize_processes, known, field="finalize_processes")

    flow_body = build_backfill_body(
        flow_name=name,
        pipeline=pipeline,
        params_type=params_type,
        per_chunk_processes=per_chunk_processes,
        finalize_processes=finalize_processes,
        chunk=chunk,
        window_start_field=window_start_field,
        window_end_field=window_end_field,
        storage_config_path=storage_config_path,
        manifest_store=manifest_store,
    )
    signature = synthesise_flow_signature(
        params_type,
        extra_parameters=[
            inspect.Parameter(
                "start_from",
                inspect.Parameter.KEYWORD_ONLY,
                default=None,
                annotation=datetime | None,
            ),
        ],
    )
    return assemble_flow(
        name=name,
        body=flow_body,
        signature=signature,
        params_type=params_type,
        settings=settings,
        config_path=config_path,
        source_file=source_file,
    )


__all__ = ["BackfillChunk", "backfill_flow"]
