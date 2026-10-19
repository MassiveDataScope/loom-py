"""Runtime body of the chunked backfill Prefect flow."""

from __future__ import annotations

import logging
import os
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from uuid import uuid4

import msgspec

from loom.core.environment import Environment, resolve_environment
from loom.etl.pipeline import ETLPipeline
from loom.etl.runner import ETLRunner
from loom.prefect._placeholders import resolve_placeholder
from loom.prefect._summary import set_run_summary
from loom.prefect.flow._common import prefect_flow_run_id, prefect_run_anchor
from loom.prefect.flow._params import record_run_params, resolve_run_params
from loom.prefect.flow._runtime import (
    build_observers,
    load_or_init_manifest,
    maybe_delete_manifest,
)
from loom.prefect.flow._signature import coerce_to_utc
from loom.prefect.manifest import ManifestStore
from loom.prefect.observer._logging_bridge import install_log_bridge, uninstall_log_bridge

_log = logging.getLogger(__name__)

BackfillChunk = Literal["hour", "day", "month", "year"]
"""Partition granularity of a :func:`~loom.prefect.backfill_flow` window.

Prefer the coarsest granularity the pipeline tolerates: when the cost of a
backfill is dominated by per-chunk overhead rather than data volume, a
multi-year window runs in a handful of ``"year"`` chunks instead of
hundreds of monthly ones.  Mind that at ``"year"`` the two window edges
also operate at year scale: the start is floored to January 1 (a window
opening in June reprocesses from the year's start) and the finalize run
pins ``window_end`` to January 1 of the *current* year.
"""


@dataclass(frozen=True)
class _ChunkAlgebra:
    """Floor/advance/label rules for one chunk granularity."""

    floor: Callable[[datetime], datetime]
    advance: Callable[[datetime], datetime]
    label: str


def _advance_month(value: datetime) -> datetime:
    year, month = divmod(value.month, 12)
    return value.replace(year=value.year + year, month=month + 1)


def _advance_year(value: datetime) -> datetime:
    return value.replace(year=value.year + 1)


_CHUNK_ALGEBRA: dict[BackfillChunk, _ChunkAlgebra] = {
    "hour": _ChunkAlgebra(
        floor=lambda v: v.replace(minute=0, second=0, microsecond=0),
        advance=lambda v: v + timedelta(hours=1),
        label="%Y%m%d%H",
    ),
    "day": _ChunkAlgebra(
        floor=lambda v: v.replace(hour=0, minute=0, second=0, microsecond=0),
        advance=lambda v: v + timedelta(days=1),
        label="%Y%m%d",
    ),
    "month": _ChunkAlgebra(
        floor=lambda v: v.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
        advance=_advance_month,
        label="%Y%m",
    ),
    "year": _ChunkAlgebra(
        floor=lambda v: v.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0),
        advance=_advance_year,
        label="%Y",
    ),
}


def build_backfill_body(
    *,
    flow_name: str,
    pipeline: type[ETLPipeline[Any]],
    params_type: type[msgspec.Struct],
    per_chunk_processes: list[str],
    finalize_processes: list[str],
    chunk: BackfillChunk,
    window_start_field: str,
    window_end_field: str,
    storage_config_path: str,
    manifest_store: ManifestStore | None,
) -> Callable[..., None]:
    """Return the ``_flow_body(**kwargs)`` callable bound to the factory state.

    Args:
        flow_name: Logical flow name (used in correlation and run ids).
        pipeline: ``ETLPipeline`` subclass to execute.
        params_type: Struct used to decode the bound parameters.
        per_chunk_processes: Stage names run once per chunk window.
        finalize_processes: Stage names run once after all chunks.
        chunk: Partition granularity used to slice the window.
        window_start_field: Datetime field holding the window start.
        window_end_field: Datetime field holding the (exclusive) window end.
        storage_config_path: Loom storage YAML path. Overridable at runtime
            via ``LOOM_STORAGE_CONFIG_PATH``.
        manifest_store: Per-slice observability manifest backend, or ``None``.

    Returns:
        The flow body callable. Returns ``None`` on success, raises on
        runner failure.
    """

    def _flow_body(**kwargs: Any) -> None:
        flow_run_id = prefect_flow_run_id()
        install_log_bridge(flow_run_id)
        try:
            _run(flow_run_id, kwargs)
        finally:
            uninstall_log_bridge()

    def _run(flow_run_id: uuid.UUID | None, kwargs: dict[str, Any]) -> None:
        anchor = prefect_run_anchor()
        environment = resolve_environment(kwargs.pop("env", None))
        start_from = _resolve_start_from(kwargs.pop("start_from", None), anchor)
        params = resolve_run_params(kwargs, params_type, anchor=anchor)
        record_run_params({**msgspec.structs.asdict(params), "start_from": start_from})

        window_start = getattr(params, window_start_field)
        window_end = getattr(params, window_end_field)
        windows = _chunk_windows(window_start, window_end, chunk, start_from)

        actual_path = os.environ.get("LOOM_STORAGE_CONFIG_PATH") or storage_config_path
        run_id_base = f"{flow_name}-{uuid4().hex[:8]}"
        for index, (chunk_start, chunk_end) in enumerate(windows, start=1):
            chunk_params = msgspec.structs.replace(
                params, **{window_start_field: chunk_start, window_end_field: chunk_end}
            )
            label = format(chunk_start, _CHUNK_ALGEBRA[chunk].label)
            _log.info(
                "backfill %s env=%s chunk %d/%d window=[%s, %s)",
                flow_name,
                environment,
                index,
                len(windows),
                chunk_start.isoformat(),
                chunk_end.isoformat(),
            )
            _run_slice(
                actual_path,
                pipeline,
                chunk_params,
                per_chunk_processes,
                correlation_id=f"{flow_name}-{label}",
                run_id=f"{run_id_base}-{label}",
                flow_run_id=flow_run_id,
                manifest_store=manifest_store,
                environment=environment,
            )

        finalize_params = msgspec.structs.replace(
            params, **{window_end_field: _floor_chunk(_now_utc(), chunk)}
        )
        _log.info(
            "backfill %s env=%s finalize include=%s", flow_name, environment, finalize_processes
        )
        _run_slice(
            actual_path,
            pipeline,
            finalize_params,
            finalize_processes,
            correlation_id=f"{flow_name}-finalize",
            run_id=f"{run_id_base}-finalize",
            flow_run_id=flow_run_id,
            manifest_store=manifest_store,
            environment=environment,
        )
        set_run_summary(_backfill_summary(flow_name, len(windows), chunk, finalize_processes))

    return _flow_body


def _run_slice(
    config_path: str,
    pipeline: type[ETLPipeline[Any]],
    params: Any,
    include: list[str],
    *,
    correlation_id: str,
    run_id: str,
    flow_run_id: uuid.UUID | None,
    manifest_store: ManifestStore | None,
    environment: Environment,
) -> None:
    """Run one backfill slice (a chunk or the finalize pass) as an isolated unit.

    Each slice loads its own manifest under ``correlation_id`` — for
    observability only — so manifests never leak across chunks.
    """
    manifest = load_or_init_manifest(manifest_store, correlation_id)
    observers = build_observers(flow_run_id, manifest_store, manifest)
    runner = ETLRunner.from_yaml(config_path, extra_observers=observers, environment=environment)
    runner.run(
        pipeline,
        params,
        include=include,
        correlation_id=correlation_id,
        run_id=run_id,
    )
    maybe_delete_manifest(manifest_store, correlation_id)


def _chunk_windows(
    window_start: datetime,
    window_end: datetime,
    chunk: BackfillChunk,
    start_from: datetime | None,
) -> list[tuple[datetime, datetime]]:
    """Slice ``[window_start, window_end)`` into ``chunk``-aligned windows, oldest first.

    ``start_from`` (if given) resumes at its own chunk, skipping earlier chunks.
    """
    cursor = _floor_chunk(window_start, chunk)
    end = _as_utc(window_end)
    if start_from is not None:
        resume = _floor_chunk(start_from, chunk)
        if resume > cursor:
            cursor = resume
    windows: list[tuple[datetime, datetime]] = []
    while cursor < end:
        nxt = _CHUNK_ALGEBRA[chunk].advance(cursor)
        windows.append((cursor, nxt))
        cursor = nxt
    return windows


def _resolve_start_from(value: Any, anchor: datetime | None) -> datetime | None:
    """Coerce a submitted ``start_from`` into a tz-aware datetime.

    Args:
        value: ``None``, a ``datetime``, a ``date``, an ISO string, or a
            placeholder.
        anchor: Instant a placeholder counts from, or ``None`` for now.

    Returns:
        The resume instant, or ``None`` when none was given.

    Raises:
        ValueError: When *value* resolves to something other than a datetime
            or a date.
    """
    if value is None:
        return None
    try:
        resolved = resolve_placeholder(value, anchor=anchor)
    except ValueError as exc:
        raise ValueError(f"start_from: {exc}") from exc
    coerced = coerce_to_utc(resolved)
    if not isinstance(coerced, datetime):
        raise ValueError(
            "start_from: expected a datetime, a date, an ISO string or a placeholder, "
            f"got {value!r}"
        )
    return coerced


def _floor_chunk(value: datetime, chunk: BackfillChunk) -> datetime:
    return _CHUNK_ALGEBRA[chunk].floor(_as_utc(value).astimezone(UTC))


def _as_utc(value: datetime) -> datetime:
    """Naive datetimes are assumed to already be UTC."""
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _now_utc() -> datetime:
    return datetime.now(tz=UTC)


def _backfill_summary(
    name: str, chunk_count: int, chunk: BackfillChunk, finalize_processes: list[str]
) -> str:
    return (
        f"{name} — {chunk_count} {chunk} chunk(s) backfilled  "
        f"finalize: {', '.join(finalize_processes)}"
    )


__all__ = ["BackfillChunk", "build_backfill_body"]
