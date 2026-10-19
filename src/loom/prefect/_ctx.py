"""Operational context shared across all ETL flows."""

from __future__ import annotations

import msgspec


class FlowCtx(msgspec.Struct, frozen=True, kw_only=True):
    """Operational context for a single ETL flow run.

    Identical for all ETLs — business parameters belong in the
    pipeline-specific ``ETLParams`` subclass.

    Args:
        correlation_id: Logical business unit. Stable across all Prefect flow
            retries (new Fargate containers). Key for the S3 manifest and for
            loom lineage propagation.
        run_id: Identifier of this specific execution. Key for loom lineage
            records. Callers provide it so Prefect and loom share the same
            traceability identifier.
        dry_run: When ``True``, steps run without writing to real destinations.
        force: When ``True``, ignores any existing manifest and reruns all steps.
        processes: Names of ``ETLProcess`` subclasses to execute. ``None`` runs
            every process. Example: ``("prepared", "model")``.
    """

    correlation_id: str
    run_id: str
    dry_run: bool = False
    force: bool = False
    processes: tuple[str, ...] | None = None


__all__ = ["FlowCtx"]
