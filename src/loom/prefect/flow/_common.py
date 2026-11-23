"""Shared helpers for Prefect flow factories (etl_flow, maintenance_flow, …)."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, cast

from loom.core.errors import InvalidType
from loom.prefect._placeholders import utc_datetime


def coerce_tags(raw: Any) -> tuple[str, ...]:
    """Parse the ``tags`` list from a raw YAML value into a tuple of strings."""
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise InvalidType(f"tags: expected a list of strings, got {type(raw).__name__}")
    for value in raw:
        if not isinstance(value, str):
            raise InvalidType(f"tags: every entry must be str, got {type(value).__name__}")
    return tuple(raw)


def prefect_flow_run_id() -> uuid.UUID | None:
    """Return the current Prefect flow run identifier.

    Returns:
        The active flow run UUID, or ``None`` outside a Prefect flow run.
    """
    try:
        from prefect.runtime import flow_run  # noqa: PLC0415

        return cast(uuid.UUID | None, flow_run.id)
    except (ImportError, AttributeError):
        return None


def prefect_run_anchor() -> datetime | None:
    """Return the instant the current flow run's placeholders count from.

    It is the run's scheduled start in UTC: the slot of a scheduled run,
    kept across its retries, or the creation time of a manual run.

    Returns:
        The anchor, or ``None`` outside a Prefect flow run.
    """
    if prefect_flow_run_id() is None:
        return None
    from prefect.runtime import flow_run  # noqa: PLC0415

    scheduled = cast(datetime | None, flow_run.scheduled_start_time)
    return utc_datetime(scheduled) if scheduled is not None else None


__all__ = ["coerce_tags", "prefect_flow_run_id", "prefect_run_anchor"]
