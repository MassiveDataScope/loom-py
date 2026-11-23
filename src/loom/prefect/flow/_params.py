"""Resolve a flow run's parameters and record the values it runs with."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime
from typing import Any
from uuid import UUID

import msgspec
from prefect import get_client
from prefect.context import FlowRunContext
from prefect.settings import PREFECT_CLIENT_MAX_RETRIES, temporary_settings

from loom.prefect._placeholders import is_placeholder, resolve_placeholder
from loom.prefect.event import PrefectEvent, PrefectKey
from loom.prefect.flow._signature import normalize_datetime_fields

_log = logging.getLogger(__name__)

_RECORD_TIMEOUT_SECONDS = 10.0


def resolve_run_params(
    raw: Mapping[str, Any],
    params_type: type[msgspec.Struct],
    *,
    anchor: datetime | None,
) -> msgspec.Struct:
    """Resolve the placeholders in *raw* against *anchor* and decode the result.

    Args:
        raw: Parameters of the run, control keys already removed.
        params_type: Struct the parameters decode into.
        anchor: Instant the placeholders count from, or ``None`` for now.

    Returns:
        The decoded parameters.
    """
    resolved = {key: resolve_placeholder(value, anchor=anchor) for key, value in raw.items()}
    return msgspec.convert(normalize_datetime_fields(resolved, params_type), type=params_type)


def record_run_params(values: Mapping[str, Any]) -> None:
    """Replace the placeholders stored on the current flow run with their values.

    The run then shows, and a resubmitted or copied run reuses, the window it
    processed. The payload is the run's stored parameters with each stored
    placeholder that has an entry in *values* replaced by it, as JSON (ISO 8601
    for dates); nothing else is added or changed. Nothing is sent when the run
    stores no placeholder or outside a flow run. The call gets one short
    attempt; a failure is logged and does not fail the run.

    Args:
        values: Resolved value of each parameter, by name.
    """
    run = _stored_run()
    if run is None:
        return
    flow_run_id, stored = run
    try:
        _update_stored_params(flow_run_id, stored, values)
    except Exception:
        _log.exception(
            PrefectEvent.PARAMETERS_RECORD_FAILED, extra={PrefectKey.RUN: str(flow_run_id)}
        )


def _update_stored_params(
    flow_run_id: UUID, stored: dict[str, Any], values: Mapping[str, Any]
) -> None:
    concrete = msgspec.to_builtins(values)
    changed = {
        key: concrete[key]
        for key, value in stored.items()
        if is_placeholder(value) and key in concrete
    }
    if not changed:
        return
    with (
        temporary_settings(updates={PREFECT_CLIENT_MAX_RETRIES: 0}),
        get_client(sync_client=True, httpx_settings={"timeout": _RECORD_TIMEOUT_SECONDS}) as client,
    ):
        client.update_flow_run(flow_run_id, parameters={**stored, **changed})


def _stored_run() -> tuple[UUID, dict[str, Any]] | None:
    ctx = FlowRunContext.get()
    if ctx is None or ctx.flow_run is None:
        return None
    return ctx.flow_run.id, dict(ctx.flow_run.parameters)


__all__ = ["record_run_params", "resolve_run_params"]
