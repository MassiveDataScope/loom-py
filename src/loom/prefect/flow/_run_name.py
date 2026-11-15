"""Compute the display name shown for each flow run in the Prefect UI."""

from __future__ import annotations

import re
from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

from loom.prefect._placeholders import resolve_placeholder
from loom.prefect.flow._common import prefect_run_anchor

_UNSAFE_CORR_CHARS = re.compile(r"[^a-zA-Z0-9_\-]")
_COMPACT_TIMESTAMP_FMT = "%Y%m%dT%H%M%S"


def _render_value(value: Any) -> str:
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(UTC).strftime(_COMPACT_TIMESTAMP_FMT)
    if isinstance(value, datetime | date):
        return value.strftime(_COMPACT_TIMESTAMP_FMT)
    return str(value)


def _sanitize(value: Any) -> str:
    return _UNSAFE_CORR_CHARS.sub("_", _render_value(value))


def _prefect_runtime_params() -> dict[str, Any]:
    try:
        from prefect.runtime import flow_run as _fr  # noqa: PLC0415

        return dict(_fr.parameters or {})
    except Exception:  # noqa: BLE001
        return {}


def _has_explicit_correlation(params: dict[str, Any], correlation_field: str | None) -> bool:
    if params.get("correlation_id"):
        return True
    return bool(correlation_field and params.get(correlation_field) is not None)


def compute_correlation_id(
    flow_name: str,
    correlation_field: str | None,
    resolved: dict[str, Any],
) -> str:
    """Return a stable correlation id for a flow run.

    Falls back to ``"<flow_name>-<random>"`` when no ``correlation_field``
    is configured. A datetime is rendered compactly, by its UTC instant, so
    the value remains safe to use in S3 keys, filesystem paths and the
    manifest store's correlation_id validator, and so two different instants
    never share an id.

    Args:
        flow_name: Logical ETL name.
        correlation_field: Name of the parameter whose value drives the
            correlation id, or ``None`` to use a random suffix.
        resolved: Parameter mapping with placeholders already resolved.

    Returns:
        Sanitised correlation id of the form ``"<flow_name>-<value>"``.
    """
    if correlation_field is None:
        return f"{flow_name}-{uuid4().hex}"
    return f"{flow_name}-{_sanitize(resolved[correlation_field])}"


def make_run_name_callback(correlation_field: str | None) -> Any:
    """Return the callable that Prefect calls to compute each run's name.

    Resolution order:
      1. Schedule-triggered runs (no explicit correlation override and a
         ``scheduled_start_time`` set by Prefect) → ``<slot UTC ISO minute>``.
      2. explicit ``correlation_id`` kwarg.
      3. ``<value of correlation_field>-<HHMMSS>``.
      4. ``<UTC timestamp>`` as last resort.

    The callable is bound to *correlation_field* so the factory
    can pass it as ``flow_run_name=`` without leaking closure state.

    Args:
        correlation_field: Name of the parameter to use as the
            correlation value, or ``None``.

    Returns:
        A callable matching Prefect's ``flow_run_name`` signature.
    """

    def _run_name(**params: Any) -> str:
        params = params or _prefect_runtime_params()
        anchor = prefect_run_anchor()
        has_explicit_correlation = _has_explicit_correlation(params, correlation_field)
        if anchor is not None and not has_explicit_correlation:
            return anchor.strftime("%Y-%m-%dT%H:%M")

        ts = datetime.now(tz=UTC).strftime("%H%M%S")
        cid = params.get("correlation_id")
        if cid:
            return f"{cid}-{ts}"
        if correlation_field and params.get(correlation_field) is not None:
            value = resolve_placeholder(params[correlation_field], anchor=anchor)
            return f"{_sanitize(value)}-{ts}"
        return datetime.now(tz=UTC).strftime(_COMPACT_TIMESTAMP_FMT)

    return _run_name


__all__ = ["compute_correlation_id", "make_run_name_callback"]
