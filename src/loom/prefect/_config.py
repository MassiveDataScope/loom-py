"""Flow configuration loader for loom.prefect."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import msgspec
import yaml


class FlowConfig(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Retry and execution policy for a single ETL flow.

    A flow retry is scheduled by the server rather than awaited in place: the
    run returns to ``AwaitingRetry`` and a worker claims it again later, so the
    container exits while it waits and hour-scale delays cost nothing.

    Args:
        flow_retries: Number of full-flow retries Prefect will attempt before
            declaring the run failed (each retry is a new container).
        flow_retry_delay_seconds: Seconds the server waits before rescheduling
            the run. Prefect's ``FlowRunPolicy`` validates this as a single
            integer, so a list of staggered delays is rejected here rather than
            failing mid-run — see :func:`_coerce_retry_delay`.

    Example::

        cfg = _load_flow_config("config/etl_flows.yaml", "daily_orders")
    """

    flow_retries: int = 2
    flow_retry_delay_seconds: int = 60


def _coerce_retry_delay(value: Any) -> int:
    """Return *value* as a flow retry delay, refusing shapes Prefect drops.

    ``Flow.__init__`` accepts a list and only ``FlowRunPolicy`` rejects it, at
    run time on the failure path. Staggered delays exist at task level only.

    Raises:
        TypeError: When *value* is a sequence of delays.
        ValueError: When *value* is negative.
    """
    if isinstance(value, (list, tuple)):
        raise TypeError(
            "flow_retry_delay_seconds must be a single number: Prefect's "
            "FlowRunPolicy rejects a list of delays. Staggered delays are a "
            "task-level feature."
        )
    delay = int(value)
    if delay < 0:
        raise ValueError("flow_retry_delay_seconds must not be negative")
    return delay


def flow_config_from_mapping(section: dict[str, Any] | None) -> FlowConfig:
    """Build a ``FlowConfig`` from a mapping, falling back to the defaults.

    Args:
        section: Mapping holding any of the ``flow_retry*`` keys. ``None`` and
            an empty mapping both yield the defaults.

    Returns:
        The resolved ``FlowConfig``.
    """
    if not section:
        return FlowConfig()
    defaults = FlowConfig()
    return FlowConfig(
        flow_retries=int(section.get("flow_retries", defaults.flow_retries)),
        flow_retry_delay_seconds=_coerce_retry_delay(
            section.get("flow_retry_delay_seconds", defaults.flow_retry_delay_seconds)
        ),
    )


def _load_flow_config(config_path: str, flow_name: str) -> FlowConfig:
    """Load ``FlowConfig`` for *flow_name* from a YAML file.

    YAML structure::

        flows:
          my_etl:
            flow_retries: 2
            flow_retry_delay_seconds: 60

    Args:
        config_path: Path to the YAML configuration file.
        flow_name: Key under ``flows`` to load.

    Returns:
        ``FlowConfig`` for the named flow, using ``FlowConfig`` defaults for
        any keys not present in the YAML section.

    Raises:
        KeyError: When ``flows`` key is missing or *flow_name* is not found.
    """
    raw = yaml.safe_load(Path(config_path).read_text())
    flows = raw["flows"]
    return flow_config_from_mapping(flows[flow_name])


__all__ = ["FlowConfig", "flow_config_from_mapping"]
