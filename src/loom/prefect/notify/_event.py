"""Frozen payload that every Notifier receives."""

from __future__ import annotations

import msgspec


class NotifyEvent(msgspec.Struct, frozen=True, kw_only=True):
    """Lifecycle snapshot sent to notifiers on terminal flow states.

    Args:
        flow_name: Logical flow name (``meta.name``).
        flow_run_name: Computed run name (e.g. correlation_id + HHMMSS).
        flow_run_url: Direct link to the flow run in the Prefect UI.
        state: Terminal Prefect state name (``"Completed"``, ``"Failed"``,
            ``"Crashed"``).
        correlation_id: Business correlation id of the run. Empty string
            when not applicable (e.g. maintenance flows).
        duration_seconds: Wall-clock time from flow start to terminal state,
            as reported by Prefect. ``None`` when unavailable.
        env: Environment of the run, resolved from the flow's ``env``
            parameter (e.g. ``"prod"``, ``"staging"``). ``None`` when not
            declared.
        message: Optional human-readable error or status message.
    """

    flow_name: str
    flow_run_name: str
    flow_run_url: str
    state: str
    correlation_id: str
    duration_seconds: float | None = None
    env: str | None = None
    message: str | None = None


__all__ = ["NotifyEvent"]
