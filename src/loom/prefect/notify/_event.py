"""Frozen payload that every Notifier receives."""

from __future__ import annotations

import msgspec

from loom.core.log_names import LogField
from loom.prefect.event import PrefectKey


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
        env: Environment the run ran in, the process one (e.g.
            ``"production"``, ``"staging"``); the hooks always fill it.
        message: Loom's one-line summary of the run, when the flow body
            wrote one; never the text of the failure.
    """

    flow_name: str
    flow_run_name: str
    flow_run_url: str
    state: str
    correlation_id: str
    duration_seconds: float | None = None
    env: str | None = None
    message: str | None = None

    def log_fields(self) -> dict[str, str]:
        """The fields a record about this notification carries: flow, state and correlation id."""
        fields: dict[str, str] = {PrefectKey.FLOW: self.flow_name, LogField.STATUS: self.state}
        if self.correlation_id:
            fields[LogField.CORRELATION_ID] = self.correlation_id
        return fields


__all__ = ["NotifyEvent"]
