"""What every notifier shares: which states it reports and the run details it shows."""

from __future__ import annotations

from loom.prefect.notify._event import NotifyEvent

FAILURE_STATES = frozenset({"Failed", "Crashed"})


def should_notify(state: str, *, on_failure: bool, on_completion: bool) -> bool:
    """Return whether a notifier with these filters reports *state*."""
    if on_failure and state in FAILURE_STATES:
        return True
    return on_completion and state == "Completed"


def run_details(event: NotifyEvent) -> list[tuple[str, str]]:
    """Return the labelled run details of *event*, omitting absent ones."""
    details = [("run", event.flow_run_name)]
    if event.env:
        details.append(("env", event.env))
    if event.correlation_id:
        details.append(("correlation", event.correlation_id))
    if event.duration_seconds is not None:
        details.append(("duration", _format_duration(event.duration_seconds)))
    return details


def _format_duration(seconds: float) -> str:
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}m {s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m {s:02d}s"


__all__ = ["FAILURE_STATES", "run_details", "should_notify"]
