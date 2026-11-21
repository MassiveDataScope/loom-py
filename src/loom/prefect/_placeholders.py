"""Placeholder DSL for dynamic ETL parameters.

Supported tokens (case-sensitive):

- ``${today}``, ``${today+Nd}``, ``${today-Nd}`` → :class:`datetime.date`
- ``${yesterday}`` → :class:`datetime.date` (alias of ``${today-1d}``)
- ``${now}``, ``${now±Nd}``, ``${now±Nh}``, ``${now±Nm}`` →
  timezone-aware UTC :class:`datetime.datetime`

Any value that looks like ``${...}`` but does not match one of the three
valid patterns raises :class:`ValueError`. Non-string values and strings
that do not look like placeholders pass through unchanged.

Tokens count from an *anchor* instant, read in UTC. Inside a Prefect flow
run the anchor is the run's scheduled start, so the processed window belongs
to the run and not to the moment it happens to execute:

- a scheduled run resolves against its slot, even when it starts late;
- a retry (by the engine or "Retry" in the UI) resolves against the same slot;
- a manual run resolves against the time it was created.

A run that starts late therefore gets ``${now}`` up to its scheduled time,
not up to the wall clock. Without an anchor the tokens use the wall clock.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from typing import Any

from loom.core.config.keys import INTERPOLATION

_TODAY_RE = re.compile(r"^\$\{today(?:([+-])(\d+)d)?\}$")
_YESTERDAY_RE = re.compile(r"^\$\{yesterday\}$")
_NOW_RE = re.compile(r"^\$\{now(?:([+-])(\d+)([dhm]))?\}$")
_ANY_DOLLAR = re.compile(r"^\$\{.*\}$")

_NOW_UNIT_KW = {"d": "days", "h": "hours", "m": "minutes"}


def _signed_int(sign: str | None, amount: str | None) -> int:
    if sign is None or amount is None:
        return 0
    magnitude = int(amount)
    return magnitude if sign == "+" else -magnitude


def utc_datetime(value: datetime) -> datetime:
    """Return *value* as a stdlib UTC ``datetime``, a naive value read as UTC.

    A subclass such as pendulum's ``DateTime``, which Prefect returns for a
    run's scheduled start, is rebuilt as a plain ``datetime``: msgspec decodes
    only the exact stdlib types, and ``DateTime.date()`` is not a stdlib
    ``date`` either.
    """
    utc = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    return datetime(
        utc.year,
        utc.month,
        utc.day,
        utc.hour,
        utc.minute,
        utc.second,
        utc.microsecond,
        tzinfo=UTC,
    )


def _base(anchor: datetime | None) -> datetime:
    return datetime.now(UTC) if anchor is None else utc_datetime(anchor)


def _resolve_today(match: re.Match[str], base: datetime) -> Any:
    sign, days = match.groups()
    return base.date() + timedelta(days=_signed_int(sign, days))


def _resolve_now(match: re.Match[str], base: datetime) -> Any:
    sign, amount, unit = match.groups()
    if sign is None:
        return base
    return base + timedelta(**{_NOW_UNIT_KW[unit]: _signed_int(sign, amount)})


def is_placeholder(value: Any) -> bool:
    """Return ``True`` when *value* is a string written as ``${...}``."""
    return isinstance(value, str) and value.startswith(INTERPOLATION)


def resolve_placeholder(value: Any, *, anchor: datetime | None = None) -> Any:
    """Resolve a placeholder string to a concrete ``date`` / ``datetime``.

    Args:
        value: Arbitrary parameter value. Only strings matching the DSL are
            resolved; every other value is returned unchanged.
        anchor: Instant the tokens count from, read in UTC (a naive value
            is taken as UTC). ``None`` uses the current time.

    Returns:
        The resolved date/datetime, or the original value if it is not a
        placeholder.

    Raises:
        ValueError: If the value looks like a placeholder (``${...}``) but
            does not match any of the supported patterns.
    """
    if not is_placeholder(value):
        return value

    base = _base(anchor)
    today_match = _TODAY_RE.match(value)
    if today_match is not None:
        return _resolve_today(today_match, base)

    if _YESTERDAY_RE.match(value) is not None:
        return base.date() - timedelta(days=1)

    now_match = _NOW_RE.match(value)
    if now_match is not None:
        return _resolve_now(now_match, base)

    if _ANY_DOLLAR.match(value) is not None:
        raise ValueError(f"invalid placeholder: {value!r}")
    return value


__all__ = ["is_placeholder", "resolve_placeholder", "utc_datetime"]
