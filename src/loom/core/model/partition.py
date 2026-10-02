"""Range partitioning of a row-scoped table: its declaration and the partitions that cover a period.

A model declares ``__partition_by__ = ("RANGE", "<column>")`` on a ``DateTime``
column of its primary key. Partitions are created ahead of time, one per day,
month or year, and are named deterministically after the table and the period
they hold: ``<table>_p<YYYY>``, ``<table>_p<YYYYMM>`` or ``<table>_p<YYYYMMDD>``.
Bounds are midnight UTC; a ``timestamp without time zone`` column reads them as
plain midnight.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Final, Literal

from loom.core.schema_names import MAX_IDENTIFIER_LENGTH

Interval = Literal["day", "month", "year"]
_INTERVALS: Final[dict[str, Interval]] = {"day": "day", "month": "month", "year": "year"}
_SUFFIX: Final[dict[str, str]] = {"day": "%Y%m%d", "month": "%Y%m", "year": "%Y"}


@dataclass(frozen=True, slots=True)
class RangePartition:
    """A table partitioned by range over ``column``."""

    column: str


@dataclass(frozen=True, slots=True)
class PartitionRange:
    """One partition: its name and the half-open range ``[lower, upper)`` it holds."""

    name: str
    lower: str
    upper: str


def range_partitions(
    table: str, start: dt.date, end: dt.date, interval: Interval = "month"
) -> tuple[PartitionRange, ...]:
    """The partitions of ``table`` covering ``[start, end)``, from the period holding ``start``.

    A ``datetime`` bound is read as its date.

    Raises:
        ValueError: When ``interval`` is unknown or a partition name would be
            longer than Postgres keeps.
    """
    period = _floor(as_date(start), validate_interval(interval))
    stop = as_date(end)
    partitions: list[PartitionRange] = []
    while period < stop:
        following = _next(period, interval)
        name = partition_name(table, period, interval)
        partitions.append(PartitionRange(name, _bound(period), _bound(following)))
        period = following
    return tuple(partitions)


def partition_name(table: str, period: dt.date, interval: Interval) -> str:
    """The name of the partition of ``table`` holding the period that starts on ``period``.

    Raises:
        ValueError: When the name would be longer than Postgres keeps.
    """
    name = f"{table}_p{period.strftime(_SUFFIX[interval])}"
    if len(name.encode()) > MAX_IDENTIFIER_LENGTH:
        raise ValueError(
            f"partition name {name!r} is longer than the {MAX_IDENTIFIER_LENGTH} bytes "
            "Postgres keeps; shorten the table name"
        )
    return name


def validate_interval(interval: str) -> Interval:
    """Return ``interval`` when it is one partitions are created by.

    Raises:
        ValueError: When it is not ``day``, ``month`` or ``year``.
    """
    known = _INTERVALS.get(interval)
    if known is None:
        raise ValueError(f"interval {interval!r} is not one of {tuple(_INTERVALS)}")
    return known


def as_date(value: dt.date | str) -> dt.date:
    """Read an ISO date string, or a ``datetime``, as a date."""
    if isinstance(value, str):
        return dt.date.fromisoformat(value)
    return value.date() if isinstance(value, dt.datetime) else value


def _floor(day: dt.date, interval: Interval) -> dt.date:
    if interval == "year":
        return day.replace(month=1, day=1)
    if interval == "month":
        return day.replace(day=1)
    return day


def _next(period: dt.date, interval: Interval) -> dt.date:
    if interval == "year":
        return period.replace(year=period.year + 1)
    if interval == "month":
        carry, month = divmod(period.month, 12)
        return period.replace(year=period.year + carry, month=month + 1)
    return period + dt.timedelta(days=1)


def _bound(day: dt.date) -> str:
    return f"{day.isoformat()} 00:00:00+00"
