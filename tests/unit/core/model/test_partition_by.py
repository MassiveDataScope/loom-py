from __future__ import annotations

import datetime as dt

import pytest

from loom.core.model import BaseModel, ColumnField, RowScoped, ScopedField
from loom.core.model.introspection import declared_partition
from loom.core.model.partition import PartitionRange, RangePartition, range_partitions
from loom.core.model.types import DateTime, Integer, String, Text


class Event(BaseModel, RowScoped):
    __tablename__ = "events"
    __partition_by__ = ("RANGE", "at")
    owner_id: str = ScopedField(String(36), primary_key=True, scope="owner")
    at: dt.datetime = ColumnField(DateTime(), primary_key=True)
    kind: str = ColumnField(Text)


class Plain(BaseModel, RowScoped):
    __tablename__ = "plain"
    owner_id: str = ScopedField(String(36), primary_key=True, scope="owner")
    id: int = ColumnField(Integer, primary_key=True)


def _model(partition_by: object, **extra: object) -> type:
    namespace: dict[str, object] = {
        "__tablename__": "events_bad",
        "__partition_by__": partition_by,
        "__annotations__": {"owner_id": str, "at": dt.datetime, "code": str, "n": int},
        "owner_id": ScopedField(String(36), primary_key=True, scope="owner"),
        "at": ColumnField(DateTime(), primary_key=True),
        "code": ColumnField(Text),
        "n": ColumnField(Integer),
        **extra,
    }
    return type("Bad", (BaseModel, RowScoped), namespace)


def test_a_range_partition_names_its_column() -> None:
    assert declared_partition(Event) == RangePartition(column="at")


def test_a_model_without_partition_by_is_not_partitioned() -> None:
    assert declared_partition(Plain) is None


@pytest.mark.parametrize("strategy", ["LIST", "HASH", "list"])
def test_list_and_hash_are_refused_by_name(strategy: str) -> None:
    model = _model((strategy, "at"))
    with pytest.raises(ValueError, match=rf"Bad: __partition_by__ strategy {strategy!r}.*RANGE"):
        declared_partition(model)


@pytest.mark.parametrize("value", ["RANGE", ("RANGE",), ("RANGE", "at", "x"), ("RANGE", 3)])
def test_a_malformed_declaration_is_refused(value: object) -> None:
    model = _model(value)
    with pytest.raises(ValueError, match=r"Bad: __partition_by__ must be \('RANGE', <column>\)"):
        declared_partition(model)


def test_an_unknown_column_is_refused() -> None:
    model = _model(("RANGE", "when"))
    with pytest.raises(ValueError, match="names unknown column 'when'"):
        declared_partition(model)


def test_a_column_outside_the_primary_key_is_refused() -> None:
    model = _model(("RANGE", "at"), at=ColumnField(DateTime()))

    with pytest.raises(ValueError, match="'at' must be part of the primary key"):
        declared_partition(model)


def test_a_column_that_is_not_a_timestamp_is_refused() -> None:
    model = _model(("RANGE", "n"), n=ColumnField(Integer, primary_key=True))

    with pytest.raises(ValueError, match="'n' must be a DateTime column"):
        declared_partition(model)


def test_a_unique_key_without_the_partition_column_is_refused() -> None:
    model = _model(("RANGE", "at"), __unique__=(("owner_id", "code"),))

    with pytest.raises(ValueError, match=r"unique key \(owner_id, code\) lacks .* 'at'"):
        declared_partition(model)


def test_a_partial_unique_without_the_partition_column_is_refused() -> None:
    model = _model(("RANGE", "at"), __partial_unique__={"live": (("owner_id", "code"), "n > 0")})

    with pytest.raises(ValueError, match=r"unique key \(owner_id, code\) lacks .* 'at'"):
        declared_partition(model)


def test_an_unscoped_model_cannot_be_partitioned() -> None:
    class Loose(BaseModel):
        __tablename__ = "loose"
        __partition_by__ = ("RANGE", "at")
        id: int = ColumnField(Integer, primary_key=True)
        at: dt.datetime = ColumnField(DateTime(), primary_key=True)

    with pytest.raises(ValueError, match="Loose: __partition_by__ is only for row-scoped models"):
        declared_partition(Loose)


def test_monthly_partitions_cover_the_range_from_the_month_of_start() -> None:
    partitions = range_partitions("events", dt.date(2026, 11, 15), dt.date(2027, 2, 1))

    assert partitions == (
        PartitionRange("events_p202611", "2026-11-01 00:00:00+00", "2026-12-01 00:00:00+00"),
        PartitionRange("events_p202612", "2026-12-01 00:00:00+00", "2027-01-01 00:00:00+00"),
        PartitionRange("events_p202701", "2027-01-01 00:00:00+00", "2027-02-01 00:00:00+00"),
    )


def test_daily_and_yearly_partitions_are_named_by_their_period() -> None:
    daily = range_partitions("events", dt.date(2026, 2, 28), dt.date(2026, 3, 2), "day")
    yearly = range_partitions("events", dt.date(2026, 6, 1), dt.date(2027, 6, 1), "year")

    assert [p.name for p in daily] == ["events_p20260228", "events_p20260301"]
    assert [(p.name, p.lower, p.upper) for p in yearly] == [
        ("events_p2026", "2026-01-01 00:00:00+00", "2027-01-01 00:00:00+00"),
        ("events_p2027", "2027-01-01 00:00:00+00", "2028-01-01 00:00:00+00"),
    ]


def test_a_datetime_bound_is_read_as_its_date() -> None:
    start = dt.datetime(2026, 1, 31, 23, 0, tzinfo=dt.UTC)

    (partition,) = range_partitions("events", start, dt.date(2026, 2, 1))

    assert partition.name == "events_p202601"


def test_an_empty_or_reversed_range_yields_nothing() -> None:
    assert range_partitions("events", dt.date(2026, 3, 1), dt.date(2026, 3, 1)) == ()
    assert range_partitions("events", dt.date(2026, 3, 1), dt.date(2026, 1, 1)) == ()


def test_an_unknown_interval_is_refused() -> None:
    start, end = dt.date(2026, 1, 1), dt.date(2026, 2, 1)
    with pytest.raises(ValueError, match="interval 'week'"):
        range_partitions("events", start, end, "week")  # type: ignore[arg-type]


def test_a_partition_name_over_63_bytes_is_refused() -> None:
    table = "t" * 56

    start, end = dt.date(2026, 1, 1), dt.date(2026, 1, 2)
    with pytest.raises(ValueError, match="longer than the 63 bytes"):
        range_partitions(table, start, end, "day")
