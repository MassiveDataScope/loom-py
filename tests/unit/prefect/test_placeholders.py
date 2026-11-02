"""Tests for loom.prefect._placeholders.resolve_placeholder.

DSL: ${today}, ${yesterday}, ${today±Nd}, ${now}, ${now±Nd|Nh|Nm}.
Anything else that looks like a placeholder must raise ValueError.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone

import msgspec
import pytest
from prefect.types import DateTime as PrefectDateTime

from loom.prefect._placeholders import resolve_placeholder


def _utc_today() -> date:
    # The tokens count in UTC; date.today() is local and differs around midnight.
    return datetime.now(UTC).date()


def test_today_returns_today_utc_date() -> None:
    assert resolve_placeholder("${today}") == _utc_today()


def test_yesterday_returns_today_minus_one_day() -> None:
    assert resolve_placeholder("${yesterday}") == _utc_today() - timedelta(days=1)


def test_today_plus_offset() -> None:
    assert resolve_placeholder("${today+3d}") == _utc_today() + timedelta(days=3)


def test_today_minus_offset() -> None:
    assert resolve_placeholder("${today-7d}") == _utc_today() - timedelta(days=7)


def test_now_returns_utc_datetime_close_to_now() -> None:
    result = resolve_placeholder("${now}")
    assert isinstance(result, datetime)
    assert result.tzinfo is not None
    delta = abs((datetime.now(UTC) - result).total_seconds())
    assert delta < 5.0


def test_now_minus_hours() -> None:
    result = resolve_placeholder("${now-1h}")
    expected = datetime.now(UTC) - timedelta(hours=1)
    assert isinstance(result, datetime)
    assert abs((expected - result).total_seconds()) < 5.0


def test_now_plus_minutes() -> None:
    result = resolve_placeholder("${now+15m}")
    expected = datetime.now(UTC) + timedelta(minutes=15)
    assert abs((expected - result).total_seconds()) < 5.0


def test_now_minus_days() -> None:
    result = resolve_placeholder("${now-2d}")
    expected = datetime.now(UTC) - timedelta(days=2)
    assert abs((expected - result).total_seconds()) < 5.0


@pytest.mark.parametrize(
    "bad",
    [
        "${tomorrow}",
        "${today+5h}",
        "${yesterday-1d}",
        "${now-1}",
        "${nows}",
        "${today+}",
        "${now+abc}",
    ],
)
def test_invalid_placeholders_raise_value_error(bad: str) -> None:
    with pytest.raises(ValueError):
        resolve_placeholder(bad)


def test_non_placeholder_string_passes_through() -> None:
    assert resolve_placeholder("hello world") == "hello world"


def test_literal_iso_date_string_passes_through() -> None:
    assert resolve_placeholder("2026-05-15") == "2026-05-15"


def test_int_passes_through() -> None:
    assert resolve_placeholder(42) == 42


def test_list_passes_through() -> None:
    assert resolve_placeholder(["ES", "FR"]) == ["ES", "FR"]


def test_none_passes_through() -> None:
    assert resolve_placeholder(None) is None


_SLOT = datetime(2026, 9, 23, 23, 30, tzinfo=UTC)


def test_today_counts_from_the_anchor_day() -> None:
    assert resolve_placeholder("${today-1d}", anchor=_SLOT) == date(2026, 9, 22)


def test_yesterday_counts_from_the_anchor_day() -> None:
    assert resolve_placeholder("${yesterday}", anchor=_SLOT) == date(2026, 9, 22)


def test_now_is_the_anchor() -> None:
    assert resolve_placeholder("${now}", anchor=_SLOT) == _SLOT


def test_now_offset_counts_from_the_anchor() -> None:
    assert resolve_placeholder("${now-1h}", anchor=_SLOT) == datetime(
        2026, 9, 23, 22, 30, tzinfo=UTC
    )


def test_anchor_is_read_in_utc() -> None:
    madrid_next_day = datetime(2026, 9, 24, 1, 30, tzinfo=timezone(timedelta(hours=2)))
    assert resolve_placeholder("${today}", anchor=madrid_next_day) == date(2026, 9, 23)
    assert resolve_placeholder("${now}", anchor=madrid_next_day) == _SLOT


def test_naive_anchor_is_read_as_utc() -> None:
    naive = _SLOT.replace(tzinfo=None)
    result = resolve_placeholder("${now}", anchor=naive)
    assert result == _SLOT
    assert result.tzinfo is UTC


def test_anchor_does_not_touch_non_placeholders() -> None:
    assert resolve_placeholder("2026-05-15", anchor=_SLOT) == "2026-05-15"


def test_anchor_keeps_rejecting_invalid_placeholders() -> None:
    with pytest.raises(ValueError):
        resolve_placeholder("${tomorrow}", anchor=_SLOT)


# Prefect hands the scheduled start over as this datetime subclass: pendulum's
# below Python 3.13, Prefect's own from 3.13 on.
_PREFECT_SLOT = PrefectDateTime(2026, 9, 23, 23, 30, 15, 123456, tzinfo=UTC)


@pytest.mark.parametrize(
    ("token", "expected"),
    [
        ("${today}", date(2026, 9, 23)),
        ("${today-1d}", date(2026, 9, 22)),
        ("${yesterday}", date(2026, 9, 22)),
    ],
)
def test_a_pendulum_anchor_yields_a_stdlib_date(token: str, expected: date) -> None:
    result = resolve_placeholder(token, anchor=_PREFECT_SLOT)
    assert type(result) is date
    assert result == expected


@pytest.mark.parametrize(
    ("token", "expected"),
    [
        ("${now}", datetime(2026, 9, 23, 23, 30, 15, 123456, tzinfo=UTC)),
        ("${now-1h}", datetime(2026, 9, 23, 22, 30, 15, 123456, tzinfo=UTC)),
        ("${now+2d}", datetime(2026, 9, 25, 23, 30, 15, 123456, tzinfo=UTC)),
    ],
)
def test_a_pendulum_anchor_yields_a_stdlib_datetime(token: str, expected: datetime) -> None:
    result = resolve_placeholder(token, anchor=_PREFECT_SLOT)
    assert type(result) is datetime
    assert result == expected


def test_values_resolved_from_a_pendulum_anchor_decode_with_msgspec() -> None:
    class Window(msgspec.Struct):
        day: date
        until: datetime

    window = msgspec.convert(
        {
            "day": resolve_placeholder("${yesterday}", anchor=_PREFECT_SLOT),
            "until": resolve_placeholder("${now}", anchor=_PREFECT_SLOT),
        },
        type=Window,
    )
    assert window.day == date(2026, 9, 22)
    assert window.until == _PREFECT_SLOT
