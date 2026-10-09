"""The answer columns take their Polars types from the declared output, whatever the rows."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal

import msgspec
import polars as pl
import pytest

from loom.ai.etl._batch import RowOutcome
from loom.ai.etl._columns import output_columns
from loom.ai.etl._frames import answers_frame
from tests.unit.ai.etl._types import Mood, Offer, Priority, PydanticReply, Rich

_RICH_SCHEMA = {
    "label": pl.String(),
    "mood": pl.String(),
    "priority": pl.Int64(),
    "day": pl.Date(),
    "seenAt": pl.Datetime("us", "UTC"),
    "amount": pl.Decimal(38, 9),
    "score": pl.Float64(),
    "flag": pl.Boolean(),
    "tags": pl.List(pl.String()),
    "offer": pl.Struct({"amount": pl.Decimal(38, 9), "currency": pl.String()}),
    "extra": pl.String(),
    "count": pl.Int64(),
}
_METADATA_SCHEMA = {
    "agent_version": pl.String(),
    "agent_status": pl.String(),
    "agent_error": pl.String(),
    "agent_input_tokens": pl.Int64(),
    "agent_output_tokens": pl.Int64(),
    "agent_cache_read_tokens": pl.Int64(),
    "agent_cache_write_tokens": pl.Int64(),
    "agent_cost_usd": pl.Float64(),
}
_ANSWER = Rich(
    label="acepta",
    mood=Mood.ANGRY,
    priority=Priority.HIGH,
    day=date(2026, 10, 9),
    seen_at=datetime(2026, 10, 9, 12, 30, tzinfo=timezone(timedelta(hours=2))),
    amount=Decimal("1500.25"),
    score=0.75,
    flag=True,
    tags=["precio"],
    offer=Offer(amount=Decimal("1400"), currency="EUR"),
    extra={"a": 1},
)


def _frame(*outcomes: RowOutcome) -> pl.DataFrame:
    keys = pl.DataFrame({"id": list(range(len(outcomes)))}, schema={"id": pl.Int64()})
    return answers_frame(keys, outcomes, output_columns(Rich), "v1")


def _answered() -> RowOutcome:
    return RowOutcome(output=msgspec.to_builtins(_ANSWER), error=None, usage=None)


def _failed() -> RowOutcome:
    return RowOutcome(output=None, error="PROVIDER_UNAVAILABLE", usage=None)


@pytest.mark.parametrize(
    "outcomes",
    [(_answered(), _failed()), (), (_failed(), _failed())],
    ids=["answered", "empty", "all errors"],
)
def test_every_column_has_the_type_the_output_declares(outcomes: tuple[RowOutcome, ...]) -> None:
    frame = _frame(*outcomes)

    assert dict(frame.schema) == {"id": pl.Int64(), **_RICH_SCHEMA, **_METADATA_SCHEMA}


def test_answers_are_read_back_as_their_declared_values() -> None:
    row = _frame(_answered()).row(0, named=True)

    assert (row["label"], row["mood"], row["priority"]) == ("acepta", "angry", 2)
    assert row["day"] == date(2026, 10, 9)
    assert row["seenAt"] == datetime(2026, 10, 9, 10, 30, tzinfo=UTC)
    assert row["amount"] == Decimal("1500.25")
    assert row["tags"] == ["precio"]
    assert row["offer"] == {"amount": Decimal("1400"), "currency": "EUR"}
    assert row["extra"] == '{"a":1}'
    assert row["count"] is None


def test_an_error_row_leaves_every_answer_column_null() -> None:
    row = _frame(_failed()).row(0, named=True)

    assert all(row[name] is None for name in _RICH_SCHEMA)


def test_a_pydantic_output_names_its_columns_by_alias() -> None:
    assert output_columns(PydanticReply).keys() == {"answer", "day", "howMany"}
    assert [column.dtype for column in output_columns(PydanticReply).values()] == [
        pl.String(),
        pl.Date(),
        pl.Int64(),
    ]
