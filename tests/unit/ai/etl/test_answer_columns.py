"""The answer columns take their Polars types from the declared output, whatever the rows."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal

import msgspec
import polars as pl
import pytest

from loom.ai.abc import AgentUsage
from loom.ai.etl._batch import RowOutcome
from loom.ai.etl._columns import output_columns
from loom.ai.etl._frames import answers_frame
from loom.core.model import loom_type
from tests.unit.ai.etl._types import (
    Coded,
    Mood,
    Node,
    Offer,
    Priority,
    PydanticBranch,
    PydanticNode,
    PydanticOffer,
    PydanticQuote,
    PydanticReply,
    PydanticTree,
    Rich,
)

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
    "agent_cost_usd": pl.Decimal(38, 12),
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


_USAGE = AgentUsage(
    input_tokens=10, output_tokens=5, requests=1, duration_ms=1, cost=Decimal("0.5")
)


@pytest.mark.parametrize(
    "field",
    [{"count": 2**63}, {"amount": "1e40"}, {"offer": {"amount": "1e40", "currency": "EUR"}}],
    ids=["int past Int64", "Decimal past (38, 9)", "nested Decimal"],
)
def test_a_value_its_column_cannot_hold_fails_only_its_row(field: dict[str, object]) -> None:
    oversized = {**msgspec.to_builtins(_ANSWER), **field}
    outcome = RowOutcome(output=oversized, error=None, usage=_USAGE)

    frame = _frame(_answered(), outcome, _answered())

    assert frame["agent_status"].to_list() == ["ok", "error", "ok"]
    assert frame["agent_error"].to_list() == [None, "OUTPUT_UNREPRESENTABLE", None]
    assert all(frame.row(1, named=True)[name] is None for name in _RICH_SCHEMA)
    assert frame["agent_cost_usd"][1] == Decimal("0.5")
    assert frame["label"].to_list() == ["acepta", None, "acepta"]


def test_a_pydantic_output_names_its_columns_by_alias() -> None:
    assert output_columns(PydanticReply).keys() == {"answer", "day", "howMany"}
    assert [column.dtype for column in output_columns(PydanticReply).values()] == [
        pl.String(),
        pl.Date(),
        pl.Int64(),
    ]


_PYDANTIC_OFFER = pl.Struct({"amount": pl.Decimal(38, 9), "currencyCode": pl.String()})


def test_a_nested_pydantic_model_is_a_struct_of_its_fields() -> None:
    columns = output_columns(PydanticQuote)

    assert {name: column.dtype for name, column in columns.items()} == {
        "best": _PYDANTIC_OFFER,
        "others": pl.List(_PYDANTIC_OFFER),
    }


def test_a_nested_pydantic_answer_is_read_back_field_by_field() -> None:
    offer = PydanticOffer(amount=Decimal("1400"), currencyCode="EUR")
    answer = loom_type(PydanticQuote).to_builtins(PydanticQuote(best=offer, others=[offer]))
    keys = pl.DataFrame({"id": [0]}, schema={"id": pl.Int64()})
    outcome = RowOutcome(output=answer, error=None, usage=None)

    row = answers_frame(keys, (outcome,), output_columns(PydanticQuote), "v1").row(0, named=True)

    assert row["best"] == {"amount": Decimal("1400"), "currencyCode": "EUR"}
    assert row["others"] == [{"amount": Decimal("1400"), "currencyCode": "EUR"}]


@pytest.mark.parametrize("output_type", [PydanticNode, Node], ids=["pydantic", "msgspec"])
def test_a_self_referencing_type_holds_its_nested_copies_as_json(output_type: type) -> None:
    columns = output_columns(output_type)
    keys = pl.DataFrame({"id": [0]}, schema={"id": pl.Int64()})
    answer = {"name": "a", "children": [{"name": "b", "children": []}]}
    outcome = RowOutcome(output=answer, error=None, usage=None)

    row = answers_frame(keys, (outcome,), columns, "v1").row(0, named=True)

    assert {name: column.dtype for name, column in columns.items()} == {
        "name": pl.String(),
        "children": pl.List(pl.String()),
    }
    assert row["children"] == ['{"name":"b","children":[]}']


def test_mutually_referencing_models_stop_at_the_first_repeated_model() -> None:
    columns = output_columns(PydanticTree)

    assert {name: column.dtype for name, column in columns.items()} == {
        "root": pl.Struct({"label": pl.String(), "tree": pl.String()}),
    }
    assert output_columns(PydanticBranch)["tree"].dtype == pl.Struct({"root": pl.String()})


def test_a_union_value_is_held_as_its_json_whatever_its_type() -> None:
    keys = pl.DataFrame({"id": [0, 1]}, schema={"id": pl.Int64()})
    outcomes = [RowOutcome(output={"code": code}, error=None, usage=None) for code in ("5", 5)]

    frame = answers_frame(keys, outcomes, output_columns(Coded), "v1")

    assert frame["code"].to_list() == ['"5"', "5"]


def test_the_cost_is_written_without_rounding_through_a_float() -> None:
    cost = Decimal("0.000000123457")
    usage = AgentUsage(input_tokens=1, output_tokens=1, requests=1, duration_ms=1, cost=cost)

    frame = _frame(RowOutcome(output=msgspec.to_builtins(_ANSWER), error=None, usage=usage))

    assert frame["agent_cost_usd"][0] == cost


@pytest.mark.parametrize(
    ("cost", "written"),
    [
        (Decimal("0.0000000000001234"), Decimal("0")),
        (Decimal("0.0000000000009"), Decimal("0.000000000001")),
        (Decimal("0.0000000000015"), Decimal("0.000000000002")),
        (Decimal("0.0000000000025"), Decimal("0.000000000002")),
        (Decimal("1.2345678901234567"), Decimal("1.234567890123")),
    ],
    ids=["below scale", "rounds up", "half to even up", "half to even down", "past scale"],
)
def test_a_cost_past_twelve_decimal_places_is_rounded_half_to_even(
    cost: Decimal, written: Decimal
) -> None:
    usage = AgentUsage(input_tokens=1, output_tokens=1, requests=1, duration_ms=1, cost=cost)

    frame = _frame(RowOutcome(output=msgspec.to_builtins(_ANSWER), error=None, usage=usage))

    assert frame["agent_status"][0] == "ok"
    assert frame["agent_cost_usd"][0] == written
