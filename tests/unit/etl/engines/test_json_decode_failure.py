"""A JSON column that does not decode fails naming the declared column, on every engine."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Annotated, Any

import msgspec
import polars as pl
import pytest

from loom.core.errors import InvalidValue
from loom.core.model.types import Numeric
from loom.etl import ETLStep, FromTable, IntoTable
from loom.etl.compiler import ETLCompiler
from loom.etl.declarative.source import JsonMismatch
from loom.etl.executor import ETLExecutor
from tests.unit.etl.engines.doubles import ModelEngine, NoParams, PolarsEngine
from tests.unit.etl.models import Buyer

_RECOGNISED = {
    "json-document": (
        pl.exceptions.ComputeError("error deserializing JSON: json parsing error"),
        True,
    ),
    "parquet-metadata": (
        pl.exceptions.ComputeError("error deserializing parquet metadata"),
        False,
    ),
    "not-a-compute-error": (ValueError("error deserializing JSON"), False),
}


class _DecodeBuyer(ETLStep[NoParams]):
    customers: FromTable = FromTable("raw.customers").parse_json("buyer", Buyer)  # type: ignore[assignment]
    target = IntoTable("staging.customers").replace()

    def execute(self, params: NoParams, *, customers: object) -> object:  # type: ignore[override]
        return customers


class _OwnJsonFailure(ETLStep[NoParams]):
    customers: FromTable = FromTable("raw.customers")
    target = IntoTable("staging.customers").replace()

    def execute(self, params: NoParams, *, customers: object) -> object:  # type: ignore[override]
        raise pl.exceptions.ComputeError("error deserializing JSON: json parsing error")


class _OwnJsonFailureOverTolerantColumn(_OwnJsonFailure):
    customers: FromTable = FromTable("raw.customers").parse_json(  # type: ignore[assignment]
        "buyer", Buyer, on_mismatch=JsonMismatch.NULL
    )


def _run(engine: ModelEngine, step: type[ETLStep[Any]]) -> None:
    ETLExecutor(engine.reader, engine.writer).run_step(ETLCompiler().compile_step(step), NoParams())


def _seed_customers(engine: ModelEngine, *documents: str) -> None:
    engine.writer.write(
        engine.frame({"buyer": list(documents)}),
        IntoTable("raw.customers").append()._to_spec(),
        None,
    )


class TestJsonDecodeFailure:
    @pytest.mark.parametrize(
        "document", ["not json", "[1, 2]"], ids=["malformed", "shape-mismatch"]
    )
    def test_a_document_that_does_not_decode_names_the_declared_column(
        self, engine: ModelEngine, document: str
    ) -> None:
        _seed_customers(engine, document)

        with pytest.raises(InvalidValue, match=r"customers\.buyer as StructType"):
            _run(engine, _DecodeBuyer)

    @pytest.mark.parametrize(
        "step",
        [_OwnJsonFailure, _OwnJsonFailureOverTolerantColumn],
        ids=["no-json-column", "only-null-mode-column"],
    )
    def test_a_step_with_no_json_column_that_can_fail_keeps_the_failure_of_its_own_code(
        self, polars_engine: PolarsEngine, step: type[ETLStep[Any]]
    ) -> None:
        _seed_customers(polars_engine, "{}")

        with pytest.raises(pl.exceptions.ComputeError):
            _run(polars_engine, step)

    @pytest.mark.parametrize(("error", "recognised"), _RECOGNISED.values(), ids=list(_RECOGNISED))
    def test_polars_recognises_only_a_json_document_refusal(
        self, polars_engine: PolarsEngine, error: Exception, recognised: bool
    ) -> None:
        assert polars_engine.reader.is_json_decode_failure(error) is recognised


_PRICE = Annotated[Decimal, Numeric(precision=10, scale=2)]
_FIELD_READ = {
    "int-from-string": (int, '"oops"', None),
    "int-from-float": (int, "1.5", None),
    "int-from-bool": (int, "true", None),
    "float-from-string": (float, '"oops"', None),
    "bool-from-number": (bool, "1", None),
    "decimal-from-string": (_PRICE, '"oops"', None),
    "decimal-from-underscored-text": (_PRICE, '"1_0"', None),
    "decimal-overflowing-precision": (_PRICE, "99999999999", None),
    "decimal-in-range": (_PRICE, "12.5", Decimal("12.50")),
    "datetime-from-string": (datetime, '"nope"', None),
    "date-from-string": (date, '"hello"', None),
    "date-from-number": (date, "5", None),
    "date-from-iso-string": (date, '"2024-01-02"', date(2024, 1, 2)),
    "str-from-bool": (str, "true", "true"),
}
_NON_FINITE = ["NaN", "Infinity", "1e999"]


def _decode(
    engine: ModelEngine, probe: type, on_mismatch: JsonMismatch, *documents: str
) -> list[Any]:
    _seed_customers(engine, *documents)
    source = FromTable("raw.customers").parse_json("buyer", probe, on_mismatch=on_mismatch)
    decoded = engine.reader.read(source._to_spec("customers"), None)
    engine.writer.write(decoded, IntoTable("staging.customers").replace()._to_spec(), None)
    return [row["buyer"] for row in engine.stored("staging.customers")]


class TestNullModeDecoding:
    @pytest.mark.parametrize(
        ("annotation", "value", "expected"), _FIELD_READ.values(), ids=list(_FIELD_READ)
    )
    def test_a_wrongly_typed_field_is_nulled_and_its_siblings_are_kept(
        self, engine: ModelEngine, annotation: Any, value: str, expected: object
    ) -> None:
        probe = msgspec.defstruct("Probe", [("v", annotation | None), ("w", int)])

        buyers = _decode(engine, probe, JsonMismatch.NULL, f'{{"v": {value}, "w": 9}}')

        assert buyers == [{"v": expected, "w": 9}]

    def test_a_document_decodes_as_alone_beside_a_refused_one(self, engine: ModelEngine) -> None:
        probe = msgspec.defstruct("Probe", [("v", int | None), ("w", int)])
        document = '{"v": 1.5, "w": 9}'

        buyers = _decode(engine, probe, JsonMismatch.NULL, document, "not json")

        assert {"v": None, "w": 9} in buyers

    @pytest.mark.parametrize("value", _NON_FINITE)
    def test_a_non_finite_number_never_raises_and_keeps_its_siblings(
        self, engine: ModelEngine, value: str
    ) -> None:
        probe = msgspec.defstruct("Probe", [("v", float | None), ("w", int)])

        buyers = _decode(engine, probe, JsonMismatch.NULL, f'{{"v": {value}, "w": 9}}')

        assert [buyer["w"] for buyer in buyers] == [9]


class _Visit(msgspec.Struct):
    seen_at: datetime | None = None


_INSTANT = datetime(2026, 11, 28, 10, tzinfo=UTC)
_TEMPORAL_READ = {
    "iso-date": (date, '"2026-11-28"', date(2026, 11, 28)),
    "instant-with-z": (datetime, '"2026-11-28T10:00:00Z"', _INSTANT),
    "instant-without-offset": (datetime, '"2026-11-28T10:00:00"', _INSTANT),
    "instant-with-space": (datetime, '"2026-11-28 10:00:00"', _INSTANT),
    "instant-with-offset": (datetime, '"2026-11-28T12:00:00+02:00"', _INSTANT),
    "instant-in-struct": (_Visit, '{"seen_at": "2026-11-28 10:00:00"}', {"seen_at": _INSTANT}),
    "instants-in-list": (
        list[datetime],
        '["2026-11-28T10:00:00", "2026-11-28T10:00:00Z"]',
        [_INSTANT, _INSTANT],
    ),
}
_INVALID_TEMPORAL = {
    "date": (date, '"yesterday"', None),
    "instant": (datetime, '"yesterday"', None),
    "instant-in-struct": (_Visit, '{"seen_at": "yesterday"}', {"seen_at": None}),
}


def _in_utc(value: Any) -> Any:
    match value:
        case datetime():
            return value.astimezone(UTC)
        case dict():
            return {name: _in_utc(item) for name, item in value.items()}
        case list():
            return [_in_utc(item) for item in value]
        case _:
            return value


def _decode_probe(
    engine: ModelEngine, annotation: Any, document: str, on_mismatch: JsonMismatch
) -> Any:
    probe = msgspec.defstruct("Probe", [("v", annotation | None)])
    (decoded,) = _decode(engine, probe, on_mismatch, f'{{"v": {document}}}')
    return _in_utc(decoded["v"])


@pytest.mark.usefixtures("session_in_utc")
class TestTemporalText:
    @pytest.mark.parametrize("on_mismatch", list(JsonMismatch), ids=lambda mode: mode.name)
    @pytest.mark.parametrize(
        ("annotation", "document", "expected"), _TEMPORAL_READ.values(), ids=list(_TEMPORAL_READ)
    )
    def test_reads_dates_and_instants_from_iso_text_as_utc(
        self,
        engine: ModelEngine,
        annotation: Any,
        document: str,
        expected: object,
        on_mismatch: JsonMismatch,
    ) -> None:
        decoded = _decode_probe(engine, annotation, document, on_mismatch)

        assert decoded == expected

    @pytest.mark.parametrize(
        ("annotation", "document", "nulled"),
        _INVALID_TEMPORAL.values(),
        ids=list(_INVALID_TEMPORAL),
    )
    def test_invalid_temporal_text_is_null_in_null_mode(
        self, engine: ModelEngine, annotation: Any, document: str, nulled: object
    ) -> None:
        assert _decode_probe(engine, annotation, document, JsonMismatch.NULL) == nulled

    @pytest.mark.parametrize(
        ("annotation", "document"),
        [case[:2] for case in _INVALID_TEMPORAL.values()],
        ids=list(_INVALID_TEMPORAL),
    )
    def test_invalid_temporal_text_is_refused_in_fail_mode(
        self, engine: ModelEngine, annotation: Any, document: str
    ) -> None:
        with pytest.raises(Exception) as refused:  # noqa: PT011
            _decode_probe(engine, annotation, document, JsonMismatch.FAIL)

        assert engine.reader.is_json_decode_failure(refused.value)
