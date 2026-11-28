"""A JSON document fitted to a frame type, with no engine involved."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from loom.etl.schema import (
    DatetimeType,
    DecimalType,
    FrameType,
    JsonFit,
    JsonMismatch,
    JsonRefusal,
    LoomDtype,
    StructField,
    StructType,
)

_INSTANT = datetime(2026, 11, 28, 10, tzinfo=UTC)
_ORDER = StructType(
    (StructField("placed", LoomDtype.DATE), StructField("quantity", LoomDtype.INT8))
)


class TestJsonFit:
    @pytest.mark.parametrize("on_mismatch", list(JsonMismatch), ids=lambda mode: mode.value)
    def test_a_document_fits_to_the_python_values_of_its_type(
        self, on_mismatch: JsonMismatch
    ) -> None:
        fit = JsonFit.of(_ORDER, on_mismatch)

        assert fit.document('{"placed": "2026-11-28T10:00:00", "quantity": 3}') == {
            "placed": date(2026, 11, 28),
            "quantity": 3,
        }

    def test_null_policy_nulls_only_the_value_that_does_not_fit(self) -> None:
        fit = JsonFit.of(_ORDER, JsonMismatch.NULL)

        assert fit.document('{"placed": "2026-11-28", "quantity": 300}') == {
            "placed": date(2026, 11, 28),
            "quantity": None,
        }

    def test_fail_policy_refuses_with_the_path_and_the_rule_of_the_value(self) -> None:
        fit = JsonFit.of(_ORDER, JsonMismatch.FAIL)

        with pytest.raises(JsonRefusal) as refused:
            fit.document('{"placed": "2026-11-28", "quantity": 300}')

        assert (refused.value.path, refused.value.rule) == (
            "$.quantity",
            "an integer from -128 to 127",
        )

    @pytest.mark.parametrize(
        ("frame_type", "document", "expected"),
        [
            (DecimalType(10, 2), '"12.5"', Decimal("12.50")),
            (DatetimeType("us", "UTC"), '"2026-11-28T12:00:00+02:00"', _INSTANT),
            (DatetimeType("us"), '"2026-11-28T12:00:00+02:00"', _INSTANT.replace(tzinfo=None)),
        ],
        ids=["decimal", "zoned-instant", "naive-instant"],
    )
    def test_a_leaf_fits_to_its_domain_value(
        self, frame_type: FrameType, document: str, expected: object
    ) -> None:
        assert JsonFit.of(frame_type, JsonMismatch.FAIL).document(document) == expected
