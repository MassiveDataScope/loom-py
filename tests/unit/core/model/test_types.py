"""Unit tests for the column type helpers in ``model.types``."""

from __future__ import annotations

import msgspec
import pytest

from loom.core.model.types import (
    JSON,
    ColumnType,
    NonNegativeInt,
    Numeric,
    Postgres,
    String,
    Uuid,
)


class TestColumnTypeRules:
    def test_a_numeric_scale_without_a_precision_is_refused(self) -> None:
        with pytest.raises(ValueError, match="Numeric: a scale needs a precision"):
            Numeric(scale=4)

    @pytest.mark.parametrize(
        ("precision", "scale"),
        [(0, None), (-3, 0), (5, -1), (5, 6)],
        ids=["zero-precision", "negative-precision", "negative-scale", "scale-over-precision"],
    )
    def test_a_numeric_outside_its_bounds_is_refused(
        self, precision: int, scale: int | None
    ) -> None:
        with pytest.raises(ValueError, match="Numeric"):
            Numeric(precision=precision, scale=scale)

    def test_an_unbounded_numeric_is_legal(self) -> None:
        assert Numeric().option("precision") is None

    @pytest.mark.parametrize(
        ("dialect_type", "portable"),
        [
            (Postgres.JSONB, JSON),
            (Postgres.UUID, Uuid),
            (Postgres.INET, String()),
            (Postgres.ARRAY(Uuid), JSON),
            (Postgres.TSVECTOR, None),
            (Uuid, Uuid),
        ],
        ids=["jsonb", "uuid", "inet", "array", "tsvector", "already-portable"],
    )
    def test_a_dialect_type_declares_its_portable_counterpart(
        self, dialect_type: ColumnType, portable: ColumnType | None
    ) -> None:
        assert dialect_type.portable() == portable

    def test_a_non_negative_int_accepts_zero_and_refuses_below(self) -> None:
        assert msgspec.convert(0, NonNegativeInt) == 0
        with pytest.raises(msgspec.ValidationError):
            msgspec.convert(-1, NonNegativeInt)
