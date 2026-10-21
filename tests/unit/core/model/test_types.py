"""Unit tests for the column type helpers in ``model.types``."""

from __future__ import annotations

import msgspec
import pytest

from loom.core.model.types import NonNegativeInt, Numeric


def test_a_numeric_scale_without_a_precision_is_refused() -> None:
    with pytest.raises(ValueError, match="Numeric: a scale needs a precision"):
        Numeric(scale=4)


def test_a_non_negative_int_accepts_zero_and_refuses_below() -> None:
    assert msgspec.convert(0, NonNegativeInt) == 0
    with pytest.raises(msgspec.ValidationError):
        msgspec.convert(-1, NonNegativeInt)
