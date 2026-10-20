"""Unit tests for the column type helpers in ``model.types``."""

from __future__ import annotations

import pytest

from loom.core.model.types import Numeric


def test_a_numeric_scale_without_a_precision_is_refused() -> None:
    with pytest.raises(ValueError, match="Numeric: a scale needs a precision"):
        Numeric(scale=4)
