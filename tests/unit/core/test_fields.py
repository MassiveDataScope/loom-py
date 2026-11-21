"""What a field value is written as: the one renderer every writer of a field shares."""

from __future__ import annotations

import math

import pytest

from loom.core.fields import (
    MASK,
    MAX_LIST_ITEMS,
    MAX_VALUE_LENGTH,
    TRUNCATION_MARKER,
    FieldValue,
    Scalar,
    Secret,
    render_value,
)
from loom.core.model.redaction import Dsn, RedactedMap


class _Card:
    def __repr__(self) -> str:
        return "Card(4111-1111-1111-1111)"


class TestRenderValue:
    @pytest.mark.parametrize("value", [None, True, 3, 2.5, "ok", ["a", 1, None], ("a", 2)])
    def test_a_scalar_or_a_short_list_of_scalars_is_written_as_it_is(self, value: object) -> None:
        rendered = render_value(value)

        assert rendered == (list(value) if isinstance(value, tuple) else value)

    @pytest.mark.parametrize(
        ("value", "name"),
        [(_Card(), "_Card"), ({"pan": "4111"}, "dict"), (["a"] * (MAX_LIST_ITEMS + 1), "list")],
    )
    def test_anything_else_is_its_type_name_never_its_repr(self, value: object, name: str) -> None:
        assert render_value(value) == name

    @pytest.mark.parametrize("secret", [Secret("hunter2"), RedactedMap({"token": "abc"})])
    def test_a_secret_is_the_mask(self, secret: object) -> None:
        assert render_value(secret) == MASK

    def test_a_dsn_is_its_masked_text(self) -> None:
        assert render_value(Dsn("postgresql://u:hunter2@db/app")) == "postgresql://u:***@db/app"

    def test_a_field_value_is_the_form_it_declares_cut_like_any_text(self) -> None:
        class _Long(FieldValue):
            def field_value(self) -> Scalar:
                return "y" * (MAX_VALUE_LENGTH + 1)

        rendered = render_value(_Long())

        assert isinstance(rendered, str)
        assert rendered.endswith(TRUNCATION_MARKER)

    def test_a_long_string_is_cut_with_a_marker(self) -> None:
        rendered = render_value("x" * (MAX_VALUE_LENGTH + 10))

        assert isinstance(rendered, str)
        assert len(rendered) == MAX_VALUE_LENGTH
        assert rendered.endswith(TRUNCATION_MARKER)

    def test_a_non_finite_float_is_text_so_the_json_stays_valid(self) -> None:
        assert render_value(math.inf) == "inf"
