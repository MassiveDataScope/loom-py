"""Delta schema parsing refuses a malformed field name with the matching class."""

from __future__ import annotations

import pytest

from loom.core.errors import InvalidType, InvalidValue
from loom.etl.engines.polars._schema import _delta_json_to_polars_schema


def _schema_with_field_name(name: object) -> dict[str, object]:
    return {"type": "struct", "fields": [{"name": name, "type": "long"}]}


class TestDeltaFieldName:
    def test_an_empty_name_is_an_invalid_value(self) -> None:
        with pytest.raises(InvalidValue, match="must not be empty"):
            _delta_json_to_polars_schema(_schema_with_field_name(""))

    def test_a_non_string_name_is_an_invalid_type(self) -> None:
        with pytest.raises(InvalidType, match="must be a str"):
            _delta_json_to_polars_schema(_schema_with_field_name(7))
