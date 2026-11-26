"""Model instances as the rows of their lake table, in both directions."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from typing import Any, Final

import msgspec
import polars as pl

from loom.core.errors import InvalidValue
from loom.core.model.convert import to_column_value, to_struct
from loom.core.model.introspection import conflict_key_of, get_column_fields
from loom.core.model.types import ColumnKind
from loom.etl.engines.polars._dtype import frame_type_to_polars
from loom.etl.schema import ColumnSchema, FrameType, LoomDtype, ModelTable

_JSON_KINDS: Final = frozenset(
    {ColumnKind.JSON, ColumnKind.POSTGRES_JSONB, ColumnKind.POSTGRES_ARRAY}
)


class ModelRows:
    """The rows of one model: its instances as values of the lake types, and back.

    Values are built per column from field names, so a model that renames its
    fields on the wire still seeds and reads by field name; only the names
    nested in a JSON column are encode names.  A struct or list column keeps
    its nested datetimes and decimals native, because the lake type of the
    field holds them and a JSON-encoded string would not.

    Args:
        model: A model with a table.
    """

    def __init__(self, model: type) -> None:
        self.table = ModelTable.from_model(model)
        self._fields = get_column_fields(model)

    @classmethod
    def by_model(cls, instances: Iterable[Any]) -> list[tuple[ModelRows, list[Any]]]:
        """Group *instances* by model class, in order of first appearance."""
        grouped: dict[type, list[Any]] = {}
        for instance in instances:
            grouped.setdefault(type(instance), []).append(instance)
        return [(cls(model), rows) for model, rows in grouped.items()]

    @property
    def lake_types(self) -> dict[str, FrameType]:
        """The lake type of each column, by column name."""
        return {column.name: column.dtype for column in self.table.schema}

    def seed_values(self, instances: list[Any]) -> list[tuple[Any, ...]]:
        """The row tuples of *instances*, in column order.

        Raises:
            InvalidValue: Two instances share a conflict key.
        """
        self._require_distinct_keys(instances)
        return [self._row_of(instance) for instance in instances]

    def instances_in(self, frame: pl.DataFrame) -> list[Any]:
        """The instances of the rows of *frame*, cast to the lake types of the model.

        Raises:
            InvalidValue: *frame* lacks a column of the model.
        """
        absent = [name for name in self.table.column_names if name not in frame.columns]
        if absent:
            raise InvalidValue(
                f"{self.table.model.__name__} rows need the columns {absent}, "
                f"which the frame lacks; it has {frame.columns}."
            )
        cast = frame.select(
            pl.col(column.name).cast(frame_type_to_polars(column.dtype))
            for column in self.table.schema
        )
        return [
            to_struct(self.table.model, self._field_values(record)) for record in cast.to_dicts()
        ]

    def _require_distinct_keys(self, instances: list[Any]) -> None:
        key = conflict_key_of(self.table.model)
        if not key:
            return
        seen = Counter(tuple(getattr(instance, name) for name in key) for instance in instances)
        repeated = [values for values, count in seen.items() if count > 1]
        if repeated:
            named = dict(zip(key, repeated[0], strict=True))
            raise InvalidValue(
                f"seed_rows: {self.table.model.__name__} repeats the key {named}; "
                f"give each row its own {list(key)}, because make() repeats what it generates."
            )

    def _row_of(self, instance: Any) -> tuple[Any, ...]:
        return tuple(
            self._lake_value(column, getattr(instance, column.name)) for column in self.table.schema
        )

    def _is_json_text(self, column: ColumnSchema) -> bool:
        return self._fields[column.name].kind in _JSON_KINDS and column.dtype is LoomDtype.UTF8

    def _lake_value(self, column: ColumnSchema, value: Any) -> Any:
        if value is None:
            return None
        if self._is_json_text(column):
            return msgspec.json.encode(to_column_value(value, json=True)).decode()
        stored = to_column_value(value)
        return str(stored) if column.dtype is LoomDtype.UTF8 else stored

    def _field_values(self, record: dict[str, Any]) -> dict[str, Any]:
        return {
            column.name: msgspec.json.decode(record[column.name])
            if record[column.name] is not None and self._is_json_text(column)
            else record[column.name]
            for column in self.table.schema
        }
