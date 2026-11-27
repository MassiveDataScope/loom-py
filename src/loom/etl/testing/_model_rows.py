"""Model instances as the rows of their lake table, in both directions."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

import msgspec
import polars as pl

from loom.core.errors import InvalidValue
from loom.core.model.convert import to_column_value, to_struct
from loom.core.model.introspection import ModelKeys
from loom.etl.engines.polars._dtype import frame_type_to_polars
from loom.etl.schema import ColumnSchema, LoomDtype, ModelTable


@dataclass(frozen=True, slots=True)
class _RowColumn:
    """One column of a model's rows: how its values go to the lake types and back."""

    name: str
    polars_dtype: pl.DataType
    is_text: bool
    is_json_text: bool

    @classmethod
    def of(cls, column: ColumnSchema, *, stores_json: bool) -> _RowColumn:
        is_text = column.dtype is LoomDtype.UTF8
        return cls(
            column.name, frame_type_to_polars(column.dtype), is_text, stores_json and is_text
        )

    def to_lake(self, value: Any) -> Any:
        if value is None:
            return None
        if self.is_json_text:
            return msgspec.json.encode(to_column_value(value, json=True)).decode()
        stored = to_column_value(value)
        return str(stored) if self.is_text else stored

    def from_lake(self, value: Any) -> Any:
        if value is None or not self.is_json_text:
            return value
        return msgspec.json.decode(value)


@dataclass(frozen=True, slots=True)
class ModelSeed:
    """The rows to seed for one model: its table and the row tuples in column order."""

    table: ModelTable
    values: list[tuple[Any, ...]]


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
        self._columns = tuple(
            _RowColumn.of(column, stores_json=column.name in self.table.json_text_columns)
            for column in self.table.schema
        )

    @classmethod
    def seeds_of(cls, instances: Iterable[Any]) -> list[ModelSeed]:
        """The seed of each model among *instances*, in order of first appearance.

        Raises:
            InvalidValue: Two instances of a model share a conflict key.
        """
        grouped: dict[type, list[Any]] = {}
        for instance in instances:
            grouped.setdefault(type(instance), []).append(instance)
        seeds = []
        for model, rows in grouped.items():
            model_rows = cls(model)
            seeds.append(ModelSeed(model_rows.table, model_rows.seed_values(rows)))
        return seeds

    @staticmethod
    def differences(expected: Sequence[Any], actual: Sequence[Any]) -> tuple[list[Any], list[Any]]:
        """The *expected* instances *actual* lacks and the *actual* ones not expected.

        Rows are compared as a multiset, by value, in any order.
        """
        wanted = [ModelRows._frozen(row) for row in expected]
        found = [ModelRows._frozen(row) for row in actual]
        rows = dict(zip([*found, *wanted], [*actual, *expected], strict=True))
        missing = (Counter(wanted) - Counter(found)).elements()
        unexpected = (Counter(found) - Counter(wanted)).elements()
        return [rows[key] for key in missing], [rows[key] for key in unexpected]

    def seed_values(self, instances: list[Any]) -> list[tuple[Any, ...]]:
        """The row tuples of *instances*, in column order.

        Raises:
            InvalidValue: Two instances share a conflict key.
        """
        self._require_distinct_keys(instances)
        return [
            tuple(column.to_lake(getattr(instance, column.name)) for column in self._columns)
            for instance in instances
        ]

    def instances_in(self, frame: pl.DataFrame) -> list[Any]:
        """The instances of the rows of *frame*, cast to the lake types of the model.

        Raises:
            InvalidValue: *frame* lacks a column of the model.
        """
        present = set(frame.columns)
        absent = [name for name in self.table.column_names if name not in present]
        if absent:
            raise InvalidValue(
                f"{self.table.model.__name__} rows need the columns {absent}, "
                f"which the frame lacks; it has {frame.columns}."
            )
        cast = frame.select(
            pl.col(column.name).cast(column.polars_dtype) for column in self._columns
        )
        return [
            to_struct(
                self.table.model,
                {
                    column.name: column.from_lake(value)
                    for column, value in zip(self._columns, row, strict=True)
                },
            )
            for row in cast.iter_rows()
        ]

    def _require_distinct_keys(self, instances: list[Any]) -> None:
        key = ModelKeys.of(self.table.model).conflict
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

    @staticmethod
    def _frozen(value: Any) -> Any:
        """A hashable stand-in for *value* that is equal exactly when the values are."""
        match value:
            case msgspec.Struct():
                return (
                    type(value),
                    tuple(ModelRows._frozen(item) for item in msgspec.structs.astuple(value)),
                )
            case list() | tuple():
                return tuple(ModelRows._frozen(item) for item in value)
            case dict():
                return frozenset((key, ModelRows._frozen(item)) for key, item in value.items())
            case set() | frozenset():
                return frozenset(ModelRows._frozen(item) for item in value)
            case _:
                return value
