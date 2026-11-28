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
    def of(cls, column: ColumnSchema, table: ModelTable) -> _RowColumn:
        return cls(
            column.name,
            frame_type_to_polars(column.dtype),
            column.dtype is LoomDtype.UTF8,
            column.name in table.json_text_columns,
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
    values: tuple[tuple[Any, ...], ...]


@dataclass(frozen=True, slots=True)
class RowDifference:
    """The rows two lists of model instances do not share, compared as a multiset by value.

    Args:
        missing: The expected instances the actual rows lack.
        unexpected: The actual instances that were not expected.
    """

    missing: tuple[Any, ...]
    unexpected: tuple[Any, ...]

    @classmethod
    def between(cls, expected: Sequence[Any], actual: Sequence[Any]) -> RowDifference:
        """The difference between *expected* and *actual* instances, in any order."""
        wanted = [cls._hashable(row) for row in expected]
        found = [cls._hashable(row) for row in actual]
        rows = dict(zip([*found, *wanted], [*actual, *expected], strict=True))
        missing = (Counter(wanted) - Counter(found)).elements()
        unexpected = (Counter(found) - Counter(wanted)).elements()
        return cls(tuple(rows[key] for key in missing), tuple(rows[key] for key in unexpected))

    @property
    def is_empty(self) -> bool:
        """Whether the rows are the same."""
        return not (self.missing or self.unexpected)

    @staticmethod
    def _hashable(value: Any) -> Any:
        """A hashable stand-in for *value* that is equal exactly when the values are."""
        match value:
            case msgspec.Struct():
                return (
                    type(value),
                    tuple(RowDifference._hashable(item) for item in msgspec.structs.astuple(value)),
                )
            case list() | tuple():
                return tuple(RowDifference._hashable(item) for item in value)
            case dict():
                return frozenset(
                    (key, RowDifference._hashable(item)) for key, item in value.items()
                )
            case set() | frozenset():
                return frozenset(RowDifference._hashable(item) for item in value)
            case _:
                return value


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
        self._columns = tuple(_RowColumn.of(column, self.table) for column in self.table.schema)

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

    def seed_values(self, instances: list[Any]) -> tuple[tuple[Any, ...], ...]:
        """The row tuples of *instances*, in column order.

        Raises:
            InvalidValue: Two instances share a conflict key.
        """
        self._require_distinct_keys(instances)
        return tuple(
            tuple(column.to_lake(getattr(instance, column.name)) for column in self._columns)
            for instance in instances
        )

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
