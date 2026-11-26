"""Translation of a model, or any annotated class, to the frame types of the lake.

Core inference settles each field's column type once; this module owns the
lake rules: what Delta can store, and what it refuses.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from functools import cache
from typing import Final, NoReturn

from loom.core.errors import InvalidType, InvalidValue
from loom.core.model.inference import (
    JsonShape,
    ListShape,
    ResolvedField,
    StructShape,
)
from loom.core.model.introspection import (
    business_key_of,
    conflict_key_of,
    get_column_fields,
    get_table_name,
)
from loom.core.model.types import ColumnKind, ColumnType
from loom.etl.schema._schema import (
    ColumnSchema,
    DatetimeType,
    DecimalType,
    FrameType,
    ListType,
    LoomDtype,
    StructField,
    StructType,
)

_MAX_DECIMAL_PRECISION: Final = 38

_Rule = Callable[["LakeTranslator", ResolvedField, ColumnType], FrameType]


@dataclass(frozen=True, slots=True)
class LakeTranslator:
    """Translates resolved fields to lake frame types, refusing what the lake cannot hold.

    Args:
        owner: Name of the class or annotation the fields belong to, for messages.
        path: Names of the enclosing fields, when translating inside a JSON shape.
    """

    owner: str
    path: tuple[str, ...] = ()

    def schema_of(self, contract: type) -> tuple[ColumnSchema, ...]:
        """The columns of an annotated class, named by field name."""
        return tuple(ColumnSchema(f.name, self.frame_type(f)) for f in self._fields_of(contract))

    def struct_of(self, contract: type) -> StructType:
        """The struct of an annotated class, its fields named by encode name."""
        return self._struct(self._fields_of(contract))

    def frame_type(self, field: ResolvedField) -> FrameType:
        """The lake type of *field*.

        Raises:
            InvalidValue: The lake cannot store the field; the message names the
                class, the field and the fix.
        """
        column_type = field.column_type
        if column_type is None:
            raise self.refusal(
                field,
                f"has no column type ({field.reason})",
                "declare it with Annotated[<type>, <column type>]",
            )
        return _RULES[column_type.type_name](self, field, column_type)

    def refusal(self, field: ResolvedField, problem: str, fix: str) -> InvalidValue:
        """The error that refuses *field* for *problem*, pointing at *fix*."""
        location = ".".join((self.owner, *self.path, field.name))
        return InvalidValue(f"{location} {problem}; {fix}.")

    def json_shape(self, field: ResolvedField) -> FrameType:
        """The lake type of a JSON field: struct, list, or text when its shape is opaque."""
        inside = replace(self, path=(*self.path, field.name))
        shape: JsonShape | None = field.shape
        match shape:
            case StructShape(fields=fields):
                return inside._struct(fields)
            case ListShape(item=item):
                return ListType(inside.frame_type(item))
            case _:
                return LoomDtype.UTF8

    def _struct(self, fields: Iterable[ResolvedField]) -> StructType:
        return StructType(tuple(StructField(f.encode_name, self.frame_type(f)) for f in fields))

    def _fields_of(self, contract: type) -> tuple[ResolvedField, ...]:
        try:
            columns = get_column_fields(contract)
        except (TypeError, NameError) as exc:
            raise InvalidType(f"Cannot introspect annotations of {contract.__name__!r}") from exc
        if not columns:
            raise InvalidType(
                f"Class {contract.__name__!r} has no type annotations. "
                "Add field annotations or pass an explicit tuple[ColumnSchema, ...] instead."
            )
        return tuple(info.resolved for info in columns.values())


def _always(frame_type: FrameType) -> _Rule:
    return lambda translator, field, column_type: frame_type


def _refused(problem: str, fix: str) -> _Rule:
    def refuse(
        translator: LakeTranslator, field: ResolvedField, column_type: ColumnType
    ) -> NoReturn:
        raise translator.refusal(field, problem, fix)

    return refuse


def _decimal(
    translator: LakeTranslator, field: ResolvedField, column_type: ColumnType
) -> FrameType:
    precision = column_type.option("precision")
    if precision is None:
        raise translator.refusal(
            field,
            "is an unbounded Numeric, which the lake cannot store",
            "declare Annotated[Decimal, Numeric(precision=p, scale=s)] with p <= 38",
        )
    if precision > _MAX_DECIMAL_PRECISION:
        raise translator.refusal(
            field,
            f"has Numeric precision {precision}, above the lake maximum of 38",
            "declare a precision of 38 or less",
        )
    return DecimalType(precision, column_type.option("scale") or 0)


def _datetime(
    translator: LakeTranslator, field: ResolvedField, column_type: ColumnType
) -> FrameType:
    return DatetimeType("us", "UTC" if column_type.option("timezone", True) else None)


def _json(translator: LakeTranslator, field: ResolvedField, column_type: ColumnType) -> FrameType:
    return translator.json_shape(field)


def _portable(
    translator: LakeTranslator, field: ResolvedField, column_type: ColumnType
) -> FrameType:
    return translator.frame_type(replace(field, column_type=column_type.portable()))


_RULES: Final[Mapping[ColumnKind, _Rule]] = {
    ColumnKind.STRING: _always(LoomDtype.UTF8),
    ColumnKind.TEXT: _always(LoomDtype.UTF8),
    ColumnKind.UUID: _always(LoomDtype.UTF8),
    ColumnKind.TINY_INTEGER: _always(LoomDtype.INT8),
    ColumnKind.SMALL_INTEGER: _always(LoomDtype.INT16),
    ColumnKind.INTEGER: _always(LoomDtype.INT32),
    ColumnKind.BIG_INTEGER: _always(LoomDtype.INT64),
    ColumnKind.FLOAT: _always(LoomDtype.FLOAT64),
    ColumnKind.BOOLEAN: _always(LoomDtype.BOOLEAN),
    ColumnKind.BYTES: _always(LoomDtype.BINARY),
    ColumnKind.DATE: _always(LoomDtype.DATE),
    ColumnKind.NUMERIC: _decimal,
    ColumnKind.DATETIME: _datetime,
    ColumnKind.JSON: _json,
    ColumnKind.POSTGRES_ARRAY: _json,
    ColumnKind.POSTGRES_JSONB: _portable,
    ColumnKind.POSTGRES_UUID: _portable,
    ColumnKind.POSTGRES_INET: _portable,
    ColumnKind.TIME: _refused(
        "is a Time, which Delta has no type for", "store it as a Text or a DateTime"
    ),
    ColumnKind.POSTGRES_TSVECTOR: _refused(
        "is a tsvector, a search index with no lake type",
        "store the text in a Text column and index it in Postgres",
    ),
}


@dataclass(frozen=True, slots=True)
class KeyProblem:
    """Why a model's conflict key cannot come from a frame: the field and the reason."""

    field: str
    reason: str


@dataclass(frozen=True, slots=True)
class ModelTable:
    """A model as a lake table: its logical name and the frame type of each column."""

    model: type
    name: str
    schema: tuple[ColumnSchema, ...]

    @classmethod
    @cache
    def from_model(cls, model: type) -> ModelTable:
        """The lake table of *model*, translated once per class.

        Raises:
            InvalidType: *model* declares no ``__tablename__``.
            InvalidValue: A column has a type the lake refuses.
        """
        name = get_table_name(model)
        return cls(model, name, LakeTranslator(model.__name__).schema_of(model))

    @property
    def column_names(self) -> tuple[str, ...]:
        """The names of the columns, in declaration order."""
        return tuple(column.name for column in self.schema)

    def column_type(self, name: str) -> FrameType | None:
        """The frame type of column *name*, ``None`` when the model has no such column."""
        return next((column.dtype for column in self.schema if column.name == name), None)

    def unknown_columns(self, names: Iterable[str]) -> tuple[str, ...]:
        """The *names* that are not columns of the model, in the order given."""
        return tuple(name for name in names if name not in self.column_names)

    def key_problem(self) -> KeyProblem | None:
        """Why the conflict key cannot come from a frame, ``None`` when it can.

        A business key always comes from the frame. A primary key does not when the
        store fills it: autoincrement, derived from a business key, or server default.
        """
        key = conflict_key_of(self.model)
        if not key:
            return KeyProblem("keys", "declares neither a business key nor a primary key")
        if business_key_of(self.model) is not None:
            return None
        columns = get_column_fields(self.model)
        for name in key:
            declaration = columns[name].field
            if declaration.autoincrement:
                return KeyProblem(name, f"primary key {name!r} is autoincrement")
            if declaration.derived is not None:
                return KeyProblem(name, f"primary key {name!r} is derived by the store")
            if declaration.server_default is not None:
                return KeyProblem(name, f"primary key {name!r} is filled by the server")
        return None

    def frame_keys(self) -> tuple[str, ...]:
        """The conflict key of the model when the frame can supply it, else ``()``."""
        return conflict_key_of(self.model) if self.key_problem() is None else ()
