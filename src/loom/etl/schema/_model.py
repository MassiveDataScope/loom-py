"""Translation of a model, or any annotated class, to the frame types of the lake.

Core inference settles each field's column type once; this module owns the
lake rules: what Delta can store, and what it refuses.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from functools import cached_property
from typing import Final

from loom.core.errors import InvalidType, InvalidValue
from loom.core.model.class_store import stored_on_class
from loom.core.model.inference import (
    JsonShape,
    ListShape,
    ResolvedField,
    StructShape,
)
from loom.core.model.introspection import (
    ModelKeys,
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

_SIMPLE_TYPES: Final[Mapping[ColumnKind, LoomDtype]] = {
    ColumnKind.STRING: LoomDtype.UTF8,
    ColumnKind.TEXT: LoomDtype.UTF8,
    ColumnKind.UUID: LoomDtype.UTF8,
    ColumnKind.TINY_INTEGER: LoomDtype.INT8,
    ColumnKind.SMALL_INTEGER: LoomDtype.INT16,
    ColumnKind.INTEGER: LoomDtype.INT32,
    ColumnKind.BIG_INTEGER: LoomDtype.INT64,
    ColumnKind.FLOAT: LoomDtype.FLOAT64,
    ColumnKind.BOOLEAN: LoomDtype.BOOLEAN,
    ColumnKind.BYTES: LoomDtype.BINARY,
    ColumnKind.DATE: LoomDtype.DATE,
}

_REFUSED_KINDS: Final[Mapping[ColumnKind, tuple[str, str]]] = {
    ColumnKind.TIME: ("is a Time, which Delta has no type for", "store it as a Text or a DateTime"),
    ColumnKind.POSTGRES_TSVECTOR: (
        "is a tsvector, a search index with no lake type",
        "store the text in a Text column and index it in Postgres",
    ),
}


class LakeRefusal(InvalidValue):
    """A field the lake cannot store.

    Args:
        message: Names the class, the field, the problem and the fix.
        field: Path of the refused field below its class, dotted when inside JSON.

    Attributes:
        field: Path of the refused field below its class.
    """

    def __init__(self, message: str, field: str) -> None:
        super().__init__(message)
        self.field = field


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
            LakeRefusal: The lake cannot store the field; the message names the
                class, the field and the fix.
        """
        column_type = field.column_type
        if column_type is None:
            raise self._refusal(
                field,
                f"has no column type ({field.reason})",
                "declare it with Annotated[<type>, <column type>]",
            )
        kind = column_type.type_name
        match kind:
            case ColumnKind.NUMERIC:
                return self._decimal(field, column_type)
            case ColumnKind.DATETIME:
                return DatetimeType("us", "UTC" if column_type.option("timezone", True) else None)
            case _ if kind in _REFUSED_KINDS:
                raise self._refusal(field, *_REFUSED_KINDS[kind])
            case _ if kind.is_json:
                return self._json_shape(field)
            case _:
                portable = column_type.portable()
                if portable is None or portable == column_type:
                    return _SIMPLE_TYPES[kind]
                return self.frame_type(replace(field, column_type=portable))

    def _refusal(self, field: ResolvedField, problem: str, fix: str) -> LakeRefusal:
        path = ".".join((*self.path, field.name))
        return LakeRefusal(f"{self.owner}.{path} {problem}; {fix}.", path)

    def _decimal(self, field: ResolvedField, column_type: ColumnType) -> DecimalType:
        precision = column_type.option("precision")
        if precision is None:
            raise self._refusal(
                field,
                "is an unbounded Numeric, which the lake cannot store",
                "declare Annotated[Decimal, Numeric(precision=p, scale=s)] with p <= 38",
            )
        if precision > _MAX_DECIMAL_PRECISION:
            raise self._refusal(
                field,
                f"has Numeric precision {precision}, above the lake maximum of 38",
                "declare a precision of 38 or less",
            )
        scale = column_type.option("scale")
        return DecimalType(precision) if scale is None else DecimalType(precision, scale)

    def _json_shape(self, field: ResolvedField) -> FrameType:
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


@dataclass(frozen=True, slots=True)
class KeyProblem:
    """Why a model's conflict key cannot come from a frame: the field and the reason."""

    field: str
    reason: str


@dataclass(frozen=True)
class FrameKey:
    """The conflict key of a model, and whether a frame can supply it."""

    model: type

    @property
    def problem(self) -> KeyProblem | None:
        """Why the key cannot come from a frame, ``None`` when it can.

        A business key always comes from the frame. A primary key does not when the
        store fills it: autoincrement, derived from a business key, or server default.
        """
        if not ModelKeys.of(self.model).conflict:
            return KeyProblem("keys", "declares neither a business key nor a primary key")
        filled = ModelKeys.of(self.model).store_filled
        if not filled:
            return None
        name, fill = next(iter(filled.items()))
        return KeyProblem(name, f"primary key {name!r} is {fill}")

    @property
    def columns(self) -> tuple[str, ...]:
        """The conflict key of the model when the frame can supply it, else ``()``."""
        return ModelKeys.of(self.model).conflict if self.problem is None else ()


@dataclass(frozen=True)
class ModelTable:
    """A model as a lake table: its logical name and the frame type of each column."""

    model: type
    name: str
    schema: tuple[ColumnSchema, ...]

    @staticmethod
    @stored_on_class("__loom_model_table__")
    def from_model(model: type) -> ModelTable:
        """The lake table of *model*, translated once per class.

        Raises:
            InvalidType: *model* declares no ``__tablename__``.
            LakeRefusal: A column has a type the lake refuses.
        """
        schema = LakeTranslator(model.__name__).schema_of(model)
        return ModelTable(model, get_table_name(model), schema)

    @cached_property
    def column_names(self) -> tuple[str, ...]:
        """The names of the columns, in declaration order."""
        return tuple(column.name for column in self.schema)

    @cached_property
    def struct_type(self) -> StructType:
        """The columns as one struct, to translate to an engine schema in one step."""
        return StructType.of_columns(self.schema)

    @cached_property
    def column_types(self) -> Mapping[str, FrameType]:
        """The frame type of each column, by column name."""
        return {column.name: column.dtype for column in self.schema}

    @cached_property
    def json_columns(self) -> tuple[ColumnSchema, ...]:
        """The columns stored as a struct or a list, which JSON text is decoded into."""
        return tuple(
            column for column in self.schema if isinstance(column.dtype, StructType | ListType)
        )

    def unknown_columns(self, names: Iterable[str]) -> tuple[str, ...]:
        """The *names* that are not columns of the model, in the order given."""
        return tuple(name for name in names if name not in self.column_types)
