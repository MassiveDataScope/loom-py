"""Translation of a model, or any annotated class, to the frame types of the lake.

Core inference settles each field's column type once; this module owns the
lake rules: what Delta can store, and what it refuses. Every annotation goes
through it, so a model, a msgspec struct, a dataclass and a plain annotated class
with the same annotations give the same schema.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from typing import Any, Final, TypeAlias, cast, get_origin

from loom.core.errors import InvalidType, InvalidValue
from loom.core.model.class_store import stored_on_class
from loom.core.model.inference import (
    Annotation,
    JsonShape,
    ListShape,
    Opaque,
    ResolvedField,
    StructShape,
)
from loom.core.model.introspection import get_column_fields, get_table_name
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


@dataclass(frozen=True, slots=True)
class _UnstorableKind:
    """Why the lake has no type for a kind of column, and what to store instead."""

    problem: str
    fix: str


_UNSTORABLE_KINDS: Final[Mapping[ColumnKind, _UnstorableKind]] = {
    ColumnKind.TIME: _UnstorableKind(
        "is a Time, which Delta has no type for", "store it as a Text or a DateTime"
    ),
    ColumnKind.POSTGRES_TSVECTOR: _UnstorableKind(
        "is a tsvector, a search index with no lake type",
        "store the text in a Text column and index it in Postgres",
    ),
}

SchemaContract: TypeAlias = tuple[ColumnSchema, ...] | type[Any]
"""Contract accepted by ``with_schema``: a column-schema tuple or an annotated class."""

JsonContract: TypeAlias = FrameType | type[Any]
"""Contract accepted by ``parse_json``: a :data:`FrameType`, an annotated class or ``list[X]``."""


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
        return tuple(ColumnSchema(f.name, self.frame_type(f)) for f in self.fields_of(contract))

    def struct_of(self, contract: type) -> StructType:
        """The struct of an annotated class, its fields named by encode name."""
        return self._struct(self.fields_of(contract))

    @classmethod
    def annotation_type(cls, annotation: Any) -> FrameType:
        """The frame type a Python annotation declares; ``X | None`` maps as ``X``.

        Args:
            annotation: A scalar type, ``list[X]``, a record (msgspec struct or
                dataclass, mapped to a struct) or a frame type (returned as-is).

        Raises:
            LakeRefusal: The lake has no type for *annotation*, or it is an unresolved
                string annotation such as a raw ``__annotations__`` value under
                ``from __future__ import annotations``.

        Example::

            >>> LakeTranslator.annotation_type(list[int] | None)
            ListType(inner=<LoomDtype.INT64: 'Int64'>)
        """
        if isinstance(annotation, FrameType):
            return annotation
        if isinstance(annotation, str):
            raise LakeRefusal(
                f"annotation.value is an unresolved string annotation {annotation!r}; "
                "pass the resolved hint, e.g. typing.get_type_hints(cls)[name].",
                "value",
            )
        return cls("annotation").frame_type(Annotation(annotation).resolved)

    @classmethod
    def json_type(cls, contract: Any) -> FrameType:
        """The frame type a JSON column decodes into.

        Args:
            contract: A frame type (returned as-is), an annotated class (a struct)
                or a ``list[X]`` alias (a list).

        Raises:
            InvalidType: *contract* is none of those.
        """
        if isinstance(contract, FrameType):
            return contract
        if get_origin(contract) is list:
            return cls.annotation_type(contract)
        if isinstance(contract, type):
            return cls(contract.__name__).struct_of(contract)
        raise InvalidType("Expected a FrameType, an annotated class, or list[...] generic.")

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
        column_type = column_type.portable() or column_type
        kind = column_type.kind
        match kind:
            case ColumnKind.NUMERIC:
                return self._decimal(field, column_type)
            case ColumnKind.DATETIME:
                return DatetimeType("us", "UTC" if column_type.option("timezone", True) else None)
            case _ if kind in _UNSTORABLE_KINDS:
                unstorable = _UNSTORABLE_KINDS[kind]
                raise self._refusal(field, unstorable.problem, unstorable.fix)
            case ColumnKind.POSTGRES_ARRAY:
                return self._list(field, cast(ListShape, field.shape))
            case _ if kind.is_json:
                return self._json_shape(field)
            case _:
                return _SIMPLE_TYPES[kind]

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
        shape: JsonShape | None = field.shape
        match shape:
            case StructShape(fields=fields):
                return self._inside(field)._struct(fields)
            case ListShape():
                return self._list(field, shape)
            case Opaque() | None:
                return LoomDtype.UTF8

    def _list(self, field: ResolvedField, shape: ListShape) -> ListType:
        return ListType(self._inside(field).frame_type(shape.item))

    def _inside(self, field: ResolvedField) -> LakeTranslator:
        return replace(self, path=(*self.path, field.name))

    def _struct(self, fields: Iterable[ResolvedField]) -> StructType:
        return StructType(tuple(StructField(f.encode_name, self.frame_type(f)) for f in fields))

    def fields_of(self, contract: type) -> tuple[ResolvedField, ...]:
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
class ModelTable:
    """A model as a lake table: its logical name and the frame type of each column.

    Args:
        model: The model the table is translated from.
        name: The logical table name, the model's ``__tablename__``.
        schema: The columns, in declaration order.
        json_text_columns: The JSON columns the lake stores as text, which are encoded
            and decoded by hand.
        column_types: The frame type of each column, by column name.
        nested_columns: The columns stored as a struct or a list, which JSON text is
            decoded into.
        struct_type: The columns as one struct, to translate to an engine schema in one step.
    """

    model: type
    name: str
    schema: tuple[ColumnSchema, ...]
    json_text_columns: frozenset[str]
    column_types: Mapping[str, FrameType]
    nested_columns: tuple[ColumnSchema, ...]
    struct_type: StructType

    @staticmethod
    @stored_on_class("__loom_model_table__")
    def from_model(model: type) -> ModelTable:
        """The lake table of *model*, translated once per class.

        Raises:
            InvalidType: *model* declares no ``__tablename__``.
            LakeRefusal: A column has a type the lake refuses.
        """
        translator = LakeTranslator(model.__name__)
        schema = translator.schema_of(model)
        return ModelTable(
            model=model,
            name=get_table_name(model),
            schema=schema,
            json_text_columns=frozenset(
                field.name for field in translator.fields_of(model) if field.stores_json_text
            ),
            column_types={column.name: column.dtype for column in schema},
            nested_columns=tuple(
                column for column in schema if isinstance(column.dtype, StructType | ListType)
            ),
            struct_type=StructType.of_columns(schema),
        )

    @property
    def column_names(self) -> tuple[str, ...]:
        """The names of the columns, in declaration order."""
        return tuple(self.column_types)

    def unknown_columns(self, names: Iterable[str]) -> tuple[str, ...]:
        """The *names* that are not columns of the model, in the order given."""
        return tuple(name for name in names if name not in self.column_types)


def resolve_schema(contract: SchemaContract) -> tuple[ColumnSchema, ...]:
    """Convert a schema contract to a ``tuple[ColumnSchema, ...]``.

    Used by ``FromTable.with_schema`` and ``FromFile.with_schema``, so callers can pass
    either an explicit tuple or an annotated class.

    Raises:
        InvalidType: If *contract* is not a supported schema contract form.
    """
    if isinstance(contract, tuple):
        return contract
    if isinstance(contract, type):
        return LakeTranslator(contract.__name__).schema_of(contract)
    raise InvalidType(
        f"Expected tuple[ColumnSchema, ...] or an annotated class, got {type(contract).__name__!r}."
    )
