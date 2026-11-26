from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum, StrEnum
from functools import cache
from types import UnionType
from typing import (
    Annotated,
    Any,
    ClassVar,
    Final,
    Literal,
    Union,
    get_args,
    get_origin,
    get_type_hints,
    is_typeddict,
)
from uuid import UUID

import msgspec

from loom.core.model.field import ColumnFieldSpec, Field
from loom.core.model.types import (
    JSON,
    BigInteger,
    Boolean,
    Bytes,
    ColumnKind,
    ColumnType,
    Date,
    DateTime,
    Float,
    JsonMarker,
    Numeric,
    String,
    Text,
    Time,
    Uuid,
)


class Outcome(StrEnum):
    """How the column type of a field was settled."""

    DECLARED = "declared"
    INFERRED = "inferred"
    UNDECLARED = "undeclared"


@dataclass(frozen=True, slots=True)
class Opaque:
    """JSON whose inner structure is not described."""


@dataclass(frozen=True, slots=True)
class StructShape:
    """JSON that is an object with these fields."""

    fields: tuple[ResolvedField, ...]


@dataclass(frozen=True, slots=True)
class ListShape:
    """JSON that is a list of items of one resolved type."""

    item: ResolvedField


JsonShape = Opaque | StructShape | ListShape


@dataclass(frozen=True, slots=True)
class ResolvedField:
    """The column type of one field, settled once from its annotation.

    ``column_type`` is ``None`` when the field is undeclared, except for a
    ``Decimal``, which keeps an unbounded ``Numeric`` as a placeholder; its
    ``reason`` says what is missing. ``shape`` describes the JSON stored in a
    ``JSON`` column and is ``None`` for any other column. ``nullable`` is
    whether the annotation admits ``None``; the field's own constraints are
    merged by the caller.
    """

    name: str
    encode_name: str
    column_type: ColumnType | None
    nullable: bool
    outcome: Outcome
    reason: str | None = None
    shape: JsonShape | None = None

    @property
    def is_undeclared(self) -> bool:
        """Whether no column type could be settled and the field needs a declaration."""
        return self.outcome is Outcome.UNDECLARED


@dataclass(frozen=True, slots=True)
class FieldSource:
    """One annotated field of a class, as its reader found it."""

    name: str
    encode_name: str
    annotation: Any
    default: Any


def union_members(hint: Any) -> tuple[Any, ...] | None:
    """Return the members of a Union annotation, normalising ``X | Y`` and ``Union[X, Y]``.

    Returns:
        The declared union members, or ``None`` when *hint* is not a Union.
    """
    if isinstance(hint, UnionType):
        return hint.__args__
    if getattr(hint, "__origin__", None) is Union:
        return getattr(hint, "__args__", ())
    return None


def union_inner_args(hint: Any) -> tuple[Any, ...] | None:
    """Return the non-``None``, non-``UnsetType`` members of a Union annotation.

    Returns:
        The meaningful union members, or ``None`` when *hint* is not a Union.
    """
    members = union_members(hint)
    if members is None:
        return None
    return tuple(a for a in members if a is not type(None) and a is not msgspec.UnsetType)


def unwrap_optional(annotation: Any) -> Any:
    """Return the single type of a union once ``None`` and ``UnsetType`` are dropped.

    ``UNSET`` means "not provided", not another type, so ``X | UnsetType``
    and ``X | None | UnsetType`` unwrap to ``X``; whether ``None`` is admitted
    is decided apart, by :func:`admits_none`.
    """
    args = union_inner_args(annotation)
    if args is None:
        return annotation
    if len(args) == 1:
        return args[0]
    return annotation


def admits_none(annotation: Any) -> bool:
    """Whether ``annotation`` accepts ``None``, through ``Annotated``, ``Literal`` and unions."""
    origin = get_origin(annotation)
    if origin is Annotated:
        return admits_none(get_args(annotation)[0])
    if origin is Literal:
        return None in get_args(annotation)
    members = union_members(annotation)
    if members is None:
        return annotation is type(None)
    return any(admits_none(member) for member in members)


def column_base(annotation: Any) -> tuple[Any, tuple[Any, ...]]:
    """Strip ``Optional`` and ``Annotated`` layers, in either order.

    Returns:
        The inner annotation and the metadata of the ``Annotated`` layers.
    """
    metadata: tuple[Any, ...] = ()
    while True:
        unwrapped = unwrap_optional(annotation)
        if get_origin(unwrapped) is Annotated:
            metadata += unwrapped.__metadata__
            unwrapped = get_args(unwrapped)[0]
        if unwrapped is annotation:
            return annotation, metadata
        annotation = unwrapped


def origin_type(annotation: Any) -> type[Any]:
    """Return the class an annotation stands for, ``object`` when it is not one."""
    origin = getattr(annotation, "__origin__", None)
    if origin is not None:
        args = getattr(annotation, "__args__", ())
        if args:
            value = args[0]
            if isinstance(value, type):
                return value
            return object
    raw = unwrap_optional(annotation)
    origin = get_origin(raw)
    if origin is not None:
        if isinstance(origin, type):
            return origin
        return object
    if isinstance(raw, type):
        return raw
    return object


def field_declaration(
    annotation: Any, declared: ColumnFieldSpec | None = None
) -> tuple[ColumnType | None, Field]:
    """The column type and ``Field`` a field declares, before any inference.

    A ``ColumnField`` supplies the ``Field``, and its column type wins over the
    one in ``Annotated``; without one, ``Annotated`` supplies both.
    """
    column_type: ColumnType | None = None
    field = Field()
    _, metadata = column_base(annotation)
    for entry in metadata:
        if isinstance(entry, ColumnType):
            column_type = entry
        elif isinstance(entry, Field):
            field = entry
    if declared is None:
        return column_type, field
    return declared.column_type or column_type, declared.field


def _struct_sources(cls: type) -> tuple[FieldSource, ...]:
    return tuple(
        FieldSource(info.name, info.encode_name, info.type, info.default)
        for info in msgspec.structs.fields(cls)
    )


def _hinted_sources(cls: type) -> tuple[FieldSource, ...]:
    hints = get_type_hints(cls, include_extras=True)
    return tuple(
        FieldSource(name, name, annotation, msgspec.NODEFAULT)
        for name, annotation in hints.items()
        if annotation is not ClassVar and get_origin(annotation) is not ClassVar
    )


@cache
def read_fields(cls: type) -> tuple[FieldSource, ...]:
    """The annotated fields of *cls*, read once per class.

    A msgspec struct reads ``msgspec.structs.fields``, which keeps ``Annotated``
    and ``encode_name``; any other annotated class reads its type hints without
    ``ClassVar``, and its encode name is its name.
    """
    reader = _struct_sources if issubclass(cls, msgspec.Struct) else _hinted_sources
    return reader(cls)


@dataclass(frozen=True, slots=True)
class _Undeclared:
    reason: str
    placeholder: ColumnType | None = None


_RULES: Final[Mapping[type, ColumnType | _Undeclared]] = {
    int: BigInteger,
    float: Float,
    bool: Boolean,
    bytes: Bytes,
    bytearray: Bytes,
    str: String(),
    datetime: DateTime(tz=True),
    date: Date,
    time: Time,
    UUID: Uuid,
    Decimal: _Undeclared("a Decimal", placeholder=Numeric()),
    timedelta: _Undeclared("a timedelta"),
}

_CONTAINERS: Final = (list, tuple, set, frozenset, dict)


def _is_record(cls: Any) -> bool:
    return isinstance(cls, type) and bool(read_fields(cls))


def _scalar_rule(cls: type) -> ColumnType | _Undeclared | None:
    if not issubclass(cls, Enum):
        return _RULES.get(cls)
    return next((_RULES[mixin] for mixin in cls.__mro__ if mixin in _RULES), None)


@dataclass(frozen=True, slots=True)
class _Resolver:
    """Resolves fields below the classes already being expanded in ``path``.

    A class met again on its own path is a recursion and resolves to
    :class:`Opaque` instead of expanding without end.
    """

    path: frozenset[type]

    def resolve(
        self, source: FieldSource, declared: ColumnFieldSpec | None = None
    ) -> ResolvedField:
        annotated_type, field = field_declaration(source.annotation, declared)
        base, metadata = column_base(source.annotation)
        if annotated_type is not None:
            return self._settled(source, base, annotated_type, Outcome.DECLARED)
        found = self._infer(base, metadata)
        if isinstance(found, _Undeclared):
            return self._settled(source, base, found.placeholder, Outcome.UNDECLARED, found.reason)
        if field.length is not None and found == String():
            found = String(field.length)
        return self._settled(source, base, found, Outcome.INFERRED)

    def _settled(
        self,
        source: FieldSource,
        base: Any,
        column_type: ColumnType | None,
        outcome: Outcome,
        reason: str | None = None,
    ) -> ResolvedField:
        return ResolvedField(
            name=source.name,
            encode_name=source.encode_name,
            column_type=column_type,
            nullable=admits_none(source.annotation),
            outcome=outcome,
            reason=reason,
            shape=None if column_type is None else self._shape(column_type, base),
        )

    def _infer(self, base: Any, metadata: tuple[Any, ...]) -> ColumnType | _Undeclared:
        if any(isinstance(entry, JsonMarker) for entry in metadata):
            return Text
        if base is Any or get_origin(base) is Literal:
            return _Undeclared("Any" if base is Any else "a Literal")
        members = union_inner_args(base)
        if members is not None:
            return self._infer_union(members)
        cls = get_origin(base) or base
        if not isinstance(cls, type):
            return _Undeclared(f"the annotation {base!r}")
        rule = _scalar_rule(cls)
        if rule is not None:
            return rule
        if issubclass(cls, Enum):
            return _Undeclared("a plain Enum")
        if issubclass(cls, _CONTAINERS) or _is_record(cls) or is_typeddict(cls):
            return JSON
        return _Undeclared(f"{cls.__name__}, a class that is not a record")

    def _infer_union(self, members: tuple[Any, ...]) -> ColumnType | _Undeclared:
        found = {self._infer(*column_base(member)) for member in members}
        if found == {JSON}:
            return JSON
        return _Undeclared("a union of types that are not all JSON")

    def _shape(self, column_type: ColumnType, base: Any) -> JsonShape | None:
        if column_type.type_name is ColumnKind.POSTGRES_ARRAY:
            return ListShape(self._declared_item(column_type.args[0]))
        if column_type.portable() != JSON:
            return None
        return self._annotation_shape(base)

    def _annotation_shape(self, base: Any) -> JsonShape:
        if union_inner_args(base) is not None:
            return Opaque()
        origin = get_origin(base)
        if origin is list and get_args(base):
            item = FieldSource("item", "item", get_args(base)[0], msgspec.NODEFAULT)
            return ListShape(self.resolve(item))
        if _is_record(base) and base not in self.path:
            below = _Resolver(self.path | {base})
            return StructShape(tuple(below.resolve(source) for source in read_fields(base)))
        return Opaque()

    @staticmethod
    def _declared_item(item: ColumnType) -> ResolvedField:
        return ResolvedField("item", "item", item, nullable=True, outcome=Outcome.DECLARED)


def resolve_fields(
    cls: type,
    declared: Mapping[str, ColumnFieldSpec] | None = None,
    *,
    exclude: Collection[str] = (),
) -> tuple[ResolvedField, ...]:
    """Resolve the column type of every annotated field of *cls*, in declaration order.

    Declared markers win over inference. It never raises on an annotation it
    cannot map: the field comes back undeclared with its reason. Only an
    annotation naming something that cannot be imported fails, as it does for
    ``typing.get_type_hints``.

    Args:
        cls: A msgspec struct, dataclass or plain annotated class.
        declared: ``ColumnField`` declarations by field name.
        exclude: Field names that are not columns, such as relations.
    """
    declared = declared or {}
    resolver = _Resolver(frozenset({cls}))
    return tuple(
        resolver.resolve(source, declared.get(source.name))
        for source in read_fields(cls)
        if source.name not in exclude
    )


def resolve_annotation(annotation: Any) -> ResolvedField:
    """Resolve the column type of one bare annotation, as a field named ``value``.

    Like :func:`resolve_fields`, it never raises on an annotation it cannot map.
    """
    source = FieldSource("value", "value", annotation, msgspec.NODEFAULT)
    return _Resolver(frozenset()).resolve(source)
