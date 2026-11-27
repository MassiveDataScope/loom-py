from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum, StrEnum
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

from loom.core.model.class_store import stored_on_class
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

    ``column_type`` is ``None`` when the field is undeclared; its ``reason`` says
    what is missing. ``shape`` describes the JSON stored in a
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

    def declaration(
        self, declared: ColumnFieldSpec | None = None
    ) -> tuple[ColumnType | None, Field]:
        """The column type and ``Field`` this field declares, before any inference.

        A ``ColumnField`` supplies the ``Field``, and its column type wins over the
        one in ``Annotated``; without one, ``Annotated`` supplies both.
        """
        column_type: ColumnType | None = None
        field = Field()
        _, metadata = Annotation(self.annotation).column_base
        for entry in metadata:
            if isinstance(entry, ColumnType):
                column_type = entry
            elif isinstance(entry, Field):
                field = entry
        if declared is None:
            return column_type, field
        return declared.column_type or column_type, declared.field


@dataclass(frozen=True, slots=True)
class Annotation:
    """One type annotation, asked what it admits and what it stands for."""

    hint: Any

    @property
    def members(self) -> tuple[Any, ...] | None:
        """The members of a Union, normalising ``X | Y`` and ``Union[X, Y]``, else ``None``."""
        if isinstance(self.hint, UnionType):
            return self.hint.__args__
        if getattr(self.hint, "__origin__", None) is Union:
            return getattr(self.hint, "__args__", ())
        return None

    @property
    def inner_args(self) -> tuple[Any, ...] | None:
        """The non-``None``, non-``UnsetType`` members of a Union; ``None`` if not one."""
        members = self.members
        if members is None:
            return None
        return tuple(a for a in members if a is not type(None) and a is not msgspec.UnsetType)

    @property
    def without_optional(self) -> Any:
        """The single type of a union once ``None`` and ``UnsetType`` are dropped.

        ``UNSET`` means "not provided", not another type, so ``X | UnsetType``
        and ``X | None | UnsetType`` unwrap to ``X``; whether ``None`` is admitted
        is decided apart, by :attr:`admits_none`.
        """
        args = self.inner_args
        if args is not None and len(args) == 1:
            return args[0]
        return self.hint

    @property
    def admits_none(self) -> bool:
        """Whether the annotation accepts ``None``, through ``Annotated``, ``Literal``, unions."""
        origin = get_origin(self.hint)
        if origin is Annotated:
            return Annotation(get_args(self.hint)[0]).admits_none
        if origin is Literal:
            return None in get_args(self.hint)
        members = self.members
        if members is None:
            return self.hint is type(None)
        return any(Annotation(member).admits_none for member in members)

    @property
    def column_base(self) -> tuple[Any, tuple[Any, ...]]:
        """Strip ``Optional`` and ``Annotated`` layers, in either order.

        Returns:
            The inner annotation and the metadata of the ``Annotated`` layers.
        """
        annotation = self.hint
        metadata: tuple[Any, ...] = ()
        while True:
            unwrapped = Annotation(annotation).without_optional
            if get_origin(unwrapped) is Annotated:
                metadata += unwrapped.__metadata__
                unwrapped = get_args(unwrapped)[0]
            if unwrapped is annotation:
                return annotation, metadata
            annotation = unwrapped

    @property
    def origin_type(self) -> type[Any]:
        """The class the annotation stands for, ``object`` when it is not one."""
        origin = getattr(self.hint, "__origin__", None)
        if origin is not None:
            args = getattr(self.hint, "__args__", ())
            if args:
                value = args[0]
                if isinstance(value, type):
                    return value
                return object
        raw = self.without_optional
        origin = get_origin(raw)
        if origin is not None:
            if isinstance(origin, type):
                return origin
            return object
        if isinstance(raw, type):
            return raw
        return object

    @property
    def resolved(self) -> ResolvedField:
        """The column type of this bare annotation, as a field named ``value``.

        Like :meth:`Record.resolve`, it never raises on an annotation it cannot map.
        """
        source = FieldSource("value", "value", self.hint, msgspec.NODEFAULT)
        return _Resolver(frozenset()).resolve(source)


@dataclass(frozen=True, slots=True)
class Record:
    """A class whose annotated fields are columns or JSON members."""

    cls: Any

    @staticmethod
    @stored_on_class("__loom_fields__")
    def _read(cls: type) -> tuple[FieldSource, ...]:
        """The annotated fields of *cls*, read once per class.

        A msgspec struct reads ``msgspec.structs.fields``, which keeps ``Annotated``
        and ``encode_name``; any other annotated class reads its type hints without
        ``ClassVar``, and its encode name is its name.
        """
        if issubclass(cls, msgspec.Struct):
            return tuple(
                FieldSource(info.name, info.encode_name, info.type, info.default)
                for info in msgspec.structs.fields(cls)
            )
        hints = get_type_hints(cls, include_extras=True)
        return tuple(
            FieldSource(name, name, annotation, msgspec.NODEFAULT)
            for name, annotation in hints.items()
            if annotation is not ClassVar and get_origin(annotation) is not ClassVar
        )

    @property
    def fields(self) -> tuple[FieldSource, ...]:
        """The annotated fields; an annotation naming something unimportable raises."""
        return self._read(self.cls)

    @property
    def is_readable(self) -> bool:
        """Whether its annotations could be read, so its fields can be listed."""
        try:
            self._read(self.cls)
        except (NameError, TypeError):
            return False
        return True

    @property
    def is_record(self) -> bool:
        """Whether it is a class with at least one readable annotated field."""
        return isinstance(self.cls, type) and self.is_readable and bool(self.fields)

    def resolve(
        self,
        declared: Mapping[str, ColumnFieldSpec] | None = None,
        *,
        exclude: Collection[str] = (),
    ) -> tuple[ResolvedField, ...]:
        """Resolve the column type of every annotated field, in declaration order.

        Declared markers win over inference. It never raises on an annotation it
        cannot map: the field comes back undeclared with its reason. Only an
        annotation naming something that cannot be imported fails, as it does for
        ``typing.get_type_hints``.

        Args:
            declared: ``ColumnField`` declarations by field name.
            exclude: Field names that are not columns, such as relations.
        """
        declared = declared or {}
        resolver = _Resolver(frozenset({self.cls}))
        return tuple(
            resolver.resolve(source, declared.get(source.name))
            for source in self.fields
            if source.name not in exclude
        )


@dataclass(frozen=True, slots=True)
class _Undeclared:
    reason: str


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
    Decimal: _Undeclared("a Decimal"),
    timedelta: _Undeclared("a timedelta"),
}

_CONTAINERS: Final = (list, tuple, set, frozenset, dict)


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
        annotated_type, field = source.declaration(declared)
        base, metadata = Annotation(source.annotation).column_base
        if annotated_type is not None:
            return self._settled(source, base, annotated_type, Outcome.DECLARED)
        found = self._infer(base, metadata)
        if isinstance(found, _Undeclared):
            return self._settled(source, base, None, Outcome.UNDECLARED, found.reason)
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
            nullable=Annotation(source.annotation).admits_none,
            outcome=outcome,
            reason=reason,
            shape=None if column_type is None else self._shape(column_type, base),
        )

    def _infer(self, base: Any, metadata: tuple[Any, ...]) -> ColumnType | _Undeclared:
        if any(isinstance(entry, JsonMarker) for entry in metadata):
            return Text
        if base is Any or get_origin(base) is Literal:
            return _Undeclared("Any" if base is Any else "a Literal")
        members = Annotation(base).inner_args
        if members is not None:
            return self._infer_union(members)
        cls = get_origin(base) or base
        if not isinstance(cls, type):
            return _Undeclared(f"the annotation {base!r}")
        rule = self._scalar_rule(cls)
        if rule is not None:
            return rule
        if issubclass(cls, Enum):
            return _Undeclared("a plain Enum")
        if issubclass(cls, _CONTAINERS) or Record(cls).is_record or is_typeddict(cls):
            return JSON
        if not Record(cls).is_readable:
            return _Undeclared(f"{cls.__name__}, a class whose annotations could not be resolved")
        return _Undeclared(f"{cls.__name__}, a class that is not a record")

    @staticmethod
    def _scalar_rule(cls: type) -> ColumnType | _Undeclared | None:
        if not issubclass(cls, Enum):
            return _RULES.get(cls)
        return next((_RULES[mixin] for mixin in cls.__mro__ if mixin in _RULES), None)

    def _infer_union(self, members: tuple[Any, ...]) -> ColumnType | _Undeclared:
        found = {self._infer(*Annotation(member).column_base) for member in members}
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
        if Annotation(base).inner_args is not None:
            return Opaque()
        origin = get_origin(base)
        if origin is list and get_args(base):
            item = FieldSource("item", "item", get_args(base)[0], msgspec.NODEFAULT)
            return ListShape(self.resolve(item))
        if Record(base).is_record and base not in self.path:
            below = _Resolver(self.path | {base})
            return StructShape(tuple(below.resolve(source) for source in Record(base).fields))
        return Opaque()

    @staticmethod
    def _declared_item(item: ColumnType) -> ResolvedField:
        return ResolvedField("item", "item", item, nullable=True, outcome=Outcome.DECLARED)
