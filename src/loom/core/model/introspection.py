from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from typing import Any, cast, get_args, get_origin, get_type_hints

import msgspec

from loom.core.errors import InvalidType, InvalidValue
from loom.core.model.class_store import stored_on_class
from loom.core.model.enums import KeyFill
from loom.core.model.field import ColumnFieldSpec, Field
from loom.core.model.inference import (
    AnnotatedClass,
    Annotation,
    FieldSource,
    ResolvedField,
)
from loom.core.model.keys import (
    BusinessKey,
    DerivedKey,
    KeyMismatch,
    is_canonical_type,
)
from loom.core.model.partition import RangePartition
from loom.core.model.privilege import Privilege
from loom.core.model.projection import Projection
from loom.core.model.relation import Relation
from loom.core.model.scoped import ScopeColumn
from loom.core.model.types import ColumnKind, ColumnType
from loom.core.schema_names import sql_identifier


@dataclass(frozen=True, slots=True)
class PartialUnique:
    """A unique index over ``columns`` restricted to the rows matching ``where``."""

    rule: str
    columns: tuple[str, ...]
    where: str


@dataclass(frozen=True, slots=True)
class ColumnFieldInfo:
    """Resolved metadata for a single column field.

    ``resolved`` carries how the column type was settled; ``column_type`` is
    ``None`` while the field is undeclared.
    """

    python_type: type
    resolved: ResolvedField
    field: Field

    @classmethod
    def from_source(
        cls, annotated: AnnotatedClass, source: FieldSource, declared: ColumnFieldSpec | None
    ) -> ColumnFieldInfo:
        """The column info of *source*, a field of *annotated*, its ``Field`` completed."""
        declaration = source.declaration(declared)
        resolved = annotated.resolve_field(source, declaration)
        return cls(
            python_type=Annotation(source.annotation).origin_type,
            resolved=resolved,
            field=_with_nullable(_with_struct_default(declaration.field, source.default), resolved),
        )

    @property
    def column_type(self) -> ColumnType | None:
        """The column type, ``None`` when none could be settled."""
        return self.resolved.column_type

    @property
    def is_json(self) -> bool:
        """Whether the column holds a JSON document or a list of items."""
        kind = self.kind
        return kind is not None and kind.is_json

    @property
    def kind(self) -> ColumnKind | None:
        """The kind of the column type, ``None`` when none could be settled."""
        column_type = self.resolved.column_type
        return None if column_type is None else column_type.kind


def _collect_inherited_dict_metadata(cls: type, attr: str) -> dict[str, Any]:
    """Merge dict metadata from the full MRO (base -> subclass)."""
    merged: dict[str, Any] = {}
    for current in reversed(cls.__mro__):
        raw = getattr(current, attr, None)
        if isinstance(raw, dict):
            merged.update(raw)
    return merged


def get_column_fields(cls: type) -> dict[str, ColumnFieldInfo]:
    """Extract column fields from a model class, minus relations and projections."""
    return dict(_column_fields(cls))


@stored_on_class("__loom_column_fields__")
def _column_fields(cls: type) -> Mapping[str, ColumnFieldInfo]:
    declared = _collect_inherited_dict_metadata(cls, "__loom_columns__")
    non_columns = set(get_relations(cls)) | set(get_projections(cls))
    annotated = AnnotatedClass(cls)
    return {
        source.name: ColumnFieldInfo.from_source(annotated, source, declared.get(source.name))
        for source in annotated.fields
        if source.name not in non_columns
    }


def _with_nullable(field: Field, resolved: ResolvedField) -> Field:
    """Resolve an undeclared ``nullable`` from the annotation; primary keys stay NOT NULL."""
    if field.nullable is not None:
        return field
    nullable = not field.primary_key and resolved.nullable
    return cast(Field, replace(field, nullable=nullable))  # type: ignore[redundant-cast]


def _with_struct_default(field: Field, struct_default: Any) -> Field:
    if field.default is not msgspec.UNSET:
        return field
    if struct_default is msgspec.NODEFAULT:
        return field
    return cast(Field, replace(field, default=struct_default))  # type: ignore[redundant-cast]


def get_relations(cls: type) -> dict[str, Relation]:
    """Return relations registered by ``LoomStructMeta``."""
    return _collect_inherited_dict_metadata(cls, "__loom_relations__")


def get_projections(cls: type) -> dict[str, Projection]:
    """Return projections registered by ``LoomStructMeta``."""
    return _collect_inherited_dict_metadata(cls, "__loom_projections__")


def get_id_attributes(cls: type) -> tuple[str, ...]:
    """Return the names of the primary-key fields, in declaration order.

    Raises:
        InvalidValue: If the model declares no primary-key field.
    """
    names = tuple(name for name, info in get_column_fields(cls).items() if info.field.primary_key)
    if not names:
        raise InvalidValue(f"No primary key field found on {cls.__name__}")
    return names


def get_id_attribute(cls: type) -> str:
    """Return the name of the first primary-key field."""
    return get_id_attributes(cls)[0]


def get_table_name(cls: type) -> str:
    """Return the ``__tablename__`` declared on the model."""
    table = getattr(cls, "__tablename__", None)
    if not isinstance(table, str):
        raise InvalidType(f"{cls.__name__} does not declare __tablename__")
    return table


def resolve_type_hints(obj: Any, *, include_extras: bool = False) -> dict[str, Any]:
    """Return the resolved annotations of *obj*, or ``{}`` when they cannot be resolved.

    An annotation naming a class that is not importable from the defining
    module — a forward reference to a model declared elsewhere, for instance —
    is not fatal for callers that read annotations as one source of evidence
    among several. They treat a missing hint as "unknown" and fall back to
    other signals, so an empty mapping is the useful answer here.

    Args:
        obj: Class, function, or module whose annotations should be resolved.
        include_extras: Keep ``Annotated[T, ...]`` metadata instead of stripping it.

    Returns:
        Mapping of attribute name to resolved annotation, empty when unresolvable.
    """
    try:
        return get_type_hints(obj, include_extras=include_extras)
    except Exception:
        return {}


def _first_union_match(
    members: tuple[Any, ...], resolve: Callable[[Any], type | None]
) -> type | None:
    """Return the first non-``None`` result of *resolve* over *members*.

    Args:
        members: Union members, as returned by :attr:`Annotation.inner_args`.
        resolve: Walker applied to each member.

    Returns:
        The first resolved class, or ``None`` when no member resolves.
    """
    for member in members:
        result = resolve(member)
        if result is not None:
            return result
    return None


def extract_model_from_hint(hint: Any) -> type | None:
    """Unwrap list, Union and ``UnsetType`` layers down to the concrete class.

    ``list[Note]``, ``Note | UnsetType`` and ``list[Note] | None`` all resolve
    to ``Note``. An annotation whose innermost element is not a class, such as
    ``list[dict[str, Any]]``, resolves to ``None``.

    Args:
        hint: Any annotation object.

    Returns:
        The wrapped class, or ``None`` when the annotation wraps no class.
    """
    union_args = Annotation(hint).inner_args
    if union_args is not None:
        return _first_union_match(union_args, extract_model_from_hint)

    origin = getattr(hint, "__origin__", None)
    args: tuple[Any, ...] = getattr(hint, "__args__", ())

    if origin is list and len(args) == 1:
        return extract_model_from_hint(args[0])

    return hint if isinstance(hint, type) else None


def list_element_type(annotation: Any) -> type | None:
    """Return ``T`` for a ``list[T]`` annotation, or ``None`` when there is no list.

    Unlike :func:`extract_model_from_hint` a list layer is required, so a bare
    ``Note`` yields ``None``. The union arms produced when ``LoomStructMeta``
    widens a relation field to ``list[T] | UnsetType`` are unwrapped first.

    Args:
        annotation: Any annotation object.

    Returns:
        The list element class, or ``None`` when absent or not a class.
    """
    union_args = Annotation(annotation).inner_args
    if union_args is not None:
        return _first_union_match(union_args, list_element_type)

    if get_origin(annotation) is list:
        args = get_args(annotation)
        if args and isinstance(args[0], type):
            return args[0]
    return None


def generic_type_arg(annotation: Any, origin: Any) -> type | None:
    """Return the single class argument of ``origin[X]``, such as ``X`` in ``RepoFor[X]``.

    Args:
        annotation: Any annotation object.
        origin: The generic origin the annotation is expected to parametrise.

    Returns:
        The class argument, or ``None`` when the origin differs, the annotation
        is not generic, or its argument is not a single class.
    """
    if get_origin(annotation) is not origin:
        return None
    args = get_args(annotation)
    if len(args) != 1 or not isinstance(args[0], type):
        return None
    return args[0]


_NON_SCALAR_KINDS: frozenset[ColumnKind | None] = frozenset(
    {
        None,
        ColumnKind.POSTGRES_TSVECTOR,
        ColumnKind.POSTGRES_ARRAY,
        *(kind for kind in ColumnKind if kind.is_json),
    }
)


def column_annotations(model: type) -> dict[str, Any]:
    """Return the annotation of every column of *model*, ``Annotated`` metadata removed.

    Args:
        model: Model whose columns are read.

    Returns:
        Mapping of column name to its annotation, in declaration order.
    """
    hints = get_type_hints(model)
    return {name: hints[name] for name in get_column_fields(model)}


def scalar_column_annotations(model: type) -> dict[str, Any]:
    """Return the annotation of each scalar column of *model*, ``Annotated`` metadata removed.

    A JSON, array or text-search column is not scalar; lists, dicts and
    structs are inferred as JSON. An enum mixed with a scalar type, such as a
    ``StrEnum`` or ``IntEnum``, takes that type's column and is scalar. No
    column type can be inferred for a plain ``Enum`` or a ``Literal`` declared
    without one; it has no column type and it is not listed.

    Args:
        model: Model whose columns are read.

    Returns:
        Mapping of column name to its annotation, in declaration order.
    """
    fields = get_column_fields(model)
    return {
        name: annotation
        for name, annotation in column_annotations(model).items()
        if fields[name].kind not in _NON_SCALAR_KINDS
    }


_SCOPE_NAME = re.compile(r"^[a-z_][a-z0-9_]*$")
_GROUPS = frozenset({"readers", "writers"})


def is_row_scoped(cls: type) -> bool:
    """Whether ``cls`` carries the ``RowScoped`` marker."""
    return bool(getattr(cls, "__row_scoped__", False))


def scope_columns(cls: type) -> tuple[ScopeColumn, ...]:
    """Resolve the scoped columns of ``cls``, enforcing rules C1 to C5 and C8.

    Raises ``ValueError`` naming the rule, the model and the column.
    """
    fields = get_column_fields(cls)
    marked = is_row_scoped(cls)
    scopes = tuple(
        ScopeColumn(
            scope=info.field.scope, column=name, on=info.field.on, elevable=info.field.elevable
        )
        for name, info in fields.items()
        if info.field.scope is not None
    )
    _check_c4(cls, fields, marked)
    if not marked:
        return ()
    _check_c3(cls, scopes)
    _check_c2(cls, scopes)
    boundary = _check_c1(cls, scopes)
    _check_c8(cls, fields, boundary)
    _check_c5(cls, fields, boundary)
    return scopes


def declared_unique(cls: type) -> tuple[tuple[str, ...], ...]:
    """UNIQUE constraints declared with ``__unique__``, and the one a business key stands for."""
    declared = _declared_column_tuples(cls, "__unique__")
    key = business_key_of(cls)
    if key is None or any(set(columns) == set(key.fields) for columns in declared):
        return declared
    return (*declared, key.fields)


@dataclass(frozen=True, slots=True)
class ModelKeys:
    """The columns that identify a duplicate row of a model, and which of them the store fills.

    ``conflict`` is the business key, else the primary key; an undeclared key gives
    the primary key, and a model without one gives ``()``. ``store_filled`` are the
    conflict-key columns a frame cannot supply: a business key always comes from the
    frame, so it gives ``{}``, while a primary key comes from the store when it is
    autoincrement, derived from a business key, or filled by a server default.
    """

    conflict: tuple[str, ...]
    store_filled: Mapping[str, KeyFill]

    @property
    def frame_supplied(self) -> tuple[str, ...]:
        """The conflict key when a frame can supply it, else ``()``."""
        return () if self.store_filled else self.conflict

    @staticmethod
    @stored_on_class("__loom_model_keys__")
    def of(model: type) -> ModelKeys:
        """The keys of *model*, derived once from its declared columns."""
        key = business_key_of(model)
        columns = get_column_fields(model)
        if key is not None:
            return ModelKeys(key.fields, {})
        conflict = tuple(name for name, info in columns.items() if info.field.primary_key)
        fills = ((name, columns[name].field.key_fill) for name in conflict)
        return ModelKeys(conflict, {name: fill for name, fill in fills if fill is not None})


@stored_on_class("__loom_business_key__")
def business_key_of(cls: type) -> BusinessKey | None:
    """The business key declared with ``__business_key__``, checked against the columns once.

    Raises:
        InvalidType: If the declaration is not a ``BusinessKey``.
        InvalidValue: If it names an unknown or nullable field, or one whose
            type has no canonical form.
    """
    key = getattr(cls, "__business_key__", None)
    if key is None:
        return None
    prefix = f"{cls.__name__}: __business_key__"
    if not isinstance(key, BusinessKey):
        raise InvalidType(f"{prefix} must be a BusinessKey, got {key!r}")
    known = get_column_fields(cls)
    for name in key.fields:
        info = known.get(name)
        if info is None:
            raise InvalidValue(f"{prefix} names unknown column {name!r}")
        if info.field.nullable:
            raise InvalidValue(f"{prefix} field {name!r} is nullable; a business key never is")
        if not is_canonical_type(info.python_type):
            raise InvalidValue(
                f"{prefix} field {name!r} is {info.python_type.__name__}, "
                "which has no canonical form"
            )
    return key


@stored_on_class("__loom_derived_key__")
def derived_key_of(cls: type) -> DerivedKey | None:
    """The primary key a model derives from its business key, or ``None`` when it derives none.

    Checks the declared business key too, as :func:`business_key_of` does, and
    builds the key's readers once per model.

    Raises:
        InvalidType: If ``derived=`` is not on a ``str`` field.
        InvalidValue: If ``derived=`` is not on the single primary-key
            field, the model declares no ``__business_key__``, or the key
            contains the derived field.
        ConfigError: If a key field is set by the store after the insert.
    """
    derived = {name: info for name, info in get_column_fields(cls).items() if info.field.derived}
    if not derived:
        business_key_of(cls)
        return None
    declared = getattr(cls, "__business_key__", None)
    if isinstance(declared, BusinessKey):
        _refuse_server_set_fields(cls, declared)
    key = business_key_of(cls)
    prefix = f"{cls.__name__}: derived key"
    if len(derived) > 1:
        raise InvalidValue(f"{prefix} is declared on {', '.join(derived)}; only one field derives")
    ((name, info),) = derived.items()
    if get_id_attributes(cls) != (name,):
        raise InvalidValue(f"{prefix} {name!r} must be the model's single primary-key field")
    if info.python_type is not str:
        raise InvalidType(f"{prefix} {name!r} must be annotated str")
    if key is None:
        raise InvalidValue(f"{prefix} {name!r} needs a __business_key__ to derive from")
    if name in key.fields:
        raise InvalidValue(f"{prefix} {name!r} cannot be part of its own business key")
    return DerivedKey(cls.__name__, name, key, cast(Any, info.field.derived))


def _refuse_server_set_fields(cls: type, key: BusinessKey) -> None:
    """Refuse a business-key field the store sets after the insert, which cannot feed the key."""
    from loom.core.config.errors import ConfigError

    fields = get_column_fields(cls)
    for name in key.fields:
        info = fields.get(name)
        if info is None:
            continue
        field = info.field
        if (
            field.server_default is None
            and field.server_onupdate is None
            and not field.autoincrement
        ):
            continue
        raise ConfigError(
            f"{cls.__name__}.{name} is in the business key of a derived id, but the store sets "
            "it after the insert (server_default, server_onupdate or autoincrement), so it "
            "cannot feed the key; use a business-time field the application sets, such as "
            "observed_at, not created_at or updated_at"
        )


def verify_keys(model: type, rows: Iterable[Mapping[str, Any]]) -> tuple[KeyMismatch, ...]:
    """Report the *rows* whose id is not the one the model's business key derives.

    Used to check data that was written outside the repository, or before a
    business key was redefined.

    Raises:
        InvalidType: If the model derives no key.
    """
    derived = derived_key_of(model)
    if derived is None:
        raise InvalidType(f"{model.__name__} derives no key")
    return derived.verify(rows)


def declared_indexes(cls: type) -> tuple[tuple[str, ...], ...]:
    """Non-unique indexes declared with ``__indexes__``."""
    return _declared_column_tuples(cls, "__indexes__")


def declared_checks(cls: type) -> Mapping[str, str]:
    """Named CHECK constraints declared with ``__checks__`` as ``{rule: sql_expression}``.

    The rule is the constraint name, or its ``%(constraint_name)s`` under a
    naming convention; the expression is SQL that loom passes through verbatim.
    """
    raw = _declared_mapping(cls, "__checks__")
    result: dict[str, str] = {}
    for rule, expression in raw.items():
        name = _rule_identifier(cls, "__checks__", rule)
        if not isinstance(expression, str):
            raise InvalidType(f"{cls.__name__}: __checks__ rule {rule!r} expression must be a str")
        if not expression.strip():
            raise InvalidValue(
                f"{cls.__name__}: __checks__ rule {rule!r} needs a non-empty SQL expression"
            )
        result[name] = expression
    return result


def declared_partial_unique(cls: type) -> tuple[PartialUnique, ...]:
    """Partial unique indexes declared with ``__partial_unique__``.

    Each entry reads ``{rule: (columns, where)}``: ``columns`` is a tuple of
    column names, ``where`` the SQL predicate passed through verbatim.
    """
    raw = _declared_mapping(cls, "__partial_unique__")
    known = get_column_fields(cls)
    return tuple(_partial_unique(cls, rule, entry, known) for rule, entry in raw.items())


def _partial_unique(
    cls: type, rule: object, entry: object, known: Mapping[str, ColumnFieldInfo]
) -> PartialUnique:
    name = _rule_identifier(cls, "__partial_unique__", rule)
    prefix = f"{cls.__name__}: __partial_unique__ rule {rule!r}"
    if not isinstance(entry, tuple):
        raise InvalidType(f"{prefix} must be a tuple")
    if len(entry) != 2:
        raise InvalidValue(f"{prefix} must be a (columns, where) pair")
    columns, where = entry
    if not isinstance(columns, tuple):
        raise InvalidType(f"{prefix} columns must be a tuple")
    if not columns:
        raise InvalidValue(f"{prefix} needs a non-empty tuple of columns")
    if not isinstance(where, str):
        raise InvalidType(f"{prefix} predicate must be a str")
    if not where.strip():
        raise InvalidValue(f"{prefix} needs a non-empty SQL predicate")
    for column in columns:
        if column not in known:
            raise InvalidValue(f"{prefix} names unknown column {column!r}")
    return PartialUnique(rule=name, columns=columns, where=where)


_PARTITION_ATTR = "__partition_by__"
_RANGE = "RANGE"
_PARTITION_KINDS = frozenset({ColumnKind.DATETIME})


def declared_partition(cls: type) -> RangePartition | None:
    """The range partitioning declared with ``__partition_by__ = ("RANGE", "<column>")``.

    The column must be a ``DateTime`` column of the primary key, and every
    unique key must contain it, as Postgres requires of a partitioned table.
    Only row-scoped models may be partitioned, so the guard protects every
    partition; LIST and HASH partitioning are not supported.

    Raises:
        InvalidType: Naming the model, when the declaration holds the wrong type.
        InvalidValue: Naming the model and what else is wrong with the declaration.
    """
    raw = getattr(cls, _PARTITION_ATTR, None)
    if raw is None:
        return None
    prefix = f"{cls.__name__}: {_PARTITION_ATTR}"
    if not is_row_scoped(cls):
        raise InvalidValue(f"{prefix} is only for row-scoped models")
    if not isinstance(raw, tuple):
        raise InvalidType(f"{prefix} must be a tuple ('RANGE', <column>), got {raw!r}")
    if len(raw) != 2:
        raise InvalidValue(f"{prefix} must be ('RANGE', <column>), got {raw!r}")
    if not all(isinstance(x, str) for x in raw):
        raise InvalidType(f"{prefix} entries must be strings, got {raw!r}")
    strategy, column = raw
    if strategy != _RANGE:
        raise InvalidValue(
            f"{prefix} strategy {strategy!r} is not supported; only RANGE partitioning is"
        )
    fields = get_column_fields(cls)
    _check_partition_column(cls, prefix, column, fields)
    _check_unique_keys_hold_partition_column(cls, prefix, column, fields)
    return RangePartition(column=column)


def _check_partition_column(
    cls: type, prefix: str, column: str, fields: Mapping[str, ColumnFieldInfo]
) -> None:
    info = fields.get(column)
    if info is None:
        raise InvalidValue(f"{prefix} names unknown column {column!r}")
    _rule_identifier(cls, _PARTITION_ATTR, column)
    if info.kind not in _PARTITION_KINDS:
        raise InvalidValue(f"{prefix} column {column!r} must be a DateTime column")
    if not info.field.primary_key:
        raise InvalidValue(f"{prefix} column {column!r} must be part of the primary key")


def _check_unique_keys_hold_partition_column(
    cls: type, prefix: str, column: str, fields: Mapping[str, ColumnFieldInfo]
) -> None:
    partials = [partial.columns for partial in declared_partial_unique(cls)]
    for key in [*_unique_keys(cls, fields), *partials]:
        if column not in key:
            raise InvalidValue(
                f"{prefix} unique key ({', '.join(key)}) lacks the partition column {column!r}"
            )


def _unique_keys(cls: type, fields: Mapping[str, ColumnFieldInfo]) -> list[tuple[str, ...]]:
    """The full unique keys besides the primary key: ``__unique__`` and ``unique=True``."""
    keys = list(declared_unique(cls))
    keys += [(name,) for name, info in fields.items() if info.field.unique]
    return keys


def _declared_mapping(cls: type, attr: str) -> Mapping[object, object]:
    raw: object = getattr(cls, attr, None) or {}
    if not isinstance(raw, Mapping):
        raise InvalidType(f"{cls.__name__}: {attr} must be a mapping of rule to declaration")
    return cast(Mapping[object, object], raw)


def _rule_identifier(cls: type, attr: str, rule: object) -> str:
    if not isinstance(rule, str):
        raise InvalidType(f"{cls.__name__}: {attr} rule {rule!r} must be a string")
    try:
        return sql_identifier(rule)
    except ValueError as exc:
        raise InvalidValue(f"{cls.__name__}: {attr} rule {rule!r}: {exc}") from exc


def declared_privileges(cls: type) -> Mapping[str, frozenset[Privilege]]:
    """Group privileges declared with ``__privileges__`` on an unscoped model."""
    raw = getattr(cls, "__privileges__", None)
    if raw is None:
        return {}
    if is_row_scoped(cls):
        raise InvalidValue(f"{cls.__name__}: __privileges__ is only for unscoped models")
    result: dict[str, frozenset[Privilege]] = {}
    for group, privileges in raw.items():
        if group not in _GROUPS:
            raise InvalidValue(f"{cls.__name__}: unknown privilege group {group!r}")
        result[group] = frozenset(_as_privilege(cls, item) for item in privileges)
    return result


def _as_privilege(cls: type, item: object) -> Privilege:
    if isinstance(item, Privilege):
        return item
    if isinstance(item, str) and item in Privilege.__members__:
        return Privilege[item]
    raise InvalidValue(f"{cls.__name__}: {item!r} is not a row privilege")


def _declared_column_tuples(cls: type, attr: str) -> tuple[tuple[str, ...], ...]:
    declared = tuple(tuple(entry) for entry in getattr(cls, attr, ()))
    known = get_column_fields(cls)
    for entry in declared:
        for column in entry:
            if column not in known:
                raise InvalidValue(f"{cls.__name__}: {attr} names unknown column {column!r}")
    return declared


def _check_c4(cls: type, fields: dict[str, ColumnFieldInfo], marked: bool) -> None:
    for name, info in fields.items():
        field = info.field
        if field.scope is not None and not marked:
            raise InvalidValue(f"C4: {cls.__name__}.{name} declares a scope on an unmarked model")
        if field.scope is None and (field.on != "both" or field.elevable):
            raise InvalidValue(
                f"C4: {cls.__name__}.{name} declares reach or elevable without a scope"
            )


def _check_c3(cls: type, scopes: tuple[ScopeColumn, ...]) -> None:
    seen: set[str] = set()
    for scope in scopes:
        if not _SCOPE_NAME.fullmatch(scope.scope):
            raise InvalidValue(
                f"C3: {cls.__name__}.{scope.column} scope {scope.scope!r} is not an identifier"
            )
        if scope.scope in seen:
            raise InvalidValue(f"C3: {cls.__name__}.{scope.column} repeats scope {scope.scope!r}")
        seen.add(scope.scope)


def _check_c2(cls: type, scopes: tuple[ScopeColumn, ...]) -> None:
    for scope in scopes:
        if scope.elevable and scope.on != "write":
            raise InvalidValue(f"C2: {cls.__name__}.{scope.column} is elevable but not write-only")


def _check_c1(cls: type, scopes: tuple[ScopeColumn, ...]) -> ScopeColumn:
    boundaries = [scope for scope in scopes if scope.is_boundary]
    if len(boundaries) != 1:
        raise InvalidValue(
            f"C1: {cls.__name__} must declare exactly one boundary scope, found {len(boundaries)}"
        )
    return boundaries[0]


def _check_c8(cls: type, fields: dict[str, ColumnFieldInfo], boundary: ScopeColumn) -> None:
    if fields[boundary.column].field.nullable:
        raise InvalidValue(
            f"C8: {cls.__name__}.{boundary.column} boundary column cannot be nullable"
        )


def _check_c5(cls: type, fields: dict[str, ColumnFieldInfo], boundary: ScopeColumn) -> None:
    primary_key = tuple(name for name, info in fields.items() if info.field.primary_key)
    for key in [primary_key, *_unique_keys(cls, fields)]:
        if boundary.column not in key:
            columns = ", ".join(key)
            raise InvalidValue(
                f"C5: {cls.__name__} key {columns} lacks the boundary column {boundary.column}"
            )
    for partial in declared_partial_unique(cls):
        if boundary.column not in partial.columns:
            columns = ", ".join(partial.columns)
            raise InvalidValue(
                f"C5: {cls.__name__} partial unique {partial.rule} ({columns}) "
                f"lacks the boundary column {boundary.column}"
            )
