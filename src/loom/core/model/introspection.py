from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, time
from decimal import Decimal
from types import UnionType
from typing import Any, ClassVar, Union, cast, get_args, get_origin, get_type_hints

import msgspec

from loom.core.model.field import ColumnFieldSpec, ColumnType, Field
from loom.core.model.privilege import Privilege
from loom.core.model.projection import Projection
from loom.core.model.relation import Relation
from loom.core.model.scoped import ScopeColumn
from loom.core.model.types import (
    JSON,
    Boolean,
    Bytes,
    DateTime,
    Float,
    Integer,
    Numeric,
    String,
)
from loom.core.schema_names import sql_identifier


@dataclass(frozen=True, slots=True)
class PartialUnique:
    """A unique index over ``columns`` restricted to the rows matching ``where``."""

    rule: str
    columns: tuple[str, ...]
    where: str


@dataclass(frozen=True, slots=True)
class ColumnFieldInfo:
    """Resolved metadata for a single column field."""

    name: str
    python_type: type
    column_type: ColumnType
    field: Field


def _collect_inherited_dict_metadata(cls: type, attr: str) -> dict[str, Any]:
    """Merge dict metadata from the full MRO (base -> subclass)."""
    merged: dict[str, Any] = {}
    for current in reversed(cls.__mro__):
        raw = getattr(current, attr, None)
        if isinstance(raw, dict):
            merged.update(raw)
    return merged


def get_column_fields(cls: type) -> dict[str, ColumnFieldInfo]:
    """Extract column fields from a model class."""
    declared_columns = _collect_inherited_dict_metadata(cls, "__loom_columns__")
    hints = get_type_hints(cls, include_extras=True)
    non_columns = set(get_relations(cls)) | set(get_projections(cls))

    result: dict[str, ColumnFieldInfo] = {}
    for struct_field in msgspec.structs.fields(cls):
        name = struct_field.name
        annotation = hints.get(name, Any)
        if name in non_columns or _is_classvar(annotation):
            continue
        result[name] = _resolve_column_field(
            name,
            annotation,
            struct_default=struct_field.default,
            declared=declared_columns.get(name),
        )
    return result


def _resolve_column_field(
    name: str,
    annotation: Any,
    *,
    struct_default: Any,
    declared: ColumnFieldSpec | None,
) -> ColumnFieldInfo:
    """Build the column metadata for one field, declared or inferred."""
    python_type = _extract_origin_type(annotation)
    if declared is not None:
        return ColumnFieldInfo(
            name=name,
            python_type=python_type,
            column_type=declared.column_type
            or _infer_column_type(annotation, field=declared.field),
            field=_with_struct_default(declared.field, struct_default),
        )

    annotated_type, annotated_field = _extract_annotated_column(annotation)
    if annotated_type is not None:
        return ColumnFieldInfo(
            name=name,
            python_type=python_type,
            column_type=annotated_type,
            field=_with_struct_default(annotated_field, struct_default),
        )

    inferred_field = _with_struct_default(Field(), struct_default)
    return ColumnFieldInfo(
        name=name,
        python_type=python_type,
        column_type=_infer_column_type(annotation, field=inferred_field),
        field=inferred_field,
    )


def _extract_annotated_column(annotation: Any) -> tuple[ColumnType | None, Field]:
    """Read the ``ColumnType`` and ``Field`` carried by ``Annotated[T, ...]``."""
    column_type: ColumnType | None = None
    field = Field()
    for entry in _extract_metadata(annotation):
        if isinstance(entry, ColumnType):
            column_type = entry
        elif isinstance(entry, Field):
            field = entry
    return column_type, field


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


def get_id_attribute(cls: type) -> str:
    """Return the name of the primary key field."""
    for name, info in get_column_fields(cls).items():
        if info.field.primary_key:
            return name
    raise ValueError(f"No primary key field found on {cls.__name__}")


def get_table_name(cls: type) -> str:
    """Return the ``__tablename__`` declared on the model."""
    table = getattr(cls, "__tablename__", None)
    if not isinstance(table, str):
        raise ValueError(f"{cls.__name__} does not declare __tablename__")
    return table


def _extract_metadata(annotation: Any) -> tuple[Any, ...]:
    """Pull metadata entries from ``Annotated[T, ...]``."""
    return getattr(annotation, "__metadata__", ())


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


def _union_members(hint: Any) -> tuple[Any, ...] | None:
    """Return the members of a Union annotation, normalising ``X | Y`` and ``Union[X, Y]``.

    Args:
        hint: Any annotation object.

    Returns:
        The declared union members, or ``None`` when *hint* is not a Union.
    """
    if isinstance(hint, UnionType):
        return hint.__args__
    if getattr(hint, "__origin__", None) is Union:
        return getattr(hint, "__args__", ())
    return None


def _union_inner_args(hint: Any) -> tuple[Any, ...] | None:
    """Return the non-``None``, non-``UnsetType`` members of a Union annotation.

    Args:
        hint: Any annotation object.

    Returns:
        The meaningful union members, or ``None`` when *hint* is not a Union.
    """
    members = _union_members(hint)
    if members is None:
        return None
    return tuple(a for a in members if a is not type(None) and a is not msgspec.UnsetType)


def _first_union_match(
    members: tuple[Any, ...], resolve: Callable[[Any], type | None]
) -> type | None:
    """Return the first non-``None`` result of *resolve* over *members*.

    Args:
        members: Union members, as returned by :func:`_union_inner_args`.
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
    union_args = _union_inner_args(hint)
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
    union_args = _union_inner_args(annotation)
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


def _extract_origin_type(annotation: Any) -> type[Any]:
    """Return the base type from ``Annotated[T, ...]``."""
    origin = getattr(annotation, "__origin__", None)
    if origin is not None:
        args = getattr(annotation, "__args__", ())
        if args:
            value = args[0]
            if isinstance(value, type):
                return value
            return object
    raw = _unwrap_optional(annotation)
    origin = get_origin(raw)
    if origin is not None:
        if isinstance(origin, type):
            return origin
        return object
    if isinstance(raw, type):
        return raw
    return object


def _unwrap_optional(annotation: Any) -> Any:
    members = _union_members(annotation)
    if members is None:
        return annotation
    args = tuple(arg for arg in members if arg is not type(None))
    if len(args) == 1:
        return args[0]
    return annotation


def _is_classvar(annotation: Any) -> bool:
    return get_origin(annotation) is ClassVar


_SCALAR_TYPE_MAP: dict[type, ColumnType] = {
    int: Integer,
    float: Float,
    bool: Boolean,
    bytes: Bytes,
    datetime: DateTime(tz=True),
    Decimal: Numeric(),
}


def _infer_column_type(annotation: Any, *, field: Field) -> ColumnType:
    base = _unwrap_optional(annotation)
    if get_origin(base) in (list, tuple, set, dict):
        return JSON
    python_type = _extract_origin_type(base)
    if python_type is str:
        return String(field.length)
    if python_type in (date, time):
        return String(None)
    return _SCALAR_TYPE_MAP.get(python_type, JSON)


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
    """Composite UNIQUE constraints declared with ``__unique__``."""
    return _declared_column_tuples(cls, "__unique__")


def declared_indexes(cls: type) -> tuple[tuple[str, ...], ...]:
    """Non-unique indexes declared with ``__indexes__``."""
    return _declared_column_tuples(cls, "__indexes__")


def declared_checks(cls: type) -> Mapping[str, str]:
    """Named CHECK constraints declared with ``__checks__`` as ``{rule: sql_expression}``.

    The rule is the constraint name, or its ``%(constraint_name)s`` under a
    naming convention; the expression is SQL that loom passes through verbatim.
    """
    raw = getattr(cls, "__checks__", None) or {}
    result: dict[str, str] = {}
    for rule, expression in raw.items():
        _rule_identifier(cls, "__checks__", rule)
        if not isinstance(expression, str) or not expression.strip():
            raise ValueError(
                f"{cls.__name__}: __checks__ rule {rule!r} needs a non-empty SQL expression"
            )
        result[rule] = expression
    return result


def declared_partial_unique(cls: type) -> tuple[PartialUnique, ...]:
    """Partial unique indexes declared with ``__partial_unique__``.

    Each entry reads ``{rule: (columns, where)}``: ``columns`` is a tuple of
    column names, ``where`` the SQL predicate passed through verbatim.
    """
    raw = getattr(cls, "__partial_unique__", None) or {}
    known = get_column_fields(cls)
    return tuple(_partial_unique(cls, rule, entry, known) for rule, entry in raw.items())


def _partial_unique(
    cls: type, rule: str, entry: object, known: Mapping[str, ColumnFieldInfo]
) -> PartialUnique:
    _rule_identifier(cls, "__partial_unique__", rule)
    prefix = f"{cls.__name__}: __partial_unique__ rule {rule!r}"
    if not isinstance(entry, tuple) or len(entry) != 2:
        raise ValueError(f"{prefix} must be a (columns, where) pair")
    columns, where = entry
    if not isinstance(columns, tuple) or not columns:
        raise ValueError(f"{prefix} needs a non-empty tuple of columns")
    if not isinstance(where, str) or not where.strip():
        raise ValueError(f"{prefix} needs a non-empty SQL predicate")
    for column in columns:
        if column not in known:
            raise ValueError(f"{prefix} names unknown column {column!r}")
    return PartialUnique(rule=rule, columns=columns, where=where)


def _rule_identifier(cls: type, attr: str, rule: object) -> None:
    try:
        sql_identifier(str(rule))
    except ValueError as exc:
        raise ValueError(f"{cls.__name__}: {attr} rule {rule!r}: {exc}") from exc


def declared_privileges(cls: type) -> Mapping[str, frozenset[Privilege]]:
    """Group privileges declared with ``__privileges__`` on an unscoped model."""
    raw = getattr(cls, "__privileges__", None)
    if raw is None:
        return {}
    if is_row_scoped(cls):
        raise ValueError(f"{cls.__name__}: __privileges__ is only for unscoped models")
    result: dict[str, frozenset[Privilege]] = {}
    for group, privileges in raw.items():
        if group not in _GROUPS:
            raise ValueError(f"{cls.__name__}: unknown privilege group {group!r}")
        result[group] = frozenset(_as_privilege(cls, item) for item in privileges)
    return result


def _as_privilege(cls: type, item: object) -> Privilege:
    if isinstance(item, Privilege):
        return item
    if isinstance(item, str) and item in Privilege.__members__:
        return Privilege[item]
    raise ValueError(f"{cls.__name__}: {item!r} is not a row privilege")


def _declared_column_tuples(cls: type, attr: str) -> tuple[tuple[str, ...], ...]:
    declared = tuple(tuple(entry) for entry in getattr(cls, attr, ()))
    known = get_column_fields(cls)
    for entry in declared:
        for column in entry:
            if column not in known:
                raise ValueError(f"{cls.__name__}: {attr} names unknown column {column!r}")
    return declared


def _check_c4(cls: type, fields: dict[str, ColumnFieldInfo], marked: bool) -> None:
    for name, info in fields.items():
        field = info.field
        if field.scope is not None and not marked:
            raise ValueError(f"C4: {cls.__name__}.{name} declares a scope on an unmarked model")
        if field.scope is None and (field.on != "both" or field.elevable):
            raise ValueError(
                f"C4: {cls.__name__}.{name} declares reach or elevable without a scope"
            )


def _check_c3(cls: type, scopes: tuple[ScopeColumn, ...]) -> None:
    seen: set[str] = set()
    for scope in scopes:
        if not _SCOPE_NAME.fullmatch(scope.scope):
            raise ValueError(
                f"C3: {cls.__name__}.{scope.column} scope {scope.scope!r} is not an identifier"
            )
        if scope.scope in seen:
            raise ValueError(f"C3: {cls.__name__}.{scope.column} repeats scope {scope.scope!r}")
        seen.add(scope.scope)


def _check_c2(cls: type, scopes: tuple[ScopeColumn, ...]) -> None:
    for scope in scopes:
        if scope.elevable and scope.on != "write":
            raise ValueError(f"C2: {cls.__name__}.{scope.column} is elevable but not write-only")


def _check_c1(cls: type, scopes: tuple[ScopeColumn, ...]) -> ScopeColumn:
    boundaries = [scope for scope in scopes if scope.is_boundary]
    if len(boundaries) != 1:
        raise ValueError(
            f"C1: {cls.__name__} must declare exactly one boundary scope, found {len(boundaries)}"
        )
    return boundaries[0]


def _check_c8(cls: type, fields: dict[str, ColumnFieldInfo], boundary: ScopeColumn) -> None:
    if fields[boundary.column].field.nullable:
        raise ValueError(f"C8: {cls.__name__}.{boundary.column} boundary column cannot be nullable")


def _check_c5(cls: type, fields: dict[str, ColumnFieldInfo], boundary: ScopeColumn) -> None:
    primary_key = tuple(name for name, info in fields.items() if info.field.primary_key)
    keys = [primary_key, *declared_unique(cls)]
    keys += [(name,) for name, info in fields.items() if info.field.unique]
    for key in keys:
        if boundary.column not in key:
            columns = ", ".join(key)
            raise ValueError(
                f"C5: {cls.__name__} key {columns} lacks the boundary column {boundary.column}"
            )
    for partial in declared_partial_unique(cls):
        if boundary.column not in partial.columns:
            columns = ", ".join(partial.columns)
            raise ValueError(
                f"C5: {cls.__name__} partial unique {partial.rule} ({columns}) "
                f"lacks the boundary column {boundary.column}"
            )
