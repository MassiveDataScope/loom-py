from __future__ import annotations

import functools
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Final

import msgspec
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    Constraint,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY as PG_ARRAY
from sqlalchemy.dialects.postgresql import INET as PG_INET
from sqlalchemy.dialects.postgresql import JSONB as PG_JSONB
from sqlalchemy.dialects.postgresql import TSVECTOR as PG_TSVECTOR
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, mapped_column, relationship
from sqlalchemy.orm import registry as sa_registry
from sqlalchemy.sql.naming import conv
from sqlalchemy.sql.schema import DEFAULT_NAMING_CONVENTION

from loom.core.backend.core_model import CoreModel, CoreProfilePlan, CoreRelationStep
from loom.core.backend.scoped_ddl import SCHEMA_KEY, register_listeners
from loom.core.config import ConfigError
from loom.core.model.enums import Cardinality, OnDelete, ServerDefault, ServerOnUpdate
from loom.core.model.field import ColumnType, Field
from loom.core.model.introspection import (
    ColumnFieldInfo,
    PartialUnique,
    declared_checks,
    declared_indexes,
    declared_partial_unique,
    declared_privileges,
    declared_unique,
    extract_model_from_hint,
    get_column_fields,
    get_id_attribute,
    get_projections,
    get_relations,
    get_table_name,
    resolve_type_hints,
    scope_columns,
)
from loom.core.model.privilege import READ_WRITE, Privilege
from loom.core.model.relation import Relation
from loom.core.model.scoped import ScopeColumn, ScopedTable
from loom.core.projection.runtime import ProjectionStep, build_projection_plan_from_steps
from loom.core.schema_names import MAX_IDENTIFIER_LENGTH, naming_convention

_SA_TYPE_MAP: dict[str, type] = {
    "String": String,
    "Integer": Integer,
    "BigInteger": BigInteger,
    "Float": Float,
    "Boolean": Boolean,
    "Text": Text,
    "JSON": JSON,
    "Bytes": LargeBinary,
    "DateTime": DateTime,
    "Numeric": Numeric,
    "Postgres.JSONB": PG_JSONB,
    "Postgres.UUID": PG_UUID,
    "Postgres.TSVECTOR": PG_TSVECTOR,
    "Postgres.INET": PG_INET,
}

_SERVER_DEFAULT_MAP = {
    ServerDefault.NOW: func.now,
}

_CARDINALITY_USELIST = {
    Cardinality.ONE_TO_ONE: False,
    Cardinality.MANY_TO_ONE: False,
    Cardinality.ONE_TO_MANY: True,
    Cardinality.MANY_TO_MANY: True,
}


class SABase(DeclarativeBase):
    """Shared declarative base for all compiled SQLAlchemy models."""


_registry: dict[type, Any] = {}
_table_registry: dict[str, Any] = {}
_core_registry: dict[type, CoreModel] = {}
_pending_relations: dict[type, dict[str, Relation]] = {}


@dataclass(slots=True)
class _Compilation:
    """Everything compiled into one ``MetaData``; the shared one is the default."""

    metadata: MetaData
    base: type
    compiled: dict[type, Any]
    tables: dict[str, Any]
    core: dict[type, CoreModel]
    pending: dict[type, dict[str, Relation]]
    scoped: dict[tuple[str | None, str], ScopedTable] = field(default_factory=dict)


_COMPILATION_KEY = "loom.compilation"
_RULE_KEY = "loom.rule"


class _Keep(Enum):
    """The default of :func:`reset_registry`: leave the naming convention as it is."""

    CONVENTION = "keep"


_KEEP_CONVENTION: Final = _Keep.CONVENTION


@functools.cache
def _shared() -> _Compilation:
    return _Compilation(
        SABase.metadata, SABase, _registry, _table_registry, _core_registry, _pending_relations
    )


def _compilation_for(metadata: MetaData | None) -> _Compilation:
    if metadata is None or metadata is SABase.metadata:
        return _shared()
    existing = metadata.info.get(_COMPILATION_KEY)
    if existing is None:
        base = sa_registry(metadata=metadata).generate_base()
        existing = _Compilation(metadata, base, {}, {}, {}, {})
        metadata.info[_COMPILATION_KEY] = existing
    return existing


def scoped_tables(metadata: MetaData | None = None) -> Mapping[tuple[str | None, str], ScopedTable]:
    """Row-scoped tables compiled into ``metadata``, keyed by ``(schema, name)``."""
    return dict(_compilation_for(metadata).scoped)


def _build_sa_column_type(col_type: ColumnType) -> Any:
    if col_type.type_name == "Postgres.ARRAY":
        if len(col_type.args) != 1:
            raise ValueError("Postgres.ARRAY expects one inner ColumnType")
        inner = col_type.args[0]
        if not isinstance(inner, ColumnType):
            raise ValueError("Postgres.ARRAY inner type must be ColumnType")
        return PG_ARRAY(_build_sa_column_type(inner))

    sa_type_cls = _SA_TYPE_MAP.get(col_type.type_name)
    if sa_type_cls is None:
        raise ValueError(f"Unsupported column type: {col_type.type_name}")
    return sa_type_cls(*col_type.args, **col_type.kwargs)


def _build_field_kwargs(field: Field) -> dict[str, Any]:
    kwargs: dict[str, Any] = {"nullable": field.nullable}
    if field.primary_key:
        kwargs["primary_key"] = True
    if field.autoincrement:
        kwargs["autoincrement"] = True
    if field.unique:
        kwargs["unique"] = True
    if field.index:
        kwargs["index"] = True
    if field.default not in (None, msgspec.UNSET):
        kwargs["default"] = field.default
    if field.server_default is not None:
        factory = _SERVER_DEFAULT_MAP.get(field.server_default)
        if factory is not None:
            kwargs["server_default"] = factory()
    if ServerOnUpdate.is_now(field.server_onupdate):
        now_expr = func.now()
        kwargs["onupdate"] = now_expr
        kwargs["server_onupdate"] = now_expr
    return kwargs


def _build_mapped_column(field_info: Any, *, inline_foreign_key: bool = True) -> Any:
    col_type = _build_sa_column_type(field_info.column_type)
    column: Field = field_info.field
    kwargs = _build_field_kwargs(column)

    if column.foreign_key is not None and inline_foreign_key:
        fk_kwargs: dict[str, Any] = {}
        if column.on_delete is not None:
            fk_kwargs["ondelete"] = column.on_delete.value
        return mapped_column(ForeignKey(column.foreign_key, **fk_kwargs), type_=col_type, **kwargs)

    return mapped_column(col_type, **kwargs)


def _resolve_fk_target_table(foreign_key: str) -> str:
    """Extract table name from a FK spec like 'products.id' -> 'products'."""
    return foreign_key.rsplit(".", 1)[0]


def _find_target_sa_class(target_table: str, comp: _Compilation) -> Any:
    """Find compiled SA class by table name (O(1) via inverse index)."""
    return comp.tables.get(target_table)


def compile_model(struct_cls: type, *, metadata: MetaData | None = None) -> Any:
    """Compile a loom ``BaseModel`` Struct into a SQLAlchemy declarative class."""
    return _compile_model(struct_cls, _compilation_for(metadata), {})


def _compile_model(
    struct_cls: type, comp: _Compilation, models_by_table: Mapping[str, type]
) -> Any:
    if struct_cls in comp.compiled:
        return comp.compiled[struct_cls]

    table_name = get_table_name(struct_cls)
    column_fields = get_column_fields(struct_cls)
    scopes = scope_columns(struct_cls)
    boundary = next((scope for scope in scopes if scope.is_boundary), None)
    constraints, composite_columns = _foreign_key_constraints(
        struct_cls, column_fields, boundary, models_by_table, comp
    )
    constraints += _declared_constraints(struct_cls, table_name)

    attrs: dict[str, Any] = {
        "__tablename__": table_name,
        "__struct_cls__": struct_cls,
    }
    for name, field_info in column_fields.items():
        attrs[name] = _build_mapped_column(
            field_info, inline_foreign_key=name not in composite_columns
        )
    if constraints:
        attrs["__table_args__"] = tuple(constraints)

    sa_cls: Any = type(struct_cls.__name__ + "SA", (comp.base,), attrs)
    _check_declared_name_lengths(struct_cls, constraints)
    _check_distinct_names(struct_cls, sa_cls.__table__)
    comp.compiled[struct_cls] = sa_cls
    comp.tables[table_name] = sa_cls
    comp.pending[struct_cls] = get_relations(struct_cls)
    table = sa_cls.__table__
    scoped = None
    if scopes:
        scoped = ScopedTable(
            schema=table.schema,
            name=table.name,
            scopes=scopes,
            privileges=frozenset(getattr(struct_cls, "__scope_privileges__", READ_WRITE)),
        )
        comp.scoped[(table.schema, table.name)] = scoped
    schema = comp.metadata.info.get(SCHEMA_KEY)
    if schema is not None:
        register_listeners(
            table,
            schema=schema,
            scoped=scoped,
            privileges=declared_privileges(struct_cls),
        )
    return sa_cls


def _declared_constraints(struct_cls: type, table_name: str) -> list[Constraint | Index]:
    """Keys, indexes and checks declared on the model, in declaration order.

    ``__unique__`` constraints are unnamed, so the metadata's naming convention
    names them. ``__checks__`` are named by their rule, which a ``ck``
    convention may use as ``%(constraint_name)s``. Indexes carry a name loom
    builds; :class:`~sqlalchemy.sql.naming.conv` marks it final so no convention
    renames it, and :func:`_check_declared_name_lengths` refuses one over
    Postgres's limit.

    Alembic's autogenerate does not compare CHECK constraints and does not
    compare the ``WHERE`` predicate of an index: adding, changing or removing a
    check on an existing table, and changing a predicate, are written by hand in
    a revision, and ``check`` does not report them as drift.
    """
    constraints: list[Constraint | Index] = [
        UniqueConstraint(*columns) for columns in declared_unique(struct_cls)
    ]
    constraints += [
        Index(
            conv(f"ix_{table_name}_{'_'.join(columns)}"),
            *columns,
            info={_RULE_KEY: ("__indexes__", ", ".join(columns))},
        )
        for columns in declared_indexes(struct_cls)
    ]
    constraints += [
        _partial_unique_index(table_name, partial)
        for partial in declared_partial_unique(struct_cls)
    ]
    constraints += [
        _check_constraint(rule, expression)
        for rule, expression in declared_checks(struct_cls).items()
    ]
    return constraints


def _check_distinct_names(struct_cls: type, table: Table) -> None:
    """Refuse a table where two constraints or indexes resolve to the same name.

    Postgres would reject the second one at DDL time. Under a naming
    convention the usual cause is ``fk_%(table_name)s_%(column_0_name)s``: every
    composite FK of a scoped table starts with the boundary column.
    """
    seen: set[str] = set()
    items: list[Constraint | Index] = [*table.constraints, *table.indexes]
    for item in items:
        if item.name is None:
            continue
        name = str(item.name)
        if name in seen:
            raise ValueError(
                f"{struct_cls.__name__}: table {table.name} has two constraints named {name!r}; "
                "name them apart, for FKs with fk_%(table_name)s_%(column_0_N_name)s"
            )
        seen.add(name)


def _partial_unique_index(table_name: str, partial: PartialUnique) -> Index:
    """The unique index of one ``__partial_unique__`` rule.

    ``partial.where`` is a DDL fragment the product declared on its model. It
    must be a static literal with no runtime input; loom checks only that it is
    a non-empty string.
    """
    return Index(
        conv(f"uq_{table_name}_{partial.rule}"),
        *partial.columns,
        unique=True,
        postgresql_where=text(partial.where),
        info={_RULE_KEY: ("__partial_unique__", partial.rule)},
    )


def _check_constraint(rule: str, expression: str) -> CheckConstraint:
    """The CHECK constraint of one ``__checks__`` rule.

    ``expression`` is a DDL fragment the product declared on its model. It must
    be a static literal with no runtime input; loom checks only that it is a
    non-empty string.
    """
    return CheckConstraint(expression, name=rule, info={_RULE_KEY: ("__checks__", rule)})


def _check_declared_name_lengths(struct_cls: type, constraints: list[Constraint | Index]) -> None:
    """Refuse a declared check or index whose final name Postgres would truncate.

    SQLAlchemy would raise only when emitting DDL for a plain name, and would
    shorten a final (``conv``) name with a hash; both are refused here instead.
    """
    for constraint in constraints:
        declared = constraint.info.get(_RULE_KEY)
        if declared is None:
            continue
        attr, rule = declared
        name = str(constraint.name)
        if len(name.encode()) > MAX_IDENTIFIER_LENGTH:
            raise ValueError(
                f"{struct_cls.__name__}: {attr} rule {rule!r} is named {name!r}, longer than "
                f"the {MAX_IDENTIFIER_LENGTH} bytes Postgres keeps; shorten it"
            )


def _target_model(
    table: str, models_by_table: Mapping[str, type], comp: _Compilation
) -> type | None:
    model = models_by_table.get(table)
    if model is not None:
        return model
    sa_cls = comp.tables.get(table)
    return getattr(sa_cls, "__struct_cls__", None)


def _foreign_key_constraints(
    struct_cls: type,
    fields: dict[str, ColumnFieldInfo],
    boundary: ScopeColumn | None,
    models_by_table: Mapping[str, type],
    comp: _Compilation,
) -> tuple[list[Any], set[str]]:
    constraints: list[Any] = []
    composite: set[str] = set()
    for name, info in fields.items():
        if info.field.foreign_key is None:
            continue
        constraint = _constraint_for_field(
            struct_cls, name, info.field, boundary, models_by_table, comp
        )
        if constraint is not None:
            constraints.append(constraint)
            composite.add(name)
    return constraints, composite


def _constraint_for_field(
    struct_cls: type,
    name: str,
    field: Field,
    boundary: ScopeColumn | None,
    models_by_table: Mapping[str, type],
    comp: _Compilation,
) -> Any | None:
    target_table, _, ref = (field.foreign_key or "").rpartition(".")
    target = _target_model(target_table, models_by_table, comp)
    if target is None:
        if boundary is not None:
            raise ValueError(
                f"{get_table_name(struct_cls)}.{name} references {target_table}, which is "
                "not compiled with it; compile both models together so C6, C7 and C9 apply"
            )
        return None
    target_boundary = next((s for s in scope_columns(target) if s.is_boundary), None)
    if boundary is None:
        _check_c7(struct_cls, target_table, target_boundary)
        return None
    if target_boundary is None:
        _check_c9(struct_cls, field.on_delete, target)
        return None
    return _composite_fk(struct_cls, name, field, boundary, target, target_boundary, ref)


def _check_c7(struct_cls: type, target_table: str, target_boundary: ScopeColumn | None) -> None:
    if target_boundary is not None:
        raise ValueError(
            f"C7: {struct_cls.__name__} is unscoped and cannot reference scoped {target_table}"
        )


def _check_c9(struct_cls: type, action: OnDelete | None, target: type) -> None:
    if action is not None and action not in (OnDelete.RESTRICT, OnDelete.NO_ACTION):
        raise ValueError(f"C9: {struct_cls.__name__} references a global table with {action}")
    for group, privileges in declared_privileges(target).items():
        if privileges & {Privilege.UPDATE, Privilege.DELETE}:
            raise ValueError(
                f"C9: {struct_cls.__name__} references {target.__name__}, writable by group {group}"
            )


def _composite_fk(
    struct_cls: type,
    column: str,
    column_field: Field,
    boundary: ScopeColumn,
    target: type,
    target_boundary: ScopeColumn,
    ref: str,
) -> ForeignKeyConstraint:
    action = column_field.on_delete
    if action in (OnDelete.SET_NULL, OnDelete.SET_DEFAULT):
        raise ValueError(f"C6: {struct_cls.__name__}.{column} cannot use {action}")
    if target_boundary.scope != boundary.scope:
        raise ValueError(
            f"C6: {struct_cls.__name__}.{column} references a table scoped by "
            f"{target_boundary.scope!r}, not {boundary.scope!r}"
        )
    _check_c6_target_key(struct_cls, column, target, target_boundary.column, ref)
    target_table = get_table_name(target)
    return ForeignKeyConstraint(
        [boundary.column, column],
        [f"{target_table}.{target_boundary.column}", f"{target_table}.{ref}"],
        ondelete=action.value if action is not None else None,
    )


def _check_c6_target_key(
    struct_cls: type, column: str, target: type, target_boundary: str, ref: str
) -> None:
    fields = get_column_fields(target)
    keys = [tuple(name for name, info in fields.items() if info.field.primary_key)]
    keys += list(declared_unique(target))
    if not any({target_boundary, ref} == set(key) for key in keys):
        raise ValueError(
            f"C6: {struct_cls.__name__}.{column} needs a key on "
            f"({target_boundary}, {ref}) in {target.__name__}"
        )


def _configure_relationships(comp: _Compilation) -> None:
    """Resolve and attach deferred relationships to compiled SA classes."""
    for struct_cls, relations in comp.pending.items():
        if relations:
            _attach_relations(comp.compiled[struct_cls], struct_cls, relations, comp)
    comp.pending.clear()


def _attach_relations(
    sa_cls: Any, struct_cls: type, relations: dict[str, Relation], comp: _Compilation
) -> None:
    hints = resolve_type_hints(struct_cls)
    for rel_name, rel in relations.items():
        target_sa = _resolve_relation_target(rel, hints.get(rel_name), comp)
        if target_sa is not None:
            sa_cls.__mapper__.add_property(
                rel_name,
                relationship(target_sa, **_relationship_kwargs(rel, comp)),
            )


def _relationship_kwargs(rel: Relation, comp: _Compilation) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "lazy": "noload",
        "uselist": _CARDINALITY_USELIST.get(rel.cardinality, True),
        "info": {
            "profiles": rel.profiles,
            "depends_on": rel.depends_on,
        },
    }
    if rel.back_populates:
        kwargs["back_populates"] = rel.back_populates
    if rel.secondary:
        kwargs["secondary"] = _resolve_secondary_table(rel.secondary, comp)
    return kwargs


def _resolve_relation_target(rel: Relation, hint: Any, comp: _Compilation) -> Any:
    """Return the SA class for the given relation, using the field annotation as primary source.

    Annotation-based lookup is exact and avoids ambiguity when multiple tables
    share the same FK column name.  Column-name and table-name scans are kept
    as fallbacks for relations without a resolvable annotation.
    """
    if rel.cardinality == Cardinality.MANY_TO_MANY:
        return _find_target_sa_by_secondary(rel.secondary, rel.foreign_key, comp)

    target_model = extract_model_from_hint(hint)
    if target_model is not None:
        sa = comp.compiled.get(target_model)
        if sa is not None:
            return sa

    if rel.cardinality in (Cardinality.ONE_TO_MANY, Cardinality.ONE_TO_ONE):
        return _find_target_sa_by_fk_column(rel.foreign_key, comp)

    return _find_target_sa_class(_resolve_fk_target_table(rel.foreign_key), comp)


def _fk_col_name(foreign_key: str) -> str:
    """Extract the bare column name from a possibly fully-qualified 'table.column' FK reference.

    Both ``"record_id"`` and ``"bench_notes.record_id"`` return ``"record_id"``.
    """
    return foreign_key.rsplit(".", 1)[-1]


def _find_target_sa_by_fk_column(foreign_key: str, comp: _Compilation) -> Any:
    """For ONE_TO_MANY: the FK column lives on the target table.

    Accepts both short (``"record_id"``) and fully-qualified
    (``"bench_notes.record_id"``) forms — the column name is normalised
    before the ``table.c`` lookup so either format finds the right class.
    """
    col_name = _fk_col_name(foreign_key)
    for _struct_cls, sa_cls in comp.compiled.items():
        table = getattr(sa_cls, "__table__", None)
        if table is None:
            continue
        if col_name in table.c:
            return sa_cls
    return None


def _find_target_sa_by_secondary(
    secondary_name: str | None, foreign_key: str, comp: _Compilation
) -> Any:
    """For MANY_TO_MANY: find the target class that is NOT the secondary table
    and is referenced by the secondary's FK columns.
    """
    if secondary_name is None:
        return None
    secondary_table = _resolve_secondary_table(secondary_name, comp)
    if secondary_table is None:
        return None

    fk_targets: set[str] = set()
    for col in secondary_table.columns:
        for fk in col.foreign_keys:
            fk_targets.add(fk.column.table.name)

    for _struct_cls, sa_cls in comp.compiled.items():
        table_name = getattr(sa_cls, "__tablename__", None)
        if table_name in fk_targets:
            table = getattr(sa_cls, "__table__", None)
            if table is not None and foreign_key not in table.c:
                return sa_cls
    return None


def _resolve_secondary_table(name: str | None, comp: _Compilation) -> Table | None:
    """Resolve a secondary table name to an actual SA Table."""
    if name is None:
        return None
    return comp.metadata.tables.get(name)


def _resolve_loader_model(loader: Any) -> type | None:
    """Extract the concrete model type from a public loader descriptor.

    Returns ``None`` for custom loaders: only a registered descriptor declares a
    ``model`` the compiler may follow.  Both direct references
    (``CountLoader(model=Note)``) and lambda-wrapped forward references
    (``CountLoader(model=lambda: Note)``) are accepted.
    """
    from loom.core.projection.loaders import is_projection_descriptor, resolve_model_reference

    if not is_projection_descriptor(loader):
        return None
    return resolve_model_reference(loader.model)


def _collect_direct_deps(struct_cls: type) -> frozenset[type]:
    """Return all ``BaseModel`` types that *struct_cls* references directly.

    Scans two sources:

    * **Relation annotations** — ``reviews: list[ProductReview]`` yields
      ``ProductReview``.  The annotation is unwrapped through
      :func:`~loom.core.model.introspection.extract_model_from_hint` so
      ``list[X]``, ``X | UnsetType``, etc. all resolve to ``X``.

    * **Projection loaders** — ``CountLoader(model=ProductReview)`` yields
      ``ProductReview``. Lambda-wrapped forward references are resolved by
      calling the callable.

    Pure ``dict`` annotations and non-``BaseModel`` types are ignored, so
    many-to-many relations typed as ``list[dict[str, Any]]`` produce no
    dependency.
    """
    from loom.core.model.base import BaseModel

    deps: set[type] = set()

    hints = resolve_type_hints(struct_cls)

    for rel_name in get_relations(struct_cls):
        hint = hints.get(rel_name)
        if hint is None:
            continue
        model = extract_model_from_hint(hint)
        if model is not None and isinstance(model, type) and issubclass(model, BaseModel):
            deps.add(model)

    for proj in get_projections(struct_cls).values():
        model = _resolve_loader_model(proj.loader)
        if model is not None and issubclass(model, BaseModel):
            deps.add(model)

    return frozenset(deps)


def _topological_sort(
    models: list[type],
    deps: dict[type, frozenset[type]],
) -> list[type]:
    """Kahn's algorithm: return *models* with dependency leaves first.

    Ties are released in the input order of *models*.  If a cycle is detected
    (remaining nodes after BFS exhaustion), the cyclic models are appended in
    their input order.  Circular references are valid because
    ``compile_model`` is idempotent and relationships are resolved after all
    models are registered.
    """
    in_degree, dependents = _dependency_graph(models, deps)
    result = _release_in_dependency_order(models, in_degree, dependents)
    processed = set(result)
    result.extend(m for m in models if m not in processed)
    return result


def _dependency_graph(
    models: list[type],
    deps: dict[type, frozenset[type]],
) -> tuple[dict[type, int], dict[type, list[type]]]:
    """Index the dependency edges that stay within *models*.

    Returns:
        ``in_degree[m]``: how many of ``m``'s deps are in *models*, and
        ``dependents[dep]``: the models depending on ``dep``, in input order.
    """
    model_set = set(models)
    in_degree: dict[type, int] = dict.fromkeys(models, 0)
    dependents: dict[type, list[type]] = {m: [] for m in models}
    for m in models:
        for dep in deps.get(m, frozenset()):
            if dep in model_set:
                in_degree[m] += 1
                dependents[dep].append(m)
    return in_degree, dependents


def _release_in_dependency_order(
    models: list[type],
    in_degree: dict[type, int],
    dependents: dict[type, list[type]],
) -> list[type]:
    """Run Kahn's BFS, consuming *in_degree*; models left in a cycle are omitted."""
    queue: deque[type] = deque(m for m in models if in_degree[m] == 0)
    result: list[type] = []
    while queue:
        node = queue.popleft()
        result.append(node)
        for dependent in dependents[node]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)
    return result


def _resolve_compile_closure(*roots: type) -> tuple[type, ...]:
    """BFS from *roots* to collect all transitive model dependencies.

    Follows relation annotations and projection loader ``model=`` references
    recursively.  Returns all discovered models in topological order
    (dependencies before dependants) so that ``compile_model`` sees each
    child model before its parent when resolving FK targets.

    Args:
        *roots: Explicitly requested model classes.

    Returns:
        Tuple of all models (roots + transitive deps) in compilation order.
    """
    seen: set[type] = set()
    all_deps: dict[type, frozenset[type]] = {}
    queue: deque[type] = deque(roots)

    while queue:
        cls = queue.popleft()
        if cls in seen:
            continue
        seen.add(cls)
        direct = _collect_direct_deps(cls)
        all_deps[cls] = direct
        for dep in direct:
            if dep not in seen:
                queue.append(dep)

    ordered = _topological_sort(list(all_deps.keys()), all_deps)
    return tuple(ordered)


def compile_all(*classes: type, metadata: MetaData | None = None) -> None:
    """Batch-compile multiple model classes, resolve relationships, build Core artifacts.

    Automatically discovers and compiles all transitive model dependencies
    found via relation type annotations and projection loader ``model=``
    references.  Passing only the root models is sufficient — related models
    do not need to be listed explicitly.

    Compilation order is topological: child models (dependency leaves) are
    compiled before their parents so FK lookups during relationship
    resolution always find the target SA class in the registry.

    Args:
        *classes: Root model classes to compile.  Transitive dependencies
            are resolved automatically.

    Example::

        # ProductReview is compiled automatically because Product.reviews
        # is annotated as list[ProductReview].
        compile_all(Product)
    """
    comp = _compilation_for(metadata)
    ordered = _resolve_compile_closure(*classes)
    models_by_table = {get_table_name(cls): cls for cls in ordered}
    for cls in ordered:
        _compile_model(cls, comp, models_by_table)
    _configure_relationships(comp)
    for cls in ordered:
        _compile_core_model(cls, comp)


def get_compiled(struct_cls: type) -> type | None:
    """Look up the compiled SA class for a given Struct model."""
    return _registry.get(struct_cls)


def get_compiled_core(struct_cls: type) -> CoreModel | None:
    """Look up the compiled :class:`~loom.core.backend.core_model.CoreModel` for a Struct model.

    Returns ``None`` if the model has not been compiled via :func:`compile_all`.

    Args:
        struct_cls: The ``BaseModel`` Struct class.

    Returns:
        :class:`CoreModel` instance with column expressions and read methods,
        or ``None``.
    """
    return _core_registry.get(struct_cls)


def get_metadata() -> MetaData:
    """Return the shared metadata for Alembic and table creation."""
    return SABase.metadata


def configured_naming_convention(value: Mapping[str, str] | None) -> dict[str, str] | None:
    """Validate the ``database.schema.naming_convention`` setting.

    Raises:
        ConfigError: Naming the setting and the first unknown kind.
    """
    try:
        return naming_convention(value)
    except ValueError as exc:
        raise ConfigError(f"database.schema.naming_convention: {exc}") from exc


def reset_registry(
    *, naming_convention: Mapping[str, str] | None | _Keep = _KEEP_CONVENTION
) -> None:
    """Clear compiled models and, when asked, set the shared metadata's naming convention.

    The runtime compiles into the shared metadata after this call, so the
    convention it receives, the one ``database.schema.naming_convention``
    declares, names the tables ``create_all`` creates exactly as the
    application metadata of the migration path does. Without the argument the
    convention is left as it is; ``None`` restores SQLAlchemy's default.
    """
    _registry.clear()
    _table_registry.clear()
    _core_registry.clear()
    _pending_relations.clear()
    _shared().scoped.clear()
    SABase.metadata.clear()
    SABase.registry.dispose()
    if naming_convention is _KEEP_CONVENTION:
        return
    SABase.metadata.naming_convention = (
        dict(naming_convention) if naming_convention else DEFAULT_NAMING_CONVENTION
    )


# ---------------------------------------------------------------------------
# Core model compilation
# ---------------------------------------------------------------------------


def _compile_core_model(struct_cls: type, comp: _Compilation) -> None:
    sa_cls = comp.compiled.get(struct_cls)
    if sa_cls is None:
        return

    table: Any = sa_cls.__table__
    id_attr = get_id_attribute(struct_cls)
    column_fields = get_column_fields(struct_cls)
    relations = get_relations(struct_cls)
    projections = get_projections(struct_cls)

    all_columns = tuple(table.c[name] for name in column_fields if name in table.c)

    profiles = _collect_profiles(relations, projections)
    relation_steps = _compile_relation_steps(struct_cls, relations, comp)
    profile_plans = _build_profile_plans(
        profiles, all_columns, id_attr, relations, relation_steps, projections, struct_cls
    )

    core_model = CoreModel(struct_cls, table, id_attr, profile_plans)
    for name, col in zip(column_fields, all_columns, strict=False):
        setattr(core_model, name, col)

    comp.core[struct_cls] = core_model


def _collect_profiles(
    relations: dict[str, Any],
    projections: dict[str, Any],
) -> set[str]:
    profiles: set[str] = {"default"}
    for rel in relations.values():
        profiles.update(rel.profiles)
    for proj in projections.values():
        profiles.update(proj.profiles)
    return profiles


def _build_profile_plans(
    profiles: set[str],
    all_columns: tuple[Any, ...],
    id_attr: str,
    relations: dict[str, Any],
    relation_steps: dict[str, CoreRelationStep],
    projections: dict[str, Any],
    struct_cls: type,
) -> dict[str, CoreProfilePlan]:
    plans: dict[str, CoreProfilePlan] = {}
    for profile in profiles:
        profile_steps = tuple(
            relation_steps[name]
            for name, rel in relations.items()
            if name in relation_steps and profile in rel.profiles
        )
        profile_relation_names = frozenset(step.attr for step in profile_steps)
        profile_projections = {
            name: proj for name, proj in projections.items() if profile in proj.profiles
        }
        resolved_steps = _resolve_projection_steps(
            profile_projections, profile_relation_names, relation_steps, struct_cls
        )
        plans[profile] = CoreProfilePlan(
            columns=all_columns,
            relation_steps=profile_steps,
            projection_plan=build_projection_plan_from_steps(resolved_steps),
            id_attr=id_attr,
        )
    return plans


def _custom_loader_prefers_memory(loader: Any) -> bool:
    """Return True if the custom loader implements the memory-path protocol.

    Checks for a callable ``load_from_object`` attribute without triggering
    descriptors, using ``inspect.getattr_static``.

    Args:
        loader: Any projection loader object.

    Returns:
        ``True`` when ``loader.load_from_object`` exists and is callable.
    """
    import inspect as _inspect

    try:
        return callable(_inspect.getattr_static(loader, "load_from_object"))
    except AttributeError:
        return False


def _resolve_descriptor_loader(
    name: str,
    loader: Any,
    profile_relation_names: frozenset[str],
    all_relation_steps: dict[str, CoreRelationStep],
    struct_cls: type,
) -> tuple[Any, bool]:
    """Resolve a public loader descriptor to ``(loader, prefer_memory)``.

    Picks memory-path when the target relation is loaded in the active profile,
    SQL-path when it is not.  Falls back to memory-path via type-hint scanning
    when the related model has not been compiled yet.

    Args:
        name: Projection field name (used in error messages).
        loader: A registered projection loader descriptor.
        profile_relation_names: Relation attributes loaded in the current profile.
        all_relation_steps: All compiled relation steps for the parent model.
        struct_cls: Parent model class (used for type-hint fallback).

    Returns:
        ``(resolved_loader, prefer_memory)`` tuple.

    Raises:
        ValueError: When no matching relation is found via compiled steps or type hints.
    """
    from loom.core.projection.loaders import find_relation_name_for_loader, make_memory_loader
    from loom.core.repository.sqlalchemy.loaders import make_sql_loader

    rel_step = _find_relation_for_loader(loader, all_relation_steps)
    if rel_step is None:
        rel_name = find_relation_name_for_loader(loader, struct_cls)
        if rel_name is None:
            raise ValueError(
                f"Projection '{name}': no relation found for "
                f"{type(loader).__name__}(model={loader.model.__name__}) "
                f"on {struct_cls.__name__}. "
                "Ensure the model has a relation typed with the target class."
            )
        return make_memory_loader(loader, rel_name), True

    if rel_step.attr in profile_relation_names:
        return make_memory_loader(loader, rel_step.attr), True

    return make_sql_loader(loader, rel_step), False


def _resolve_projection_steps(
    projections: dict[str, Any],
    profile_relation_names: frozenset[str],
    all_relation_steps: dict[str, CoreRelationStep],
    struct_cls: type,
) -> dict[str, ProjectionStep]:
    """Resolve each projection to a ``ProjectionStep`` with the right loader strategy.

    For a registered loader descriptor the compiler picks between memory-path
    (when the target relation is loaded in this profile) and SQL-path (when it is
    not).  Custom loaders are auto-detected via capability inspection
    (``load_from_object`` vs ``load_many``).

    Args:
        projections: Profile-filtered projection metadata.
        profile_relation_names: Relation attribute names loaded in this profile.
        all_relation_steps: Compiled relation steps for all relations on the model.
        struct_cls: Parent model class.

    Returns:
        Mapping of field name to resolved :class:`ProjectionStep`.

    Raises:
        ValueError: If a descriptor loader cannot be matched to a relation step.
    """
    from loom.core.projection.loaders import is_projection_descriptor

    steps: dict[str, ProjectionStep] = {}
    for name, proj in projections.items():
        loader = proj.loader
        if is_projection_descriptor(loader):
            resolved_loader, prefer_memory = _resolve_descriptor_loader(
                name, loader, profile_relation_names, all_relation_steps, struct_cls
            )
        else:
            resolved_loader, prefer_memory = loader, _custom_loader_prefers_memory(loader)

        steps[name] = ProjectionStep(
            name=name,
            projection=proj,
            prefer_memory=prefer_memory,
            loader=resolved_loader,
        )
    return steps


def _find_relation_for_loader(
    loader: Any,
    all_relation_steps: dict[str, CoreRelationStep],
) -> CoreRelationStep | None:
    """Find the compiled relation step matching a public loader descriptor.

    Uses ``loader.via`` if provided, otherwise matches by ``related_struct``.

    Args:
        loader: A registered projection loader descriptor.
        all_relation_steps: All compiled relation steps for the parent model.

    Returns:
        Matching :class:`CoreRelationStep`, or ``None`` if not found.
    """
    via = getattr(loader, "via", None)
    if via is not None:
        return all_relation_steps.get(via)

    target_model = loader.model
    for step in all_relation_steps.values():
        if step.related_struct is target_model:
            return step
    return None


def _compile_relation_steps(
    struct_cls: type,
    relations: dict[str, Any],
    comp: _Compilation,
) -> dict[str, CoreRelationStep]:
    steps: dict[str, CoreRelationStep] = {}
    for rel_name, rel in relations.items():
        step = _compile_relation_step(struct_cls, rel_name, rel, comp)
        if step is not None:
            steps[rel_name] = step
    return steps


def _compile_relation_step(
    struct_cls: type,
    rel_name: str,
    rel: Relation,
    comp: _Compilation,
) -> CoreRelationStep | None:
    if rel.cardinality in (Cardinality.ONE_TO_MANY, Cardinality.ONE_TO_ONE):
        return _compile_one_to_x_step(struct_cls, rel_name, rel, comp)
    if rel.cardinality is Cardinality.MANY_TO_ONE:
        return _compile_many_to_one_step(struct_cls, rel_name, rel, comp)
    if rel.cardinality is Cardinality.MANY_TO_MANY:
        return _compile_many_to_many_step(struct_cls, rel_name, rel, comp)
    return None


def _compile_one_to_x_step(
    struct_cls: type,
    rel_name: str,
    rel: Relation,
    comp: _Compilation,
) -> CoreRelationStep | None:
    hint = resolve_type_hints(struct_cls).get(rel_name)
    target_sa = _resolve_relation_target(rel, hint, comp)
    if target_sa is None:
        return None
    related_struct = getattr(target_sa, "__struct_cls__", None)
    if related_struct is None:
        return None
    target_table: Any = target_sa.__table__
    target_cols = _target_cols(related_struct, target_table)
    return CoreRelationStep(
        attr=rel_name,
        cardinality=rel.cardinality,
        target_table=target_table,
        target_cols=target_cols,
        related_struct=related_struct,
        owner_pk_col=get_id_attribute(struct_cls),
        fk_col=_fk_col_name(rel.foreign_key),
        pk_col=get_id_attribute(struct_cls),
    )


def _compile_many_to_one_step(
    struct_cls: type,
    rel_name: str,
    rel: Relation,
    comp: _Compilation,
) -> CoreRelationStep | None:
    target_table_name, target_pk_name = rel.foreign_key.rsplit(".", 1)
    target_sa = _find_target_sa_class(target_table_name, comp)
    if target_sa is None:
        return None
    related_struct = getattr(target_sa, "__struct_cls__", None)
    if related_struct is None:
        return None
    owner_fk_col = _find_owner_fk_col(struct_cls, rel.foreign_key)
    if owner_fk_col is None:
        return None
    target_table: Any = target_sa.__table__
    target_cols = _target_cols(related_struct, target_table)
    return CoreRelationStep(
        attr=rel_name,
        cardinality=rel.cardinality,
        target_table=target_table,
        target_cols=target_cols,
        related_struct=related_struct,
        owner_pk_col=get_id_attribute(struct_cls),
        fk_col=owner_fk_col,
        pk_col=target_pk_name,
    )


def _compile_many_to_many_step(
    struct_cls: type,
    rel_name: str,
    rel: Relation,
    comp: _Compilation,
) -> CoreRelationStep | None:
    if rel.secondary is None:
        return None
    secondary_table: Any = comp.metadata.tables.get(rel.secondary)
    if secondary_table is None:
        return None
    owner_table_name = get_table_name(struct_cls)
    secondary_owner_fk, secondary_target_fk, target_table_name = _inspect_secondary(
        secondary_table, owner_table_name
    )
    if secondary_owner_fk is None or secondary_target_fk is None or target_table_name is None:
        return None
    target_sa = _find_target_sa_class(target_table_name, comp)
    if target_sa is None:
        return None
    related_struct = getattr(target_sa, "__struct_cls__", None)
    if related_struct is None:
        return None
    target_table: Any = target_sa.__table__
    target_cols = _target_cols(related_struct, target_table)
    return CoreRelationStep(
        attr=rel_name,
        cardinality=rel.cardinality,
        target_table=target_table,
        target_cols=target_cols,
        related_struct=related_struct,
        owner_pk_col=get_id_attribute(struct_cls),
        fk_col=secondary_owner_fk,
        pk_col=get_id_attribute(related_struct),
        secondary_table=secondary_table,
        secondary_target_fk=secondary_target_fk,
    )


def _inspect_secondary(
    secondary_table: Any,
    owner_table_name: str,
) -> tuple[str | None, str | None, str | None]:
    owner_fk: str | None = None
    target_fk: str | None = None
    target_table_name: str | None = None
    for col in secondary_table.columns:
        for fk in col.foreign_keys:
            if fk.column.table.name == owner_table_name:
                owner_fk = col.name
            else:
                target_fk = col.name
                target_table_name = fk.column.table.name
    return owner_fk, target_fk, target_table_name


def _target_cols(related_struct: type, target_table: Any) -> tuple[Any, ...]:
    col_fields = get_column_fields(related_struct)
    return tuple(target_table.c[name] for name in col_fields if name in target_table.c)


def _find_owner_fk_col(struct_cls: type, foreign_key: str) -> str | None:
    for name, col_info in get_column_fields(struct_cls).items():
        if col_info.field.foreign_key == foreign_key:
            return name
    return None
