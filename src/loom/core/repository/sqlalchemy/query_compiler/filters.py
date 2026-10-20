"""FilterSpec → SQLAlchemy WHERE clause compiler."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from sqlalchemy import JSON, and_, or_
from sqlalchemy.sql.expression import ClauseElement

from loom.core.repository.abc.query import FilterGroup, FilterOp, FilterSpec
from loom.core.repository.sqlalchemy.query_compiler.errors import UnsafeFilterError
from loom.core.repository.sqlalchemy.query_compiler.paths import resolve_column
from loom.core.repository.sqlalchemy.query_compiler.subquery import (
    compile_exists,
    compile_not_exists,
)
from loom.core.repository.sqlalchemy.values import to_column_value

_RELATION_OPS = frozenset([FilterOp.EXISTS, FilterOp.NOT_EXISTS])
_SIMPLE_OPS: dict[FilterOp, Callable[[Any, Any], Any]] = {
    FilterOp.EQ: lambda col, value: col == value,
    FilterOp.NE: lambda col, value: col != value,
    FilterOp.GT: lambda col, value: col > value,
    FilterOp.GTE: lambda col, value: col >= value,
    FilterOp.LT: lambda col, value: col < value,
    FilterOp.LTE: lambda col, value: col <= value,
    FilterOp.IN: lambda col, value: col.in_(value),
    FilterOp.LIKE: lambda col, value: col.like(value),
    FilterOp.ILIKE: lambda col, value: col.ilike(value),
    FilterOp.IS_NULL: lambda col, _value: col.is_(None),
}


def compile_filter_group(
    sa_model: type[Any],
    group: FilterGroup,
    allowed_fields: frozenset[str],
) -> Any:
    """Compile a :class:`FilterGroup` into a SQLAlchemy boolean clause.

    Args:
        sa_model: Root SQLAlchemy mapped model class.
        group: Filter group to compile.
        allowed_fields: Permitted field names.  Empty frozenset allows all.

    Returns:
        A SQLAlchemy clause element (``and_`` or ``or_``).

    Raises:
        UnsafeFilterError: If a field is not in ``allowed_fields``.
        FilterPathError: If a field path cannot be resolved.
    """
    clauses = [_compile_spec(sa_model, spec, allowed_fields) for spec in group.filters]
    if group.op == "OR":
        return or_(*clauses)
    return and_(*clauses)


def _compile_spec(
    sa_model: type[Any],
    spec: FilterSpec,
    allowed_fields: frozenset[str],
) -> Any:
    root_field = spec.field.split(".")[0]
    if allowed_fields and root_field not in allowed_fields:
        raise UnsafeFilterError(root_field)

    if spec.op == FilterOp.EXISTS:
        return compile_exists(sa_model, spec.field)
    if spec.op == FilterOp.NOT_EXISTS:
        return compile_not_exists(sa_model, spec.field)

    col = resolve_column(sa_model, spec.field)
    return _apply_op(col, spec.op, spec.value)


def _apply_op(col: Any, op: FilterOp, value: Any) -> Any:
    handler = _SIMPLE_OPS.get(op)
    if handler is not None:
        return handler(col, _column_value(col, op, value))
    raise ValueError(f"Unsupported FilterOp: {op!r}")


def _column_value(col: Any, op: FilterOp, value: Any) -> Any:
    """Convert *value*, or each item of an ``IN`` list, as the repository writes a value.

    A SQLAlchemy expression, and an ``IN`` value that is not a list or tuple,
    reach SQLAlchemy as they are.
    """
    json = isinstance(getattr(col, "type", None), JSON)
    if op is FilterOp.IN:
        if not isinstance(value, list | tuple):
            return value
        return [_bound_value(item, json=json) for item in value]
    return _bound_value(value, json=json)


def _bound_value(value: Any, *, json: bool) -> Any:
    if isinstance(value, ClauseElement):
        return value
    return to_column_value(value, json=json)
