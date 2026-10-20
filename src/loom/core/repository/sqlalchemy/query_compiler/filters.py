"""FilterSpec → SQLAlchemy WHERE clause compiler."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

from sqlalchemy import and_, or_
from sqlalchemy.sql.expression import ClauseElement

from loom.core.errors.codes import ErrorCode
from loom.core.repository.abc.query import FilterGroup, FilterOp, FilterSpec
from loom.core.repository.sqlalchemy.query_compiler.errors import UnsafeFilterError
from loom.core.repository.sqlalchemy.query_compiler.paths import resolve_column
from loom.core.repository.sqlalchemy.query_compiler.subquery import (
    compile_exists,
    compile_not_exists,
)
from loom.core.repository.sqlalchemy.values import (
    is_json_column,
    refuse_aware_time,
    to_column_value,
)

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
    """Apply *op*, converting *value* as the repository converts a value it writes.

    A SQLAlchemy expression, and an ``IN`` value that is a string or not
    iterable, reach SQLAlchemy as they are.
    """
    handler = _SIMPLE_OPS.get(op)
    if handler is None:
        raise ValueError(f"Unsupported FilterOp: {op!r}")
    if isinstance(value, ClauseElement):
        return handler(col, value)
    json = is_json_column(col)
    if op is not FilterOp.IN:
        return handler(col, _bound_value(col, value, json=json))
    if isinstance(value, str | bytes) or not isinstance(value, Iterable):
        return handler(col, value)
    return handler(col, [_bound_value(col, item, json=json) for item in value])


def _bound_value(col: Any, value: Any, *, json: bool) -> Any:
    """Convert a filter value as the write path does, refusing what the write refuses."""
    if not json:
        refuse_aware_time(col.key, value, code=ErrorCode.QUERY_VALIDATION)
    return to_column_value(value, json=json)
