"""SortSpec → SQLAlchemy ORDER BY clause compiler."""

from __future__ import annotations

from typing import Any

from loom.core.repository.sqlalchemy.query_compiler.paths import resolve_column
from loom.core.stores.repository.query import SortSpec


def compile_order_by(sa_model: type[Any], sort: tuple[SortSpec, ...]) -> list[Any]:
    """Compile a sequence of :class:`SortSpec` into SQLAlchemy ORDER BY clauses.

    Args:
        sa_model: Root SQLAlchemy mapped model class.
        sort: Ordered sort directives.

    Returns:
        List of SQLAlchemy column expressions with direction applied.
        Empty list when ``sort`` is empty.

    Raises:
        FilterPathError: If a field path cannot be resolved.

    Example::

        clauses = compile_order_by(ProductSA, (SortSpec("price", "DESC"),))
        stmt = stmt.order_by(*clauses)
    """
    result: list[Any] = []
    for spec in sort:
        col = resolve_column(sa_model, spec.field)
        result.append(col.desc() if spec.direction == "DESC" else col.asc())
    return result
