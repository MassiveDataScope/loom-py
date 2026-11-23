"""SQL-path projection loaders.

The descriptor registry and its factory table are internal to Loom. An
application supplies its own loader object instead, which both paths pass
through untouched.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Sequence
from typing import Any, Protocol

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.selectable import FromClause

from loom.core.errors import InternalError
from loom.core.projection.loaders import (
    CountLoader,
    ExistsLoader,
    JoinFieldsLoader,
    is_projection_descriptor,
)


class ProjectionLoader(Protocol):
    """Protocol for batch-loading derived field values for a set of parent entities."""

    async def load_many(
        self, session: AsyncSession, parent_ids: Sequence[object]
    ) -> dict[object, Any]:
        """Load derived values for the given parent ids.

        Args:
            session: Active SQLAlchemy async session.
            parent_ids: Primary keys of the parent entities.

        Returns:
            Mapping from parent id to the computed value.
        """
        ...


class _SqlCountLoader:
    """SQL loader that counts related rows per parent id."""

    __slots__ = ("_table", "_fk_col")

    def __init__(self, descriptor: CountLoader, table: FromClause, fk_col: str) -> None:
        self._table = table
        self._fk_col = fk_col

    async def load_many(
        self, session: AsyncSession, parent_ids: Sequence[object]
    ) -> dict[object, int]:
        if not parent_ids:
            return {}
        fk_column = self._table.c[self._fk_col]
        stmt = (
            select(fk_column, func.count().label("count"))
            .where(fk_column.in_(list(parent_ids)))
            .group_by(fk_column)
        )
        rows = await session.execute(stmt)
        return {row[0]: int(row[1]) for row in rows.all()}


class _SqlExistsLoader:
    """SQL loader that checks existence of related rows per parent id."""

    __slots__ = ("_table", "_fk_col")

    def __init__(self, descriptor: ExistsLoader, table: FromClause, fk_col: str) -> None:
        self._table = table
        self._fk_col = fk_col

    async def load_many(
        self, session: AsyncSession, parent_ids: Sequence[object]
    ) -> dict[object, bool]:
        if not parent_ids:
            return {}
        fk_column = self._table.c[self._fk_col]
        stmt = select(fk_column).where(fk_column.in_(list(parent_ids))).distinct()
        rows = await session.execute(stmt)
        return {row[0]: True for row in rows.all()}


class _SqlJoinFieldsLoader:
    """SQL loader that fetches selected columns from a related table, grouped by parent."""

    __slots__ = ("_table", "_fk_col", "_value_columns")

    def __init__(self, descriptor: JoinFieldsLoader, table: FromClause, fk_col: str) -> None:
        self._table = table
        self._fk_col = fk_col
        self._value_columns = descriptor.value_columns

    async def load_many(
        self, session: AsyncSession, parent_ids: Sequence[object]
    ) -> dict[object, list[dict[str, Any]]]:
        if not parent_ids:
            return {}
        fk_column = self._table.c[self._fk_col]
        selected = [fk_column] + [self._table.c[col] for col in self._value_columns]
        stmt = select(*selected).where(fk_column.in_(list(parent_ids)))
        rows = await session.execute(stmt)
        grouped: dict[object, list[dict[str, Any]]] = defaultdict(list)
        for row in rows.all():
            parent_id = row[0]
            payload = {name: row[i + 1] for i, name in enumerate(self._value_columns)}
            grouped[parent_id].append(payload)
        return dict(grouped)


_SqlLoaderFactory = Callable[[Any, FromClause, str], Any]

_SQL_LOADER_FACTORIES: dict[type, _SqlLoaderFactory] = {
    CountLoader: _SqlCountLoader,
    ExistsLoader: _SqlExistsLoader,
    JoinFieldsLoader: _SqlJoinFieldsLoader,
}
"""SQL-path counterpart of every projection descriptor."""


def _find_sql_loader_factory(loader_type: type) -> _SqlLoaderFactory | None:
    """Look up the factory for *loader_type*, honouring descriptor subclasses."""
    for klass in loader_type.__mro__:
        factory = _SQL_LOADER_FACTORIES.get(klass)
        if factory is not None:
            return factory
    return None


def build_sql_loader(loader: Any, rel_step: Any) -> Any:
    """Create an SQL-path loader from a public descriptor and a compiled relation step.

    Args:
        loader: A projection loader descriptor, or any custom loader.
        rel_step: A ``CoreRelationStep`` providing ``target_table`` and ``fk_col``.

    Returns:
        The SQL loader built by the matching factory, or ``loader`` unchanged
        when it is a custom loader.

    Raises:
        InternalError: When *loader* is a registered descriptor with no SQL-path factory.
    """
    factory = _find_sql_loader_factory(type(loader))
    if factory is not None:
        return factory(loader, rel_step.target_table, rel_step.fk_col)
    if is_projection_descriptor(loader):
        raise InternalError(
            f"Projection descriptor {type(loader).__name__} has no SQL-path loader "
            "factory registered; add one to _SQL_LOADER_FACTORIES in "
            "loom.stores.sqlalchemy.loaders."
        )
    return loader
