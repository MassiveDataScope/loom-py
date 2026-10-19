"""Core read artifact compiled from a ``BaseModel`` declaration.

:class:`CoreModel` is the read-only counterpart of the ORM-compiled SA class.
It is created automatically by :func:`~loom.core.backend.sqlalchemy.compile_all`
and retrieved via :func:`~loom.core.backend.sqlalchemy.get_compiled_core`.

Usage example::

    ProductCore = get_compiled_core(Product)

    stmt = ProductCore.select("default").where(ProductCore.price > 100)
    products = await ProductCore.fetch_all(session, stmt, profile="default")

Column expressions are available as instance attributes::

    ProductCore.id        # → SA Column expression
    ProductCore.name      # → SA Column expression

The ORM class (``ProductSA``) is kept exclusively for writes and Alembic.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import Column, Select, Table, select

from loom.core.model.convert import to_struct
from loom.core.model.enums import Cardinality
from loom.core.projection.runtime import ProjectionPlan, execute_projection_plan

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

_LOOM_OWNER_PK = "_loom_owner_pk"

_USELIST_CARDINALITIES = frozenset({Cardinality.ONE_TO_MANY, Cardinality.MANY_TO_MANY})


# ---------------------------------------------------------------------------
# Compiled plan structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CoreRelationStep:
    """Compiled batch-load descriptor for one relation in a Core read profile.

    Field semantics per cardinality:

    * ``ONE_TO_MANY`` / ``ONE_TO_ONE``:
      ``fk_col`` = FK column on the *target* table (e.g. ``"bench_record_id"``).
      ``pk_col`` = PK column on the *owner* table (e.g. ``"id"``).

    * ``MANY_TO_ONE``:
      ``fk_col`` = FK column on the *owner* row (e.g. ``"category_id"``).
      ``pk_col`` = PK column on the *target* table (e.g. ``"id"``).

    * ``MANY_TO_MANY``:
      ``fk_col`` = owner FK column in the secondary table.
      ``pk_col`` = target column the secondary table's FK references (a key
      or unique column).
      ``secondary_table`` / ``secondary_target_fk`` required.
    """

    attr: str
    cardinality: Cardinality
    target_table: Table
    target_cols: tuple[Column, ...]  # type: ignore[type-arg]
    related_struct: type
    owner_pk_col: str
    fk_col: str
    pk_col: str
    secondary_table: Table | None = None
    secondary_target_fk: str | None = None


@dataclass(frozen=True, slots=True)
class CoreProfilePlan:
    """Compiled read plan for one profile — columns, relation steps, projections."""

    columns: tuple[Column, ...]  # type: ignore[type-arg]
    relation_steps: tuple[CoreRelationStep, ...]
    projection_plan: ProjectionPlan
    id_attr: str


# ---------------------------------------------------------------------------
# Core entity adapter — bridges RowMapping + relation data to memory loaders
# ---------------------------------------------------------------------------


class _CoreEntityAdapter:
    """Wraps a Core ``RowMapping`` plus pre-loaded relation data.

    ``__slots__`` keeps the adapter lean and guarantees relation data is read
    from the preloaded mapping rather than a mutable instance dictionary.
    """

    __slots__ = ("_row", "_relations")

    def __init__(self, row: Any, relations: dict[str, Any]) -> None:
        object.__setattr__(self, "_row", row)
        object.__setattr__(self, "_relations", relations)

    def __getattr__(self, name: str) -> Any:
        relations: dict[str, Any] = object.__getattribute__(self, "_relations")
        if name in relations:
            return relations[name]
        row: Any = object.__getattribute__(self, "_row")
        try:
            return row[name]
        except KeyError:
            raise AttributeError(name) from None


# ---------------------------------------------------------------------------
# CoreModel — public read artifact
# ---------------------------------------------------------------------------


class CoreModel:
    """Compiled Core read artifact for a Struct model.

    Created by :func:`~loom.core.backend.sqlalchemy.compile_all`.
    Column expressions are set as instance attributes at compile time::

        ProductCore.id    # SA Column expression usable in WHERE clauses
        ProductCore.name  # SA Column expression

    Args:
        struct_cls: The source ``BaseModel`` Struct class.
        table: The compiled SA ``Table``.
        id_attr: Name of the primary-key field on ``struct_cls``.
        profiles: Mapping of profile name → :class:`CoreProfilePlan`.
    """

    def __init__(
        self,
        struct_cls: type,
        table: Table,
        id_attr: str,
        profiles: dict[str, CoreProfilePlan],
    ) -> None:
        self._struct_cls = struct_cls
        self._table = table
        self._id_attr = id_attr
        self._profiles = profiles

    def _get_plan(self, profile: str) -> CoreProfilePlan:
        plan = self._profiles.get(profile)
        if plan is None:
            raise KeyError(
                f"Profile '{profile}' is not defined for {self._struct_cls.__name__}Core."
            )
        return plan

    def select(self, profile: str = "default") -> Select:  # type: ignore[type-arg]
        """Return a Core ``SELECT`` statement for the given profile's columns.

        Args:
            profile: Profile name. Defaults to ``"default"``.

        Returns:
            A SQLAlchemy ``Select`` targeting this model's table.
        """
        plan = self._get_plan(profile)
        return select(*plan.columns).select_from(self._table)

    async def fetch_one(
        self,
        session: AsyncSession,
        stmt: Select,  # type: ignore[type-arg]
        profile: str = "default",
    ) -> Any | None:
        """Execute ``stmt``, load relations, run projections, return Struct or ``None``.

        Args:
            session: Active async SQLAlchemy session.
            stmt: ``SELECT`` statement (typically from :meth:`select`).
            profile: Profile name. Defaults to ``"default"``.

        Returns:
            Assembled Struct instance or ``None`` if no row matched.
        """
        plan = self._get_plan(profile)
        result = await session.execute(stmt)
        row = result.mappings().first()
        if row is None:
            return None
        assembled = await self._assemble(session, [row], plan)
        return assembled[0] if assembled else None

    async def fetch_all(
        self,
        session: AsyncSession,
        stmt: Select,  # type: ignore[type-arg]
        profile: str = "default",
    ) -> list[Any]:
        """Execute ``stmt``, batch-load relations, run projections, return ``list[Struct]``.

        Relations declared in the profile are loaded one after another on
        ``session``, with at most one statement each.  Projection steps run
        after relation loading.

        Args:
            session: Active async SQLAlchemy session.
            stmt: ``SELECT`` statement (typically from :meth:`select`).
            profile: Profile name. Defaults to ``"default"``.

        Returns:
            List of assembled Struct instances (empty if no rows matched).
        """
        plan = self._get_plan(profile)
        result = await session.execute(stmt)
        rows = list(result.mappings())
        if not rows:
            return []
        return await self._assemble(session, rows, plan)

    async def fetch_all_with_total(
        self,
        session: AsyncSession,
        stmt: Select,  # type: ignore[type-arg]
        profile: str = "default",
        *,
        total_alias: str = "__loom_total_count",
    ) -> tuple[list[Any], int]:
        """Execute a paged statement that includes a window total-count column.

        The statement must include a labeled column matching ``total_alias``
        (for example ``func.count().over().label("__loom_total_count")``).
        Returns assembled entities plus the total count extracted from the first row.
        """
        plan = self._get_plan(profile)
        result = await session.execute(stmt)
        rows = list(result.mappings())
        if not rows:
            return [], 0

        total_count = int(rows[0][total_alias] or 0)
        stripped_rows = [{k: value for k, value in row.items() if k != total_alias} for row in rows]
        return await self._assemble(session, stripped_rows, plan), total_count

    # ------------------------------------------------------------------
    # Internal assembly
    # ------------------------------------------------------------------

    async def _assemble(
        self,
        session: AsyncSession,
        rows: list[Any],
        plan: CoreProfilePlan,
    ) -> list[Any]:
        relation_data = await _load_relations(session, plan.relation_steps, rows)
        adapters = [_make_adapter(row, plan, relation_data) for row in rows]
        projection_values = await execute_projection_plan(
            plan.projection_plan,
            objs=adapters,
            id_attr=plan.id_attr,
            backend_context=session,
        )
        return [
            _build_struct(self._struct_cls, row, i, plan, relation_data, projection_values)
            for i, row in enumerate(rows)
        ]


# ---------------------------------------------------------------------------
# Assembly helpers
# ---------------------------------------------------------------------------


async def _load_relations(
    session: AsyncSession,
    steps: tuple[CoreRelationStep, ...],
    rows: list[Any],
) -> dict[str, dict[Any, Any]]:
    return {step.attr: await _fetch_relation_batch(session, step, rows) for step in steps}


def _make_adapter(
    row: Any,
    plan: CoreProfilePlan,
    relation_data: dict[str, dict[Any, Any]],
) -> _CoreEntityAdapter:
    owner_pk = row[plan.id_attr]
    relations = {
        step.attr: relation_data[step.attr].get(
            owner_pk, [] if step.cardinality in _USELIST_CARDINALITIES else None
        )
        for step in plan.relation_steps
    }
    return _CoreEntityAdapter(row, relations)


def _build_struct(
    struct_cls: type,
    row: Any,
    index: int,
    plan: CoreProfilePlan,
    relation_data: dict[str, dict[Any, Any]],
    projection_values: dict[int, dict[str, Any]],
) -> Any:
    owner_pk = row[plan.id_attr]
    relation_kwargs = {
        step.attr: relation_data[step.attr].get(
            owner_pk, [] if step.cardinality in _USELIST_CARDINALITIES else None
        )
        for step in plan.relation_steps
    }
    return to_struct(
        struct_cls, {**dict(row), **relation_kwargs, **projection_values.get(index, {})}
    )


# ---------------------------------------------------------------------------
# Relation batch fetchers
# ---------------------------------------------------------------------------


async def _fetch_relation_batch(
    session: AsyncSession,
    step: CoreRelationStep,
    parent_rows: list[Any],
) -> dict[Any, Any]:
    fetcher = _RELATION_FETCHERS.get(step.cardinality)
    if fetcher is None:
        return {}
    result: dict[Any, Any] = await fetcher(session, step, parent_rows)
    return result


async def _fetch_one_to_many(
    session: AsyncSession,
    step: CoreRelationStep,
    parent_rows: Sequence[Any],
) -> dict[Any, list[Any]]:
    """Batch-load ONE_TO_MANY: dict[owner_pk → list[Struct]]."""
    parent_ids = list({row[step.owner_pk_col] for row in parent_rows})
    fk_col = step.target_table.c[step.fk_col]
    stmt = select(*step.target_cols).where(fk_col.in_(parent_ids))
    result = await session.execute(stmt)
    by_parent: dict[Any, list[Any]] = {}
    for row in result.mappings():
        key = row[step.fk_col]
        by_parent.setdefault(key, []).append(_build_related(step, row))
    return by_parent


async def _fetch_one_to_one(
    session: AsyncSession,
    step: CoreRelationStep,
    parent_rows: Sequence[Any],
) -> dict[Any, Any]:
    """Batch-load ONE_TO_ONE: dict[owner_pk → Struct | None]."""
    parent_ids = list({row[step.owner_pk_col] for row in parent_rows})
    fk_col = step.target_table.c[step.fk_col]
    stmt = select(*step.target_cols).where(fk_col.in_(parent_ids))
    result = await session.execute(stmt)
    return {row[step.fk_col]: _build_related(step, row) for row in result.mappings()}


async def _fetch_many_to_one(
    session: AsyncSession,
    step: CoreRelationStep,
    parent_rows: Sequence[Any],
) -> dict[Any, Any]:
    """Batch-load MANY_TO_ONE: dict[owner_pk → Struct | None]."""
    owner_to_fk = {
        row[step.owner_pk_col]: row[step.fk_col]
        for row in parent_rows
        if row[step.fk_col] is not None
    }
    if not owner_to_fk:
        return {}
    fk_values = list(set(owner_to_fk.values()))
    pk_col = step.target_table.c[step.pk_col]
    stmt = select(*step.target_cols).where(pk_col.in_(fk_values))
    result = await session.execute(stmt)
    fk_to_related = {row[step.pk_col]: _build_related(step, row) for row in result.mappings()}
    return {owner_pk: fk_to_related.get(fk_val) for owner_pk, fk_val in owner_to_fk.items()}


async def _fetch_many_to_many(
    session: AsyncSession,
    step: CoreRelationStep,
    parent_rows: Sequence[Any],
) -> dict[Any, list[Any]]:
    """Batch-load MANY_TO_MANY: dict[owner_pk → list[Struct]]."""
    if step.secondary_table is None or step.secondary_target_fk is None:
        return {}
    parent_ids = list({row[step.owner_pk_col] for row in parent_rows})
    owner_fk = step.secondary_table.c[step.fk_col]
    target_fk = step.secondary_table.c[step.secondary_target_fk]
    target_pk = step.target_table.c[step.pk_col]
    stmt = (
        select(*step.target_cols, owner_fk.label(_LOOM_OWNER_PK))
        .join(step.secondary_table, target_fk == target_pk)
        .where(owner_fk.in_(parent_ids))
    )
    result = await session.execute(stmt)
    by_parent: dict[Any, list[Any]] = {}
    for row in result.mappings():
        key = row[_LOOM_OWNER_PK]
        by_parent.setdefault(key, []).append(_build_related(step, row))
    return by_parent


def _build_related(step: CoreRelationStep, row: Any) -> Any:
    return step.related_struct(**{col.key: row[col.key] for col in step.target_cols})


_RELATION_FETCHERS: dict[Cardinality, Any] = {
    Cardinality.ONE_TO_MANY: _fetch_one_to_many,
    Cardinality.ONE_TO_ONE: _fetch_one_to_one,
    Cardinality.MANY_TO_ONE: _fetch_many_to_one,
    Cardinality.MANY_TO_MANY: _fetch_many_to_many,
}
