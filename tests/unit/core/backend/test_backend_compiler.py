from __future__ import annotations

from collections.abc import Generator
from datetime import date, time
from enum import Enum
from typing import Annotated, Any, Literal, cast

import pytest
from sqlalchemy import MetaData
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from loom.core.backend.sqlalchemy import (
    compile_all,
    compile_model,
    get_compiled,
    get_metadata,
    reset_registry,
)
from loom.core.model import (
    BaseModel,
    Cardinality,
    ColumnField,
    OnDelete,
    ProjectionField,
    RelationField,
    TimestampedModel,
)
from loom.core.model.types import String
from loom.core.projection.loaders import ExistsLoader


class _Product(BaseModel):
    __tablename__ = "test_products"
    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=120)
    price: float = ColumnField()

    reviews: list[dict[str, object]] = RelationField(
        foreign_key="product_id",
        cardinality=Cardinality.ONE_TO_MANY,
        on_delete=OnDelete.CASCADE,
        profiles=("with_details",),
        depends_on=("test_reviews:product_id",),
    )


class _Review(BaseModel):
    __tablename__ = "test_reviews"
    id: int = ColumnField(primary_key=True, autoincrement=True)
    product_id: int = ColumnField(foreign_key="test_products.id", on_delete=OnDelete.CASCADE)
    comment: str = ColumnField(length=255)


class _ProjectionParent(BaseModel):
    __tablename__ = "test_projection_parent"
    id: int = ColumnField(primary_key=True, autoincrement=True)
    notes: list[dict[str, object]] = RelationField(
        foreign_key="parent_id",
        cardinality=Cardinality.ONE_TO_MANY,
        on_delete=OnDelete.CASCADE,
        profiles=("with_details",),
    )
    has_notes: bool = ProjectionField(
        loader=ExistsLoader(model=lambda: _ProjectionNote),  # type: ignore[arg-type]
        profiles=("with_details",),
        default=False,
    )


class _ProjectionNote(BaseModel):
    __tablename__ = "test_projection_note"
    id: int = ColumnField(primary_key=True, autoincrement=True)
    parent_id: int = ColumnField(
        foreign_key="test_projection_parent.id", on_delete=OnDelete.CASCADE
    )


# ---------------------------------------------------------------------------
# Regression fixtures: fully-qualified FK format ("table.column") on ONE_TO_MANY
# ---------------------------------------------------------------------------


class _FqNote(BaseModel):
    """Child model whose FK column is 'record_id'."""

    __tablename__ = "fq_test_notes"
    id: int = ColumnField(primary_key=True, autoincrement=True)
    record_id: int = ColumnField(foreign_key="fq_test_records.id", on_delete=OnDelete.CASCADE)


class _FqRecord(BaseModel):
    """Parent model with ONE_TO_MANY relation declared via fully-qualified FK 'table.column'."""

    __tablename__ = "fq_test_records"
    id: int = ColumnField(primary_key=True, autoincrement=True)
    title: str = ColumnField(length=120)
    notes: list[_FqNote] = RelationField(
        foreign_key="fq_test_notes.record_id",  # fully-qualified — the bug trigger
        cardinality=Cardinality.ONE_TO_MANY,
        on_delete=OnDelete.CASCADE,
        profiles=("with_details",),
    )
    has_notes: bool = ProjectionField(
        loader=ExistsLoader(model=_FqNote),
        profiles=("default", "with_details"),
        default=False,
    )


class _AuditEntity(TimestampedModel):
    __tablename__ = "test_audit_entities"
    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=80)


class _LegacyAuditEntity(BaseModel):
    __tablename__ = "test_legacy_audit_entities"
    id: int = ColumnField(primary_key=True, autoincrement=True)
    updated_at: str = ColumnField(server_onupdate="now", nullable=True)


class _Shift(BaseModel):
    __tablename__ = "test_shifts"
    id: int = ColumnField(primary_key=True, autoincrement=True)
    day: date = ColumnField()
    starts_at: time = ColumnField()


@pytest.fixture(autouse=True)
def _clean_registry() -> Generator[None, None, None]:
    reset_registry()
    yield
    reset_registry()


class _Shade(Enum):
    DARK = "dark"


class _ShadedItem(BaseModel):
    __tablename__ = "test_shaded_items"
    shade: _Shade
    id: int = ColumnField(primary_key=True, autoincrement=True)


class _ModedItem(BaseModel):
    __tablename__ = "test_moded_items"
    id: int = ColumnField(primary_key=True, autoincrement=True)
    mode: Literal["a", "b"] | None = ColumnField(nullable=True, default=None)


class _DeclaredShadeItem(BaseModel):
    __tablename__ = "test_declared_shade_items"
    id: int = ColumnField(primary_key=True, autoincrement=True)
    shade: _Shade = ColumnField(String(8))
    mode: Annotated[Literal["a", "b"], String(1)] | None = None
    kind: Annotated[Literal["a", "b"], String(1)] | None = ColumnField(nullable=True)


class TestUndeclaredColumnTypes:
    @pytest.mark.parametrize(("model", "field"), [(_ShadedItem, "shade"), (_ModedItem, "mode")])
    def test_a_plain_enum_or_literal_without_a_column_type_is_refused(
        self, model: type, field: str
    ) -> None:
        with pytest.raises(
            ValueError, match=rf"{model.__name__}\.{field}: .*ColumnField\(String\(20\)\)"
        ):
            compile_all(model, metadata=MetaData())

    def test_a_declared_column_type_compiles(self) -> None:
        metadata = MetaData()

        compile_all(_DeclaredShadeItem, metadata=metadata)

        columns = metadata.tables["test_declared_shade_items"].c
        types = (str(columns.shade.type), str(columns.mode.type), str(columns.kind.type))
        assert types == ("VARCHAR(8)", "VARCHAR(1)", "VARCHAR(1)")
        assert columns.kind.nullable is True


class TestCompileModel:
    def test_columns_and_pk(self) -> None:
        sa_cls = cast(Any, compile_model(_Product))
        table = sa_cls.__table__
        assert sa_cls.__tablename__ == "test_products"
        assert {c.name for c in table.columns} >= {"id", "name", "price"}
        assert [c.name for c in table.primary_key.columns] == ["id"]

    def test_foreign_key_on_delete(self) -> None:
        compile_model(_Product)
        sa_cls = cast(Any, compile_model(_Review))
        fk = next(iter(sa_cls.__table__.c.product_id.foreign_keys))
        assert fk.column.table.name == "test_products"
        assert fk.ondelete == "CASCADE"

    def test_timestamped_model_does_not_emit_unset_defaults(self) -> None:
        sa_cls = cast(Any, compile_model(_AuditEntity))
        created = sa_cls.__table__.c.created_at
        updated = sa_cls.__table__.c.updated_at

        assert created.default is None
        assert updated.default is None
        assert created.server_default is not None
        assert updated.server_default is not None
        assert updated.onupdate is not None

    def test_server_onupdate_string_now_is_supported_for_backward_compatibility(self) -> None:
        sa_cls = cast(Any, compile_model(_LegacyAuditEntity))
        updated = sa_cls.__table__.c.updated_at
        assert updated.onupdate is not None

    def test_date_and_time_fields_compile_to_date_and_time_columns(self) -> None:
        sa_cls = cast(Any, compile_model(_Shift))

        ddl = str(CreateTable(sa_cls.__table__).compile(dialect=postgresql.dialect()))

        assert "day DATE NOT NULL" in ddl
        assert "starts_at TIME WITHOUT TIME ZONE NOT NULL" in ddl


class TestCompileAll:
    def test_batch_compile_and_relationships(self) -> None:
        compile_all(_Product, _Review)
        tables = set(get_metadata().tables.keys())
        assert {"test_products", "test_reviews"} <= tables

        sa_product = get_compiled(_Product)
        assert sa_product is not None
        assert hasattr(sa_product, "reviews")

    def test_idempotent(self) -> None:
        first = compile_model(_Product)
        second = compile_model(_Product)
        assert first is second

    def test_unknown_returns_none(self) -> None:
        assert get_compiled(_Product) is None


class TestFqForeignKeyOtmCompilation:
    """Regression: ONE_TO_MANY with fully-qualified FK format 'table.column'.

    Before the fix, ``_find_target_sa_by_fk_column("fq_test_notes.record_id")``
    searched ``table.c`` for the full string and found nothing, so:
    - the SA relationship was never added to the mapper;
    - the CoreRelationStep for 'notes' was never compiled;
    - ``_resolve_projection_steps`` raised ValueError or fell back to a broken
      memory loader at runtime.

    After the fix the column name is normalised (``rsplit(".", 1)[-1]``) before
    the ``table.c`` lookup, so both the SA mapper relationship and the
    CoreRelationStep are built correctly.
    """

    def test_compile_all_does_not_raise(self) -> None:
        compile_all(_FqRecord, _FqNote)

    def test_sa_relationship_attached_to_mapper(self) -> None:
        compile_all(_FqRecord, _FqNote)
        sa_record = get_compiled(_FqRecord)
        assert sa_record is not None
        assert hasattr(sa_record, "notes")

    def test_notes_core_relation_step_compiled(self) -> None:
        from loom.core.backend.sqlalchemy import get_compiled_core

        compile_all(_FqRecord, _FqNote)
        core = get_compiled_core(_FqRecord)
        assert core is not None
        plan = core._get_plan("with_details")
        assert any(step.attr == "notes" for step in plan.relation_steps)

    def test_fk_col_is_bare_column_name(self) -> None:
        """CoreRelationStep.fk_col must be 'record_id', not 'fq_test_notes.record_id'."""
        from loom.core.backend.sqlalchemy import get_compiled_core

        compile_all(_FqRecord, _FqNote)
        core = get_compiled_core(_FqRecord)
        assert core is not None
        plan = core._get_plan("with_details")
        notes_step = next(s for s in plan.relation_steps if s.attr == "notes")
        assert "." not in notes_step.fk_col
        assert notes_step.fk_col == "record_id"

    def test_has_notes_uses_sql_path_in_default_profile(self) -> None:
        """In 'default' profile 'notes' is not loaded → SQL path (prefer_memory=False) expected."""
        from loom.core.backend.sqlalchemy import get_compiled_core

        compile_all(_FqRecord, _FqNote)
        core = get_compiled_core(_FqRecord)
        assert core is not None
        plan = core._get_plan("default")
        assert plan.projection_plan is not None
        all_steps = [s for level in plan.projection_plan.levels for s in level]
        has_notes_step = next((s for s in all_steps if s.name == "has_notes"), None)
        assert has_notes_step is not None
        assert has_notes_step.prefer_memory is False

    def test_has_notes_uses_memory_path_in_with_details_profile(self) -> None:
        """In 'with_details' profile 'notes' IS loaded → memory path expected."""
        from loom.core.backend.sqlalchemy import get_compiled_core

        compile_all(_FqRecord, _FqNote)
        core = get_compiled_core(_FqRecord)
        assert core is not None
        plan = core._get_plan("with_details")
        assert plan.projection_plan is not None
        all_steps = [s for level in plan.projection_plan.levels for s in level]
        has_notes_step = next((s for s in all_steps if s.name == "has_notes"), None)
        assert has_notes_step is not None
        assert has_notes_step.prefer_memory is True
