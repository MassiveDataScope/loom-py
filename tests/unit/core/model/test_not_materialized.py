"""A model used for introspection without a persisted table or repository."""

from __future__ import annotations

import pytest
from sqlalchemy import MetaData

from loom.core.backend.sqlalchemy import compile_all, compile_model, get_compiled, get_compiled_core
from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer
from loom.core.model import BaseModel, ColumnField, NotMaterialized
from loom.core.model.materialization import is_materialized
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey, load_catalog
from loom.core.repository.abc import CursorCodec, RepoFor
from loom.core.repository.sqlalchemy.registry import (
    build_sqlalchemy_repository_registration_module,
)
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.use_case.errors import UnservedModelError
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.use_case import UseCase


class _AuditView(BaseModel, NotMaterialized):
    __tablename__ = "audit_views"
    __store__ = "audit"
    id: int = ColumnField(primary_key=True)


class _Stored(BaseModel):
    __tablename__ = "stored_rows"
    __store__ = "audit"
    id: int = ColumnField(primary_key=True)


class _ReadView(UseCase[_AuditView, None]):
    def __init__(self, records: RepoFor[_AuditView]) -> None:
        self.records = records

    async def execute(self, **kwargs: object) -> None:
        return None


def test_marker_is_inherited_and_regular_models_remain_materialized() -> None:
    class Child(_AuditView):
        pass

    assert not is_materialized(_AuditView)
    assert not is_materialized(Child)
    assert is_materialized(_Stored)


def test_compile_all_skips_marked_models_and_keeps_regular_tables() -> None:
    metadata = MetaData()

    compile_all(_AuditView, _Stored, metadata=metadata)

    assert set(metadata.tables) == {"stored_rows"}
    assert get_compiled(_AuditView, metadata=metadata) is None
    assert get_compiled_core(_AuditView, metadata=metadata) is None


def test_compile_model_refuses_a_marked_model() -> None:
    metadata = MetaData()

    with pytest.raises(TypeError, match="NotMaterialized"):
        compile_model(_AuditView, metadata=metadata)
    assert not metadata.tables


def test_store_with_only_marked_model_is_retained() -> None:
    catalog = load_catalog(
        ConfigContext.from_dict({"stores": {"audit": {"backend": "sqlalchemy"}}})
    )

    assert catalog.models_by_store([_AuditView]) == {
        DEFAULT_STORE: (),
        StoreKey("audit"): (_AuditView,),
    }


def test_repository_registration_skips_marker_and_verify_names_it() -> None:
    manager = SessionManager(
        "sqlite+aiosqlite:///:memory:",
        store=StoreKey("audit"),
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
    )
    container = LoomContainer()
    module = build_sqlalchemy_repository_registration_module(
        manager, [_AuditView, _Stored], cursor_codec=CursorCodec(signing="ephemeral")
    )
    module(container)

    assert not container.has_repo_mapping(_AuditView)
    assert container.has_repo_mapping(_Stored)
    factory = UseCaseFactory(container)
    factory.register(_ReadView)
    with pytest.raises(UnservedModelError, match="NotMaterialized") as error:
        factory.verify()
    assert error.value.model is _AuditView
