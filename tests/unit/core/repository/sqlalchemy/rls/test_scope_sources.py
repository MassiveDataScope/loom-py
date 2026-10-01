from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy import MetaData

from loom.core.backend.sqlalchemy import compile_all, scoped_tables
from loom.core.config import ConfigError
from loom.core.identity import Identity, reset_identity, set_identity
from loom.core.locator import Application, DatabaseConfig, SchemaConfig
from loom.core.model import BaseModel, ColumnField, RowScoped, ScopedField
from loom.core.model.types import Integer, String, Text
from loom.core.repository.sqlalchemy.rls import (
    register_scope_source,
    rls_session_settings,
)
from loom.core.repository.sqlalchemy.rls.sources import clear_scope_sources, resolve_binding
from loom.core.repository.sqlalchemy.session_settings import settings_statement


class Note(BaseModel, RowScoped):
    __tablename__ = "notes"
    key: str = ScopedField(String(36), primary_key=True, scope="holder")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    editor: str = ScopedField(Text, scope="editor", on="write", elevable=True)
    region: int = ScopedField(Integer, scope="region", on="read")


@pytest.fixture(autouse=True)
def _clean_sources() -> Iterator[None]:
    clear_scope_sources()
    yield
    clear_scope_sources()


def _application(**sources: str) -> Application:
    metadata = MetaData()
    compile_all(Note, metadata=metadata)
    return Application(
        models=(Note,),
        metadata=metadata,
        database=DatabaseConfig(url="postgresql+asyncpg://u:p@localhost/db", schema=SchemaConfig()),
        bootstrap=None,
        scoped=scoped_tables(metadata),
        scope_sources=sources
        or {"holder": "identity.subject", "editor": "request.editor", "region": "identity.region"},
    )


def _as(identity: Identity | None):
    return set_identity(identity) if identity is not None else None


def test_three_part_keys_are_accepted_by_the_settings_statement() -> None:
    clause, params = settings_statement({"loom.scope.holder": "x", "loom.scope.editor.any": "on"})

    assert "set_config(:k0, :v0, true)" in str(clause)
    assert params == {
        "k0": "loom.scope.holder",
        "v0": "x",
        "k1": "loom.scope.editor.any",
        "v1": "on",
    }


def test_a_registered_request_source_is_published_under_request_and_cannot_repeat() -> None:
    register_scope_source("editor", lambda: "ana")

    assert resolve_binding("request.editor") == "ana"
    with pytest.raises(ValueError, match="editor"):
        register_scope_source("editor", lambda: "bob")


def test_identity_bindings_read_the_subject_and_the_verified_attributes() -> None:
    token = set_identity(Identity(subject="sub-1", attributes={"region": "7"}))
    try:
        assert resolve_binding("identity.subject") == "sub-1"
        assert resolve_binding("identity.region") == "7"
        assert resolve_binding("identity.missing") is None
    finally:
        reset_identity(token)


def test_an_unregistered_request_source_is_a_configuration_error() -> None:
    application = _application()

    with pytest.raises(ConfigError, match=r"request\.editor"):
        rls_session_settings(application)


def test_the_provider_emits_every_key_as_empty_when_nobody_is_authenticated() -> None:
    register_scope_source("editor", lambda: None)
    provider = rls_session_settings(_application())

    assert provider() == {
        "loom.scope.holder": "",
        "loom.scope.editor": "",
        "loom.scope.editor.any": "",
        "loom.scope.region": "",
    }


def test_the_provider_emits_the_resolved_values_and_the_elevation_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom.core.repository.sqlalchemy.rls import provider as provider_module

    monkeypatch.setattr(provider_module, "elevated_scopes", lambda: frozenset({"editor"}))
    register_scope_source("editor", lambda: "ana")
    provider = rls_session_settings(_application())
    token = set_identity(Identity(subject="sub-1", attributes={"region": "7"}))
    try:
        assert provider() == {
            "loom.scope.holder": "sub-1",
            "loom.scope.editor": "ana",
            "loom.scope.editor.any": "on",
            "loom.scope.region": "7",
        }
    finally:
        reset_identity(token)


def test_non_string_source_values_are_rendered_as_text() -> None:
    register_scope_source("editor", lambda: 42)
    provider = rls_session_settings(_application())

    assert provider()["loom.scope.editor"] == "42"


def test_only_elevable_scopes_get_a_flag() -> None:
    register_scope_source("editor", lambda: "ana")
    provider = rls_session_settings(_application())

    assert "loom.scope.holder.any" not in provider()
    assert "loom.scope.region.any" not in provider()


def test_product_settings_are_merged_but_may_not_touch_the_scope_namespace() -> None:
    register_scope_source("editor", lambda: "ana")
    merged = rls_session_settings(_application(), product=lambda: {"shop.locale": "es"})
    assert merged()["shop.locale"] == "es"

    intruding = rls_session_settings(_application(), product=lambda: {"loom.scope.holder": "x"})
    with pytest.raises(ValueError, match=r"loom\.scope\."):
        intruding()


def test_a_product_provider_returning_none_still_yields_every_scope_key() -> None:
    register_scope_source("editor", lambda: "ana")
    provider = rls_session_settings(_application(), product=lambda: None)

    assert set(provider()) == {
        "loom.scope.holder",
        "loom.scope.editor",
        "loom.scope.editor.any",
        "loom.scope.region",
    }
