"""Every reader of persistence configuration goes through the store catalog."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.config import ConfigContext, ConfigError
from loom.core.model import BaseModel, ColumnField, Integer
from loom.core.persistence.stores import (
    DEFAULT_STORE,
    StoreCatalog,
    StoreKey,
    load_catalog,
    require_single_store,
)
from tests.helpers.composition import composition_source


class _AuditRecord(BaseModel):
    __tablename__ = "reader_audit_records"
    __store__ = "audit"
    id: int = ColumnField(Integer, primary_key=True)


class _DefaultRecord(BaseModel):
    __tablename__ = "reader_default_records"
    id: int = ColumnField(Integer, primary_key=True)


def test_the_composition_refuses_a_retired_section_in_memory_too() -> None:
    source = composition_source(
        ConfigContext.from_dict({"app": {"name": "x"}, "database": {"url": "sqlite://"}})
    )

    with pytest.raises(ConfigError, match="stores.default"):
        source.backend_name(default="none")


def test_the_default_store_names_the_composed_backend() -> None:
    source = composition_source(
        ConfigContext.from_dict({"app": {"name": "x"}, "stores": {"default": {"backend": "none"}}})
    )

    assert source.backend_name(default="sqlalchemy") == "none"


def test_a_store_section_resolves_against_the_whole_configuration_without_its_backend() -> None:
    ctx = ConfigContext.from_dict(
        {
            "app": {"name": "orders"},
            "stores": {"default": {"backend": "sqlalchemy", "url": "sqlite:///${app.name}.db"}},
        }
    )

    settings = load_catalog(ctx).specs[DEFAULT_STORE].settings()

    assert settings == {"url": "sqlite:///orders.db"}


def _catalog(stores: dict[str, Any]) -> StoreCatalog:
    return load_catalog(ConfigContext.from_dict({"stores": stores}))


def test_an_application_composes_models_of_the_default_store_only() -> None:
    require_single_store([_DefaultRecord])

    with pytest.raises(ConfigError, match=r"_AuditRecord belongs to store 'audit'"):
        require_single_store([_DefaultRecord, _AuditRecord])


class _ClickRecord(BaseModel):
    __tablename__ = "reader_click_records"
    __store__ = "events"
    id: int = ColumnField(Integer, primary_key=True)


_CLICKHOUSE = {"backend": "clickhouse", "url": "clickhouse://ch:8123/analytics"}


def test_a_clickhouse_store_is_a_connection_only_kind_of_the_catalog() -> None:
    spec = _catalog({"events": _CLICKHOUSE}).specs[StoreKey("events")]

    assert (spec.backend, spec.serves_models) == ("clickhouse", False)
    assert spec.settings() == {"url": "clickhouse://ch:8123/analytics"}


def test_an_unknown_kind_names_the_persistence_backends_and_the_connection_only_ones() -> None:
    with pytest.raises(ConfigError) as error:
        _catalog({"events": {"backend": "kafka"}})

    message = str(error.value)
    assert "Unknown persistence backend 'kafka'. Registered backends:" in message
    assert "Connection-only stores: clickhouse." in message


def test_a_model_of_a_named_clickhouse_store_fails_at_composition() -> None:
    catalog = _catalog({"default": {"backend": "none"}, "events": _CLICKHOUSE})

    with pytest.raises(ConfigError, match=r"_ClickRecord belongs to store 'events'"):
        require_single_store([_ClickRecord])
    with pytest.raises(ConfigError, match="connection-only clickhouse store"):
        catalog.models_by_store([_ClickRecord])


def test_a_clickhouse_default_store_fails_when_the_catalog_loads() -> None:
    with pytest.raises(ConfigError, match=r"stores\.default\.backend is 'clickhouse'"):
        _catalog({"default": _CLICKHOUSE})


def test_a_clickhouse_default_store_is_not_composed_as_a_backend() -> None:
    source = composition_source(
        ConfigContext.from_dict({"app": {"name": "x"}, "stores": {"default": _CLICKHOUSE}})
    )

    with pytest.raises(ConfigError, match=r"stores\.default\.backend is 'clickhouse'"):
        source.backend_name(default="none")
