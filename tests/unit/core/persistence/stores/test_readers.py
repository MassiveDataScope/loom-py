"""Every reader of persistence configuration goes through the store catalog."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from loom.core.config import ConfigContext, ConfigError
from loom.core.config.overrides import load_application_config
from loom.core.environment import Environment
from loom.core.model import BaseModel, ColumnField, Integer, LoomStruct
from loom.core.persistence.stores import (
    DEFAULT_STORE,
    StoreCatalog,
    StoreKey,
    load_catalog,
    require_default_store,
)
from loom.streaming.nodes._table import IntoTable
from loom.streaming.nodes._table.config import resolve_sqlalchemy_table_config
from tests.helpers.composition import composition_source


class _AuditRecord(BaseModel):
    __tablename__ = "reader_audit_records"
    __store__ = "audit"
    id: int = ColumnField(Integer, primary_key=True)


class _DefaultRecord(BaseModel):
    __tablename__ = "reader_default_records"
    id: int = ColumnField(Integer, primary_key=True)


class _Row(LoomStruct):
    id: int


def _write(tmp_path: Path, document: dict[str, Any]) -> str:
    path = tmp_path / "base.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return str(path)


def test_loading_an_application_file_refuses_a_retired_database_section(tmp_path: Path) -> None:
    path = _write(tmp_path, {"app": {"name": "x"}, "database": {"url": "sqlite://"}})

    with pytest.raises(ConfigError, match=r"database was removed.*stores\.default"):
        load_application_config(path, environment=Environment.DEVELOPMENT)


def test_an_override_cannot_bring_a_retired_key_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _write(tmp_path, {"app": {"name": "x"}, "persistence": {"cursors": {}}})
    monkeypatch.setenv("LOOM__PERSISTENCE__BACKEND", "mongo")

    with pytest.raises(ConfigError, match=r"persistence\.backend was removed"):
        load_application_config(path, environment=Environment.DEVELOPMENT)


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
    catalog = _catalog({"default": {"backend": "none"}, "audit": {"backend": "none"}})
    require_default_store([_DefaultRecord], catalog)

    with pytest.raises(ConfigError, match=r"_AuditRecord belongs to store 'audit'"):
        require_default_store([_DefaultRecord, _AuditRecord], catalog)


class _ClickRecord(BaseModel):
    __tablename__ = "reader_click_records"
    __store__ = "events"
    id: int = ColumnField(Integer, primary_key=True)


_CLICKHOUSE = {"backend": "clickhouse", "url": "clickhouse://ch:8123/analytics"}


def test_a_clickhouse_store_is_a_connection_only_kind_of_the_catalog() -> None:
    spec = _catalog({"events": _CLICKHOUSE}).specs[StoreKey("events")]

    assert (spec.backend, spec.connection_only) == ("clickhouse", True)
    assert spec.settings() == {"url": "clickhouse://ch:8123/analytics"}


def test_an_unknown_kind_names_the_persistence_backends_and_the_connection_only_ones() -> None:
    with pytest.raises(ConfigError) as error:
        _catalog({"events": {"backend": "kafka"}})

    message = str(error.value)
    assert "Unknown persistence backend 'kafka'. Registered backends:" in message
    assert "Connection-only stores: clickhouse." in message


@pytest.mark.parametrize(
    ("stores", "model", "store"),
    [
        ({"default": {"backend": "none"}, "events": _CLICKHOUSE}, _ClickRecord, "events"),
        ({"default": _CLICKHOUSE}, _DefaultRecord, "default"),
    ],
    ids=["named-store", "default-store"],
)
def test_a_model_of_a_clickhouse_store_fails_at_composition(
    stores: dict[str, Any], model: type[BaseModel], store: str
) -> None:
    catalog = _catalog(stores)

    with pytest.raises(ConfigError, match=rf"belongs to store '{store}', a connection-only"):
        require_default_store([model], catalog)
    with pytest.raises(ConfigError, match="connection-only clickhouse store"):
        catalog.models_by_store([model])


def test_a_clickhouse_default_store_is_not_composed_as_a_backend() -> None:
    source = composition_source(
        ConfigContext.from_dict({"app": {"name": "x"}, "stores": {"default": _CLICKHOUSE}})
    )

    with pytest.raises(ConfigError, match=r"stores\.default\.backend is 'clickhouse'"):
        source.backend_name(default="none")


def _sink(config: dict[str, Any]) -> object:
    node = IntoTable(payload=_Row, table="orders", name="orders_sink")
    return resolve_sqlalchemy_table_config(node, ConfigContext.from_dict(config))


def test_a_relational_sink_refuses_a_store_of_another_backend() -> None:
    config = {
        "stores": {"events": {"backend": "mongo", "url": "mongodb://h", "database": "app"}},
        "streaming": {"sinks": {"orders_sink": {"store": "events"}}},
    }

    with pytest.raises(ConfigError, match=r"stores\.events\.backend is 'mongo'"):
        _sink(config)


def test_a_relational_sink_refuses_an_undeclared_store() -> None:
    config = {"streaming": {"sinks": {"orders_sink": {"store": "warehouse"}}}}

    with pytest.raises(ValueError, match=r"store 'warehouse' is not declared"):
        _sink(config)


def test_a_relational_sink_names_the_renamed_database_key() -> None:
    config = {"streaming": {"sinks": {"orders_sink": {"database": "warehouse"}}}}

    with pytest.raises(ValueError, match="'database' was renamed to 'store'"):
        _sink(config)
