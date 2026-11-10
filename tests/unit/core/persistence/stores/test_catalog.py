"""The store catalog's configuration and model assignment contract."""

from __future__ import annotations

import pytest

from loom.core.config import ConfigContext, ConfigError
from loom.core.model import BaseModel, ColumnField, Integer
from loom.core.persistence.stores import (
    DEFAULT_STORE,
    InStore,
    StoreKey,
    load_catalog,
    store_of,
)


def test_in_store_rejects_invalid_key() -> None:
    with pytest.raises(ConfigError):
        InStore(StoreKey("bad-key"))


class DefaultRecord(BaseModel):
    __tablename__ = "default_records"
    id: int = ColumnField(Integer, primary_key=True)


class AuditRecord(BaseModel):
    __tablename__ = "audit_records"
    __store__ = "audit"
    id: int = ColumnField(Integer, primary_key=True)


class SecondDefaultRecord(BaseModel):
    __tablename__ = "second_default_records"
    id: int = ColumnField(Integer, primary_key=True)


def test_default_store_loads_its_backend_and_own_config_section() -> None:
    ctx = ConfigContext.from_dict(
        {
            "stores": {
                "default": {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///:memory:"},
            },
            "service": {"name": "api"},
        }
    )

    catalog = load_catalog(ctx)
    spec = catalog.specs[DEFAULT_STORE]

    assert len(catalog.specs) == 1
    assert isinstance(StoreKey("default"), str)
    assert StoreKey("default") == DEFAULT_STORE
    assert spec.key == DEFAULT_STORE
    assert spec.backend == "sqlalchemy"
    assert spec.settings() == {"url": "sqlite+aiosqlite:///:memory:"}
    with pytest.raises(AttributeError):
        spec.backend = "mongo"  # pyright: ignore[reportAttributeAccessIssue]


def test_catalog_loads_all_declared_store_keys_and_backends() -> None:
    catalog = load_catalog(
        ConfigContext.from_dict(
            {
                "stores": {
                    "default": {"backend": "sqlalchemy"},
                    "audit": {"backend": "sqlalchemy"},
                    "sessions": {"backend": "dynamodb"},
                    "legacy": {
                        "backend": "mongo",
                        "url": "mongodb://localhost",
                        "database": "test",
                    },
                }
            }
        )
    )

    assert {key: spec.backend for key, spec in catalog.specs.items()} == {
        StoreKey("default"): "sqlalchemy",
        StoreKey("audit"): "sqlalchemy",
        StoreKey("sessions"): "dynamodb",
        StoreKey("legacy"): "mongo",
    }
    assert catalog.specs[StoreKey("default")].settings() == {}


def test_absent_stores_section_loads_an_empty_catalog() -> None:
    catalog = load_catalog(ConfigContext.from_dict({"service": {"name": "api"}}))

    assert catalog.specs == {}


def test_unknown_backend_names_the_store_and_backend() -> None:
    ctx = ConfigContext.from_dict({"stores": {"audit": {"backend": "redis"}}})

    with pytest.raises(ConfigError) as error:
        load_catalog(ctx)

    assert "audit" in str(error.value)
    assert "redis" in str(error.value)


def test_missing_backend_names_the_store() -> None:
    ctx = ConfigContext.from_dict({"stores": {"audit": {"url": "sqlite:///audit.db"}}})

    with pytest.raises(ConfigError, match="audit"):
        load_catalog(ctx)


@pytest.mark.parametrize("key", ["my-store", ""])
def test_invalid_store_key_is_refused_naming_the_key(key: str) -> None:
    ctx = ConfigContext.from_dict({"stores": {key: {"backend": "mongo"}}})

    with pytest.raises(ConfigError) as error:
        load_catalog(ctx)

    assert repr(key) in str(error.value)


def test_retired_database_section_names_stores_replacement() -> None:
    ctx = ConfigContext.from_dict({"database": {"url": "sqlite:///old.db"}})

    with pytest.raises(ConfigError) as error:
        load_catalog(ctx)

    assert "database" in str(error.value)
    assert "stores." in str(error.value)


def test_retired_named_database_section_names_stores_replacement() -> None:
    ctx = ConfigContext.from_dict({"database": {"audit": {"url": "sqlite:///old.db"}}})

    with pytest.raises(ConfigError) as error:
        load_catalog(ctx)

    assert "database.audit" in str(error.value)
    assert "stores." in str(error.value)


def test_retired_persistence_backend_names_stores_replacement() -> None:
    ctx = ConfigContext.from_dict({"persistence": {"backend": "mongo"}})

    with pytest.raises(ConfigError) as error:
        load_catalog(ctx)

    assert "persistence.backend" in str(error.value)
    assert "stores." in str(error.value)


def test_retired_persistence_mongo_names_stores_replacement() -> None:
    ctx = ConfigContext.from_dict({"persistence": {"mongo": {"uri": "mongodb://localhost"}}})

    with pytest.raises(ConfigError) as error:
        load_catalog(ctx)

    assert "persistence.mongo" in str(error.value)
    assert "stores." in str(error.value)


def test_retired_persistence_dynamodb_names_stores_replacement() -> None:
    ctx = ConfigContext.from_dict({"persistence": {"dynamodb": {"region": "eu-west-1"}}})

    with pytest.raises(ConfigError) as error:
        load_catalog(ctx)

    assert "persistence.dynamodb" in str(error.value)
    assert "stores." in str(error.value)


def test_persistence_cursors_alone_does_not_require_stores() -> None:
    ctx = ConfigContext.from_dict({"persistence": {"cursors": {"enabled": True}}})

    assert load_catalog(ctx).specs == {}


def test_legacy_database_model_attribute_names_store_replacement() -> None:
    class LegacyRecord(BaseModel):
        __tablename__ = "legacy_records"
        __database__ = "x"
        id: int = ColumnField(Integer, primary_key=True)

    catalog = load_catalog(
        ConfigContext.from_dict(
            {
                "stores": {
                    "default": {
                        "backend": "mongo",
                        "url": "mongodb://localhost",
                        "database": "test",
                    }
                }
            }
        )
    )

    with pytest.raises(ConfigError) as error:
        catalog.models_by_store([LegacyRecord])

    assert "__database__" in str(error.value)
    assert "__store__" in str(error.value)
    assert "LegacyRecord" in str(error.value)


def test_store_of_defaults_a_model_without_store_to_default() -> None:
    assert store_of(DefaultRecord) == DEFAULT_STORE


def test_store_of_reads_a_models_declared_store() -> None:
    assert store_of(AuditRecord) == StoreKey("audit")


def test_models_by_store_groups_default_first_and_preserves_model_order() -> None:
    catalog = load_catalog(
        ConfigContext.from_dict(
            {
                "stores": {
                    "audit": {"backend": "mongo", "url": "mongodb://localhost", "database": "test"},
                    "default": {"backend": "sqlalchemy"},
                }
            }
        )
    )

    groups = catalog.models_by_store([AuditRecord, DefaultRecord, SecondDefaultRecord])

    assert groups == {
        DEFAULT_STORE: (DefaultRecord, SecondDefaultRecord),
        StoreKey("audit"): (AuditRecord,),
    }
    assert list(groups) == [DEFAULT_STORE, StoreKey("audit")]


def test_models_by_store_keeps_an_undeclared_default_and_drops_a_store_without_models() -> None:
    catalog = load_catalog(
        ConfigContext.from_dict(
            {"stores": {"audit": {"backend": "mongo", "url": "mongodb://localhost"}}}
        )
    )

    assert catalog.models_by_store([DefaultRecord]) == {DEFAULT_STORE: (DefaultRecord,)}


def test_model_declaring_an_unknown_store_names_model_and_store() -> None:
    catalog = load_catalog(
        ConfigContext.from_dict(
            {
                "stores": {
                    "default": {
                        "backend": "mongo",
                        "url": "mongodb://localhost",
                        "database": "test",
                    }
                }
            }
        )
    )

    with pytest.raises(ConfigError) as error:
        catalog.models_by_store([AuditRecord])

    assert "AuditRecord" in str(error.value)
    assert "audit" in str(error.value)
