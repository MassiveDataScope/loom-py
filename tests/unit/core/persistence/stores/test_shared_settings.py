"""``stores.settings`` holds the settings every store shares and is no store."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.config import ConfigContext, ConfigError
from loom.core.model import BaseModel, ColumnField, Integer
from loom.core.stores.catalog import (
    DEFAULT_STORE,
    StoreCatalog,
    StoreSetting,
    load_catalog,
    store_of,
)

_STORE: dict[str, Any] = {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///:memory:"}


def _load(stores: dict[str, Any]) -> StoreCatalog:
    return load_catalog(ConfigContext.from_dict({"stores": stores}))


class TestSharedSettings:
    def test_the_shared_settings_are_not_a_store(self) -> None:
        catalog = _load(
            {
                "default": _STORE,
                "settings": {"rollback_timeout": 5, "cursors": {"signing": "ephemeral"}},
            }
        )

        assert list(catalog.specs) == [DEFAULT_STORE]

    def test_a_store_named_settings_is_refused(self) -> None:
        with pytest.raises(ConfigError, match=r"stores\.settings.*declares no store"):
            _load({"settings": _STORE})

    def test_a_model_cannot_belong_to_a_store_named_settings(self) -> None:
        class AcmeSettingsRecord(BaseModel):
            __tablename__ = "acme_settings_records"
            __store__ = "settings"
            id: int = ColumnField(Integer, primary_key=True)

        with pytest.raises(ConfigError, match=r"'settings' is reserved"):
            store_of(AcmeSettingsRecord)

    def test_an_unknown_shared_setting_is_refused(self) -> None:
        with pytest.raises(ConfigError, match=r"stores\.settings\.pool is not a store setting"):
            _load({"default": _STORE, "settings": {"pool": {"size": 1}}})

    def test_shared_settings_that_are_not_a_mapping_are_refused(self) -> None:
        with pytest.raises(ConfigError, match=r"stores\.settings must map"):
            _load({"default": _STORE, "settings": 5})

    @pytest.mark.parametrize("setting", list(StoreSetting), ids=lambda setting: setting.value)
    def test_a_store_cannot_override_a_shared_setting(self, setting: StoreSetting) -> None:
        with pytest.raises(
            ConfigError, match=rf"stores\.default\.settings\.{setting.value} is global"
        ):
            _load({"default": {**_STORE, "settings": {setting.value: {}}}})

    def test_a_store_override_points_at_the_shared_key(self) -> None:
        with pytest.raises(
            ConfigError, match=r"set it once under stores\.settings\.rollback_timeout"
        ):
            _load({"default": {**_STORE, "settings": {"rollback_timeout": 2}}})

    def test_a_store_cannot_name_another_key_in_its_settings(self) -> None:
        with pytest.raises(ConfigError, match=r"stores\.default\.settings\.pool is not a setting"):
            _load({"default": {**_STORE, "settings": {"pool": {}}}})

    def test_a_store_section_hands_its_backend_neither_backend_nor_settings(self) -> None:
        catalog = _load({"default": {**_STORE, "settings": {}}})

        assert catalog.specs[DEFAULT_STORE].section() == {"url": "sqlite+aiosqlite:///:memory:"}


class TestRetiredPersistenceSection:
    @pytest.mark.parametrize(
        ("section", "replacement"),
        [
            ({"cursors": {"signing": "ephemeral"}}, "stores.settings.cursors"),
            ({"rollback_timeout": 5}, "stores.settings.rollback_timeout"),
            ({"backend": "mongo"}, "stores.default.backend"),
            ({"pool": {}}, "stores.settings"),
            ({}, "stores.settings"),
        ],
        ids=["cursors", "rollback-timeout", "backend", "unknown-key", "empty"],
    )
    def test_any_persistence_key_names_its_replacement(
        self, section: dict[str, Any], replacement: str
    ) -> None:
        ctx = ConfigContext.from_dict({"persistence": section})

        with pytest.raises(ConfigError, match=rf"persistence.*was removed.*{replacement}"):
            load_catalog(ctx)
