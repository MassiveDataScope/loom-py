"""Store capabilities are asked of each store's backend on first read, once."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import Any, ClassVar

import pytest

import loom.core.plugins.entrypoints as entrypoints_module
from loom.core.config import ConfigContext, ConfigError
from loom.core.stores.catalog import (
    DEFAULT_STORE,
    StoreCapabilities,
    StoreKey,
    StoreSpec,
    load_catalog,
)

_ALL = StoreCapabilities(can_serve_models=True, is_transactional=True)
_NOTHING = StoreCapabilities(can_serve_models=False, is_transactional=False)
_MODELS_ONLY = StoreCapabilities(can_serve_models=True, is_transactional=False)
_NO_ROLLBACK = StoreCapabilities(can_serve_models=True, is_transactional=False)


class TestBuiltinStoreCapabilities:
    @pytest.mark.parametrize(
        ("backend", "settings", "expected"),
        [
            ("sqlalchemy", {}, _ALL),
            ("dynamodb", {}, _MODELS_ONLY),
            ("none", {}, _NOTHING),
            (
                "mongo",
                {"url": "mongodb://localhost", "database": "test"},
                _NO_ROLLBACK,
            ),
            (
                "mongo",
                {"url": "mongodb://localhost", "database": "test", "transactions": True},
                _ALL,
            ),
        ],
    )
    def test_builtin_capabilities_follow_store_configuration(
        self, backend: str, settings: dict[str, Any], expected: StoreCapabilities
    ) -> None:
        spec = load_catalog(
            ConfigContext.from_dict({"stores": {"default": {"backend": backend, **settings}}})
        ).specs[DEFAULT_STORE]
        assert spec.capabilities == expected

    def test_clickhouse_has_fixed_connection_only_capabilities(self) -> None:
        spec = load_catalog(
            ConfigContext.from_dict({"stores": {"sink": {"backend": "clickhouse"}}})
        ).specs[StoreKey("sink")]
        assert spec.capabilities == _NOTHING

    def test_capabilities_are_immutable(self) -> None:
        capabilities = StoreCapabilities(can_serve_models=True, is_transactional=True)
        with pytest.raises(FrozenInstanceError):
            capabilities.is_transactional = False  # type: ignore[misc]

    def test_capabilities_take_keywords_only(self) -> None:
        with pytest.raises(TypeError):
            StoreCapabilities(True, True, True)  # type: ignore[misc]

    def test_mongo_capabilities_never_resolve_the_connection(self) -> None:
        ctx = ConfigContext.from_dict(
            {
                "stores": {
                    "default": {
                        "backend": "mongo",
                        "url": "${oc.env:LOOM_TEST_UNSET_MONGO_URL}",
                        "transactions": True,
                    }
                }
            }
        )
        assert load_catalog(ctx).specs[DEFAULT_STORE].capabilities == _ALL


class _EntryPoint:
    group = "loom.stores.backends"

    def __init__(self, name: str, target: type) -> None:
        self.name = name
        self.dist = None
        self._target = target

    def load(self) -> type:
        return self._target


class _EntryPoints:
    def __init__(self, *entries: _EntryPoint) -> None:
        self.entries = entries

    def select(self, *, group: str) -> tuple[_EntryPoint, ...]:
        return tuple(entry for entry in self.entries if entry.group == group)


class _CustomBackend:
    name: ClassVar[str] = "custom"
    calls = 0

    def capabilities(self, spec: StoreSpec) -> StoreCapabilities:
        type(self).calls += 1
        return StoreCapabilities(
            can_serve_models=True,
            is_transactional=spec.section().get("transactional", False),
        )


class _LegacyBackend:
    name: ClassVar[str] = "legacy"


class TestPluginStoreCapabilities:
    def test_plugin_capabilities_are_asked_lazily_once_per_store(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            entrypoints_module,
            "entry_points",
            lambda: _EntryPoints(_EntryPoint("custom", _CustomBackend)),
        )
        monkeypatch.setattr(_CustomBackend, "calls", 0)
        ctx = ConfigContext.from_dict(
            {
                "stores": {
                    "default": {"backend": "custom", "transactional": True},
                    "other": {"backend": "custom"},
                }
            }
        )
        catalog = load_catalog(ctx)
        assert _CustomBackend.calls == 0
        assert catalog.specs[DEFAULT_STORE].capabilities == _ALL
        assert catalog.specs[DEFAULT_STORE].capabilities == _ALL
        assert catalog.specs[StoreKey("other")].capabilities == _NO_ROLLBACK
        assert _CustomBackend.calls == 2

    def test_a_plugin_without_capabilities_fails_naming_backend_and_method(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            entrypoints_module,
            "entry_points",
            lambda: _EntryPoints(_EntryPoint("legacy", _LegacyBackend)),
        )
        spec = load_catalog(
            ConfigContext.from_dict({"stores": {"default": {"backend": "legacy"}}})
        ).specs[DEFAULT_STORE]

        with pytest.raises(ConfigError, match=r"'legacy'.*required method 'capabilities'"):
            _ = spec.capabilities
