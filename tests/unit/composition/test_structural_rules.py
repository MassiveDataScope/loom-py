"""Start-up and ``loom check`` reject the same structural errors with the same message."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import pytest
import yaml

from loom.composition import Recipe
from loom.composition.phases import build_stores
from loom.composition.project import Project
from loom.core.config import ConfigError
from loom.core.connections.aws import AwsClientConfig, AwsClients
from loom.core.discovery.base import DiscoveryResult
from loom.core.stores import SERVER_PROFILE
from loom.core.stores import catalog as catalog_module
from loom.core.stores import registry as registry_module
from loom.core.stores.catalog import StoreCatalog

WriteConfig = Callable[[Mapping[str, object]], str]

_NO_REST: tuple[str, ...] = ("app.rest",)


def _start_up(config: str) -> object:
    return Project.load(config, environment="test").catalog


def _check(config: str) -> object:
    return Project.check(config, environment="test", excluded=_NO_REST)


@pytest.fixture
def write_config(tmp_path: Path) -> WriteConfig:
    def write(document: Mapping[str, object]) -> str:
        path = tmp_path / "app.yaml"
        path.write_text(yaml.safe_dump(document), encoding="utf-8")
        return str(path)

    return write


class TestSameRulesAtStartUpAndInLoomCheck:
    @pytest.mark.parametrize(
        ("document", "message"),
        [
            (
                {"app": {"name": "acme", "discovery": {"mode": "modules"}}},
                "app.discovery is removed",
            ),
            ({"app": {"code_path": "."}}, "app.name is required"),
            ({"celery": {}}, "app.name is required"),
            (
                {"app": {"name": "acme"}, "stores": {"main": {"backend": "typo"}}},
                "Unknown store backend 'typo'",
            ),
        ],
        ids=["discovery", "no name", "no app section", "backend typo"],
    )
    def test_both_refuse_with_the_same_message(
        self,
        write_config: WriteConfig,
        document: Mapping[str, object],
        message: str,
    ) -> None:
        config = write_config(document)

        with pytest.raises(ConfigError, match=message) as at_start_up:
            _start_up(config)
        with pytest.raises(ConfigError, match=message) as in_check:
            _check(config)

        assert str(at_start_up.value) == str(in_check.value)

    def test_a_placeholder_application_name_counts_as_present(
        self,
        write_config: WriteConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("ACME_APP_NAME", "acme")
        config = write_config({"app": {"name": "${oc.env:ACME_APP_NAME}"}})

        assert Project.load(config, environment="test").app.name == "acme"
        assert Project.check(config, environment="test", excluded=_NO_REST).config is not None

    def test_check_leaves_a_placeholder_application_name_unresolved(
        self, write_config: WriteConfig
    ) -> None:
        config = write_config({"app": {"name": "${oc.env:ACME_NAME_NEVER_SET}"}})

        assert Project.check(config, environment="test", excluded=_NO_REST).config is not None


class TestBackendsLoadedPerEntryPoint:
    @pytest.fixture
    def loaded(self, monkeypatch: pytest.MonkeyPatch) -> list[str]:
        names: list[str] = []
        original = registry_module.load_entry_point

        def spy(group: str, name: str, **options: Any) -> object:
            names.append(name)
            return original(group, name, **options)

        monkeypatch.setattr(registry_module, "load_entry_point", spy)
        return names

    @pytest.fixture
    def config(self, write_config: WriteConfig) -> str:
        return write_config(
            {
                "app": {"name": "acme"},
                "stores": {
                    "main": {"backend": "sqlalchemy", "url": "sqlite+aiosqlite://"},
                    "documents": {"backend": "mongo", "uri": "mongodb://localhost"},
                    "warehouse": {"backend": "clickhouse", "host": "localhost"},
                },
            }
        )

    def test_check_loads_every_declared_backend_but_the_connection_only_one(
        self, config: str, loaded: list[str]
    ) -> None:
        _check(config)

        assert sorted(loaded) == ["mongo", "sqlalchemy"]

    def test_start_up_loads_only_the_backends_of_stores_serving_models(
        self, config: str, loaded: list[str]
    ) -> None:
        project = Project.load(config, environment="test")
        recipe = Recipe(
            profile=SERVER_PROFILE,
            discovered=DiscoveryResult(models=(), use_cases=(), interfaces=()),
            default_store_backend="none",
        )

        build_stores(project, recipe, AwsClients(AwsClientConfig()))

        assert loaded == ["none"]

    def test_a_registered_backend_no_store_declares_is_never_loaded(
        self, write_config: WriteConfig, loaded: list[str]
    ) -> None:
        config = write_config({"app": {"name": "acme"}, "stores": {"main": {"backend": "none"}}})

        _check(config)

        assert loaded == ["none"]


class TestRequireInstalled:
    def test_an_empty_catalog_resolves_nothing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            catalog_module,
            "resolve_store_backend",
            lambda *args, **kwargs: pytest.fail("no store is declared"),
        )

        StoreCatalog(specs={}).require_installed()
