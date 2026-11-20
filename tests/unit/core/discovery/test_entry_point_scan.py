"""``app.entry_points``: the modules of installed entry points are scanned like ``app.packages``."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from loom.composition.checked import CheckedProject
from loom.composition.project import AppConfig
from loom.core.config import ConfigContext, ConfigError
from loom.core.discovery.scanner import PackageScanner
from tests.helpers.package_writer import DistributionWriter, PackageWriter

_GROUP = "acme.plugins"
_MODEL = (
    "from loom.core.model import BaseModel\n"
    "class {name}(BaseModel):\n"
    "    __tablename__ = '{table}'\n"
    "    id: int\n"
)
_USE_CASE = (
    "from loom.core.use_case.use_case import UseCase\n"
    "class {name}(UseCase[object, str]):\n"
    "    async def execute(self) -> str:\n"
    "        return 'pong'\n"
)


class TestEntryPointScan:
    def test_finds_a_model_and_a_use_case_of_an_entry_point_module(
        self, write_package: PackageWriter, install_distribution: DistributionWriter
    ) -> None:
        write_package(
            "acme_plugin_a",
            {
                "models.py": _MODEL.format(name="Plugged", table="plugged"),
                "cases.py": _USE_CASE.format(name="PluggedPing"),
            },
        )
        install_distribution("acme-plugin-a", {_GROUP: ["acme_plugin_a"]})

        result = PackageScanner(entry_points=[_GROUP]).discover()

        assert [model.__name__ for model in result.models] == ["Plugged"]
        assert [use_case.__name__ for use_case in result.use_cases] == ["PluggedPing"]

    def test_a_module_in_packages_and_entry_points_is_scanned_once(
        self, write_package: PackageWriter, install_distribution: DistributionWriter
    ) -> None:
        write_package("acme_plugin_b", {"models.py": _MODEL.format(name="Shared", table="shared")})
        write_package("acme_plugin_c", {"models.py": _MODEL.format(name="Other", table="other")})
        install_distribution("acme-plugin-bc", {_GROUP: ["acme_plugin_b", "acme_plugin_c"]})

        result = PackageScanner(["acme_plugin_b"], entry_points=[_GROUP]).discover()

        assert [model.__name__ for model in result.models] == ["Shared", "Other"]

    def test_the_order_does_not_depend_on_how_the_groups_are_declared(
        self, write_package: PackageWriter, install_distribution: DistributionWriter
    ) -> None:
        for name in ("acme_plugin_x", "acme_plugin_y"):
            write_package(name, {"models.py": _MODEL.format(name=name.title(), table=name)})
        install_distribution("acme-plugin-y", {"acme.first": ["acme_plugin_y"]})
        install_distribution("acme-plugin-x", {"acme.second": ["acme_plugin_x"]})

        forward = PackageScanner(entry_points=["acme.first", "acme.second"]).discover()
        backward = PackageScanner(entry_points=["acme.second", "acme.first"]).discover()

        assert forward.models == backward.models
        assert [model.__module__ for model in forward.models] == [
            "acme_plugin_x.models",
            "acme_plugin_y.models",
        ]

    def test_a_group_without_an_installed_entry_point_is_named(self) -> None:
        with pytest.raises(ConfigError, match="'acme.unknown'.*no installed distribution"):
            PackageScanner(entry_points=["acme.unknown"]).discover()


class TestUnknownGroupFailsAtStartupAndInCheck:
    def test_startup_refuses_it(self) -> None:
        ctx = ConfigContext.from_dict({"app": {"name": "acme", "entry_points": ["acme.unknown"]}})

        with pytest.raises(ConfigError, match="'acme.unknown'.*no installed distribution"):
            AppConfig.from_config(ctx)

    def test_loom_check_refuses_it(self, tmp_path: Path) -> None:
        document = {"app": {"name": "acme", "entry_points": ["acme.unknown"]}}
        config = tmp_path / "app.yaml"
        config.write_text(yaml.safe_dump(document), encoding="utf-8")

        with pytest.raises(ConfigError, match="'acme.unknown'.*no installed distribution"):
            CheckedProject.load(str(config), environment="test")

    def test_an_installed_group_passes_both(
        self,
        tmp_path: Path,
        write_package: PackageWriter,
        install_distribution: DistributionWriter,
    ) -> None:
        write_package("acme_plugin_f", {})
        install_distribution("acme-plugin-f", {_GROUP: ["acme_plugin_f"]})
        document = {"app": {"name": "acme", "entry_points": [_GROUP]}}
        config = tmp_path / "app.yaml"
        config.write_text(yaml.safe_dump(document), encoding="utf-8")

        checked = CheckedProject.load(str(config), environment="test")

        assert checked.config.raw("app.entry_points") == [_GROUP]
        assert AppConfig.from_config(ConfigContext.from_dict(document)).entry_points == [_GROUP]
