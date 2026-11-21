"""``app.entry_points``: the modules of installed entry points are scanned like ``app.packages``."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from loom.cli.app import app
from loom.composition.checked import CheckedProject
from loom.composition.project import Project
from loom.core.checks import CheckScope, enforce_deploy_checks
from loom.core.config import ConfigContext, ConfigError
from loom.core.config.entry_points import EntryPointGroup
from loom.core.discovery.checks import EMPTY_ENTRY_POINT_GROUP_CHECK
from loom.core.discovery.scanner import PackageScanner
from loom.core.environment import Environment
from tests.helpers.loggers import CapturedLog
from tests.helpers.package_writer import DistributionWriter, PackageWriter

_GROUP = "acme.plugins"
_CHECK = EMPTY_ENTRY_POINT_GROUP_CHECK
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

        result = PackageScanner(entry_points=[EntryPointGroup(_GROUP)]).discover()

        assert [model.__name__ for model in result.models] == ["Plugged"]
        assert [use_case.__name__ for use_case in result.use_cases] == ["PluggedPing"]

    def test_a_module_in_packages_and_entry_points_is_scanned_once(
        self, write_package: PackageWriter, install_distribution: DistributionWriter
    ) -> None:
        write_package("acme_plugin_b", {"models.py": _MODEL.format(name="Shared", table="shared")})
        write_package("acme_plugin_c", {"models.py": _MODEL.format(name="Other", table="other")})
        install_distribution("acme-plugin-bc", {_GROUP: ["acme_plugin_b", "acme_plugin_c"]})

        result = PackageScanner(
            ["acme_plugin_b"], entry_points=[EntryPointGroup(_GROUP)]
        ).discover()

        assert [model.__name__ for model in result.models] == ["Shared", "Other"]

    def test_the_order_does_not_depend_on_how_the_groups_are_declared(
        self, write_package: PackageWriter, install_distribution: DistributionWriter
    ) -> None:
        for name in ("acme_plugin_x", "acme_plugin_y"):
            write_package(name, {"models.py": _MODEL.format(name=name.title(), table=name)})
        install_distribution("acme-plugin-y", {"acme.first": ["acme_plugin_y"]})
        install_distribution("acme-plugin-x", {"acme.second": ["acme_plugin_x"]})

        forward = PackageScanner(
            entry_points=[EntryPointGroup("acme.first"), EntryPointGroup("acme.second")]
        ).discover()
        backward = PackageScanner(
            entry_points=[EntryPointGroup("acme.second"), EntryPointGroup("acme.first")]
        ).discover()

        assert forward.models == backward.models
        assert [model.__module__ for model in forward.models] == [
            "acme_plugin_x.models",
            "acme_plugin_y.models",
        ]

    def test_a_group_without_an_installed_entry_point_scans_nothing(self) -> None:
        result = PackageScanner(entry_points=[EntryPointGroup("acme.unknown")]).discover()

        assert result.models == ()

    def test_a_group_without_an_installed_entry_point_is_listed_by_empty_groups(
        self, write_package: PackageWriter, install_distribution: DistributionWriter
    ) -> None:
        write_package("acme_plugin_g", {})
        install_distribution("acme-plugin-g", {_GROUP: ["acme_plugin_g"]})

        scanner = PackageScanner(
            entry_points=[
                EntryPointGroup("acme.unknown"),
                EntryPointGroup(_GROUP),
                EntryPointGroup("acme.other"),
            ]
        )

        assert scanner.empty_groups() == ["acme.unknown", "acme.other"]

    def test_an_optional_group_without_an_installed_entry_point_is_not_listed(self) -> None:
        scanner = PackageScanner(
            entry_points=[
                EntryPointGroup("acme.typo"),
                EntryPointGroup("acme.plugins_maybe", is_optional=True),
            ]
        )

        assert scanner.empty_groups() == ["acme.typo"]

    def test_an_optional_group_with_entry_points_still_loads_them(
        self, write_package: PackageWriter, install_distribution: DistributionWriter
    ) -> None:
        write_package("acme_plugin_o", {"models.py": _MODEL.format(name="Maybe", table="maybe")})
        install_distribution("acme-plugin-o", {_GROUP: ["acme_plugin_o"]})

        result = PackageScanner(entry_points=[EntryPointGroup(_GROUP, is_optional=True)]).discover()

        assert [model.__name__ for model in result.models] == ["Maybe"]


class TestEntryPointGroupDeclaration:
    @pytest.mark.parametrize(
        ("declared", "expected"),
        [
            ("acme.plugins", EntryPointGroup("acme.plugins")),
            ({"group": "acme.plugins"}, EntryPointGroup("acme.plugins")),
            (
                {"group": "acme.plugins", "optional": True},
                EntryPointGroup("acme.plugins", is_optional=True),
            ),
        ],
        ids=["string", "mapping-required", "mapping-optional"],
    )
    def test_an_item_is_decoded_into_one_group(
        self, declared: object, expected: EntryPointGroup
    ) -> None:
        assert EntryPointGroup.from_declared(declared) == expected

    def test_an_unknown_key_is_refused_naming_it(self) -> None:
        with pytest.raises(ValueError, match="'optionl'"):
            EntryPointGroup.from_declared({"group": "acme.plugins", "optionl": True})

    def test_the_application_config_refuses_an_unknown_key_naming_it(self, tmp_path: Path) -> None:
        entry = {"group": "acme.plugins", "optionl": True}

        with pytest.raises(ConfigError, match="optionl"):
            Project.load(_write_config(tmp_path, [entry]), environment="test")


def _write_config(tmp_path: Path, entry_points: list[object]) -> str:
    document = {"app": {"name": "acme", "entry_points": entry_points}}
    config = tmp_path / "app.yaml"
    config.write_text(yaml.safe_dump(document), encoding="utf-8")
    return str(config)


def _empty_group_findings(checked: CheckedProject) -> list[str]:
    return [result.message for result in checked.report.results if result.id == _CHECK]


class TestEmptyGroupWarnsAtStartupAndInCheck:
    def test_startup_accepts_the_group_and_logs_nothing_of_its_own(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        project = Project.load(_write_config(tmp_path, ["acme.unknown"]), environment="test")

        discovered = project.discovered

        assert discovered.models == ()
        assert CapturedLog(caplog, "loom.core.discovery.scanner").calls == []

    def test_the_startup_deploy_checks_report_it(self) -> None:
        raw = {"app": {"name": "acme", "entry_points": ["acme.unknown"]}}

        report = enforce_deploy_checks(
            ConfigContext.from_dict(raw), Environment.PRODUCTION, scope=CheckScope()
        )

        assert report is not None
        assert [result.id for result in report.results] == [_CHECK]

    def test_loom_check_warns_with_the_stable_id_naming_the_group(self, tmp_path: Path) -> None:
        checked = CheckedProject.load(_write_config(tmp_path, ["acme.unknown"]), environment="test")

        assert _empty_group_findings(checked) == [
            "app.entry_points group 'acme.unknown' has no installed entry point."
        ]

    def test_loom_check_warning_is_not_an_error(self, tmp_path: Path) -> None:
        checked = CheckedProject.load(_write_config(tmp_path, ["acme.unknown"]), environment="test")

        assert _CHECK not in [result.id for result in checked.report.errors]

    def test_the_warning_can_be_silenced_for_an_optional_group(self, tmp_path: Path) -> None:
        document = {
            "app": {"name": "acme", "entry_points": ["acme.unknown"]},
            "checks": {"silenced": [_CHECK]},
        }
        config = tmp_path / "app.yaml"
        config.write_text(yaml.safe_dump(document), encoding="utf-8")

        checked = CheckedProject.load(str(config), environment="test")

        assert _empty_group_findings(checked) == []

    def test_a_placeholder_group_is_not_judged(self, tmp_path: Path) -> None:
        checked = CheckedProject.load(
            _write_config(tmp_path, ["${oc.env:ACME_GROUP,acme.unknown}"]), environment="test"
        )

        assert _empty_group_findings(checked) == []

    def test_an_installed_group_passes_both(
        self,
        tmp_path: Path,
        write_package: PackageWriter,
        install_distribution: DistributionWriter,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        write_package("acme_plugin_f", {})
        install_distribution("acme-plugin-f", {_GROUP: ["acme_plugin_f"]})
        config = _write_config(tmp_path, [_GROUP])

        checked = CheckedProject.load(config, environment="test")
        discovered = checked.discovered

        assert _empty_group_findings(checked) == []
        assert discovered.models == ()
        assert CapturedLog(caplog, "loom.core.discovery.scanner").calls == []


_OPTIONAL_UNKNOWN = {"group": "acme.unknown", "optional": True}


class TestOptionalGroup:
    def test_an_optional_empty_group_gives_no_warning(self, tmp_path: Path) -> None:
        checked = CheckedProject.load(
            _write_config(tmp_path, [_OPTIONAL_UNKNOWN]), environment="test"
        )

        assert _empty_group_findings(checked) == []

    def test_a_required_empty_group_still_warns(self, tmp_path: Path) -> None:
        checked = CheckedProject.load(
            _write_config(tmp_path, [{"group": "acme.unknown"}]), environment="test"
        )

        assert _empty_group_findings(checked) == [
            "app.entry_points group 'acme.unknown' has no installed entry point."
        ]

    def test_a_mixed_list_warns_only_for_the_required_group(self, tmp_path: Path) -> None:
        checked = CheckedProject.load(
            _write_config(tmp_path, ["acme.typo", _OPTIONAL_UNKNOWN, {"group": "acme.other"}]),
            environment="test",
        )

        assert _empty_group_findings(checked) == [
            "app.entry_points group 'acme.typo', 'acme.other' has no installed entry point."
        ]

    @pytest.mark.parametrize(
        ("entry", "warns"),
        [(_OPTIONAL_UNKNOWN, False), ("acme.unknown", True)],
        ids=["optional", "required"],
    )
    def test_loom_check_output_names_the_check_only_for_a_required_group(
        self, tmp_path: Path, entry: object, warns: bool
    ) -> None:
        config = _write_config(tmp_path, [entry])

        result = CliRunner().invoke(app, ["check", "--deploy", config, "--skip", "app.rest"])

        assert (_CHECK in result.output) is warns, result.output
