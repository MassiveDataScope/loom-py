from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from loom.celery.adapter import CeleryAdapter
from loom.composition import Project, Recipe
from loom.core.config import ConfigError
from loom.core.discovery.base import DiscoveryResult
from loom.core.discovery.scanner import PackageScanner
from loom.core.stores import SERVER_PROFILE
from loom.rest.fastapi.adapter import FastAPIAdapter
from loom.serverless.aws.lambda_ import LambdaAdapter
from tests.helpers.package_writer import PackageWriter

_MODEL = (
    "from loom.core.model import BaseModel\n"
    "class {name}(BaseModel):\n"
    "    __tablename__ = '{table}'\n"
    "    id: int\n"
)


class TestPackageScan:
    @pytest.mark.parametrize(
        "files",
        [
            pytest.param({"models.py": _MODEL.format(name="Product", table="products")}, id="flat"),
            pytest.param(
                {
                    "nested/__init__.py": "",
                    "nested/deeper/__init__.py": "",
                    "nested/deeper/models.py": _MODEL.format(name="Product", table="products"),
                },
                id="nested",
            ),
        ],
    )
    def test_finds_a_model_at_any_depth(
        self, write_package: PackageWriter, files: dict[str, str]
    ) -> None:
        write_package("acme_scan", files)

        result = PackageScanner(["acme_scan"]).discover()

        assert [model.__name__ for model in result.models] == ["Product"]

    def test_finds_nothing_outside_the_listed_package(self, write_package: PackageWriter) -> None:
        write_package("acme_inside", {"models.py": _MODEL.format(name="Inside", table="inside")})
        write_package("acme_outside", {"models.py": _MODEL.format(name="Outside", table="outside")})

        result = PackageScanner(["acme_inside"]).discover()

        assert [model.__name__ for model in result.models] == ["Inside"]

    def test_skips_reexports_abstract_classes_and_inherited_table_names(
        self, write_package: PackageWriter
    ) -> None:
        write_package(
            "acme_filters",
            {
                "__init__.py": "from .models import Product\n",
                "models.py": (
                    "from abc import abstractmethod\n"
                    "from loom.core.model import BaseModel\n"
                    "from loom.core.di.installer import Installer\n"
                    "class Product(BaseModel):\n"
                    "    __tablename__ = 'products'\n"
                    "    id: int\n"
                    "class Child(Product):\n"
                    "    pass\n"
                    "class AbstractInstaller(Installer):\n"
                    "    @abstractmethod\n"
                    "    def install(self, container): ...\n"
                    "class ConcreteInstaller(AbstractInstaller):\n"
                    "    def install(self, container): pass\n"
                ),
            },
        )

        result = PackageScanner(["acme_filters"]).discover()

        assert [model.__name__ for model in result.models] == ["Product"]
        assert [installer.__name__ for installer in result.installers] == ["ConcreteInstaller"]

    def test_a_module_that_fails_to_import_is_named(self, write_package: PackageWriter) -> None:
        write_package("acme_broken", {"broken.py": "raise RuntimeError('boom')\n"})

        with pytest.raises(ConfigError, match="acme_broken.broken") as error:
            PackageScanner(["acme_broken"]).discover()

        assert isinstance(error.value.__cause__, RuntimeError)

    def test_a_missing_package_is_named(self) -> None:
        with pytest.raises(ConfigError, match="acme_missing"):
            PackageScanner(["acme_missing"]).discover()

    def test_a_module_entry_is_scanned_alone_and_a_package_entry_walked(
        self, write_package: PackageWriter
    ) -> None:
        root = write_package(
            "acme_walked",
            {"deep/__init__.py": "", "deep/m.py": _MODEL.format(name="Deep", table="deep")},
        )
        (root.parent / "acme_alone.py").write_text(_MODEL.format(name="Alone", table="alone"))
        (root.parent / "acme_sibling.py").write_text(_MODEL.format(name="Sibling", table="sibling"))

        result = PackageScanner(["acme_alone", "acme_walked"]).discover()

        assert [model.__name__ for model in result.models] == ["Alone", "Deep"]

    def test_a_class_imported_from_outside_the_listed_packages_is_not_collected(
        self, write_package: PackageWriter
    ) -> None:
        write_package("acme_foreign", {"models.py": _MODEL.format(name="Foreign", table="foreign")})
        write_package(
            "acme_home",
            {
                "models.py": "from acme_foreign.models import Foreign\n"
                + _MODEL.format(name="Home", table="home")
            },
        )

        result = PackageScanner(["acme_home"]).discover()

        assert [model.__name__ for model in result.models] == ["Home"]

    def test_the_order_of_the_entries_does_not_change_the_result(
        self, write_package: PackageWriter
    ) -> None:
        for name in ("acme_zulu", "acme_alpha", "acme_mike"):
            write_package(name, {"models.py": _MODEL.format(name=name.title(), table=name)})

        forward = PackageScanner(["acme_alpha", "acme_mike", "acme_zulu"]).discover()
        shuffled = PackageScanner(["acme_zulu", "acme_alpha", "acme_mike"]).discover()

        assert forward == shuffled

    def test_a_directory_with_modules_and_no_init_is_refused_naming_it(
        self, write_package: PackageWriter
    ) -> None:
        write_package("acme_gap", {"loose/models.py": _MODEL.format(name="Loose", table="loose")})

        with pytest.raises(ConfigError, match=r"acme_gap/loose.*__init__\.py"):
            PackageScanner(["acme_gap"]).discover()

    def test_a_directory_with_no_modules_and_no_init_is_ignored(
        self, write_package: PackageWriter
    ) -> None:
        write_package("acme_assets", {"static/readme.txt": "x"})

        assert PackageScanner(["acme_assets"]).discover().models == ()

    def test_duplicate_table_names_name_both_modules(self, write_package: PackageWriter) -> None:
        write_package(
            "acme_duplicates",
            {
                "first.py": _MODEL.format(name="First", table="same"),
                "second.py": _MODEL.format(name="Second", table="same"),
            },
        )

        with pytest.raises(ConfigError, match="acme_duplicates.first.*acme_duplicates.second"):
            PackageScanner(["acme_duplicates"]).discover()

    def test_orders_installers_by_module_and_qualname(self, write_package: PackageWriter) -> None:
        installer = "class {name}(Installer):\n    def install(self, container): pass\n"
        header = "from loom.core.di.installer import Installer\n"
        write_package(
            "acme_order",
            {
                "zeta.py": header + installer.format(name="Alpha"),
                "alpha.py": header + installer.format(name="Zeta") + installer.format(name="Beta"),
            },
        )

        result = PackageScanner(["acme_order"]).discover()

        assert [(cls.__module__, cls.__name__) for cls in result.installers] == [
            ("acme_order.alpha", "Beta"),
            ("acme_order.alpha", "Zeta"),
            ("acme_order.zeta", "Alpha"),
        ]


_JOB = (
    "from loom.core.job.job import Job\n"
    "class AcmeJob(Job[None]):\n"
    "    async def execute(self) -> None: ...\n"
)


_USE_CASE = (
    "from loom.core.use_case.use_case import UseCase\n"
    "class AcmePing(UseCase[object, str]):\n"
    "    async def execute(self) -> str:\n"
    "        return 'pong'\n"
)
_CELERY = "celery:\n  broker_url: memory://\n  result_backend: cache+memory://\n"


class _Adapter:
    def recipe(self, project: Project) -> Recipe:
        return Recipe(profile=SERVER_PROFILE, discovered=project.discovered, lenient=True)

    def bind(self, application: object) -> object:
        return application


class TestProjectScan:
    def test_runs_discovered_installers_in_module_order(
        self, tmp_path: Path, write_package: PackageWriter
    ) -> None:
        write_package(
            "acme_installers",
            {
                "alpha.py": (
                    "from loom.core.di.installer import Installer\n"
                    "class First(Installer):\n"
                    "    def install(self, container): container.register_instance(str, 'first')\n"
                ),
                "beta.py": (
                    "from loom.core.di.installer import Installer\n"
                    "class Second(Installer):\n"
                    "    def install(self, container):\n"
                    "        assert container.resolve(str) == 'first'\n"
                    "        container.register_instance(str, 'second')\n"
                ),
            },
        )
        config = tmp_path / "app.yaml"
        config.write_text("app:\n  name: acme\n  packages: [acme_installers]\n")

        application = Project.load(str(config), environment="test").compose(_Adapter())

        assert application.kernel.container.resolve(str) == "second"

    @pytest.mark.parametrize(
        ("adapter", "sections"),
        [
            pytest.param(lambda: CeleryAdapter(), _CELERY, id="celery"),
            pytest.param(
                lambda: LambdaAdapter(function="worker"),
                "lambda:\n  functions:\n    worker:\n      trigger:\n        source: jobs\n",
                id="lambda-jobs",
            ),
            pytest.param(
                lambda: FastAPIAdapter(), "stores:\n  default:\n    backend: none\n", id="fastapi"
            ),
            pytest.param(lambda: _Adapter(), "", id="plain"),
        ],
    )
    def test_scans_once_however_many_times_the_components_are_read(
        self,
        tmp_path: Path,
        write_package: PackageWriter,
        monkeypatch: pytest.MonkeyPatch,
        adapter: Callable[[], Any],
        sections: str,
    ) -> None:
        write_package("acme_once", {"jobs.py": _JOB, "cases.py": _USE_CASE})
        config = tmp_path / "app.yaml"
        config.write_text("app:\n  name: acme\n  packages: [acme_once]\n" + sections)
        scans: list[int] = []
        scan = PackageScanner.discover

        def counted(self: PackageScanner, **kwargs: Any) -> DiscoveryResult:
            scans.append(1)
            return scan(self, **kwargs)

        monkeypatch.setattr(PackageScanner, "discover", counted)
        project = Project.load(str(config), environment="test")

        project.compose(adapter())
        project.compose(_Adapter())

        assert len(scans) == 1

    def test_ignores_hidden_and_dunder_directories_without_reading_them(
        self, write_package: PackageWriter
    ) -> None:
        write_package(
            "acme_pruned",
            {
                "models.py": _MODEL.format(name="Kept", table="kept"),
                ".hidden/x.py": "raise RuntimeError('never imported')",
                "__snapshots__/x.py": "raise RuntimeError('never imported')",
            },
        )

        result = PackageScanner(["acme_pruned"]).discover()

        assert [model.__name__ for model in result.models] == ["Kept"]

    def test_refuses_the_removed_discovery_block(self, tmp_path: Path) -> None:
        config = tmp_path / "app.yaml"
        config.write_text("app:\n  name: acme\n  discovery:\n    mode: modules\n")

        with pytest.raises(ConfigError, match="app.discovery is removed"):
            Project.load(str(config), environment="test")
