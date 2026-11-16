from __future__ import annotations

import sys
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from loom.composition import Project, Recipe
from loom.core.config import ConfigError
from loom.core.discovery.scanner import PackageScanner
from loom.core.stores import SERVER_PROFILE

PackageWriter = Callable[[str, dict[str, str]], Path]

_MODEL = (
    "from loom.core.model import BaseModel\n"
    "class {name}(BaseModel):\n"
    "    __tablename__ = '{table}'\n"
    "    id: int\n"
)


@pytest.fixture
def write_package(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[PackageWriter]:
    """Write a package of *files* under ``tmp_path``, importable until the test ends."""
    monkeypatch.syspath_prepend(str(tmp_path))
    written: list[str] = []

    def write(name: str, files: dict[str, str]) -> Path:
        written.append(name)
        root = tmp_path / name
        for relative, source in {"__init__.py": "", **files}.items():
            target = root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(source)
        return root

    yield write
    for name in tuple(sys.modules):
        if name.split(".")[0] in written:
            sys.modules.pop(name)


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

    def test_an_entry_that_is_a_module_is_refused(
        self, tmp_path: Path, write_package: PackageWriter
    ) -> None:
        write_package("acme_host", {})
        (tmp_path / "acme_module.py").write_text("")

        with pytest.raises(ConfigError, match="acme_module.*not a package"):
            PackageScanner(["acme_module"]).discover()

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


class _Adapter:
    def recipe(self, project: Project) -> Recipe:
        return Recipe(profile=SERVER_PROFILE, discovered=project.discover(), lenient=True)

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

    def test_refuses_the_removed_discovery_block(self, tmp_path: Path) -> None:
        config = tmp_path / "app.yaml"
        config.write_text("app:\n  name: acme\n  discovery:\n    mode: modules\n")

        with pytest.raises(ConfigError, match="app.discovery is removed"):
            Project.load(str(config), environment="test")
