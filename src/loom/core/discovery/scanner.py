"""Recursive package discovery for application components."""

from __future__ import annotations

import importlib
import os
import pkgutil
import sys
from collections.abc import Sequence
from types import ModuleType
from typing import TypeVar

from loom.core.config import ConfigError
from loom.core.discovery._utils import (
    _append_unique,
    collect_from_modules,
    collect_use_cases_from_interfaces,
)
from loom.core.discovery.base import DiscoveryResult
from loom.core.model import BaseModel
from loom.core.stores.catalog import store_of

ClassT = TypeVar("ClassT", bound=type)


class PackageScanner:
    """Discover application classes in configured packages and their subpackages."""

    def __init__(self, packages: Sequence[str]) -> None:
        self._packages = packages

    def discover(self, *, agents: Sequence[str] = ()) -> DiscoveryResult:
        """Return the classes of the configured modules and packages, subpackages included.

        Args:
            agents: The ``app.agents`` artifact paths or globs, carried into the result.

        Returns:
            The discovered classes, each group ordered by module then qualified name.

        Raises:
            ConfigError: When an entry cannot be imported, a directory inside a
                package holds modules but no ``__init__.py``, or one store
                serves the same table from two models.
        """
        found = collect_from_modules(self._modules())
        models = self._ordered(found.models)
        self._refuse_duplicate_tables(models)
        seen = set(found.use_cases)
        for use_case in collect_use_cases_from_interfaces(found.interfaces):
            _append_unique(found.use_cases, seen, use_case)
        return DiscoveryResult(
            models=models,
            use_cases=self._ordered(found.use_cases),
            interfaces=self._ordered(found.interfaces),
            jobs=self._ordered(found.jobs),
            installers=self._ordered(found.installers),
            agent_specs=tuple(agents),
        )

    def _modules(self) -> list[ModuleType]:
        modules: dict[str, ModuleType] = {}
        for package_name in self._packages:
            package = self._import(package_name)
            modules[package.__name__] = package
            self._refuse_directories_without_init(package)
            for found in pkgutil.walk_packages(
                getattr(package, "__path__", ()),
                f"{package.__name__}.",
                onerror=self._raise_import_error,
            ):
                modules[found.name] = self._import(found.name)
        return list(modules.values())

    def _refuse_directories_without_init(self, package: ModuleType) -> None:
        for root in getattr(package, "__path__", ()):
            for current, subdirectories, files in os.walk(root):
                subdirectories[:] = sorted(
                    name for name in subdirectories if not name.startswith((".", "__"))
                )
                if self._holds_loose_modules(files):
                    raise ConfigError(
                        f"Directory {current!r} inside app.packages entry "
                        f"{package.__name__!r} holds Python modules but no __init__.py, so the "
                        "scan would skip it: add an __init__.py"
                    )

    @staticmethod
    def _holds_loose_modules(files: Sequence[str]) -> bool:
        return "__init__.py" not in files and any(name.endswith(".py") for name in files)

    def _refuse_duplicate_tables(self, models: Sequence[type[BaseModel]]) -> None:
        by_table: dict[tuple[str, str], type[BaseModel]] = {}
        for model in models:
            store, table = store_of(model), vars(model)["__tablename__"]
            previous = by_table.setdefault((store, table), model)
            if previous is not model:
                raise ConfigError(
                    f"Store {store!r} serves table {table!r} twice: "
                    f"{previous.__module__}.{previous.__qualname__} and "
                    f"{model.__module__}.{model.__qualname__}"
                )

    @staticmethod
    def _ordered(classes: Sequence[ClassT]) -> tuple[ClassT, ...]:
        return tuple(sorted(classes, key=lambda cls: (cls.__module__, cls.__qualname__)))

    def _import(self, module_name: str) -> ModuleType:
        try:
            return importlib.import_module(module_name)
        except Exception as exc:
            raise ConfigError(f"Cannot import app.packages module {module_name!r}: {exc}") from exc

    def _raise_import_error(self, module_name: str) -> None:
        cause = sys.exc_info()[1]
        raise ConfigError(f"Cannot import app.packages module {module_name!r}: {cause}") from cause
