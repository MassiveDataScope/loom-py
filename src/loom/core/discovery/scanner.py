"""Recursive package discovery for application components."""

from __future__ import annotations

import importlib
import importlib.metadata
import os
import pkgutil
import sys
from collections.abc import Sequence
from types import ModuleType
from typing import TypeVar

from loom.core.config import ConfigError
from loom.core.config.entry_points import EntryPointGroup
from loom.core.discovery._utils import collect_from_modules
from loom.core.discovery.base import DiscoveryResult

ClassT = TypeVar("ClassT", bound=type)


class PackageScanner:
    """Discover application classes in configured packages and their subpackages.

    Args:
        packages: The ``app.packages`` entries.
        entry_points: The ``app.entry_points`` groups; the module of every
            installed entry point in them is scanned like a package entry. A
            group with no installed entry point is a valid, empty state; a
            required one is reported by ``loom check`` and the deploy checks
            (``loom.discovery.W001``), an optional one is not.
    """

    def __init__(
        self, packages: Sequence[str] = (), *, entry_points: Sequence[EntryPointGroup] = ()
    ) -> None:
        self._packages = packages
        self._entry_points = entry_points

    def package_names(self) -> list[str]:
        """Return the modules to scan: ``app.packages``, then the entry-point modules, sorted."""
        return [*self._packages, *self._entry_point_modules()]

    def _entry_point_modules(self) -> list[str]:
        modules: set[str] = set()
        for group in self._entry_points:
            provided = importlib.metadata.entry_points(group=group.name)
            modules.update(entry_point.module for entry_point in provided)
        return sorted(modules)

    def empty_groups(self) -> list[str]:
        """Return the required groups with no installed entry point, in declared order."""
        return [
            group.name
            for group in self._entry_points
            if not group.is_optional and not importlib.metadata.entry_points(group=group.name)
        ]

    def discover(self, *, agents: Sequence[str] = ()) -> DiscoveryResult:
        """Return the classes of the configured modules and packages, subpackages included.

        Args:
            agents: The ``app.agents`` artifact paths or globs, carried into the result.

        Returns:
            The discovered classes, each group ordered by module then qualified name.

        Raises:
            ConfigError: When an entry cannot be imported, a directory inside a
                package holds modules but no ``__init__.py``.
        """
        found = collect_from_modules(self._modules())
        models = self._ordered(found.models)
        return DiscoveryResult(
            models=models,
            use_cases=self._ordered(found.use_cases),
            jobs=self._ordered(found.jobs),
            installers=self._ordered(found.installers),
            callbacks=self._ordered(found.callbacks),
            classes=self._ordered(found.classes),
            agent_specs=tuple(agents),
        )

    def _modules(self) -> list[ModuleType]:
        modules: dict[str, ModuleType] = {}
        for package_name in self.package_names():
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

    @staticmethod
    def _ordered(classes: Sequence[ClassT]) -> tuple[ClassT, ...]:
        return tuple(sorted(classes, key=lambda cls: (cls.__module__, cls.__qualname__)))

    def _import(self, module_name: str) -> ModuleType:
        try:
            return importlib.import_module(module_name)
        except Exception as exc:
            raise ConfigError(f"Cannot import discovered module {module_name!r}: {exc}") from exc

    def _raise_import_error(self, module_name: str) -> None:
        cause = sys.exc_info()[1]
        raise ConfigError(f"Cannot import discovered module {module_name!r}: {cause}") from cause
