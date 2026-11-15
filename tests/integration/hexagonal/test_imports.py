"""H1: acme shipping's use-case module imports no loom infrastructure and names only ports.

The adapters module does import ``loom.ai``, which shows the check sees an import.
"""

from __future__ import annotations

import ast
import importlib.util
import inspect
from pathlib import Path

import pytest

from tests.integration.hexagonal import adapters, shipping
from tests.integration.hexagonal.shipping import CustomerNotifier, ShipmentQueries, TrackShipment

INFRASTRUCTURE = ("loom.ai", "loom.rest", "loom.celery", "loom.serverless")


def _imported_modules(path: Path) -> set[str]:
    imported: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.add(node.module)
    return imported


class TestTheUseCaseModule:
    @pytest.mark.parametrize("package", INFRASTRUCTURE)
    def test_imports_nothing_from_an_infrastructure_package(self, package: str) -> None:
        imported = _imported_modules(Path(inspect.getfile(shipping)))

        assert importlib.util.find_spec(package) is not None
        assert {
            name for name in imported if name == package or name.startswith(f"{package}.")
        } == set()

    def test_sees_the_infrastructure_the_adapters_import(self) -> None:
        assert "loom.ai.abc" in _imported_modules(Path(inspect.getfile(adapters)))

    def test_its_use_case_names_only_ports(self) -> None:
        parameters = list(inspect.signature(TrackShipment.__init__).parameters.values())[1:]

        assert {p.name: p.annotation for p in parameters} == {
            "queries": ShipmentQueries.__name__,
            "notifier": CustomerNotifier.__name__,
        }
        assert all(p.default is inspect.Parameter.empty for p in parameters)
        assert inspect.isabstract(ShipmentQueries)
        assert inspect.isabstract(CustomerNotifier)
