from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[3] / "src"

_APP_MODELS = """
from loom.core.model import BaseModel, ColumnField
from loom.core.model.types import Integer


class Plain:
    pass


class Kind(BaseModel):
    __tablename__ = "kinds"
    id: int = ColumnField(Integer, primary_key=True)
"""

_WITHOUT_FASTAPI = """
import sys

sys.modules["fastapi"] = None

import loom.stores.sqlalchemy.alembic
from loom.core.discovery import PackageScanner

result = PackageScanner(["app_models"]).discover()

assert [model.__name__ for model in result.models] == ["Kind"], result
assert result.interfaces == (), result
assert "loom.rest" not in sys.modules, sorted(m for m in sys.modules if m.startswith("loom.rest"))
"""


class TestLocatorImportContainment:
    def test_the_locator_and_package_scan_work_without_fastapi(self, tmp_path: Path) -> None:
        package = tmp_path / "app_models"
        package.mkdir()
        (package / "__init__.py").write_text(_APP_MODELS, encoding="utf-8")

        result = subprocess.run(
            [sys.executable, "-c", _WITHOUT_FASTAPI],
            capture_output=True,
            text=True,
            check=False,
            env={**os.environ, "PYTHONPATH": os.pathsep.join((str(_SRC), str(tmp_path)))},
        )

        assert result.returncode == 0, result.stderr
