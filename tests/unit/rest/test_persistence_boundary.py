"""SC-001: ``create_app`` knows no persistence backend.

The source-level check reads ``auto.py`` as text so a backend import or a
name comparison cannot slip back in; the boot check runs in a clean
subprocess so modules imported by unrelated suites cannot mask a leak.
"""

from __future__ import annotations

import ast
import os
import re
import subprocess
import sys
from pathlib import Path

import loom.rest.fastapi.auto as auto_module
from tests.unit.rest._fixture_app import UUID4_ID_FIELD, write_project

_SRC = Path(__file__).resolve().parents[3] / "src"
_AUTO_SOURCE = Path(auto_module.__file__).read_text(encoding="utf-8")
_BACKEND_COMPARISON = re.compile(r"\bbackend\s*(==|!=)")

_NON_RELATIONAL_APP_SCRIPT = """
import sys
sys.modules["sqlalchemy"] = None

from loom.rest.fastapi.auto import create_app

app = create_app(sys.argv[1])
assert app is not None
"""


def _imported_modules(source: str) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            modules.add(node.module)
    return modules


def test_auto_imports_no_repository_backend() -> None:
    leaked = {m for m in _imported_modules(_AUTO_SOURCE) if m.startswith("loom.core.repository")}

    assert leaked == set()


def test_auto_compares_no_backend_name() -> None:
    assert _BACKEND_COMPARISON.search(_AUTO_SOURCE) is None


def test_dynamodb_app_boots_without_sqlalchemy(tmp_path: Path) -> None:
    config_path = write_project(
        tmp_path,
        persistence={
            "backend": "dynamodb",
            "dynamodb": {
                "region": "eu-west-1",
                "table": "records",
                "url": "http://localhost:8000",
            },
        },
        database=None,
    )
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join([str(_SRC), str(_SRC.parent)]),
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
    }

    _assert_boots_without_sqlalchemy(config_path, env)


def test_mongo_app_boots_without_sqlalchemy(tmp_path: Path) -> None:
    config_path = write_project(
        tmp_path,
        persistence={
            "backend": "mongo",
            "mongo": {"url": "mongodb://localhost:27017", "database": "records"},
        },
        database=None,
        id_field=UUID4_ID_FIELD,
    )
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(_SRC), str(_SRC.parent)])}

    _assert_boots_without_sqlalchemy(config_path, env)


def _assert_boots_without_sqlalchemy(config_path: str, env: dict[str, str]) -> None:
    result = subprocess.run(
        [sys.executable, "-c", _NON_RELATIONAL_APP_SCRIPT, config_path],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0, f"create_app failed:\n{result.stderr}"
