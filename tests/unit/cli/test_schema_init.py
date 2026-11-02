from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import yaml
from typer.testing import CliRunner

from loom.cli.app import app

runner = CliRunner()
SRC = Path(__file__).resolve().parents[3] / "src"

DERIVED = {
    "name": "notes",
    "guard": "loom_guard_notes",
    "groups": {"readers": "notes_readers", "writers": "notes_writers"},
    "version_tables": {"structure": "alembic_version", "data": "alembic_version_data"},
}


def test_init_prints_the_derived_names() -> None:
    result = runner.invoke(app, ["schema", "init", "notes"])

    assert result.exit_code == 0, result.output
    assert yaml.safe_load(result.output) == {
        "stores": {"default": {"backend": "sqlalchemy", "schema": DERIVED}}
    }


def test_init_merges_into_an_existing_configuration_without_touching_other_keys(
    tmp_path: Path,
) -> None:
    config = tmp_path / "loom.yaml"
    config.write_text(
        yaml.safe_dump(
            {
                "app": {"name": "x"},
                "stores": {
                    "default": {"backend": "sqlalchemy", "url": "u", "schema": {"mode": "external"}}
                },
            }
        )
    )

    result = runner.invoke(app, ["schema", "init", "notes", "--config", str(config)])

    assert result.exit_code == 0, result.output
    written = yaml.safe_load(config.read_text())
    assert written["app"] == {"name": "x"}
    assert written["stores"]["default"]["url"] == "u"
    assert written["stores"]["default"]["schema"] == {"mode": "external", **DERIVED}


def test_init_is_idempotent(tmp_path: Path) -> None:
    config = tmp_path / "loom.yaml"
    config.write_text(
        yaml.safe_dump({"stores": {"default": {"backend": "sqlalchemy", "schema": DERIVED}}})
    )

    result = runner.invoke(app, ["schema", "init", "notes", "--config", str(config)])

    assert result.exit_code == 0, result.output
    assert yaml.safe_load(config.read_text()) == {
        "stores": {"default": {"backend": "sqlalchemy", "schema": DERIVED}}
    }


def test_init_never_overwrites_a_name_the_product_changed(tmp_path: Path) -> None:
    config = tmp_path / "loom.yaml"
    changed = {**DERIVED, "guard": "notes_guard"}
    config.write_text(
        yaml.safe_dump({"stores": {"default": {"backend": "sqlalchemy", "schema": changed}}})
    )

    result = runner.invoke(app, ["schema", "init", "notes", "--config", str(config)])

    assert result.exit_code == 1
    assert "stores.default.schema.guard" in result.output
    assert yaml.safe_load(config.read_text()) == {
        "stores": {"default": {"backend": "sqlalchemy", "schema": changed}}
    }


def test_init_rejects_a_schema_name_postgres_would_change() -> None:
    result = runner.invoke(app, ["schema", "init", "Notes"])

    assert result.exit_code == 1
    assert "Notes" in result.output


def test_init_fills_a_null_database_section(tmp_path: Path) -> None:
    config = tmp_path / "loom.yaml"
    config.write_text("app:\n  name: x\nstores:\n")

    result = runner.invoke(app, ["schema", "init", "notes", "--config", str(config)])

    assert result.exit_code == 0, result.output
    assert yaml.safe_load(config.read_text()) == {
        "app": {"name": "x"},
        "stores": {"default": {"backend": "sqlalchemy", "schema": DERIVED}},
    }


def test_init_refuses_a_configuration_that_is_not_a_mapping(tmp_path: Path) -> None:
    config = tmp_path / "loom.yaml"
    config.write_text("- notes\n")

    result = runner.invoke(app, ["schema", "init", "notes", "--config", str(config)])

    assert result.exit_code == 1
    assert "mapping" in result.output
    assert config.read_text() == "- notes\n"


def test_init_refuses_a_database_section_that_is_not_a_mapping(tmp_path: Path) -> None:
    config = tmp_path / "loom.yaml"
    config.write_text("stores: postgres\n")

    result = runner.invoke(app, ["schema", "init", "notes", "--config", str(config)])

    assert result.exit_code == 1
    assert "stores" in result.output
    assert config.read_text() == "stores: postgres\n"


def test_init_reports_a_section_the_product_replaced_by_a_value(tmp_path: Path) -> None:
    config = tmp_path / "loom.yaml"
    config.write_text(
        yaml.safe_dump(
            {"stores": {"default": {"backend": "sqlalchemy", "schema": {"groups": "custom"}}}}
        )
    )

    result = runner.invoke(app, ["schema", "init", "notes", "--config", str(config)])

    assert result.exit_code == 1
    assert "stores.default.schema.groups" in result.output


def _run(script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PYTHONPATH": str(SRC)},
    )


def test_init_works_without_sqlalchemy() -> None:
    result = _run(
        "import sys\n"
        "sys.modules['sqlalchemy'] = None\n"
        "sys.modules['alembic'] = None\n"
        "from typer.testing import CliRunner\n"
        "from loom.cli.app import app\n"
        "result = CliRunner().invoke(app, ['schema', 'init', 'notes'])\n"
        "assert result.exit_code == 0, (result.output, result.exception)\n"
        "print(result.output)\n"
    )

    assert result.returncode == 0, result.stderr
    assert yaml.safe_load(result.stdout) == {
        "stores": {"default": {"backend": "sqlalchemy", "schema": DERIVED}}
    }


def test_loom_without_the_cli_extra_prints_the_install_hint() -> None:
    result = _run("import sys\nsys.modules['typer'] = None\nfrom loom.cli import main\nmain()\n")

    assert result.returncode == 1
    assert 'pip install "loom-kernel[cli]"' in result.stderr
    assert "Traceback" not in result.stderr
