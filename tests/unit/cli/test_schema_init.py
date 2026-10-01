from __future__ import annotations

from pathlib import Path

import yaml
from typer.testing import CliRunner

from loom.cli import app

runner = CliRunner()

DERIVED = {
    "name": "notes",
    "guard": "loom_guard_notes",
    "groups": {"readers": "notes_readers", "writers": "notes_writers"},
    "version_tables": {"structure": "alembic_version", "data": "alembic_version_data"},
}


def test_init_prints_the_derived_names() -> None:
    result = runner.invoke(app, ["schema", "init", "notes"])

    assert result.exit_code == 0, result.output
    assert yaml.safe_load(result.output) == {"database": {"schema": DERIVED}}


def test_init_merges_into_an_existing_configuration_without_touching_other_keys(
    tmp_path: Path,
) -> None:
    config = tmp_path / "loom.yaml"
    config.write_text(
        yaml.safe_dump(
            {"app": {"name": "x"}, "database": {"url": "u", "schema": {"mode": "external"}}}
        )
    )

    result = runner.invoke(app, ["schema", "init", "notes", "--config", str(config)])

    assert result.exit_code == 0, result.output
    written = yaml.safe_load(config.read_text())
    assert written["app"] == {"name": "x"}
    assert written["database"]["url"] == "u"
    assert written["database"]["schema"] == {"mode": "external", **DERIVED}


def test_init_is_idempotent(tmp_path: Path) -> None:
    config = tmp_path / "loom.yaml"
    config.write_text(yaml.safe_dump({"database": {"schema": DERIVED}}))

    result = runner.invoke(app, ["schema", "init", "notes", "--config", str(config)])

    assert result.exit_code == 0, result.output
    assert yaml.safe_load(config.read_text()) == {"database": {"schema": DERIVED}}


def test_init_never_overwrites_a_name_the_product_changed(tmp_path: Path) -> None:
    config = tmp_path / "loom.yaml"
    changed = {**DERIVED, "guard": "notes_guard"}
    config.write_text(yaml.safe_dump({"database": {"schema": changed}}))

    result = runner.invoke(app, ["schema", "init", "notes", "--config", str(config)])

    assert result.exit_code == 1
    assert "database.schema.guard" in result.output
    assert yaml.safe_load(config.read_text()) == {"database": {"schema": changed}}


def test_init_rejects_a_schema_name_postgres_would_change() -> None:
    result = runner.invoke(app, ["schema", "init", "Notes"])

    assert result.exit_code == 1
    assert "Notes" in result.output
