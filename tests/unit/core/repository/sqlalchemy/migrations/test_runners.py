"""Runner configuration that needs no database (T019)."""

from __future__ import annotations

import pytest

from loom.core.repository.sqlalchemy.migrations import alembic_config
from loom.core.repository.sqlalchemy.migrations.runners import (
    include_object_for,
    validate_timeout,
)
from loom.core.repository.sqlalchemy.rls import SchemaNames

URL = "postgresql+asyncpg://migrator:secret@localhost/app"


def test_a_data_script_location_selects_the_data_tree(tmp_path) -> None:
    config = alembic_config(str(tmp_path / "alembic" / "data"), URL)

    assert config.attributes["tree"] == "data"
    assert config.get_main_option("script_location") == str(tmp_path / "alembic" / "data")
    assert config.get_main_option("sqlalchemy.url") == URL


def test_any_other_script_location_selects_the_structural_tree(tmp_path) -> None:
    config = alembic_config(str(tmp_path / "alembic"), URL)

    assert config.attributes["tree"] == "structural"


def test_the_url_keeps_its_percent_signs_for_configparser(tmp_path) -> None:
    url = "postgresql+asyncpg://m:p%40ss@localhost/app"

    config = alembic_config(str(tmp_path / "alembic"), url)

    assert config.get_main_option("sqlalchemy.url") == url


@pytest.mark.parametrize("value", ["5s", "250ms", "2min", "0"])
def test_timeouts_accept_postgres_durations(value: str) -> None:
    assert validate_timeout(value) == value


@pytest.mark.parametrize("value", ["5s; DROP TABLE x", "", "five", "5 s", "1h"])
def test_timeouts_reject_anything_else(value: str) -> None:
    with pytest.raises(ValueError, match="timeout"):
        validate_timeout(value)


def test_autogenerate_never_drops_a_table_the_models_do_not_declare() -> None:
    include_object = include_object_for(SchemaNames.derived("notes"))

    assert include_object(None, "orphan", "table", True, None) is False
    assert include_object(None, "alembic_version", "table", True, None) is False
    assert include_object(None, "alembic_version_data", "table", False, object()) is False
    assert include_object(None, "notes", "table", True, object()) is True
    assert include_object(None, "notes", "table", False, None) is True
    assert include_object(None, "body", "column", True, None) is True


def test_autogenerate_leaves_the_declared_version_tables_alone() -> None:
    names = SchemaNames(
        guard="g_notes",
        readers="grp_r",
        writers="grp_w",
        version_table="v_struct",
        data_version_table="v_data",
    )
    include_object = include_object_for(names)

    assert include_object(None, "v_struct", "table", True, object()) is False
    assert include_object(None, "v_data", "table", False, object()) is False
    assert include_object(None, "alembic_version", "table", False, object()) is True


def test_the_locator_puts_the_code_path_on_sys_path(tmp_path, monkeypatch) -> None:
    import sys

    from loom.core.locator import load_application

    package = tmp_path / "code" / "located_pkg"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "models.py").write_text(
        "from loom.core.model import BaseModel, ColumnField\n"
        "from loom.core.model.types import Integer\n"
        "class Located(BaseModel):\n"
        "    __tablename__ = 'located'\n"
        "    id: int = ColumnField(Integer, primary_key=True)\n"
    )
    config = tmp_path / "app.yaml"
    config.write_text(
        "app:\n"
        "  name: located\n"
        f"  code_path: {tmp_path / 'code'}\n"
        "  discovery:\n"
        "    mode: modules\n"
        "    modules:\n"
        "      include: [located_pkg.models]\n"
        "database:\n"
        "  url: sqlite+aiosqlite://\n"
    )
    monkeypatch.setattr(sys, "path", list(sys.path))

    application = load_application(str(config))

    assert "located" in application.metadata.tables
