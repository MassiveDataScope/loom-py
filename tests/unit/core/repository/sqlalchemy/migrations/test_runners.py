"""Runner configuration that needs no database (T019)."""

from __future__ import annotations

import pytest

from loom.core.repository.sqlalchemy.migrations import alembic_config
from loom.core.repository.sqlalchemy.migrations.runners import validate_timeout

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
