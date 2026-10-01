from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

from loom.core.config.errors import ConfigError
from loom.core.locator import Application, load_application

SCOPED_MODULE = "tests.integration.agnosticism.notes"
PLAIN_MODULE = "tests.unit.core.locator_fixtures.plain"


def _config(module: str, **schema: object) -> dict[str, object]:
    return {
        "app": {
            "name": "fixture",
            "discovery": {"mode": "modules", "modules": {"include": [module]}},
        },
        "database": {"url": "postgresql+asyncpg://u:p@localhost/db", "schema": schema},
    }


def _scoped_schema() -> dict[str, object]:
    return {
        "mode": "external",
        "name": "notes",
        "roles": {"owner": "notes_owner", "migrator": "notes_migrator"},
        "database_users": {
            "notes_ro": {"login": True, "access": "read"},
            "notes_rw": {"login": True, "access": "write"},
            "notes_ops": {"login": True, "access": "bypass"},
        },
        "scopes": {"owner": "identity.subject", "editor": "request.editor"},
    }


def _write(tmp_path: Path, name: str, config: dict[str, object]) -> str:
    path = tmp_path / name
    path.write_text(yaml.safe_dump(config))
    return str(path)


def test_a_scoped_application_is_loaded_with_its_own_metadata_and_bootstrap(
    tmp_path: Path,
) -> None:
    application = load_application(
        _write(tmp_path, "a.yaml", _config(SCOPED_MODULE, **_scoped_schema()))
    )

    assert isinstance(application, Application)
    assert {model.__name__ for model in application.models} == {"Note", "NoteItem", "NoteEvent"}
    assert set(application.metadata.tables) == {"notes", "note_items", "note_events"}
    assert set(application.scoped) == {(None, "notes"), (None, "note_items"), (None, "note_events")}
    assert application.bootstrap is not None
    assert application.bootstrap.schema == "notes"
    assert application.bootstrap.roles.owner == "notes_owner"
    assert application.database.url == "postgresql+asyncpg://u:p@localhost/db"
    assert application.scope_sources == {"owner": "identity.subject", "editor": "request.editor"}


def test_access_is_parsed_for_every_database_user(tmp_path: Path) -> None:
    application = load_application(
        _write(tmp_path, "a.yaml", _config(SCOPED_MODULE, **_scoped_schema()))
    )

    assert application.bootstrap is not None
    users = application.bootstrap.database_users
    assert users["notes_ro"].access == "read"
    assert users["notes_rw"].access == "write"
    assert users["notes_ops"].access == "bypass"
    assert users["notes_ops"].login is True


def test_an_unknown_access_value_names_the_user(tmp_path: Path) -> None:
    schema = _scoped_schema()
    schema["database_users"] = {"notes_ro": {"login": True, "access": "admin"}}

    with pytest.raises(ConfigError, match=r"notes_ro.*admin"):
        load_application(_write(tmp_path, "a.yaml", _config(SCOPED_MODULE, **schema)))


@pytest.mark.parametrize("missing", ["name", "roles", "database_users", "scopes"])
def test_a_missing_schema_key_names_itself(tmp_path: Path, missing: str) -> None:
    schema = _scoped_schema()
    del schema[missing]

    with pytest.raises(ConfigError, match=rf"database\.schema\.{missing}"):
        load_application(_write(tmp_path, "a.yaml", _config(SCOPED_MODULE, **schema)))


def test_a_declared_scope_without_a_source_names_the_scope(tmp_path: Path) -> None:
    schema = _scoped_schema()
    schema["scopes"] = {"owner": "identity.subject"}

    with pytest.raises(ConfigError, match=r"database\.schema\.scopes\.editor"):
        load_application(_write(tmp_path, "a.yaml", _config(SCOPED_MODULE, **schema)))


def test_a_scope_source_must_be_an_identity_or_request_reference(tmp_path: Path) -> None:
    schema = _scoped_schema()
    schema["scopes"] = {"owner": "env.subject", "editor": "request.editor"}

    with pytest.raises(ConfigError, match=r"scopes\.owner.*env\.subject"):
        load_application(_write(tmp_path, "a.yaml", _config(SCOPED_MODULE, **schema)))


def test_an_application_without_scoped_models_needs_no_schema_keys(tmp_path: Path) -> None:
    application = load_application(
        _write(tmp_path, "p.yaml", _config(PLAIN_MODULE, mode="create_all"))
    )

    assert application.bootstrap is None
    assert application.scoped == {}
    assert application.scope_sources == {}
    assert set(application.metadata.tables) == {"widgets"}


def test_two_applications_share_no_metadata(tmp_path: Path) -> None:
    scoped = load_application(
        _write(tmp_path, "a.yaml", _config(SCOPED_MODULE, **_scoped_schema()))
    )
    plain = load_application(_write(tmp_path, "p.yaml", _config(PLAIN_MODULE, mode="create_all")))

    assert scoped.metadata is not plain.metadata
    assert set(scoped.metadata.tables).isdisjoint(plain.metadata.tables)


def test_the_config_path_defaults_to_the_loom_config_variable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(
        "LOOM_CONFIG", _write(tmp_path, "p.yaml", _config(PLAIN_MODULE, mode="create_all"))
    )

    assert set(load_application().metadata.tables) == {"widgets"}


def test_a_missing_loom_config_variable_is_named(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOOM_CONFIG", raising=False)

    with pytest.raises(ConfigError, match="LOOM_CONFIG"):
        load_application()


def test_a_relative_code_path_resolves_against_the_config_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "project"
    package = project / "src" / "relcodepath_models"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "models.py").write_text(
        "from loom.core.model import BaseModel, ColumnField\n\n\n"
        "class Gadget(BaseModel):\n"
        '    __tablename__ = "gadgets"\n'
        "    id: int = ColumnField(primary_key=True, autoincrement=True)\n"
    )
    config = _config("relcodepath_models.models", mode="create_all")
    app_section = config["app"]
    assert isinstance(app_section, dict)
    app_section["code_path"] = "src"
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    monkeypatch.setattr(sys, "path", list(sys.path))

    application = load_application(_write(project, "loom.yaml", config))

    assert set(application.metadata.tables) == {"gadgets"}


def test_an_invalid_schema_name_is_a_config_error_naming_the_key(tmp_path: Path) -> None:
    schema = {**_scoped_schema(), "name": "Notes"}

    with pytest.raises(ConfigError, match=r"database\.schema"):
        load_application(_write(tmp_path, "bad.yaml", _config(SCOPED_MODULE, **schema)))
