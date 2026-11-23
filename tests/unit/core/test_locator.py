from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest
import yaml
from sqlalchemy import MetaData

from loom.core.config.errors import ConfigError
from loom.core.locator import DEFAULT_CODE_PATH, add_code_path
from loom.core.stores.catalog import StoreKey
from loom.stores.sqlalchemy.alembic import StoreSchema, load_store_schema
from loom.stores.sqlalchemy.event import SqlalchemyEvent, SqlalchemyKey
from loom.stores.sqlalchemy.rls import SchemaNames

SCOPED_MODULE = "tests.integration.agnosticism.notes"
PLAIN_MODULE = "tests.unit.core.locator_fixtures.plain"
TWO_STORES_MODULE = "tests.unit.core.locator_fixtures.two_stores"
AUDIT_URL = "postgresql+asyncpg://u:p@localhost/audit"


def _config(module: str, **schema: object) -> dict[str, object]:
    return {
        "app": {
            "name": "fixture",
            "packages": [module],
        },
        "stores": {
            "default": {
                "backend": "sqlalchemy",
                "url": "postgresql+asyncpg://u:p@localhost/db",
                "schema": schema,
            }
        },
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
        "guard": "notes_guard",
        "groups": {"readers": "notes_group_r", "writers": "notes_group_w"},
        "version_tables": {"structure": "notes_versions", "data": "notes_data_versions"},
    }


def _write(tmp_path: Path, name: str, config: dict[str, object]) -> str:
    path = tmp_path / name
    path.write_text(yaml.safe_dump(config))
    return str(path)


class TestScopedApplicationSchema:
    def test_a_scoped_application_is_loaded_with_its_own_metadata_and_bootstrap(
        self,
        tmp_path: Path,
    ) -> None:
        application = load_store_schema(
            _write(tmp_path, "a.yaml", _config(SCOPED_MODULE, **_scoped_schema()))
        )

        assert isinstance(application, StoreSchema)
        assert {model.__name__ for model in application.models} == {"Note", "NoteItem", "NoteEvent"}
        assert set(application.metadata.tables) == {"notes", "note_items", "note_events"}
        assert set(application.scoped) == {
            (None, "notes"),
            (None, "note_items"),
            (None, "note_events"),
        }
        assert application.bootstrap is not None
        assert application.bootstrap.schema == "notes"
        assert application.bootstrap.roles.owner == "notes_owner"
        assert application.bootstrap.names == SchemaNames(
            guard="notes_guard",
            readers="notes_group_r",
            writers="notes_group_w",
            version_table="notes_versions",
            data_version_table="notes_data_versions",
        )
        assert application.database.url == "postgresql+asyncpg://u:p@localhost/db"
        assert application.scope_sources == {
            "owner": "identity.subject",
            "editor": "request.editor",
        }

    def test_access_is_parsed_for_every_database_user(self, tmp_path: Path) -> None:
        application = load_store_schema(
            _write(tmp_path, "a.yaml", _config(SCOPED_MODULE, **_scoped_schema()))
        )

        assert application.bootstrap is not None
        users = application.bootstrap.database_users
        assert users["notes_ro"].access == "read"
        assert users["notes_rw"].access == "write"
        assert users["notes_ops"].access == "bypass"
        assert users["notes_ops"].login is True

    def test_an_unknown_access_value_names_the_user(self, tmp_path: Path) -> None:
        schema = _scoped_schema()
        schema["database_users"] = {"notes_ro": {"login": True, "access": "admin"}}

        config_path = _write(tmp_path, "a.yaml", _config(SCOPED_MODULE, **schema))

        with pytest.raises(ConfigError, match=r"notes_ro.*admin"):
            load_store_schema(config_path)

    @pytest.mark.parametrize(
        "missing",
        ["name", "roles", "database_users", "scopes", "guard", "groups", "version_tables"],
    )
    def test_a_missing_schema_key_names_itself(self, tmp_path: Path, missing: str) -> None:
        schema = _scoped_schema()
        del schema[missing]

        config_path = _write(tmp_path, "a.yaml", _config(SCOPED_MODULE, **schema))

        with pytest.raises(ConfigError, match=rf"stores\.default\.schema\.{missing}"):
            load_store_schema(config_path)

    def test_a_missing_name_points_at_schema_init(self, tmp_path: Path) -> None:
        schema = _scoped_schema()
        del schema["guard"]

        config_path = _write(tmp_path, "a.yaml", _config(SCOPED_MODULE, **schema))

        with pytest.raises(ConfigError, match="loom schema init"):
            load_store_schema(config_path)

    def test_groups_sharing_a_name_with_a_role_are_rejected(self, tmp_path: Path) -> None:
        schema = _scoped_schema()
        schema["groups"] = {"readers": "notes_owner", "writers": "notes_group_w"}

        config_path = _write(tmp_path, "a.yaml", _config(SCOPED_MODULE, **schema))

        with pytest.raises(ConfigError, match=r"stores\.default\.schema:.*distinct names"):
            load_store_schema(config_path)

    def test_a_version_table_that_is_not_an_identifier_is_rejected(self, tmp_path: Path) -> None:
        schema = _scoped_schema()
        schema["version_tables"] = {"structure": "Versions", "data": "notes_data_versions"}

        config_path = _write(tmp_path, "a.yaml", _config(SCOPED_MODULE, **schema))

        with pytest.raises(
            ConfigError, match=r"stores\.default\.schema:.*not a usable SQL identifier"
        ):
            load_store_schema(config_path)

    def test_a_declared_scope_without_a_source_names_the_scope(self, tmp_path: Path) -> None:
        schema = _scoped_schema()
        schema["scopes"] = {"owner": "identity.subject"}

        config_path = _write(tmp_path, "a.yaml", _config(SCOPED_MODULE, **schema))

        with pytest.raises(ConfigError, match=r"stores\.default\.schema\.scopes\.editor"):
            load_store_schema(config_path)

    @pytest.mark.parametrize(
        ("scopes", "match"),
        [
            ({"owner": "env.subject", "editor": "request.editor"}, r"scopes\.owner.*env\.subject"),
            (
                {"owner": "identity.subject", "editor": "request.editor", "spare": "env.spare"},
                r"scopes\.spare.*env\.spare",
            ),
        ],
        ids=["declared", "undeclared"],
    )
    def test_a_scope_source_must_be_an_identity_or_request_reference(
        self, tmp_path: Path, scopes: dict[str, str], match: str
    ) -> None:
        schema = _scoped_schema()
        schema["scopes"] = scopes

        config_path = _write(tmp_path, "a.yaml", _config(SCOPED_MODULE, **schema))

        with pytest.raises(ConfigError, match=match):
            load_store_schema(config_path)

    def test_an_application_without_scoped_models_needs_no_schema_keys(
        self, tmp_path: Path
    ) -> None:
        application = load_store_schema(
            _write(tmp_path, "p.yaml", _config(PLAIN_MODULE, mode="create_all"))
        )

        assert application.bootstrap is None
        assert application.scoped == {}
        assert application.scope_sources == {}
        assert set(application.metadata.tables) == {"widgets"}

    def test_two_applications_share_no_metadata(self, tmp_path: Path) -> None:
        scoped = load_store_schema(
            _write(tmp_path, "a.yaml", _config(SCOPED_MODULE, **_scoped_schema()))
        )
        plain = load_store_schema(
            _write(tmp_path, "p.yaml", _config(PLAIN_MODULE, mode="create_all"))
        )

        assert scoped.metadata is not plain.metadata
        assert set(scoped.metadata.tables).isdisjoint(plain.metadata.tables)


class TestLocatorConfigPath:
    def test_the_config_path_defaults_to_the_config_dir_variable(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write(tmp_path, "base.yaml", _config(PLAIN_MODULE, mode="create_all"))
        monkeypatch.setenv("LOOM_CONFIG_DIR", str(tmp_path))

        assert set(load_store_schema().metadata.tables) == {"widgets"}

    def test_an_explicit_path_wins_over_the_config_dir_variable(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        variable = tmp_path / "variable"
        variable.mkdir()
        _write(variable, "base.yaml", _config(SCOPED_MODULE, **_scoped_schema()))
        monkeypatch.setenv("LOOM_CONFIG_DIR", str(variable))
        explicit = _write(tmp_path, "p.yaml", _config(PLAIN_MODULE, mode="create_all"))

        assert set(load_store_schema(explicit).metadata.tables) == {"widgets"}

    def test_a_missing_default_directory_names_both_sources(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("LOOM_CONFIG_DIR", raising=False)
        monkeypatch.chdir(tmp_path)

        with pytest.raises(ConfigError, match="LOOM_CONFIG_DIR"):
            load_store_schema()

    def test_a_relative_code_path_resolves_against_the_config_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
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
        config = _config("relcodepath_models", mode="create_all")
        app_section = config["app"]
        assert isinstance(app_section, dict)
        app_section["code_path"] = "src"
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        monkeypatch.chdir(elsewhere)
        monkeypatch.setattr(sys, "path", list(sys.path))

        application = load_store_schema(_write(project, "loom.yaml", config))

        assert set(application.metadata.tables) == {"gadgets"}


class TestCompiledSchemaNaming:
    def test_an_invalid_schema_name_is_a_config_error_naming_the_key(self, tmp_path: Path) -> None:
        schema = {**_scoped_schema(), "name": "Notes"}

        config_path = _write(tmp_path, "bad.yaml", _config(SCOPED_MODULE, **schema))

        with pytest.raises(ConfigError, match=r"stores\.default\.schema"):
            load_store_schema(config_path)

    def test_a_naming_convention_names_the_constraints_of_the_compiled_metadata(
        self,
        tmp_path: Path,
    ) -> None:
        convention = {"pk": "pk_%(table_name)s", "ck": "ck_%(table_name)s_%(constraint_name)s"}
        schema = {**_scoped_schema(), "naming_convention": convention}

        application = load_store_schema(
            _write(tmp_path, "a.yaml", _config(SCOPED_MODULE, **schema))
        )

        assert application.metadata.naming_convention["pk"] == "pk_%(table_name)s"
        assert application.metadata.tables["notes"].primary_key.name == "pk_notes"

    def test_without_a_naming_convention_the_metadata_keeps_sqlalchemy_defaults(
        self,
        tmp_path: Path,
    ) -> None:
        application = load_store_schema(
            _write(tmp_path, "a.yaml", _config(SCOPED_MODULE, **_scoped_schema()))
        )

        assert application.metadata.naming_convention == MetaData().naming_convention
        assert application.metadata.tables["notes"].primary_key.name is None

    def test_a_naming_convention_for_an_unknown_kind_names_the_key(self, tmp_path: Path) -> None:
        schema = {**_scoped_schema(), "naming_convention": {"primary": "pk_%(table_name)s"}}

        config_path = _write(tmp_path, "bad.yaml", _config(SCOPED_MODULE, **schema))

        with pytest.raises(
            ConfigError, match=r"stores\.default\.schema\.naming_convention.*unknown kind"
        ):
            load_store_schema(config_path)


class TestLocatorEnvironmentOverlay:
    def test_a_config_directory_layers_the_overlay_of_the_environment(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        base = _config(PLAIN_MODULE, mode="create_all")
        _write(tmp_path, "base.yaml", base)
        _write(
            tmp_path,
            "staging.yaml",
            {
                "stores": {
                    "default": {"backend": "sqlalchemy", "url": "postgresql+asyncpg://u:p@stage/db"}
                }
            },
        )
        monkeypatch.setenv("LOOM_ENV", "staging")
        monkeypatch.setenv("LOOM_CONFIG_DIR", str(tmp_path))

        application = load_store_schema()

        assert application.database.url == "postgresql+asyncpg://u:p@stage/db"


class TestAddCodePath:
    """Each test swaps ``sys.path`` for a fresh list, so the real one is never touched."""

    def test_a_relative_directory_resolves_against_the_config_and_goes_first(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "src").mkdir()
        monkeypatch.setattr(sys, "path", ["/already/there"])

        resolved = add_code_path(DEFAULT_CODE_PATH, tmp_path)

        assert resolved == (tmp_path / "src").resolve()
        assert sys.path == [str(resolved), "/already/there"]

    def test_a_missing_directory_is_not_added(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(sys, "path", ["/already/there"])

        add_code_path("missing", tmp_path)

        assert sys.path == ["/already/there"]

    def test_a_directory_already_on_the_path_is_not_added_twice(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "src").mkdir()
        resolved = str((tmp_path / "src").resolve())
        monkeypatch.setattr(sys, "path", [resolved])

        add_code_path("src", tmp_path)

        assert sys.path == [resolved]


@pytest.fixture
def two_stores(tmp_path: Path) -> str:
    config = _config(TWO_STORES_MODULE)
    config["stores"] = {**config["stores"], "audit": {"backend": "sqlalchemy", "url": AUDIT_URL}}
    return _write(tmp_path, "two.yaml", config)


class TestStoreSections:
    @pytest.mark.parametrize(
        ("store", "table", "url", "skipped"),
        [
            ("default", "widgets", "postgresql+asyncpg://u:p@localhost/db", "audit"),
            ("audit", "gadgets", AUDIT_URL, "default"),
        ],
    )
    def test_a_store_is_loaded_with_its_own_models_and_section_and_names_the_others(
        self,
        two_stores: str,
        store: str,
        table: str,
        url: str,
        skipped: str,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.INFO):
            application = load_store_schema(two_stores, store=StoreKey(store))

        loaded = (set(application.metadata.tables), application.store, application.path)
        left_out = [r for r in caplog.records if r.msg == SqlalchemyEvent.STORES_NOT_LOADED]
        assert loaded == ({table}, store, f"stores.{store}")
        assert application.database.url == url
        assert len(left_out) == 1
        assert left_out[0].__dict__[SqlalchemyKey.STORE] == store
        assert left_out[0].__dict__[SqlalchemyKey.SKIPPED_STORES] == [skipped]


@pytest.fixture
def three_stores_dir(tmp_path: Path) -> Path:
    config = _config(TWO_STORES_MODULE)
    config["stores"] = {
        **config["stores"],
        "audit": {"backend": "sqlalchemy", "url": AUDIT_URL},
        "ledger": {"backend": "sqlalchemy", "url": AUDIT_URL},
    }
    _write(tmp_path, "base.yaml", config)
    return tmp_path


class TestStoreSchemaMetadata:
    @pytest.mark.parametrize(
        ("store", "tables"),
        [("default", {"widgets"}), ("audit", {"gadgets"}), ("ledger", set())],
    )
    def test_holds_the_tables_of_the_asked_store_and_only_those(
        self, three_stores_dir: Path, store: str, tables: set[str]
    ) -> None:
        path = str(three_stores_dir / "base.yaml")

        metadata = load_store_schema(path, store=StoreKey(store)).metadata

        assert set(metadata.tables) == tables

    def test_an_undeclared_store_names_the_missing_section(self, three_stores_dir: Path) -> None:
        with pytest.raises(ConfigError, match=r"stores\.missing"):
            load_store_schema(str(three_stores_dir / "base.yaml"), store=StoreKey("missing"))

    def test_reads_the_default_config_directory_without_a_path(
        self, three_stores_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("LOOM_CONFIG_DIR", str(three_stores_dir))

        assert set(load_store_schema().metadata.tables) == {"widgets"}
