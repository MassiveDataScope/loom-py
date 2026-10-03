"""The naming convention names the same constraints on the runtime and the migration paths."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
import yaml
from sqlalchemy import MetaData

from loom.core.backend.sqlalchemy import get_metadata, reset_registry
from loom.core.config import ConfigContext, ConfigError
from loom.core.locator import load_application
from loom.core.repository.sqlalchemy.backend import SQLAlchemyBackend
from tests.integration.agnosticism import rosters


@pytest.fixture(autouse=True)
def _shared_registry() -> Iterator[None]:
    reset_registry(naming_convention=None)
    yield
    reset_registry(naming_convention=None)


def _config_file(tmp_path: Path, naming_convention: dict[str, str]) -> Path:
    config = {
        "app": {
            "name": "rosters",
            "discovery": {"mode": "modules", "modules": {"include": [rosters.__name__]}},
        },
        "database": {
            "url": "sqlite+aiosqlite:///",
            "schema": {
                "mode": "external",
                "name": "rosters",
                "roles": {"owner": "rosters_owner", "migrator": "rosters_migrator"},
                "database_users": {"rosters_rw": {"login": True, "access": "write"}},
                "scopes": dict(rosters.SCOPE_BINDINGS),
                "guard": "loom_guard_rosters",
                "groups": {"readers": "rosters_readers", "writers": "rosters_writers"},
                "version_tables": {"structure": "alembic_version", "data": "alembic_data"},
                "naming_convention": naming_convention,
            },
        },
    }
    path = tmp_path / "loom.yaml"
    path.write_text(yaml.safe_dump(config))
    return path


def _names(metadata: MetaData) -> dict[str, set[str]]:
    return {
        name: {str(item.name) for item in (*table.constraints, *table.indexes) if item.name}
        for name, table in metadata.tables.items()
    }


def test_runtime_compilation_names_constraints_as_the_migration_metadata_does(
    tmp_path: Path,
) -> None:
    path = _config_file(tmp_path, rosters.NAMING_CONVENTION)
    application = load_application(str(path))
    wiring = SQLAlchemyBackend().build(ConfigContext.from_yaml(str(path)), rosters.MODELS)

    assert wiring.prepare_models is not None
    wiring.prepare_models(rosters.MODELS)

    assert _names(get_metadata()) == _names(application.metadata)
    assert "fk_seats_tenant_id_roster_id" in _names(get_metadata())["seats"]


def test_an_unknown_kind_in_the_runtime_naming_convention_names_the_key(tmp_path: Path) -> None:
    path = _config_file(tmp_path, {"primary": "pk_%(table_name)s"})

    backend, context = SQLAlchemyBackend(), ConfigContext.from_yaml(str(path))
    with pytest.raises(ConfigError, match=r"database\.schema\.naming_convention.*'primary'"):
        backend.build(context, rosters.MODELS)


def test_resetting_the_registry_without_a_convention_keeps_the_one_set() -> None:
    reset_registry(naming_convention={"pk": "pk_%(table_name)s"})

    reset_registry()

    assert get_metadata().naming_convention["pk"] == "pk_%(table_name)s"


def test_resetting_the_registry_with_none_restores_the_default_convention() -> None:
    reset_registry(naming_convention={"pk": "pk_%(table_name)s"})

    reset_registry(naming_convention=None)

    assert get_metadata().naming_convention == MetaData().naming_convention


async def test_the_lifespan_teardown_restores_the_default_convention() -> None:
    ctx = ConfigContext.from_dict(
        {
            "app": {"name": "demo"},
            "database": {
                "url": "sqlite+aiosqlite:///",
                "schema": {"naming_convention": {"pk": "pk_%(table_name)s"}},
            },
        }
    )
    wiring = SQLAlchemyBackend().build(ctx, ())
    assert wiring.prepare_models is not None
    assert wiring.lifespan_init is not None
    wiring.prepare_models(())

    async with wiring.lifespan_init():
        assert get_metadata().naming_convention["pk"] == "pk_%(table_name)s"

    assert get_metadata().naming_convention == MetaData().naming_convention
