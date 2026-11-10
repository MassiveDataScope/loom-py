"""``loom schema bootstrap --clear-audit-boundary`` (spec 009, operator procedure for ``B``)."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from typer.testing import CliRunner

from loom.cli import schema as schema_cli
from loom.cli.app import app
from loom.core.repository.sqlalchemy import rls
from loom.core.repository.sqlalchemy.rls import BootstrapConfig, DatabaseRoles, SchemaNames

runner = CliRunner()


@pytest.fixture
def sent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> list[dict[str, object]]:
    """The ``configure`` documents ``bootstrap`` sends."""
    documents: list[dict[str, object]] = []
    bootstrap = BootstrapConfig(
        schema="app",
        roles=DatabaseRoles(owner="app_owner", migrator="app_migrator"),
        database_users={},
        names=SchemaNames.derived("app"),
    )

    async def apply(url: str, settings: BootstrapConfig, passwords: dict[str, str]) -> None:
        documents.append(settings.document())

    monkeypatch.setattr(
        schema_cli, "load_application", lambda path: SimpleNamespace(bootstrap=bootstrap)
    )
    monkeypatch.setattr(rls, "apply_bootstrap", apply)
    monkeypatch.setenv("LOOM_SUPERUSER_URL", "postgresql+asyncpg://su@db/app")
    monkeypatch.setenv("LOOM_DB_PASSWORD_APP_MIGRATOR", "m1grator")
    return documents


def _bootstrap(tmp_path: Path, *flags: str) -> Any:
    config = tmp_path / "base.yaml"
    config.write_text("{}\n")
    return runner.invoke(app, ["schema", "bootstrap", "--config", str(config), *flags])


def test_the_flag_asks_configure_to_clear_the_boundary(
    tmp_path: Path, sent: list[dict[str, object]]
) -> None:
    result = _bootstrap(tmp_path, "--clear-audit-boundary")

    assert result.exit_code == 0, result.output
    assert [document["clear_audit_boundary"] for document in sent] == [True]


def test_without_the_flag_the_boundary_is_kept(
    tmp_path: Path, sent: list[dict[str, object]]
) -> None:
    result = _bootstrap(tmp_path)

    assert result.exit_code == 0, result.output
    assert [document["clear_audit_boundary"] for document in sent] == [False]
