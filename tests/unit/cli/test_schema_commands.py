"""``loom schema bootstrap``, ``verify`` and ``audit drop-expired``: the deploy pipeline steps."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from typer.testing import CliRunner

from loom.cli import schema as schema_cli
from loom.cli.app import app
from loom.core.repository.sqlalchemy import rls
from loom.core.repository.sqlalchemy.rls import DatabaseUser, Finding, Report

runner = CliRunner()


def _application(users: dict[str, DatabaseUser]) -> Any:
    bootstrap = SimpleNamespace(
        roles=SimpleNamespace(migrator="app_migrator"), database_users=users
    )
    return SimpleNamespace(bootstrap=bootstrap)


@pytest.fixture
def config(tmp_path: Path) -> Path:
    path = tmp_path / "base.yaml"
    path.write_text("{}\n")
    return path


def test_bootstrap_reads_the_superuser_url_and_the_migrator_and_login_passwords(
    config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[str, dict[str, str]]] = []

    async def apply(url: str, bootstrap: Any, passwords: dict[str, str]) -> None:
        calls.append((url, dict(passwords)))

    users = {
        "app_rw": DatabaseUser(login=True, access="write"),
        "app_nologin": DatabaseUser(login=False, access="read"),
    }
    monkeypatch.setattr(schema_cli, "load_application", lambda path: _application(users))
    monkeypatch.setattr(rls, "apply_bootstrap", apply)
    monkeypatch.setenv("LOOM_SUPERUSER_URL", "postgresql+asyncpg://su@db/app")
    monkeypatch.setenv("LOOM_DB_PASSWORD_APP_RW", "s3cret")
    monkeypatch.setenv("LOOM_DB_PASSWORD_APP_MIGRATOR", "m1grator")

    result = runner.invoke(app, ["schema", "bootstrap", "--config", str(config)])

    assert result.exit_code == 0, result.output
    assert calls == [
        ("postgresql+asyncpg://su@db/app", {"app_migrator": "m1grator", "app_rw": "s3cret"})
    ]


def test_bootstrap_names_the_missing_password_variable(
    config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    users = {"app_rw": DatabaseUser(login=True, access="write")}
    monkeypatch.setattr(schema_cli, "load_application", lambda path: _application(users))
    monkeypatch.setenv("LOOM_SUPERUSER_URL", "postgresql+asyncpg://su@db/app")
    monkeypatch.setenv("LOOM_DB_PASSWORD_APP_MIGRATOR", "m1grator")
    monkeypatch.delenv("LOOM_DB_PASSWORD_APP_RW", raising=False)

    result = runner.invoke(app, ["schema", "bootstrap", "--config", str(config)])

    assert result.exit_code == 1
    assert "LOOM_DB_PASSWORD_APP_RW" in result.output


def test_bootstrap_without_the_superuser_url_fails(
    config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(schema_cli, "load_application", lambda path: _application({}))
    monkeypatch.setenv("LOOM_DB_PASSWORD_APP_MIGRATOR", "m1grator")
    monkeypatch.delenv("LOOM_SUPERUSER_URL", raising=False)

    result = runner.invoke(app, ["schema", "bootstrap", "--config", str(config)])

    assert result.exit_code == 1
    assert "LOOM_SUPERUSER_URL" in result.output


def test_verify_prints_one_finding_per_line_and_exits_one(
    config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report = Report(
        ok=False,
        findings=(Finding("orders", "audit.trigger", "present", "missing"),),
        expired_audit_partitions=("audit_log_p202401",),
    )

    async def check(url: str, application: Any) -> Report:
        assert url == "postgresql+asyncpg://verifier@db/app"
        return report

    monkeypatch.setattr(schema_cli, "load_application", lambda path: _application({}))
    monkeypatch.setattr(rls, "verify", check)
    monkeypatch.setenv("LOOM_VERIFY_URL", "postgresql+asyncpg://verifier@db/app")

    result = runner.invoke(app, ["schema", "verify", "--config", str(config)])

    assert result.exit_code == 1
    assert "orders audit.trigger: expected present, found missing" in result.output
    assert "audit_log_p202401" in result.output


def test_verify_clean_exits_zero(config: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    async def check(url: str, application: Any) -> Report:
        return Report(ok=True, findings=())

    monkeypatch.setattr(schema_cli, "load_application", lambda path: _application({}))
    monkeypatch.setattr(rls, "verify", check)
    monkeypatch.delenv("LOOM_VERIFY_URL", raising=False)
    monkeypatch.setenv("LOOM_SUPERUSER_URL", "postgresql+asyncpg://su@db/app")

    result = runner.invoke(app, ["schema", "verify", "--config", str(config)])

    assert result.exit_code == 0, result.output


@pytest.mark.parametrize(
    ("dropped", "expected"),
    [("audit_log_p202401", "audit_log_p202401"), (None, "no audit partition")],
)
def test_drop_expired_drops_at_most_one_partition(
    config: Path, monkeypatch: pytest.MonkeyPatch, dropped: str | None, expected: str
) -> None:
    actions: list[str] = []

    async def as_migrator(action: Any) -> str | None:
        actions.append(action.__name__)
        return dropped

    monkeypatch.setattr(schema_cli, "_as_migrator", as_migrator)

    result = runner.invoke(app, ["schema", "audit", "drop-expired"])

    assert result.exit_code == 0, result.output
    assert actions == ["drop_expired_audit_partition"]
    assert expected in result.output


def test_ensure_partitions_reports_how_many_were_created(
    config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    actions: list[str] = []

    async def as_migrator(action: Any) -> int:
        actions.append(action.__name__)
        return 1

    monkeypatch.setattr(schema_cli, "_as_migrator", as_migrator)

    result = runner.invoke(app, ["schema", "audit", "ensure-partitions"])

    assert result.exit_code == 0, result.output
    assert actions == ["ensure_audit_partitions"]
    assert "created 1 audit partition" in result.output
