"""``loom check --deploy``: the deploy checks for CI, without starting the app.

The command loads a config directory (``base.yaml`` plus the overlay of the
environment) or explicit files, runs every deploy check whatever the
environment, prints each finding with its id and hint, and exits non-zero when
an error remains.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from loom.cli.app import app

runner = CliRunner()
SRC = Path(__file__).resolve().parents[3] / "src"

_SAFE_APP: dict[str, Any] = {
    "name": "demo",
    "rest": {
        "docs_url": None,
        "redoc_url": None,
        "openapi_url": None,
        "auth": {"jwt": {"secret_path": "${secrets:/demo/jwt}"}},
    },
}
_UNAUTHENTICATED_APP: dict[str, Any] = {
    "name": "demo",
    "rest": {"docs_url": None, "redoc_url": None, "openapi_url": None},
}


def _write(path: Path, document: dict[str, Any]) -> Path:
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return path


def _project(tmp_path: Path) -> Path:
    """A base without authentication that only the production overlay fixes."""
    _write(tmp_path / "base.yaml", {"app": _UNAUTHENTICATED_APP})
    _write(tmp_path / "production.yaml", {"app": _SAFE_APP})
    return tmp_path


def test_a_clean_configuration_passes(tmp_path: Path) -> None:
    result = runner.invoke(
        app, ["check", "--deploy", "--env", "production", str(_project(tmp_path))]
    )

    assert result.exit_code == 0, result.output
    assert "loom.rest.E001" not in result.output


def test_the_environment_picks_the_overlay_of_the_directory(tmp_path: Path) -> None:
    result = runner.invoke(app, ["check", "--deploy", "--env", "staging", str(_project(tmp_path))])

    assert result.exit_code == 1
    assert "loom.rest.E001" in result.output


def test_without_env_the_selector_variable_picks_the_overlay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LOOM_ENV", "production")

    result = runner.invoke(app, ["check", "--deploy", str(_project(tmp_path))])

    assert result.exit_code == 0, result.output


def test_an_error_prints_its_id_and_hint_and_fails(tmp_path: Path) -> None:
    config = _write(tmp_path / "app.yaml", {"app": _UNAUTHENTICATED_APP})

    result = runner.invoke(app, ["check", "--deploy", str(config)])

    assert result.exit_code == 1
    assert "loom.rest.E001" in result.output
    assert "app.rest.auth.jwt" in result.output


def test_warnings_alone_pass(tmp_path: Path) -> None:
    document = {"app": {**_SAFE_APP, "rest": {**_SAFE_APP["rest"], "docs_url": "/docs"}}}
    config = _write(tmp_path / "app.yaml", document)

    result = runner.invoke(app, ["check", "--deploy", str(config)])

    assert result.exit_code == 0, result.output
    assert "loom.rest.W001" in result.output


def test_explicit_files_are_merged_left_to_right(tmp_path: Path) -> None:
    base = _write(tmp_path / "one.yaml", {"app": _UNAUTHENTICATED_APP})
    overlay = _write(tmp_path / "two.yaml", {"app": _SAFE_APP})

    result = runner.invoke(app, ["check", "--deploy", str(base), str(overlay)])

    assert result.exit_code == 0, result.output


def test_silenced_ids_are_listed(tmp_path: Path) -> None:
    document = {"app": _UNAUTHENTICATED_APP, "checks": {"silenced": ["loom.rest.E001"]}}
    config = _write(tmp_path / "app.yaml", document)

    result = runner.invoke(app, ["check", "--deploy", str(config)])

    assert result.exit_code == 0, result.output
    assert "silenced: loom.rest.E001" in result.output


def test_an_unknown_environment_fails_naming_the_valid_ones(tmp_path: Path) -> None:
    result = runner.invoke(app, ["check", "--deploy", "--env", "prod", str(_project(tmp_path))])

    assert result.exit_code == 1
    assert "production, staging, development, test" in result.output


def test_without_deploy_the_command_says_what_it_needs(tmp_path: Path) -> None:
    result = runner.invoke(app, ["check", str(_project(tmp_path))])

    assert result.exit_code == 2
    assert "--deploy" in result.output


def _run_cli(*args: str, blocked: tuple[str, ...] = ()) -> subprocess.CompletedProcess[str]:
    """Run ``loom`` in a clean interpreter, with *blocked* modules made unimportable."""
    script = (
        "import sys\n"
        f"for name in {blocked!r}:\n"
        "    sys.modules[name] = None\n"
        f"sys.argv = ['loom', *{args!r}]\n"
        "from loom.cli import main\n"
        "main()\n"
    )
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PYTHONPATH": str(SRC)},
    )


def test_a_fresh_process_loads_the_checks_of_every_installed_area(tmp_path: Path) -> None:
    config = _write(tmp_path / "app.yaml", {"app": _UNAUTHENTICATED_APP})

    result = _run_cli("check", "--deploy", str(config))

    assert result.returncode == 1, result.stderr
    assert "loom.rest.E001" in result.stdout


def test_without_the_rest_extra_the_rest_checks_do_not_apply(tmp_path: Path) -> None:
    observability = {"log": {"config": {"renderer": "console"}}}
    config = _write(
        tmp_path / "app.yaml", {"app": _UNAUTHENTICATED_APP, "observability": observability}
    )

    result = _run_cli("check", "--deploy", str(config), blocked=("fastapi",))

    assert result.returncode == 0, result.stderr
    assert "loom.logger.W001" in result.stdout
    assert "loom.rest" not in result.stdout


_APP_CHECK_SOURCE = """\
from loom.core.checks import Finding, deploy_check


@deploy_check("clidemo.billing.E001")
def _sandbox(config):
    if not config.has("billing.sandbox"):
        return None
    return Finding(message="billing uses the sandbox", hint="unset billing.sandbox")
"""


def test_the_application_check_modules_run_from_its_code_path(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "clidemo_checks.py").write_text(_APP_CHECK_SOURCE)
    document = {
        "app": _SAFE_APP,
        "billing": {"sandbox": True},
        "checks": {"modules": ["clidemo_checks"]},
    }
    config = _write(tmp_path / "app.yaml", document)

    result = _run_cli("check", "--deploy", str(config))

    assert result.returncode == 1, result.stderr
    assert "clidemo.billing.E001" in result.stdout


def test_an_override_variable_is_checked_like_the_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _write(tmp_path / "app.yaml", {"app": _SAFE_APP})
    monkeypatch.setenv("LOOM__APP__REST__DOCS_URL", "/docs")

    result = runner.invoke(app, ["check", "--deploy", str(config)])

    assert result.exit_code == 0, result.output
    assert "loom.rest.W001" in result.output
