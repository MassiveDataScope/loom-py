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
from tests.helpers.deploy_checks import write_failing_check_module

runner = CliRunner()
SRC = Path(__file__).resolve().parents[3] / "src"

_SAFE_APP: dict[str, Any] = {
    "name": "demo",
    "rest": {
        "docs_url": None,
        "redoc_url": None,
        "openapi_url": None,
        "auth": {
            "jwt": {
                "secret_path": "${secrets:/demo/jwt}",
                "audience": "demo-api",
                "issuer": "demo-idp",
            }
        },
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


class TestCheckDeploy:
    def test_a_clean_configuration_passes(self, tmp_path: Path) -> None:
        result = runner.invoke(
            app, ["check", "--deploy", "--env", "production", str(_project(tmp_path))]
        )

        assert result.exit_code == 0, result.output
        assert "loom.rest.E001" not in result.output

    def test_the_environment_picks_the_overlay_of_the_directory(self, tmp_path: Path) -> None:
        result = runner.invoke(
            app, ["check", "--deploy", "--env", "staging", str(_project(tmp_path))]
        )

        assert result.exit_code == 1
        assert "loom.rest.E001" in result.output

    def test_without_env_the_selector_variable_picks_the_overlay(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("LOOM_ENV", "production")

        result = runner.invoke(app, ["check", "--deploy", str(_project(tmp_path))])

        assert result.exit_code == 0, result.output

    def test_an_error_prints_its_id_and_hint_and_fails(self, tmp_path: Path) -> None:
        config = _write(tmp_path / "app.yaml", {"app": _UNAUTHENTICATED_APP})

        result = runner.invoke(app, ["check", "--deploy", str(config)])

        assert result.exit_code == 1
        assert "loom.rest.E001" in result.output
        assert "app.rest.auth.jwt" in result.output

    def test_warnings_alone_pass(self, tmp_path: Path) -> None:
        document = {"app": {**_SAFE_APP, "rest": {**_SAFE_APP["rest"], "docs_url": "/docs"}}}
        config = _write(tmp_path / "app.yaml", document)

        result = runner.invoke(app, ["check", "--deploy", str(config)])

        assert result.exit_code == 0, result.output
        assert "loom.rest.W001" in result.output

    def test_explicit_files_are_merged_left_to_right(self, tmp_path: Path) -> None:
        base = _write(tmp_path / "one.yaml", {"app": _UNAUTHENTICATED_APP})
        overlay = _write(tmp_path / "two.yaml", {"app": _SAFE_APP})

        result = runner.invoke(app, ["check", "--deploy", str(base), str(overlay)])

        assert result.exit_code == 0, result.output

    def test_silenced_ids_are_listed(self, tmp_path: Path) -> None:
        document = {"app": _UNAUTHENTICATED_APP, "checks": {"silenced": ["loom.rest.E001"]}}
        config = _write(tmp_path / "app.yaml", document)

        result = runner.invoke(app, ["check", "--deploy", str(config)])

        assert result.exit_code == 0, result.output
        assert "silenced: loom.rest.E001" in result.output

    def test_an_unknown_environment_fails_naming_the_valid_ones(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["check", "--deploy", "--env", "prod", str(_project(tmp_path))])

        assert result.exit_code == 1
        assert "production, staging, development, test" in result.output

    def test_without_deploy_the_command_says_what_it_needs(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["check", str(_project(tmp_path))])

        assert result.exit_code == 2
        assert "--deploy" in result.output

    def test_a_fresh_process_loads_the_checks_of_every_installed_area(self, tmp_path: Path) -> None:
        config = _write(
            tmp_path / "app.yaml",
            {
                "app": _UNAUTHENTICATED_APP,
                "stores": {
                    "default": {"backend": "sqlalchemy", "url": "${oc.env:DATABASE_URL}"},
                    "settings": {"cursors": {"signing": "unsigned"}},
                },
            },
        )

        result = _run_cli("check", "--deploy", str(config))

        assert result.returncode == 1, result.stderr
        assert "loom.rest.E001" in result.stdout
        assert "loom.cursors.E001" in result.stdout

    def test_a_declared_app_rest_without_fastapi_fails_naming_path_module_and_extras(
        self, tmp_path: Path
    ) -> None:
        config = _write(tmp_path / "app.yaml", {"app": _UNAUTHENTICATED_APP})

        result = _run_cli("check", "--deploy", str(config), blocked=("fastapi",))

        assert result.returncode == 1
        assert "app.rest" in result.stderr
        assert "'fastapi'" in result.stderr
        assert "loom-kernel[rest]" in result.stderr
        assert "loom-kernel[lambda]" in result.stderr
        assert "--skip app.rest" in result.stderr
        assert "loom.rest.E001" not in result.stdout

    def test_skipping_app_rest_runs_the_rest_of_the_checks_without_fastapi(
        self, tmp_path: Path
    ) -> None:
        observability = {"log": {"config": {"renderer": "console"}}}
        config = _write(
            tmp_path / "app.yaml", {"app": _UNAUTHENTICATED_APP, "observability": observability}
        )

        result = _run_cli(
            "check", "--deploy", "--skip", "app.rest", str(config), blocked=("fastapi",)
        )

        assert result.returncode == 0, result.stderr
        assert "loom.logger.W001" in result.stdout
        assert (
            "skipped: app.entry_points (not declared), app.rest (left out by the process)"
            in result.stdout
        )

    def test_an_undeclared_app_rest_is_skipped_and_never_imported(self, tmp_path: Path) -> None:
        config = _write(tmp_path / "worker.yaml", {"app": {"name": "acme"}})

        result = _run_cli("check", "--deploy", str(config), blocked=("fastapi",))

        assert result.returncode == 0, result.stderr
        assert "skipped: app.entry_points (not declared), app.rest (not declared)" in result.stdout

    def test_a_skip_path_that_matches_nothing_is_a_usage_error(self, tmp_path: Path) -> None:
        config = _write(tmp_path / "app.yaml", {"app": _SAFE_APP})

        result = runner.invoke(app, ["check", "--deploy", "--skip", "loom.rest", str(config)])

        assert result.exit_code == 2
        assert "loom.rest matches no check set" in result.output

    def test_the_report_lists_the_paths_it_checked(self, tmp_path: Path) -> None:
        observability = {"log": {"config": {"renderer": "console"}}}
        config = _write(tmp_path / "app.yaml", {"app": _SAFE_APP, "observability": observability})

        result = runner.invoke(app, ["check", "--deploy", str(config)])

        assert "checked: app.rest, observability.log" in result.output
        assert (
            "skipped: app.entry_points (not declared), stores (not declared), "
            "stores.settings.cursors (not declared)"
        ) in result.output

    @pytest.mark.parametrize(
        "options", [(), ("--skip", "app.rest")], ids=["plain", "skipping-rest"]
    )
    def test_an_event_trigger_naming_no_job_fails_whatever_is_skipped(
        self, tmp_path: Path, options: tuple[str, ...]
    ) -> None:
        trigger = {"source": "eventbridge", "job": "Missing", "identity": {"subject": "svc:x"}}
        document = {"app": _SAFE_APP, "lambda": {"functions": {"events": {"trigger": trigger}}}}
        config = _write(tmp_path / "app.yaml", document)

        result = runner.invoke(app, ["check", "--deploy", *options, str(config)])

        assert result.exit_code == 1
        assert "'Missing', which is no discovered Job" in result.output

    def test_a_missing_server_package_does_not_hide_the_rest_checks(self, tmp_path: Path) -> None:
        config = _write(tmp_path / "app.yaml", {"app": _UNAUTHENTICATED_APP})

        result = _run_cli("check", "--deploy", str(config), blocked=("uvicorn", "httptools"))

        assert result.returncode == 1, result.stderr
        assert "loom.rest.E001" in result.stdout

    def test_the_application_check_modules_run_from_its_code_path(self, tmp_path: Path) -> None:
        check = write_failing_check_module(tmp_path / "src")
        area, key = check.trigger.split(".")
        document = {"app": _SAFE_APP, area: {key: True}, "checks": check.checks_section()}
        config = _write(tmp_path / "app.yaml", document)

        result = _run_cli("check", "--deploy", str(config))

        assert result.returncode == 1, result.stderr
        assert check.check_id in result.stdout

    def test_an_override_variable_is_checked_like_the_files(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config = _write(tmp_path / "app.yaml", {"app": _SAFE_APP})
        monkeypatch.setenv("LOOM__OBSERVABILITY__LOG__CONFIG__RENDERER", "console")

        result = runner.invoke(app, ["check", "--deploy", str(config)])

        assert result.exit_code == 0, result.output
        assert "loom.logger.W001" in result.output

    def test_skip_leaves_out_an_area_for_a_process_serving_no_http(self, tmp_path: Path) -> None:
        config = _write(
            tmp_path / "worker.yaml",
            {"app": {"name": "acme"}, "celery": {"broker_url": "memory://"}},
        )

        result = runner.invoke(app, ["check", "--deploy", "--skip", "app.rest", str(config)])

        assert result.exit_code == 0, result.output
        assert (
            "skipped: app.entry_points (not declared), app.rest (left out by the process)"
            in result.output
        )
        assert "loom.rest.E001" not in result.output

    def test_a_secret_placeholder_in_the_logger_config_does_not_break_the_check(
        self,
        tmp_path: Path,
    ) -> None:
        logger_config = {"renderer": "console", "level": "${secrets:/logging/level}"}
        document = {"app": _SAFE_APP, "observability": {"log": {"config": logger_config}}}
        config = _write(tmp_path / "app.yaml", document)

        result = runner.invoke(app, ["check", "--deploy", str(config)])

        assert result.exit_code == 0, result.output
        assert "loom.logger.W001" in result.output

    @pytest.mark.parametrize("code_path", ["${oc.env:CODE_DIR}", 5, ["src"]])
    def test_a_code_path_that_is_not_a_literal_path_fails(
        self, tmp_path: Path, code_path: object
    ) -> None:
        config = _write(tmp_path / "app.yaml", {"app": {**_SAFE_APP, "code_path": code_path}})

        result = runner.invoke(app, ["check", "--deploy", str(config)])

        assert result.exit_code == 1
        assert "app.code_path" in result.output
