"""Start-up and ``loom check`` run the same deploy checks for one configuration and scope."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from loom.celery.adapter import CeleryAdapter
from loom.composition.project import Project
from loom.core.checks import (
    CheckScope,
    DeployCheckError,
    enforce_deploy_checks,
    run_deploy_checks,
)
from loom.core.config import ConfigContext
from loom.core.environment import Environment
from loom.rest.fastapi.adapter import FastAPIAdapter
from tests.helpers.composition import composition_project

_JWT = {"secret_path": "${secrets:/acme/jwt}", "audience": "acme-api", "issuer": "acme-idp"}
_DOCUMENT = {
    "app": {"name": "acme", "entry": "fastapi", "rest": {"auth": {"jwt": _JWT}}},
    "stores": {"main": {"backend": "sqlalchemy", "schema": {"audit": {"enabled": False}}}},
}
_USE_CASE = (
    "from loom.core.use_case.use_case import UseCase\n"
    "class AcmePing(UseCase[object, str]):\n"
    "    async def execute(self) -> str:\n"
    "        return 'pong'\n"
)


def _ids(report: object) -> list[str]:
    return [result.id for result in report.results]  # type: ignore[attr-defined]


class TestStartUpAndCheckParity:
    def test_both_report_the_same_ids_for_one_configuration_and_scope(self, tmp_path: Path) -> None:
        path = tmp_path / "app.yaml"
        path.write_text(yaml.safe_dump(_DOCUMENT), encoding="utf-8")
        scope = CheckScope(required=frozenset({"app.rest"}))

        checked = Project.check(str(path), environment="test")
        started = enforce_deploy_checks(
            ConfigContext.from_dict(_DOCUMENT), Environment.PRODUCTION, scope=scope
        )

        assert started is not None
        assert _ids(checked.report) == _ids(started)
        assert "loom.audit.W001" in _ids(started)


_LAMBDA = {
    "app": {"name": "acme", "entry": "lambda"},
    "lambda": {"handler": "acme.handler", "functions": {}},
}


def _lambda_with(function: dict[str, object]) -> dict[str, object]:
    return {**_LAMBDA, "lambda": {"handler": "acme.handler", "functions": {"public": function}}}


class TestLambdaHttpParity:
    def test_check_requires_app_rest_for_a_lambda_http_function_as_start_up_does(
        self, tmp_path: Path
    ) -> None:
        document = _lambda_with({"trigger": {"source": "http", "interfaces": ["x"]}})
        path = tmp_path / "app.yaml"
        path.write_text(yaml.safe_dump(document), encoding="utf-8")

        checked = Project.check(str(path), environment="test")
        started = run_deploy_checks(
            ConfigContext.from_dict(document), scope=CheckScope(required=frozenset({"app.rest"}))
        )

        assert _ids(checked.report) == _ids(started)
        assert "loom.rest.E001" in _ids(started)

    def test_a_lambda_without_an_http_function_does_not_require_app_rest(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / "app.yaml"
        path.write_text(
            yaml.safe_dump(_lambda_with({"trigger": {"source": "jobs"}})), encoding="utf-8"
        )

        checked = Project.check(str(path), environment="test")

        assert "app.rest (not declared)" in [s.describe() for s in checked.report.skipped]


class TestRecipeScopes:
    def test_a_fastapi_process_without_app_rest_still_fails_on_the_missing_authentication(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        package = tmp_path / "acme_scope_pkg"
        package.mkdir()
        (package / "__init__.py").write_text("")
        (package / "cases.py").write_text(_USE_CASE)
        monkeypatch.syspath_prepend(str(tmp_path))
        project = composition_project(
            ConfigContext.from_dict({"app": {"name": "acme", "packages": ["acme_scope_pkg"]}}),
            environment=Environment.PRODUCTION,
        )

        with pytest.raises(DeployCheckError, match=r"loom\.rest\.E001"):
            project.compose(FastAPIAdapter())

    def test_a_worker_reports_app_rest_as_left_out_by_the_process(self) -> None:
        document = {
            "app": {"name": "acme", "rest": {}},
            "celery": {"broker_url": "memory://", "result_backend": "cache+memory://"},
        }
        project = composition_project(
            ConfigContext.from_dict(document), environment=Environment.PRODUCTION
        )
        recipe = CeleryAdapter(jobs=[]).recipe(project)

        report = enforce_deploy_checks(
            project.config, project.environment, scope=recipe.check_scope
        )

        assert report is not None
        assert "app.rest (left out by the process)" in [s.describe() for s in report.skipped]
        assert not report.errors
