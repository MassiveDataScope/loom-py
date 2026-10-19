"""The REST deploy checks and how ``create_app`` applies the environment.

``loom.rest.E001`` refuses a deployed API that authenticates nobody;
``loom.rest.W001`` warns that the API publishes its schema. ``create_app``
resolves the environment once, loads ``base.yaml`` plus the overlay of the
environment from a config directory, runs the deploy checks in production and
staging, and hands the environment to the use cases through the container.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient

from loom.core.checks import DeployCheckError, run_deploy_checks
from loom.core.config import ConfigContext
from loom.core.config.errors import ConfigError
from loom.core.config.overrides import load_application_config
from loom.core.environment import ENVIRONMENT_VARIABLE
from loom.core.identity import Identity
from loom.rest.auth import RequestCredentials
from loom.rest.checks import AUTHENTICATION_CHECK, SCHEMA_CHECK
from loom.rest.fastapi.auto import create_app
from tests.helpers.deploy_checks import write_failing_check_module
from tests.unit.rest._fixture_app import write_project

_NO_DOCS: dict[str, Any] = {"docs_url": None, "redoc_url": None, "openapi_url": None}
_CUSTOM_AUTH: dict[str, Any] = {**_NO_DOCS, "auth": {"custom": True}}
_PRODUCTION = "production"
_CHECKS_LOGGER = "loom.core.checks"


class _HeaderAuthenticator:
    """A mechanism supplied in code, unknown to the configuration."""

    name = "header"
    provides_roles = False

    async def authenticate(self, credentials: RequestCredentials) -> Identity | None:
        subject = credentials.header("x-subject")
        return None if subject is None else Identity(subject=subject, mechanism=self.name)


def _rest_ids(app: dict[str, Any]) -> list[str]:
    report = run_deploy_checks(ConfigContext.from_dict({"app": app}))
    return [r.id for r in report.results if r.id.startswith("loom.rest.")]


class TestRestChecks:
    def test_an_api_without_authentication_fails_e001(self) -> None:
        assert _rest_ids({"name": "demo", "rest": _NO_DOCS}) == [AUTHENTICATION_CHECK]

    def test_a_configured_jwt_section_passes_e001(self) -> None:
        rest = {**_NO_DOCS, "auth": {"jwt": {"secret_path": "${oc.env:UNSET_SECRET}"}}}

        assert _rest_ids({"name": "demo", "rest": rest}) == []

    def test_a_jwt_given_by_a_placeholder_fails_e001_even_when_it_resolves_to_null(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        document = {
            "shared": {"jwt": {"secret_path": "/k"}},
            "app": {"name": "demo", "rest": {**_NO_DOCS, "auth": {"jwt": "${shared.jwt}"}}},
        }
        path = tmp_path / "app.yaml"
        path.write_text(yaml.safe_dump(document), encoding="utf-8")
        monkeypatch.setenv("LOOM__SHARED__JWT", "")

        report = run_deploy_checks(load_application_config(str(path)))

        assert AUTHENTICATION_CHECK in [r.id for r in report.results]

    def test_authentication_declared_as_custom_passes_e001(self) -> None:
        assert _rest_ids({"name": "demo", "rest": _CUSTOM_AUTH}) == []

    def test_custom_set_to_false_still_fails_e001(self) -> None:
        rest = {**_NO_DOCS, "auth": {"custom": False}}

        assert _rest_ids({"name": "demo", "rest": rest}) == [AUTHENTICATION_CHECK]

    def test_the_default_documentation_urls_publish_the_schema(self) -> None:
        rest = {"auth": {"jwt": {"secret_path": "/k"}}}

        assert _rest_ids({"name": "demo", "rest": rest}) == [SCHEMA_CHECK]

    @pytest.mark.parametrize("still_published", ["docs_url", "redoc_url", "openapi_url"])
    def test_any_documentation_url_left_on_warns_naming_it(self, still_published: str) -> None:
        rest = {**_NO_DOCS, still_published: "/somewhere", "auth": {"jwt": {"secret_path": "/k"}}}

        report = run_deploy_checks(ConfigContext.from_dict({"app": {"rest": rest}}))

        (result,) = [r for r in report.results if r.id == SCHEMA_CHECK]
        assert f"app.rest.{still_published}" in result.message


class TestCreateAppInADeployedEnvironment:
    def test_without_a_selector_the_app_starts_as_production_and_fails_loudly(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config = write_project(tmp_path, rest=_NO_DOCS)
        monkeypatch.delenv(ENVIRONMENT_VARIABLE, raising=False)

        with pytest.raises(DeployCheckError) as caught:
            create_app(config)

        assert AUTHENTICATION_CHECK in str(caught.value)
        assert f"{ENVIRONMENT_VARIABLE}=development" in str(caught.value)

    def test_a_local_environment_runs_no_check(self, tmp_path: Path) -> None:
        app = create_app(write_project(tmp_path), environment="development")

        assert app.title == "Loom API"

    def test_an_authenticator_declared_as_custom_starts_in_production(self, tmp_path: Path) -> None:
        config = write_project(tmp_path, rest=_CUSTOM_AUTH)

        app = create_app(config, environment=_PRODUCTION, authenticator=_HeaderAuthenticator())

        assert app.title == "Loom API"

    def test_a_warning_is_logged_and_the_app_starts(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        config = write_project(tmp_path, rest={"auth": {"custom": True}})

        with caplog.at_level(logging.WARNING, logger=_CHECKS_LOGGER):
            create_app(config, environment="staging", authenticator=_HeaderAuthenticator())

        assert any(SCHEMA_CHECK in r.getMessage() for r in caplog.records)

    def test_a_silenced_error_lets_the_app_start_and_is_logged(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        config = write_project(tmp_path, rest=_NO_DOCS)
        _merge_yaml(Path(config), {"checks": {"silenced": [AUTHENTICATION_CHECK]}})

        with caplog.at_level(logging.WARNING, logger=_CHECKS_LOGGER):
            create_app(config, environment=_PRODUCTION)

        assert any(
            "silenced" in r.getMessage() and AUTHENTICATION_CHECK in r.getMessage()
            for r in caplog.records
        )


class TestAuthenticationSuppliedInCode:
    def test_an_authenticator_without_custom_true_is_refused(self, tmp_path: Path) -> None:
        config = write_project(tmp_path, rest=_NO_DOCS)
        authenticator = _HeaderAuthenticator()

        with pytest.raises(ConfigError, match=r"app\.rest\.auth\.custom"):
            create_app(config, environment="development", authenticator=authenticator)

    def test_custom_true_without_an_authenticator_is_refused(self, tmp_path: Path) -> None:
        config = write_project(tmp_path, rest=_CUSTOM_AUTH)

        with pytest.raises(ConfigError, match="authenticator"):
            create_app(config, environment="development")

    def test_custom_true_beside_a_jwt_section_is_refused(self, tmp_path: Path) -> None:
        jwt = {"secret_path": "/k", "algorithms": ["HS256"], "audience": "demo"}
        rest = {**_NO_DOCS, "auth": {"custom": True, "jwt": jwt}}
        config = write_project(tmp_path, rest=rest)
        authenticator = _HeaderAuthenticator()

        with pytest.raises(ConfigError, match="Exactly one"):
            create_app(config, environment="development", authenticator=authenticator)


def test_the_checks_of_the_application_modules_run_from_its_code_path(tmp_path: Path) -> None:
    check = write_failing_check_module(tmp_path)
    area, key = check.trigger.split(".")
    config = write_project(tmp_path, rest=_NO_DOCS)
    checks = {**check.checks_section(), "silenced": [AUTHENTICATION_CHECK]}
    _merge_yaml(Path(config), {"checks": checks, area: {key: True}})

    with pytest.raises(DeployCheckError, match=check.check_id):
        create_app(config, environment=_PRODUCTION)


def test_a_removed_logger_key_stops_startup(tmp_path: Path) -> None:
    observability = {"log": {"config": {"environment": "dev"}}}
    config = write_project(tmp_path, observability=observability)

    with pytest.raises(ConfigError, match="environment"):
        create_app(config, environment="development")


class TestOverridesFromVariables:
    def test_an_override_switching_off_authentication_stops_startup(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config = write_project(tmp_path, rest={**_NO_DOCS, "auth": {"custom": True}})
        monkeypatch.setenv("LOOM__APP__REST__AUTH__CUSTOM", "false")

        with pytest.raises(ConfigError, match="reviewed"):
            create_app(config, environment=_PRODUCTION)

    def test_an_override_reaches_the_application(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config = write_project(tmp_path)
        monkeypatch.setenv("LOOM__APP__REST__TITLE", "From A Variable")

        app = create_app(config, environment="development")

        assert app.title == "From A Variable"

    @pytest.mark.parametrize(
        ("variable", "typo"),
        [
            ("LOOM__APP__REST__DOCS_ULR", "docs_ulr"),
            ("LOOM__APP__REST__OPENAPI_ULR", "openapi_ulr"),
            ("LOOM__APP__REST__CORS__ALLOW_ORIGNS", "allow_origns"),
            ("LOOM__OBSERVABILITY__LOG__ENABLD", "enabld"),
            ("LOOM__OBSERVABILITY__OTEL__ENABLD", "enabld"),
        ],
    )
    def test_a_deep_typo_under_app_rest_or_observability_stops_startup(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, variable: str, typo: str
    ) -> None:
        config = write_project(tmp_path)
        monkeypatch.setenv(variable, "null")

        with pytest.raises(ConfigError, match=typo):
            create_app(config, environment="development")

    def test_a_misspelt_key_inside_a_known_section_stops_startup(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config = write_project(tmp_path)
        monkeypatch.setenv("LOOM__OBSERVABILITY__LOG__CONFIG__LEVLE", "DEBUG")

        with pytest.raises(ConfigError, match="levle"):
            create_app(config, environment="development")


def test_a_misspelt_celery_key_stops_startup(tmp_path: Path) -> None:
    config = write_project(tmp_path, rest=_NO_DOCS)
    _merge_yaml(Path(config), {"celery": {"broker_urll": "memory://"}})

    with pytest.raises(ConfigError, match="broker_urll"):
        create_app(config, environment="development")


class TestCreateAppFromAConfigDirectory:
    def test_the_overlay_of_the_environment_overrides_the_base(self, tmp_path: Path) -> None:
        config_dir = _config_dir(tmp_path)

        app = create_app(config_dir=str(config_dir), environment="development")

        assert app.title == "Local API"

    def test_the_variable_selects_the_overlay(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config_dir = _config_dir(tmp_path)
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, "development")

        app = create_app(config_dir=str(config_dir))

        assert app.title == "Local API"

    def test_files_and_a_directory_together_are_refused(self, tmp_path: Path) -> None:
        config_dir = _config_dir(tmp_path)
        base = str(config_dir / "base.yaml")

        with pytest.raises(ConfigError, match="config_dir"):
            create_app(base, config_dir=str(config_dir))

    def test_no_configuration_at_all_is_refused(self) -> None:
        with pytest.raises(ConfigError, match="config_dir"):
            create_app()


def test_a_use_case_injects_the_resolved_environment(tmp_path: Path) -> None:
    module = "loom_environment_fixture_app"
    (tmp_path / f"{module}.py").write_text(_ENVIRONMENT_APP, encoding="utf-8")
    config = tmp_path / "app.yaml"
    config.write_text(
        yaml.safe_dump(
            {
                "app": {
                    "name": "environment-demo",
                    "code_path": ".",
                    "discovery": {
                        "mode": "interfaces",
                        "interfaces": {"modules": [module], "warn_recommended": False},
                    },
                },
                "database": {"url": "sqlite+aiosqlite:///"},
                # Explicit about cursors ahead of #371, whose default "signed"
                # needs a key. In development no deploy check runs.
                "persistence": {"cursors": {"signing": "ephemeral"}},
            }
        ),
        encoding="utf-8",
    )

    with TestClient(create_app(str(config), environment="development")) as client:
        response = client.get("/environment/")

    assert response.json() == "development"


_ENVIRONMENT_APP = '''\
"""Fixture app whose use case injects the resolved environment."""

from __future__ import annotations

from typing import Any

from loom.core.environment import Environment
from loom.core.model import BaseModel, ColumnField
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface, RestRoute


class EnvironmentRecord(BaseModel):
    __tablename__ = "environment_records_fixture"

    id: int = ColumnField(primary_key=True, autoincrement=True)


class CurrentEnvironment(UseCase[EnvironmentRecord, str]):
    def __init__(self, environment: Environment) -> None:
        super().__init__()
        self._environment = environment

    async def execute(self, **kwargs: Any) -> str:
        return str(self._environment)


class EnvironmentInterface(RestInterface[str]):
    prefix = "/environment"
    routes = (RestRoute(use_case=CurrentEnvironment, method="GET", path="/"),)
'''


def _merge_yaml(path: Path, extra: dict[str, Any]) -> None:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    document.update(extra)
    path.write_text(yaml.safe_dump(document), encoding="utf-8")


def _config_dir(tmp_path: Path) -> Path:
    """Turn the fixture project into ``base.yaml`` plus a development overlay."""
    base = Path(write_project(tmp_path, rest={**_NO_DOCS, "title": "Base API"}))
    base.rename(tmp_path / "base.yaml")
    (tmp_path / "development.yaml").write_text(
        yaml.safe_dump({"app": {"rest": {"title": "Local API"}}}), encoding="utf-8"
    )
    return tmp_path
