"""Deploy checks: a registry of id-tagged checks run at startup when deployed.

Each check has a stable id ``<area>.<E|W><nnn>``. In production and staging an
error fails startup and a warning is logged; ``checks.silenced`` lists the ids
accepted on purpose, and the startup log says which ones they are.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pytest

from loom.core.checks import (
    CheckLevel,
    DeployCheckError,
    DeployCheckRegistry,
    Finding,
    enforce_deploy_checks,
    run_deploy_checks,
)
from loom.core.config import ConfigContext
from loom.core.config.errors import ConfigError
from loom.core.environment import Environment

_ERROR_ID = "loom.sample.E001"
_WARNING_ID = "loom.sample.W001"
_LOGGER = "loom.core.checks"


def _registry(*, error: bool = True, warning: bool = True) -> DeployCheckRegistry:
    registry = DeployCheckRegistry()

    @registry.register(_ERROR_ID)
    def _error_check(config: ConfigContext) -> Finding | None:
        if not error or config.has("sample.safe"):
            return None
        return Finding(message="sample is unsafe", hint="set sample.safe")

    @registry.register(_WARNING_ID)
    def _warning_check(config: ConfigContext) -> Finding | None:
        del config
        return Finding(message="sample is loud", hint="quiet it") if warning else None

    return registry


def _config(**raw: Any) -> ConfigContext:
    return ConfigContext.from_dict(raw)


class TestRegistry:
    @pytest.mark.parametrize(
        "check_id",
        ["E001", "loom.sample.X001", "loom.sample.E01", "Loom.sample.E001", "loom..E001"],
    )
    def test_an_id_outside_the_scheme_is_refused(self, check_id: str) -> None:
        registry = DeployCheckRegistry()

        with pytest.raises(ValueError, match="check id"):
            registry.register(check_id)

    def test_an_id_registered_twice_is_refused(self) -> None:
        registry = _registry()

        with pytest.raises(ValueError, match=_ERROR_ID):
            registry.register(_ERROR_ID)

    def test_an_application_registers_under_its_own_area(self) -> None:
        registry = DeployCheckRegistry()

        registry.register("billing.invoices.W001")(lambda config: None)

        assert registry.ids() == ("billing.invoices.W001",)


class TestRunDeployChecks:
    def test_the_level_comes_from_the_id(self) -> None:
        report = run_deploy_checks(_config(), registry=_registry())

        assert {(r.id, r.level) for r in report.results} == {
            (_ERROR_ID, CheckLevel.ERROR),
            (_WARNING_ID, CheckLevel.WARNING),
        }

    def test_a_result_carries_the_message_and_hint_of_its_finding(self) -> None:
        report = run_deploy_checks(_config(), registry=_registry(warning=False))

        (result,) = report.results
        assert (result.id, result.message, result.hint) == (
            _ERROR_ID,
            "sample is unsafe",
            "set sample.safe",
        )

    def test_a_check_that_finds_nothing_reports_nothing(self) -> None:
        report = run_deploy_checks(_config(sample={"safe": True}), registry=_registry())

        assert [r.id for r in report.results] == [_WARNING_ID]

    def test_a_silenced_id_does_not_run_and_is_reported_as_silenced(self) -> None:
        config = _config(checks={"silenced": [_ERROR_ID]})

        report = run_deploy_checks(config, registry=_registry())

        assert [r.id for r in report.results] == [_WARNING_ID]
        assert report.silenced == (_ERROR_ID,)

    def test_a_silenced_entry_outside_the_id_scheme_is_a_config_error(self) -> None:
        config = _config(checks={"silenced": ["E001"]})

        with pytest.raises(ConfigError, match="'E001'"):
            run_deploy_checks(config, registry=_registry())

    def test_a_skipped_area_does_not_run(self) -> None:
        report = run_deploy_checks(_config(), registry=_registry(), skip=("loom.sample",))

        assert report.results == ()

    def test_a_skipped_id_does_not_run(self) -> None:
        report = run_deploy_checks(_config(), registry=_registry(), skip=(_WARNING_ID,))

        assert [r.id for r in report.results] == [_ERROR_ID]

    def test_a_skip_entry_matches_whole_segments_only(self) -> None:
        report = run_deploy_checks(_config(), registry=_registry(), skip=("loom.sam",))

        assert len(report.results) == 2


class TestEnforceDeployChecks:
    @pytest.mark.parametrize("environment", [Environment.DEVELOPMENT, Environment.TEST])
    def test_local_environments_run_no_check(self, environment: Environment) -> None:
        report = enforce_deploy_checks(_config(), environment, registry=_registry())

        assert report is None

    @pytest.mark.parametrize("environment", [Environment.PRODUCTION, Environment.STAGING])
    def test_an_error_fails_startup_in_a_deployed_environment(
        self, environment: Environment
    ) -> None:
        with pytest.raises(DeployCheckError) as caught:
            enforce_deploy_checks(_config(), environment, registry=_registry())

        message = str(caught.value)
        assert f"{_ERROR_ID}: sample is unsafe" in message
        assert "set sample.safe" in message
        assert f"'{environment}'" in message

    def test_the_failure_says_how_to_select_a_local_environment(self) -> None:
        with pytest.raises(DeployCheckError) as caught:
            enforce_deploy_checks(_config(), Environment.PRODUCTION, registry=_registry())

        assert "LOOM_ENV=development" in str(caught.value)
        assert "checks.silenced" in str(caught.value)

    def test_the_error_lists_the_failed_results(self) -> None:
        with pytest.raises(DeployCheckError) as caught:
            enforce_deploy_checks(_config(), Environment.PRODUCTION, registry=_registry())

        assert [r.id for r in caught.value.results] == [_ERROR_ID]

    def test_a_warning_is_logged_and_startup_goes_on(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            report = enforce_deploy_checks(
                _config(sample={"safe": True}), Environment.PRODUCTION, registry=_registry()
            )

        assert report is not None
        assert [r.id for r in report.results] == [_WARNING_ID]
        assert any(
            _WARNING_ID in r.getMessage() and "quiet it" in r.getMessage() for r in caplog.records
        )

    def test_the_silenced_ids_are_logged_as_a_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        config = _config(checks={"silenced": [_ERROR_ID]})

        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            enforce_deploy_checks(config, Environment.STAGING, registry=_registry())

        silenced = [r for r in caplog.records if "silenced" in r.getMessage()]
        assert len(silenced) == 1
        assert silenced[0].levelno == logging.WARNING
        assert _ERROR_ID in silenced[0].getMessage()


_APP_CHECK_MODULE = """\
from loom.core.checks import Finding, deploy_check


@deploy_check("{area}.E001")
def _unsafe(config):
    if not config.has("{area}.unsafe"):
        return None
    return Finding(message="{area} is unsafe", hint="unset {area}.unsafe")
"""


class TestApplicationCheckModules:
    def test_a_listed_module_is_imported_and_its_checks_run(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
    ) -> None:
        area = f"appchecks{abs(hash(request.node.nodeid)) % 10**8}"
        (tmp_path / f"{area}_checks.py").write_text(_APP_CHECK_MODULE.format(area=area))
        monkeypatch.syspath_prepend(str(tmp_path))
        config = _config(checks={"modules": [f"{area}_checks"]}, **{area: {"unsafe": True}})

        report = run_deploy_checks(config)

        assert [r.id for r in report.results if r.id.startswith(area)] == [f"{area}.E001"]

    def test_a_module_that_cannot_be_imported_is_a_config_error(self) -> None:
        config = _config(checks={"modules": ["no_such_package.checks"]})

        with pytest.raises(ConfigError, match="no_such_package.checks"):
            run_deploy_checks(config, registry=_registry())

    def test_an_unknown_key_under_checks_is_a_config_error(self) -> None:
        config = _config(checks={"silence": [_ERROR_ID]})

        with pytest.raises(ConfigError, match="silence"):
            run_deploy_checks(config, registry=_registry())
