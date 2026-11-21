"""Deploy checks: a registry of id-tagged checks run at startup when deployed.

Each check has a stable id ``<area>.<E|W><nnn>``. In production and staging an
error fails startup and a warning is logged; ``checks.silenced`` lists the ids
accepted on purpose, and the startup log says which ones they are.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from loom.core import checks as checks_module
from loom.core.checks import (
    DEPLOY_CHECKS_GROUP,
    CheckLevel,
    CheckScope,
    CheckSet,
    DeployCheckError,
    Finding,
    InstalledChecks,
    SkipReason,
    enforce_deploy_checks,
    run_deploy_checks,
)
from loom.core.config import ConfigContext
from loom.core.config.errors import ConfigError
from loom.core.environment import Environment
from loom.core.plugins.entrypoints import list_entry_points
from tests.helpers.deploy_checks import write_failing_check_module

_ERROR_ID = "loom.sample.E001"
_WARNING_ID = "loom.sample.W001"
_LOGGER = "loom.core.checks"


def _registry(*, error: bool = True, warning: bool = True) -> CheckSet:
    registry = CheckSet()

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
        registry = CheckSet()

        with pytest.raises(ValueError, match="check id"):
            registry.register(check_id)

    def test_an_id_registered_twice_is_refused(self) -> None:
        registry = _registry()

        with pytest.raises(ValueError, match=_ERROR_ID):
            registry.register(_ERROR_ID)

    def test_an_application_registers_under_its_own_area(self) -> None:
        registry = CheckSet()
        finding = Finding(message="sandbox gateway", hint="use the live one")
        registry.register("billing.invoices.W001")(lambda config: finding)

        report = run_deploy_checks(_config(), application=registry)

        assert [r.id for r in report.results] == ["billing.invoices.W001"]


@dataclass(frozen=True)
class _Entry:
    """An entry point named *name* whose load runs *load*."""

    name: str
    load: Callable[[], object]
    module: str = "acme.checks"
    dist: object = None


def _set_of(check_id: str, finding: Finding | None = None) -> CheckSet:
    check_set = CheckSet()
    check_set.register(check_id)(lambda config: finding)
    return check_set


def _install(monkeypatch: pytest.MonkeyPatch, *entries: _Entry) -> list[str]:
    reads: list[str] = []

    def listed(group: str) -> tuple[_Entry, ...]:
        reads.append(group)
        return entries

    monkeypatch.setattr(checks_module, "list_entry_points", listed)
    return reads


def _missing(name: str) -> Callable[[], object]:
    def load() -> object:
        raise ModuleNotFoundError(f"No module named {name!r}", name=name)

    return load


class TestInstalledChecks:
    def test_the_path_is_the_name_of_the_entry_point(self) -> None:
        entry = _Entry("app.rest", lambda: CheckSet())

        assert InstalledChecks(entry).path == "app.rest"  # type: ignore[arg-type]

    def test_loom_registers_its_own_check_sets_by_path(self) -> None:
        names = {entry.name for entry in list_entry_points(DEPLOY_CHECKS_GROUP)}

        assert {
            "observability.log",
            "stores",
            "stores.settings.cursors",
            "app.rest",
            "app.entry_points",
        } <= names

    def test_a_missing_module_of_loom_propagates_unchanged(self) -> None:
        entry = _Entry("app.rest", _missing("loom.x"))

        with pytest.raises(ModuleNotFoundError) as caught:
            InstalledChecks(entry).load()  # type: ignore[arg-type]

        assert caught.value.name == "loom.x"

    def test_a_missing_third_party_module_is_a_config_error_naming_path_and_module(self) -> None:
        entry = _Entry("billing", _missing("acme_gateway.client"))

        with pytest.raises(ConfigError, match=r"billing.*'acme_gateway'.*--skip billing"):
            InstalledChecks(entry).load()  # type: ignore[arg-type]

    def test_the_extras_that_install_the_module_are_named(self) -> None:
        entry = next(e for e in list_entry_points(DEPLOY_CHECKS_GROUP) if e.name == "app.rest")
        broken = _Entry(entry.name, _missing("fastapi"), entry.module, entry.dist)

        with pytest.raises(ConfigError) as caught:
            InstalledChecks(broken).load()  # type: ignore[arg-type]

        message = str(caught.value)
        assert "app.rest" in message
        assert "'fastapi'" in message
        assert "[rest]" in message
        assert "[lambda]" in message


class TestCheckScope:
    @pytest.mark.parametrize(
        ("scope", "raw", "reason"),
        [
            (CheckScope(), {"billing": {}}, None),
            (CheckScope(), {}, SkipReason.NOT_DECLARED),
            (CheckScope(required=frozenset({"billing"})), {}, None),
            (CheckScope(excluded=frozenset({"billing"})), {"billing": {}}, SkipReason.EXCLUDED),
            (
                CheckScope(required=frozenset({"billing"}), excluded=frozenset({"billing"})),
                {},
                SkipReason.EXCLUDED,
            ),
        ],
        ids=["declared", "not-declared", "required", "excluded", "excluded-beats-required"],
    )
    def test_the_reason_a_path_is_skipped(
        self, scope: CheckScope, raw: dict[str, Any], reason: SkipReason | None
    ) -> None:
        assert scope.skip_reason("billing", _config(**raw)) is reason

    def test_a_process_serving_no_http_excludes_app_rest(self) -> None:
        scope = CheckScope.without_rest()

        assert scope.skip_reason("app.rest", _config(app={"rest": {}})) is SkipReason.EXCLUDED


class TestInstalledSetsRun:
    def test_a_declared_path_runs_and_is_listed_under_checked(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        finding = Finding(message="gateway is sandbox", hint="use live")
        _install(
            monkeypatch,
            _Entry("billing", lambda: _set_of("billing.gateway.E001", finding)),
        )

        report = run_deploy_checks(_config(billing={"gateway": "sandbox"}), application=CheckSet())

        assert [r.id for r in report.results] == ["billing.gateway.E001"]
        assert report.checked == ("billing",)
        assert report.skipped == ()

    def test_an_undeclared_path_is_skipped_and_its_entry_is_never_loaded(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        loaded: list[str] = []

        def load() -> CheckSet:
            loaded.append("billing")
            return CheckSet()

        _install(monkeypatch, _Entry("billing", load))

        report = run_deploy_checks(_config(), application=CheckSet())

        assert loaded == []
        assert [s.describe() for s in report.skipped] == ["billing (not declared)"]
        assert report.checked == ()

    def test_a_declared_path_the_process_excludes_is_left_out_by_the_process(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install(monkeypatch, _Entry("billing", lambda: _set_of("billing.gateway.E001", None)))
        scope = CheckScope(excluded=frozenset({"billing"}))

        report = run_deploy_checks(_config(billing={}), scope=scope, application=CheckSet())

        assert [s.describe() for s in report.skipped] == ["billing (left out by the process)"]

    def test_a_used_path_missing_a_third_party_module_fails_whatever_is_silenced(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install(monkeypatch, _Entry("billing", _missing("acme_gateway")))
        config = _config(billing={}, checks={"silenced": ["billing.gateway.E001"]})

        with pytest.raises(ConfigError, match="billing"):
            run_deploy_checks(config, application=CheckSet())

    def test_the_same_id_in_two_sets_is_refused(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install(
            monkeypatch,
            _Entry("billing", lambda: _set_of(_ERROR_ID)),
            _Entry("shipping", lambda: _set_of(_ERROR_ID)),
        )

        with pytest.raises(ValueError, match=_ERROR_ID):
            run_deploy_checks(_config(billing={}, shipping={}), application=CheckSet())

    def test_each_run_scopes_its_own_configuration(self, monkeypatch: pytest.MonkeyPatch) -> None:
        finding = Finding(message="unsafe", hint="fix")
        _install(monkeypatch, _Entry("billing", lambda: _set_of("billing.gateway.E001", finding)))

        first = run_deploy_checks(_config(billing={}), application=CheckSet())
        second = run_deploy_checks(_config(), application=CheckSet())

        assert [r.id for r in first.results] == ["billing.gateway.E001"]
        assert second.results == ()

    def test_the_installed_entry_points_are_read_once_per_run(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        reads = _install(monkeypatch)

        run_deploy_checks(_config(), application=CheckSet())

        assert reads == [DEPLOY_CHECKS_GROUP]

    @pytest.mark.parametrize("environment", [Environment.DEVELOPMENT, Environment.TEST])
    def test_a_local_environment_reads_no_entry_point(
        self, monkeypatch: pytest.MonkeyPatch, environment: Environment
    ) -> None:
        reads = _install(monkeypatch)

        enforce_deploy_checks(_config(billing={}), environment, application=CheckSet())

        assert reads == []


class TestRunDeployChecks:
    def test_the_level_comes_from_the_id(self) -> None:
        report = run_deploy_checks(_config(), application=_registry())

        assert {(r.id, r.level) for r in report.results} == {
            (_ERROR_ID, CheckLevel.ERROR),
            (_WARNING_ID, CheckLevel.WARNING),
        }

    def test_a_check_that_finds_nothing_reports_nothing(self) -> None:
        report = run_deploy_checks(_config(sample={"safe": True}), application=_registry())

        assert [r.id for r in report.results] == [_WARNING_ID]

    def test_a_silenced_id_does_not_run_and_is_reported_as_silenced(self) -> None:
        config = _config(checks={"silenced": [_ERROR_ID]})

        report = run_deploy_checks(config, application=_registry())

        assert [r.id for r in report.results] == [_WARNING_ID]
        assert report.silenced == (_ERROR_ID,)

    def test_a_silenced_entry_outside_the_id_scheme_is_a_config_error(self) -> None:
        config = _config(checks={"silenced": ["E001"]})

        registry = _registry()

        with pytest.raises(ConfigError, match="'E001'"):
            run_deploy_checks(config, application=registry)


class TestEnforceDeployChecks:
    @pytest.mark.parametrize("environment", [Environment.DEVELOPMENT, Environment.TEST])
    def test_local_environments_run_no_check_and_say_so_once(
        self, environment: Environment, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            report = enforce_deploy_checks(_config(), environment, application=_registry())

        skipped = [r.getMessage() for r in caplog.records if "skipped" in r.getMessage()]
        assert report is None
        assert len(skipped) == 1
        assert f"'{environment}'" in skipped[0]
        assert "LOOM_ENV" in skipped[0]

    @pytest.mark.parametrize("environment", [Environment.PRODUCTION, Environment.STAGING])
    def test_an_error_fails_startup_in_a_deployed_environment(
        self, environment: Environment
    ) -> None:
        config = _config()
        registry = _registry()

        with pytest.raises(DeployCheckError) as caught:
            enforce_deploy_checks(config, environment, application=registry)

        message = str(caught.value)
        assert [r.id for r in caught.value.results] == [_ERROR_ID]
        assert f"{_ERROR_ID}: sample is unsafe" in message
        assert "set sample.safe" in message
        assert f"'{environment}'" in message
        assert "LOOM_ENV=development" in message
        assert "checks.silenced" in message

    def test_a_warning_is_logged_and_startup_goes_on(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            report = enforce_deploy_checks(
                _config(sample={"safe": True}), Environment.PRODUCTION, application=_registry()
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
            enforce_deploy_checks(config, Environment.STAGING, application=_registry())

        silenced = [r for r in caplog.records if "silenced" in r.getMessage()]
        assert len(silenced) == 1
        assert silenced[0].levelno == logging.WARNING
        assert _ERROR_ID in silenced[0].getMessage()


class TestApplicationCheckModules:
    def test_a_listed_module_is_imported_and_its_checks_run(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        check = write_failing_check_module(tmp_path)
        monkeypatch.syspath_prepend(str(check.directory))
        area, key = check.trigger.split(".")
        config = _config(checks=check.checks_section(), **{area: {key: True}})

        report = run_deploy_checks(config)

        assert [r.id for r in report.results if r.id.startswith(area)] == [check.check_id]

    def test_a_module_failing_at_import_is_a_config_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "broken_checks_module.py").write_text("raise RuntimeError('boom')\n")
        monkeypatch.syspath_prepend(str(tmp_path))
        config = _config(checks={"modules": ["broken_checks_module"]})

        registry = _registry()

        with pytest.raises(ConfigError, match="broken_checks_module"):
            run_deploy_checks(config, application=registry)

    def test_a_module_that_cannot_be_imported_is_a_config_error(self) -> None:
        config = _config(checks={"modules": ["no_such_package.checks"]})

        registry = _registry()

        with pytest.raises(ConfigError, match="no_such_package.checks"):
            run_deploy_checks(config, application=registry)

    def test_an_unknown_key_under_checks_is_a_config_error(self) -> None:
        config = _config(checks={"silence": [_ERROR_ID]})

        registry = _registry()

        with pytest.raises(ConfigError, match="silence"):
            run_deploy_checks(config, application=registry)


class TestChecksPlaceholders:
    @pytest.mark.parametrize(
        "checks",
        [
            {"silenced": ["${oc.env:SILENCED}"]},
            {"modules": ["${oc.env:CHECK_MODULE}"]},
            {"silenced": "${shared.silenced}"},
        ],
    )
    def test_a_placeholder_in_the_checks_section_is_a_config_error(
        self, checks: dict[str, Any]
    ) -> None:
        config = _config(checks=checks, shared={"silenced": [_ERROR_ID]})

        registry = _registry()

        with pytest.raises(ConfigError, match="the checks section holds a placeholder"):
            run_deploy_checks(config, application=registry)
