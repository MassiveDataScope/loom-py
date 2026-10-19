"""``ETLRunner`` resolves the environment through the central resolver.

The runner takes the logger defaults from the environment and, when built
from a configuration file, runs the deploy checks in production and staging,
like ``create_app`` and the Celery worker.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import msgspec
import pytest
import yaml

from loom.core.checks import DeployCheckError
from loom.core.environment import ENVIRONMENT_VARIABLE, Environment
from loom.etl.lineage._config import ETLObservabilityConfig
from loom.etl.runner import ETLRunner
from tests.helpers.deploy_checks import FailingCheckModule, write_failing_check_module

_STORAGE: dict[str, Any] = {"defaults": {"table_path": {"uri": "/tmp/lake"}}}


@pytest.fixture
def check(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FailingCheckModule:
    module = write_failing_check_module(tmp_path)
    monkeypatch.syspath_prepend(str(module.directory))
    return module


def _yaml(tmp_path: Path, check: FailingCheckModule) -> str:
    area, key = check.trigger.split(".")
    document = {"storage": _STORAGE, "checks": check.checks_section(), area: {key: True}}
    path = tmp_path / "etl.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return str(path)


@pytest.mark.parametrize("environment", ["production", "staging"])
def test_a_deployed_runner_from_yaml_runs_the_deploy_checks(
    tmp_path: Path, check: FailingCheckModule, environment: str
) -> None:
    path = _yaml(tmp_path, check)

    with pytest.raises(DeployCheckError, match=check.check_id):
        ETLRunner.from_yaml(path, environment=environment)


def test_the_selector_variable_reaches_the_runner(
    tmp_path: Path, check: FailingCheckModule, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _yaml(tmp_path, check)
    monkeypatch.setenv(ENVIRONMENT_VARIABLE, "production")

    with pytest.raises(DeployCheckError, match=check.check_id):
        ETLRunner.from_yaml(path)


def test_a_local_runner_runs_no_check(tmp_path: Path, check: FailingCheckModule) -> None:
    runner = ETLRunner.from_yaml(_yaml(tmp_path, check), environment="development")

    assert isinstance(runner, ETLRunner)


def test_the_logger_takes_the_environment_of_the_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "loom.core.observability.runtime.configure_logging_from_values",
        lambda **kwargs: captured.update(kwargs),
    )
    config = msgspec.convert(
        {"log": {"enabled": True, "config": {"level": "INFO"}}}, ETLObservabilityConfig
    )

    ETLRunner.from_dict(_STORAGE, msgspec.to_builtins(config), environment="test")

    assert captured["environment"] is Environment.TEST


def test_an_override_variable_reaches_the_storage_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "etl.yaml"
    path.write_text(yaml.safe_dump({"storage": _STORAGE}), encoding="utf-8")
    monkeypatch.setenv("LOOM__STORAGE__DEFAULTS__TABLE_PATH__URI", "/tmp/overridden")
    captured: dict[str, Any] = {}

    def _capture(cls: type[ETLRunner], config: Any, *args: Any, **kwargs: Any) -> None:
        captured["config"] = config

    monkeypatch.setattr(ETLRunner, "from_config", classmethod(_capture))

    ETLRunner.from_yaml(str(path), environment="development")

    assert captured["config"].defaults.table_path.uri == "/tmp/overridden"
