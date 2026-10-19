"""``ETLRunner`` resolves the environment through the central resolver.

The runner takes the logger defaults from the environment and, when built
from a configuration file, runs the deploy checks in production and staging,
like ``create_app`` and the Celery worker.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import msgspec
import pytest
import yaml

from loom.core.checks import DeployCheckError
from loom.core.environment import ENVIRONMENT_VARIABLE, Environment
from loom.etl.lineage._config import ETLObservabilityConfig
from loom.etl.runner import ETLRunner
from tests.helpers.deploy_checks import FailingCheckModule, write_failing_check_module


@pytest.fixture
def check(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FailingCheckModule:
    module = write_failing_check_module(tmp_path)
    monkeypatch.syspath_prepend(str(module.directory))
    return module


def _storage(tmp_path: Path) -> dict[str, Any]:
    return {"defaults": {"table_path": {"uri": str(tmp_path / "lake")}}}


def _yaml(tmp_path: Path, check: FailingCheckModule) -> str:
    area, key = check.trigger.split(".")
    document = {
        "storage": _storage(tmp_path),
        "checks": check.checks_section(),
        area: {key: True},
    }
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


def test_the_logger_takes_the_environment_of_the_runner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "loom.core.observability.runtime.configure_logging_from_values",
        lambda **kwargs: captured.update(kwargs),
    )
    config = msgspec.convert(
        {"log": {"enabled": True, "config": {"level": "INFO"}}}, ETLObservabilityConfig
    )

    ETLRunner.from_dict(_storage(tmp_path), msgspec.to_builtins(config), environment="test")

    assert captured["environment"] is Environment.TEST


def test_an_override_variable_reaches_the_storage_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "etl.yaml"
    path.write_text(yaml.safe_dump({"storage": _storage(tmp_path)}), encoding="utf-8")
    overridden = str(tmp_path / "overridden")
    monkeypatch.setenv("LOOM__STORAGE__DEFAULTS__TABLE_PATH__URI", overridden)

    with patch.object(ETLRunner, "_assemble") as assemble:
        ETLRunner.from_yaml(str(path), environment="development")

    assert assemble.call_args.args[0].defaults.table_path.uri == overridden


def test_a_config_directory_layers_the_overlay_of_the_environment(
    tmp_path: Path, check: FailingCheckModule
) -> None:
    area, key = check.trigger.split(".")
    base = {"storage": _storage(tmp_path), "checks": check.checks_section()}
    (tmp_path / "base.yaml").write_text(yaml.safe_dump(base), encoding="utf-8")
    overlay = {area: {key: True}}
    (tmp_path / "staging.yaml").write_text(yaml.safe_dump(overlay), encoding="utf-8")

    with pytest.raises(DeployCheckError, match=check.check_id):
        ETLRunner.from_yaml(str(tmp_path), environment="staging")


def test_a_config_directory_without_the_overlay_loads_the_base(
    tmp_path: Path, check: FailingCheckModule
) -> None:
    base = {"storage": _storage(tmp_path), "checks": check.checks_section()}
    (tmp_path / "base.yaml").write_text(yaml.safe_dump(base), encoding="utf-8")

    runner = ETLRunner.from_yaml(str(tmp_path), environment="production")

    assert isinstance(runner, ETLRunner)


def test_the_checks_run_before_the_agent_runner_is_built(
    tmp_path: Path, check: FailingCheckModule
) -> None:
    path = _yaml(tmp_path, check)
    make_agent_runner = MagicMock()

    with (
        patch("loom.etl.runner.core.make_agent_runner", make_agent_runner),
        pytest.raises(DeployCheckError),
    ):
        ETLRunner.from_yaml(path, environment="production")

    make_agent_runner.assert_not_called()
