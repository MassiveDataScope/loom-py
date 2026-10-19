"""Prefect flows take their environment from the central resolver.

The flow parameter ``env`` and the keys of a flow YAML's ``environments``
block are :class:`~loom.core.environment.Environment` values. A run resolves
``env`` (else ``LOOM_ENV``, else production) and hands it to the runner; a
deployment records the environment it was deployed for; an unknown value or
key fails.
"""

from __future__ import annotations

import sys
import textwrap
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock

import msgspec
import prefect
import pytest
import yaml

from loom.core.checks import DeployCheckError
from loom.core.config.errors import ConfigError
from loom.core.environment import ENVIRONMENT_VARIABLE, Environment
from loom.etl import ETLParams, ETLPipeline, ETLProcess, ETLStep, FromTable, IntoTable
from loom.etl.maintenance import MaintenanceStep
from loom.prefect import backfill_flow, deploy_flow, etl_flow, maintenance_flow
from loom.prefect._flow_yaml import extract_pool_config
from loom.prefect.flow import _backfill_body, _body, _maintenance
from loom.prefect.flow._hooks import _env_from_params
from loom.prefect.flow._signature import synthesise_flow_signature
from tests.helpers.deploy_checks import write_failing_check_module

_SOURCE_FILE = "/app/src/flows/env_orders.py"


class _Params(ETLParams, frozen=True):  # type: ignore[misc]
    updated_at_from: datetime
    updated_at_to: datetime


class _Step(ETLStep[_Params]):
    src = FromTable("raw.events")
    target = IntoTable("staging.events").replace()

    def execute(self, params: _Params, *, src: Any = None, **frames: Any) -> Any:
        return src


class _Process(ETLProcess[_Params]):
    steps = [_Step]


class _Pipeline(ETLPipeline[_Params]):
    processes = [_Process]


class _MaintenanceParams(msgspec.Struct, frozen=True):
    dry_run: bool = False


class _Maintenance(MaintenanceStep[_MaintenanceParams]):
    operations = []


_FLOW_YAML = textwrap.dedent(
    """\
    params: {}
    environments:
      production:
        work_pool: loom-fargate
      staging:
        work_pool: loom-staging
    """
)
_WINDOW = {
    "updated_at_from": datetime(2024, 1, 1, tzinfo=UTC),
    "updated_at_to": datetime(2024, 1, 2, tzinfo=UTC),
}


def _flow_yaml(tmp_path: Path, text: str = _FLOW_YAML) -> str:
    path = tmp_path / "flow.yaml"
    path.write_text(text, encoding="utf-8")
    return str(path)


def _etl(tmp_path: Path) -> Any:
    return etl_flow(
        name="env-orders",
        pipeline=_Pipeline,
        params_type=_Params,
        config_path=_flow_yaml(tmp_path),
        source_file=_SOURCE_FILE,
    )


class TestFlowParameter:
    def test_env_defaults_to_none_so_the_resolver_decides(self) -> None:
        signature = synthesise_flow_signature(_Params)

        assert signature.parameters["env"].default is None

    def test_the_notified_environment_is_the_resolved_one(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, "staging")

        assert _env_from_params(MagicMock(parameters={})) == "staging"


class TestEnvironmentsBlock:
    def test_keys_become_environments(self) -> None:
        pools = extract_pool_config({"environments": {"production": {"work_pool": "p"}}})

        assert list(pools) == [Environment.PRODUCTION]
        assert all(isinstance(key, Environment) for key in pools)

    def test_an_unknown_key_fails_naming_it(self) -> None:
        with pytest.raises(ConfigError, match="'prod'"):
            extract_pool_config({"environments": {"prod": {"work_pool": "p"}}})

    def test_a_flow_yaml_with_an_unknown_key_fails_at_build(self, tmp_path: Path) -> None:
        text = "params: {}\nenvironments:\n  dev:\n    work_pool: p\n"
        path = _flow_yaml(tmp_path, text)

        with pytest.raises(ConfigError, match="'dev'"):
            etl_flow(
                name="env-orders",
                pipeline=_Pipeline,
                params_type=_Params,
                config_path=path,
                source_file=_SOURCE_FILE,
            )


class TestRuns:
    @pytest.fixture
    def from_yaml(self, monkeypatch: pytest.MonkeyPatch) -> MagicMock:
        recorder = MagicMock()
        # Runs each process inline: a real Prefect task outside a flow run
        # starts a temporary API server.
        monkeypatch.setattr(prefect, "task", lambda **_kwargs: lambda fn: fn)
        for module in (_body, _backfill_body):
            monkeypatch.setattr(module.ETLRunner, "from_yaml", recorder)
            monkeypatch.setattr(module, "build_observers", lambda *a, **k: [])
            monkeypatch.setattr(module, "install_log_bridge", lambda *a, **k: None)
            monkeypatch.setattr(module, "uninstall_log_bridge", lambda *a, **k: None)
            monkeypatch.setattr(module, "prefect_flow_run_id", lambda: None)
        return recorder

    def test_an_etl_run_hands_its_environment_to_the_runner(
        self, tmp_path: Path, from_yaml: MagicMock
    ) -> None:
        _etl(tmp_path).fn(env="staging", **_WINDOW)

        assert from_yaml.call_args.kwargs["environment"] is Environment.STAGING

    def test_without_env_the_run_takes_the_selector_variable(
        self, tmp_path: Path, from_yaml: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, "development")

        _etl(tmp_path).fn(**_WINDOW)

        assert from_yaml.call_args.kwargs["environment"] is Environment.DEVELOPMENT

    def test_an_unknown_env_fails_the_run(self, tmp_path: Path, from_yaml: MagicMock) -> None:
        flow = _etl(tmp_path)

        with pytest.raises(ConfigError, match="'prod'"):
            flow.fn(env="prod", **_WINDOW)

    def test_a_backfill_slice_hands_its_environment_to_the_runner(
        self, tmp_path: Path, from_yaml: MagicMock
    ) -> None:
        flow = backfill_flow(
            name="env-backfill",
            pipeline=_Pipeline,
            params_type=_Params,
            config_path=_flow_yaml(tmp_path),
            source_file=_SOURCE_FILE,
            per_chunk_processes=["_Process"],
            finalize_processes=["_Process"],
            window_start_field="updated_at_from",
            window_end_field="updated_at_to",
            chunk="day",
        )

        flow.fn(env="production", **_WINDOW)

        environments = {call.kwargs["environment"] for call in from_yaml.call_args_list}
        assert environments == {Environment.PRODUCTION}

    def test_a_deployed_maintenance_run_runs_the_deploy_checks(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        check = write_failing_check_module(tmp_path)
        monkeypatch.syspath_prepend(str(check.directory))
        area, key = check.trigger.split(".")
        storage = tmp_path / "storage.yaml"
        storage.write_text(
            yaml.safe_dump(
                {
                    "storage": {"defaults": {"table_path": {"uri": str(tmp_path / "lake")}}},
                    "checks": check.checks_section(),
                    area: {key: True},
                }
            ),
            encoding="utf-8",
        )
        monkeypatch.setattr(_maintenance, "install_log_bridge", lambda *a, **k: None)
        monkeypatch.setattr(_maintenance, "uninstall_log_bridge", lambda *a, **k: None)
        monkeypatch.setattr(_maintenance, "prefect_flow_run_id", lambda: None)
        flow = maintenance_flow(
            name="env-maintenance",
            step=_Maintenance,
            params_type=_MaintenanceParams,
            config_path=_flow_yaml(tmp_path),
            source_file=_SOURCE_FILE,
            storage_config_path=str(storage),
        )

        with pytest.raises(DeployCheckError, match=check.check_id):
            flow.fn(env="production")


class TestDeployments:
    @staticmethod
    def _deploy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, **kwargs: Any) -> MagicMock:
        flow = _etl(tmp_path)
        module = ModuleType("flows.env_orders")
        module.__file__ = _SOURCE_FILE
        module.env_orders = flow  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, module.__name__, module)
        sourced = MagicMock()
        sourced.deploy = MagicMock(return_value="deployment-id")
        flow.from_source = MagicMock(return_value=sourced)
        deploy_flow(flow, **kwargs)
        return sourced.deploy

    def test_without_env_the_selector_variable_picks_the_pool(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, "staging")

        deploy = self._deploy(tmp_path, monkeypatch)

        assert deploy.call_args.kwargs["work_pool_name"] == "loom-staging"

    def test_the_deployment_records_its_environment(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        deploy = self._deploy(tmp_path, monkeypatch, env="production")

        assert deploy.call_args.kwargs["parameters"]["env"] == "production"

    def test_an_unknown_env_fails_the_deployment(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        with pytest.raises(ConfigError, match="'dev'"):
            self._deploy(tmp_path, monkeypatch, env="dev")
