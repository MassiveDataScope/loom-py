"""Tests for ``discover_and_deploy_etls(config=...)`` (US5 scenarios 2, 3, 5, 6; SC-003)."""

from __future__ import annotations

import os
import textwrap
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from prefect.flows import Flow

from loom.core.config import ConfigError
from loom.prefect._meta import LOOM_ETL_META_ATTR, ETLFlowMeta
from loom.prefect.deploy import discover_and_deploy_etls

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "prefect" / "etls"
PIPELINES = "tests.fixtures.prefect.pipelines"
ENV_VAR = "LOOM_ETL_CONFIG"
ENTRYPOINT_MODULE = "loom.prefect.deploy.entrypoint"


class _Recorder:
    """Stands in for ``Flow.from_source`` and records what the deployer passes."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.deploy = MagicMock(return_value="deployment-id")

    def __call__(self, **kwargs: Any) -> _Recorder:
        self.calls.append({**kwargs, "env_at_call": os.environ.get(ENV_VAR)})
        return self


@pytest.fixture
def recorder(monkeypatch: pytest.MonkeyPatch) -> _Recorder:
    rec = _Recorder()
    monkeypatch.setattr(Flow, "from_source", rec)
    return rec


def _write(tmp_path: Path, name: str, body: str) -> Path:
    path = tmp_path / name
    path.write_text(textwrap.dedent(body), encoding="utf-8")
    return path


def _deploy_names(recorder: _Recorder) -> list[str]:
    return [call.kwargs["name"] for call in recorder.deploy.call_args_list]


# --- US5 scenario 2: etls mapping ---------------------------------------------


def test_etls_file_registers_one_deployment_per_key(recorder: _Recorder) -> None:
    ids = discover_and_deploy_etls(config=str(FIXTURES / "billing.yaml"))
    assert ids == ["deployment-id", "deployment-id"]
    assert sorted(_deploy_names(recorder)) == ["invoice_sync", "monthly_close"]
    assert [call["entrypoint"] for call in recorder.calls] == [
        f"{ENTRYPOINT_MODULE}.monthly_close",
        f"{ENTRYPOINT_MODULE}.invoice_sync",
    ]


# --- US5 scenario 3: glob -----------------------------------------------------


def test_glob_deploys_every_matching_file(recorder: _Recorder) -> None:
    discover_and_deploy_etls(config=str(FIXTURES / "*.yaml"))
    assert sorted(_deploy_names(recorder)) == [
        "daily-orders",
        "invoice_sync",
        "monthly_close",
        "orders_reconcile",
    ]


def test_non_etl_file_in_glob_fails_before_any_deployment(
    tmp_path: Path, recorder: _Recorder
) -> None:
    _write(tmp_path, "orders.yaml", f"etl: orders\npipeline: {PIPELINES}.OrdersPipeline\n")
    stray = _write(tmp_path, "storage.yaml", "storage:\n  tables: {}\n")
    with pytest.raises(ConfigError, match=str(stray)):
        discover_and_deploy_etls(config=str(tmp_path / "*.yaml"))
    assert recorder.calls == []
    recorder.deploy.assert_not_called()


# --- US5 scenario 6: exclusivity ----------------------------------------------


def test_both_or_neither_source_is_an_error() -> None:
    with pytest.raises(ValueError, match="flows_package.*config"):
        discover_and_deploy_etls(flows_package="pkg", config="etls.yaml")
    with pytest.raises(ValueError, match="flows_package.*config"):
        discover_and_deploy_etls()


# --- FR-044: env contract -----------------------------------------------------


def test_variable_is_exported_around_from_source_and_restored_when_previously_unset(
    monkeypatch: pytest.MonkeyPatch, recorder: _Recorder
) -> None:
    monkeypatch.delenv(ENV_VAR, raising=False)
    config_uri = str(FIXTURES / "daily_orders.yaml")
    discover_and_deploy_etls(config=config_uri)
    assert recorder.calls[0]["env_at_call"] == config_uri
    assert ENV_VAR not in os.environ


def test_variable_is_restored_to_previous_value(
    monkeypatch: pytest.MonkeyPatch, recorder: _Recorder
) -> None:
    monkeypatch.setenv(ENV_VAR, "previous")
    config_uri = str(FIXTURES / "daily_orders.yaml")
    discover_and_deploy_etls(config=config_uri)
    assert recorder.calls[0]["env_at_call"] == config_uri
    assert os.environ[ENV_VAR] == "previous"


def test_job_variables_env_records_uri_and_keeps_user_keys(
    tmp_path: Path, recorder: _Recorder
) -> None:
    path = _write(
        tmp_path,
        "orders.yaml",
        f"""\
        etl: orders
        pipeline: {PIPELINES}.OrdersPipeline
        environments:
          prod:
            job_variables:
              cpu: 512
              env:
                OTHER: keep
        """,
    )
    discover_and_deploy_etls(config=str(path))
    job_variables = recorder.deploy.call_args.kwargs["job_variables"]
    assert job_variables == {"cpu": 512, "env": {"OTHER": "keep", ENV_VAR: str(path)}}


def test_declaration_setting_the_variable_itself_is_rejected_naming_the_etl(
    tmp_path: Path, recorder: _Recorder
) -> None:
    path = _write(
        tmp_path,
        "orders.yaml",
        f"""\
        etl: orders
        pipeline: {PIPELINES}.OrdersPipeline
        environments:
          prod:
            job_variables:
              env:
                {ENV_VAR}: user-value
        """,
    )
    with pytest.raises(ConfigError, match=f"'orders'.*{ENV_VAR}"):
        discover_and_deploy_etls(config=str(path))
    assert recorder.calls == []
    recorder.deploy.assert_not_called()


def test_job_variables_env_as_a_list_is_rejected_before_any_deployment(
    tmp_path: Path, recorder: _Recorder
) -> None:
    path = _write(
        tmp_path,
        "orders.yaml",
        f"""\
        etl: orders
        pipeline: {PIPELINES}.OrdersPipeline
        environments:
          prod:
            job_variables:
              env: [FOO=bar]
        """,
    )
    with pytest.raises(ConfigError, match="'orders'.*prod.*job_variables.env"):
        discover_and_deploy_etls(config=str(path))
    assert recorder.calls == []
    recorder.deploy.assert_not_called()


def test_offending_declaration_after_a_valid_one_deploys_nothing(
    tmp_path: Path, recorder: _Recorder
) -> None:
    path = _write(
        tmp_path,
        "etls.yaml",
        f"""\
        etls:
          good:
            pipeline: {PIPELINES}.OrdersPipeline
          bad:
            pipeline: {PIPELINES}.OrdersPipeline
            environments:
              staging:
                job_variables:
                  env:
                    {ENV_VAR}: user-value
        """,
    )
    with pytest.raises(ConfigError, match=f"'bad'.*staging.*{ENV_VAR}"):
        discover_and_deploy_etls(config=str(path))
    assert recorder.calls == []
    recorder.deploy.assert_not_called()


# --- SC-003: package path and YAML path agree ---------------------------------


def _write_flows_package(tmp_path: Path, name: str) -> str:
    package = tmp_path / name
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "daily.py").write_text(
        textwrap.dedent(
            f"""\
            from loom.prefect import etl_flow
            from {PIPELINES} import OrdersParams, OrdersPipeline

            daily_orders = etl_flow(
                name="daily-orders",
                pipeline=OrdersPipeline,
                params_type=OrdersParams,
                config_path={str(FIXTURES / "daily_orders.yaml")!r},
                source_file=__file__,
                storage_config_path="/srv/orders/config.yaml",
            )
            """
        ),
        encoding="utf-8",
    )
    return name


def test_a_package_exposing_a_flow_under_another_attribute_deploys_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, recorder: _Recorder
) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    package = _write_flows_package(tmp_path, "flows_pkg_misnamed")
    misnamed = (tmp_path / package / "daily.py").read_text(encoding="utf-8")
    (tmp_path / package / "late.py").write_text(
        misnamed.replace("daily_orders = ", "orders = ").replace("daily-orders", "late-orders"),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="'late-orders' as attribute 'late_orders'"):
        discover_and_deploy_etls(flows_package=package)

    assert recorder.calls == []
    recorder.deploy.assert_not_called()


def _without_env(kwargs: Mapping[str, Any]) -> dict[str, Any]:
    job_variables = {k: v for k, v in kwargs["job_variables"].items() if k != "env"}
    return {**kwargs, "job_variables": job_variables}


def test_package_and_yaml_paths_pass_prefect_the_same_deployment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, recorder: _Recorder
) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    package = _write_flows_package(tmp_path, "flows_pkg_sc003")

    discover_and_deploy_etls(flows_package=package)
    from_package = recorder.deploy.call_args.kwargs
    discover_and_deploy_etls(config=str(FIXTURES / "daily_orders.yaml"))
    from_yaml = recorder.deploy.call_args.kwargs

    assert "env" not in from_package["job_variables"]
    assert from_yaml["job_variables"]["env"] == {ENV_VAR: str(FIXTURES / "daily_orders.yaml")}
    assert _without_env(from_yaml) == from_package
    assert recorder.calls[0]["entrypoint"].endswith("daily.py:daily_orders")
    assert recorder.calls[1]["entrypoint"] == f"{ENTRYPOINT_MODULE}.daily_orders"


# --- US5 scenario 5: the real Flow.from_source --------------------------------


def test_real_from_source_rebuilds_flow_through_the_entrypoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv(ENV_VAR, raising=False)
    path = _write(
        tmp_path,
        "orders.yaml",
        f"""\
        etl: daily-orders
        pipeline: {PIPELINES}.OrdersPipeline
        correlation_field: run_date
        tags: [orders]
        environments:
          prod:
            job_variables:
              working_dir: {tmp_path}
        """,
    )
    sourced: list[Flow[Any, Any]] = []

    def fake_deploy(self: Flow[Any, Any], **kwargs: Any) -> str:
        sourced.append(self)
        return "deployment-id"

    monkeypatch.setattr(Flow, "deploy", fake_deploy)

    ids = discover_and_deploy_etls(config=str(path))

    assert ids == ["deployment-id"]
    (flow_obj,) = sourced
    meta: ETLFlowMeta = getattr(flow_obj, LOOM_ETL_META_ATTR)
    assert meta.name == "daily-orders"
    assert meta.config_path == str(path)
    assert meta.correlation_field == "run_date"
    assert meta.tags == ("orders",)
    assert flow_obj._entrypoint == f"{ENTRYPOINT_MODULE}.daily_orders"
    assert ENV_VAR not in os.environ
