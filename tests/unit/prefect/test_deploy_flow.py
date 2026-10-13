"""deploy_flow registers the Prefect deployment of one flow built by a loom factory."""

from __future__ import annotations

import textwrap
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import prefect
import pytest

from loom.etl import ETLParams, ETLPipeline, ETLProcess, ETLStep, FromTable, IntoTable
from loom.prefect import deploy_flow, etl_flow
from loom.prefect.deploy import deploy_flow as deploy_flow_from_deploy


class _Params(ETLParams, frozen=True):  # type: ignore[misc]
    updated_at_from: datetime


class _Step(ETLStep[_Params]):
    src = FromTable("raw.events")
    target = IntoTable("staging.events").replace()

    def execute(self, params: _Params, *, src: Any = None) -> Any:
        return src


class _Process(ETLProcess[_Params]):
    steps = [_Step]


class _Pipeline(ETLPipeline[_Params]):
    processes = [_Process]


_YAML = textwrap.dedent(
    """\
    tags: [mongo]
    schedule:
      cron: "0 6 * * *"
      timezone: Europe/Madrid
    params:
      updated_at_from: ${today-1d}
    environments:
      prod:
        work_pool: loom-fargate
        job_variables:
          image: orders:1.2.3
          cpu: "1024"
          working_dir: /app/src
    """
)


def _flow(tmp_path: Path, yaml: str = _YAML) -> Any:
    cfg = tmp_path / "daily_orders.yaml"
    cfg.write_text(yaml, encoding="utf-8")
    return etl_flow(
        name="daily-orders",
        pipeline=_Pipeline,
        params_type=_Params,
        config_path=str(cfg),
        source_file="/app/src/flows/daily_orders.py",
    )


def _stub_prefect(flow: Any) -> tuple[MagicMock, MagicMock]:
    sourced = MagicMock()
    sourced.deploy = MagicMock(return_value="deployment-id")
    flow.from_source = MagicMock(return_value=sourced)
    return flow.from_source, sourced.deploy


def test_registers_the_deployment_described_by_the_flow_yaml(tmp_path: Path) -> None:
    flow = _flow(tmp_path)
    from_source, deploy = _stub_prefect(flow)

    deployment_id = deploy_flow(flow, work_pool="unused-pool", env="prod")

    assert deployment_id == "deployment-id"
    assert from_source.call_args.kwargs == {
        "source": "/app/src",
        "entrypoint": "flows/daily_orders.py:daily_orders",
    }
    kwargs = deploy.call_args.kwargs
    assert kwargs["name"] == "daily-orders"
    assert kwargs["work_pool_name"] == "loom-fargate"
    assert kwargs["image"] == "orders:1.2.3"
    assert kwargs["job_variables"] == {"cpu": "1024", "working_dir": "/app/src"}
    assert kwargs["tags"] == ["daily-orders", "mongo"]
    assert kwargs["parameters"] == {"updated_at_from": "${today-1d}"}
    assert len(kwargs["schedules"]) == 1


def test_falls_back_to_the_given_work_pool_for_an_env_the_yaml_does_not_declare(
    tmp_path: Path,
) -> None:
    flow = _flow(tmp_path)
    _, deploy = _stub_prefect(flow)

    deploy_flow_from_deploy(flow, work_pool="loom-docker", env="dev")

    kwargs = deploy.call_args.kwargs
    assert kwargs["work_pool_name"] == "loom-docker"
    assert "image" not in kwargs
    assert kwargs["job_variables"] == {}


def test_refuses_a_flow_not_built_by_a_loom_factory() -> None:
    @prefect.flow(name="plain")
    def plain() -> None:
        return None

    with pytest.raises(TypeError, match="no loom ETL metadata"):
        deploy_flow(plain)
