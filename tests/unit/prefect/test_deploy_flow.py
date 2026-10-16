"""deploy_flow registers the Prefect deployment of one flow built by a loom factory."""

from __future__ import annotations

import importlib.util
import sys
import textwrap
from datetime import datetime
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock

import msgspec
import prefect
import pytest

from loom.etl import ETLParams, ETLPipeline, ETLProcess, ETLStep, FromTable, IntoTable
from loom.etl.maintenance import MaintenanceStep
from loom.prefect import backfill_flow, deploy_flow, etl_flow, maintenance_flow
from loom.prefect._meta import LOOM_ETL_META_ATTR
from loom.prefect.deploy import deploy_flow as deploy_flow_from_deploy
from loom.prefect.deploy import entrypoint

_SOURCE_FILE = "/app/src/flows/daily_orders.py"


class _Params(ETLParams, frozen=True):  # type: ignore[misc]
    updated_at_from: datetime
    updated_at_to: datetime


class _Step(ETLStep[_Params]):
    src = FromTable("raw.events")
    target = IntoTable("staging.events").replace()

    def execute(self, params: _Params, *, src: Any = None) -> Any:
        return src


class _Process(ETLProcess[_Params]):
    steps = [_Step]


class _Pipeline(ETLPipeline[_Params]):
    processes = [_Process]


class _MaintenanceParams(msgspec.Struct, frozen=True):
    dry_run: bool = False


class _Maintenance(MaintenanceStep[_MaintenanceParams]):
    operations = []


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


def _config(tmp_path: Path, yaml: str = _YAML) -> str:
    cfg = tmp_path / "daily_orders.yaml"
    cfg.write_text(yaml, encoding="utf-8")
    return str(cfg)


def _flow(tmp_path: Path, yaml: str = _YAML) -> Any:
    return etl_flow(
        name="daily-orders",
        pipeline=_Pipeline,
        params_type=_Params,
        config_path=_config(tmp_path, yaml),
        source_file=_SOURCE_FILE,
    )


def _expose(monkeypatch: pytest.MonkeyPatch, **attributes: Any) -> None:
    module = ModuleType("flows.daily_orders")
    module.__file__ = _SOURCE_FILE
    for attribute, value in attributes.items():
        setattr(module, attribute, value)
    monkeypatch.setitem(sys.modules, module.__name__, module)


def _stub_prefect(flow: Any) -> tuple[MagicMock, MagicMock]:
    sourced = MagicMock()
    sourced.deploy = MagicMock(return_value="deployment-id")
    flow.from_source = MagicMock(return_value=sourced)
    return flow.from_source, sourced.deploy


def test_registers_the_deployment_described_by_the_flow_yaml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    flow = _flow(tmp_path)
    _expose(monkeypatch, daily_orders=flow)
    from_source, deploy = _stub_prefect(flow)

    deployment_id = deploy_flow(flow, env="prod", work_pool="unused-pool")

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
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    flow = _flow(tmp_path)
    _expose(monkeypatch, daily_orders=flow)
    _, deploy = _stub_prefect(flow)

    deploy_flow_from_deploy(flow, env="dev", work_pool="loom-docker")

    kwargs = deploy.call_args.kwargs
    assert kwargs["work_pool_name"] == "loom-docker"
    assert "image" not in kwargs
    assert kwargs["job_variables"] == {}


def test_refuses_an_env_without_a_work_pool_before_registering(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    flow = _flow(tmp_path)
    _expose(monkeypatch, daily_orders=flow)
    from_source, _ = _stub_prefect(flow)

    with pytest.raises(ValueError, match="no work pool for flow 'daily-orders' in env 'dev'"):
        deploy_flow(flow, env="dev")

    from_source.assert_not_called()


def test_takes_env_only_by_keyword(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    flow = _flow(tmp_path)
    _expose(monkeypatch, daily_orders=flow)
    from_source, _ = _stub_prefect(flow)

    with pytest.raises(TypeError, match="env"):
        deploy_flow(flow)  # type: ignore[call-arg]
    with pytest.raises(TypeError, match="positional"):
        deploy_flow(flow, "prod")  # type: ignore[misc]

    from_source.assert_not_called()


def test_refuses_a_flow_its_module_exposes_under_another_attribute(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    flow = _flow(tmp_path)
    _expose(monkeypatch, orders=flow)
    from_source, _ = _stub_prefect(flow)

    with pytest.raises(ValueError, match="'daily-orders' as attribute 'daily_orders'"):
        deploy_flow(flow, env="prod")

    from_source.assert_not_called()


def test_refuses_a_flow_whose_module_getattr_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(attribute: str) -> Any:
        raise RuntimeError(f"cannot build {attribute}")

    flow = _flow(tmp_path)
    _expose(monkeypatch, __getattr__=fail)
    from_source, _ = _stub_prefect(flow)

    with pytest.raises(ValueError, match="'daily-orders' as attribute 'daily_orders'") as raised:
        deploy_flow(flow, env="prod")

    assert isinstance(raised.value.__cause__, RuntimeError)
    from_source.assert_not_called()


def test_refuses_a_flow_built_from_a_yaml_declaration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    flow = etl_flow(
        name="daily-orders",
        pipeline=_Pipeline,
        params_type=_Params,
        config_path=_config(tmp_path),
        source_file=entrypoint.__file__,
    )
    from_source, _ = _stub_prefect(flow)

    with pytest.raises(TypeError, match=r"discover_and_deploy_etls\(config=\.\.\.\)"):
        deploy_flow(flow, env="prod")

    from_source.assert_not_called()


def _symlinked_flow_dir(tmp_path: Path) -> tuple[Path, Path]:
    """A ``real`` directory with a flow file that must not be imported, and a link to it."""
    real_dir = tmp_path / "real"
    real_dir.mkdir()
    (real_dir / "daily_orders.py").write_text(
        "raise RuntimeError('imported a second time')\n", encoding="utf-8"
    )
    linked_dir = tmp_path / "linked"
    linked_dir.symlink_to(real_dir, target_is_directory=True)
    return real_dir, linked_dir


def _expose_from(monkeypatch: pytest.MonkeyPatch, flow: Any, module_file: Path) -> None:
    module = ModuleType("linked_flows.daily_orders")
    module.__file__ = str(module_file)
    module.daily_orders = flow  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, module.__name__, module)


def test_reads_the_module_imported_through_a_symlinked_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_dir, linked_dir = _symlinked_flow_dir(tmp_path)
    flow = etl_flow(
        name="daily-orders",
        pipeline=_Pipeline,
        params_type=_Params,
        config_path=_config(tmp_path),
        source_file=str(real_dir / "daily_orders.py"),
    )
    _expose_from(monkeypatch, flow, linked_dir / "daily_orders.py")
    _, deploy = _stub_prefect(flow)

    assert deploy_flow(flow, env="prod") == "deployment-id"
    deploy.assert_called_once()


def test_reads_the_imported_module_when_the_metadata_names_it_through_a_symlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_dir, linked_dir = _symlinked_flow_dir(tmp_path)
    flow = etl_flow(
        name="daily-orders",
        pipeline=_Pipeline,
        params_type=_Params,
        config_path=_config(tmp_path),
        source_file=str(real_dir / "daily_orders.py"),
    )
    meta = getattr(flow, LOOM_ETL_META_ATTR)
    linked_source = str(linked_dir / "daily_orders.py")
    setattr(flow, LOOM_ETL_META_ATTR, msgspec.structs.replace(meta, source_file=linked_source))
    _expose_from(monkeypatch, flow, real_dir / "daily_orders.py")
    _, deploy = _stub_prefect(flow)

    assert deploy_flow(flow, env="prod") == "deployment-id"
    deploy.assert_called_once()


def test_refuses_a_flow_whose_source_file_does_not_exist(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    flow = _flow(tmp_path)
    monkeypatch.setattr(sys, "path", list(sys.path))
    from_source, _ = _stub_prefect(flow)

    with pytest.raises(ValueError, match="is not an importable Python file"):
        deploy_flow(flow, env="prod")

    from_source.assert_not_called()


_ON_DEMAND_MODULE = """\
from datetime import datetime
from pathlib import Path
from typing import Any

from loom.etl import ETLParams, ETLPipeline, ETLProcess, ETLStep, FromTable, IntoTable
from loom.prefect import etl_flow


class Params(ETLParams, frozen=True):
    updated_at_from: datetime


class Step(ETLStep[Params]):
    src = FromTable("raw.events")
    target = IntoTable("staging.events").replace()

    def execute(self, params: Params, *, src: Any = None) -> Any:
        return src


class Process(ETLProcess[Params]):
    steps = [Step]


class Pipeline(ETLPipeline[Params]):
    processes = [Process]


def __getattr__(attribute: str) -> Any:
    if attribute != "daily_orders":
        raise AttributeError(attribute)
    return etl_flow(
        name="daily-orders",
        pipeline=Pipeline,
        params_type=Params,
        config_path=str(Path(__file__).with_name("daily_orders.yaml")),
        source_file=__file__,
    )
"""


def _on_demand_flow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    _config(tmp_path)
    source = tmp_path / "on_demand_flows.py"
    source.write_text(_ON_DEMAND_MODULE, encoding="utf-8")
    spec = importlib.util.spec_from_file_location("on_demand_flows", source)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module.daily_orders


@pytest.mark.parametrize("imported", [True, False], ids=["imported", "not-imported"])
def test_deploys_a_flow_its_module_builds_on_demand(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, imported: bool
) -> None:
    flow = _on_demand_flow(tmp_path, monkeypatch)
    if not imported:
        monkeypatch.delitem(sys.modules, "on_demand_flows")
    from_source, deploy = _stub_prefect(flow)

    monkeypatch.setattr(sys, "path", list(sys.path))

    deploy_flow(flow, env="prod")

    assert from_source.call_args.kwargs["entrypoint"] == "on_demand_flows.py:daily_orders"
    assert deploy.call_args.kwargs["name"] == "daily-orders"


def test_imports_a_flows_file_whose_top_level_imports_a_sibling_module(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    flow = _on_demand_flow(tmp_path, monkeypatch)
    source = tmp_path / "on_demand_flows.py"
    source.write_text(
        f"import on_demand_sibling  # noqa: F401\n{_ON_DEMAND_MODULE}", encoding="utf-8"
    )
    (tmp_path / "on_demand_sibling.py").write_text("", encoding="utf-8")
    monkeypatch.delitem(sys.modules, "on_demand_flows")
    monkeypatch.setattr(sys, "path", [p for p in sys.path if p != str(tmp_path)])
    request.addfinalizer(lambda: sys.modules.pop("on_demand_sibling", None))
    from_source, _ = _stub_prefect(flow)

    deploy_flow(flow, env="prod")

    assert from_source.call_args.kwargs["entrypoint"] == "on_demand_flows.py:daily_orders"


def test_refuses_a_flows_file_whose_top_level_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    flow = _on_demand_flow(tmp_path, monkeypatch)
    (tmp_path / "on_demand_flows.py").write_text("import on_demand_missing\n", encoding="utf-8")
    monkeypatch.delitem(sys.modules, "on_demand_flows")
    monkeypatch.setattr(sys, "path", list(sys.path))
    from_source, _ = _stub_prefect(flow)

    with pytest.raises(ValueError, match="on_demand_missing"):
        deploy_flow(flow, env="prod")

    from_source.assert_not_called()
    assert "on_demand_flows" not in sys.modules


def test_deploys_a_backfill_flow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    flow = backfill_flow(
        name="daily-orders",
        pipeline=_Pipeline,
        params_type=_Params,
        config_path=_config(tmp_path),
        source_file=_SOURCE_FILE,
        per_chunk_processes=["_Process"],
        finalize_processes=[],
        window_start_field="updated_at_from",
        window_end_field="updated_at_to",
    )
    _expose(monkeypatch, daily_orders=flow)
    from_source, deploy = _stub_prefect(flow)

    deploy_flow(flow, env="prod")

    assert from_source.call_args.kwargs["entrypoint"] == "flows/daily_orders.py:daily_orders"
    assert deploy.call_args.kwargs["work_pool_name"] == "loom-fargate"


def test_deploys_a_maintenance_flow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    flow = maintenance_flow(
        name="daily-orders",
        step=_Maintenance,
        params_type=_MaintenanceParams,
        config_path=_config(tmp_path),
        source_file=_SOURCE_FILE,
    )
    _expose(monkeypatch, daily_orders=flow)
    from_source, deploy = _stub_prefect(flow)

    deploy_flow(flow, env="prod")

    assert from_source.call_args.kwargs["entrypoint"] == "flows/daily_orders.py:daily_orders"
    assert deploy.call_args.kwargs["image"] == "orders:1.2.3"


def test_refuses_a_flow_not_built_by_a_loom_factory() -> None:
    @prefect.flow(name="plain")
    def plain() -> None:
        return None

    with pytest.raises(TypeError, match="no loom ETL metadata"):
        deploy_flow(plain, env="prod")
