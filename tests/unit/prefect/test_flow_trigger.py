"""A deployment may run after another one completes, inheriting some of its parameters."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from prefect.events import DeploymentEventTrigger
from prefect.server.schemas.core import FlowRun
from prefect.utilities.schema_tools.hydration import HydrationContext, hydrate

from loom.etl import ETLParams, ETLPipeline, ETLProcess, ETLStep, FromTable, IntoTable
from loom.prefect import etl_flow
from loom.prefect._meta import LOOM_ETL_META_ATTR, FlowTrigger
from loom.prefect.deploy._discovery import _deploy_single
from loom.prefect.flow._assemble import flow_settings_from_mapping
from loom.prefect.flow._params import resolve_run_params


class _Params(ETLParams, frozen=True):  # type: ignore[misc]
    updated_at_from: datetime
    updated_at_to: datetime
    run_day: date
    source: str
    batch: int = 1
    until: datetime | None = None


class _Step(ETLStep[_Params]):
    src = FromTable("raw.events")
    target = IntoTable("staging.events").replace()

    def execute(self, params: _Params, *, src: Any = None, **frames: Any) -> Any:
        return src


class _Process(ETLProcess[_Params]):
    steps = [_Step]


class _Pipeline(ETLPipeline[_Params]):
    processes = [_Process]


_BASE_YAML = """\
params:
  updated_at_from: ${today-1d}
  updated_at_to: ${today}
  run_day: ${yesterday}
  source: web
environments:
  prod:
    work_pool: loom-fargate
    job_variables:
      image: sample:dev
      cpu: "1024"
"""

_TRIGGER_YAML = """\
trigger:
  after: messages_daily
  inherit_params: [updated_at_from, updated_at_to, run_day, source]
"""


def _flow(tmp_path: Path, extra: str = "") -> Any:
    cfg = tmp_path / "etl.yaml"
    cfg.write_text(_BASE_YAML + extra, encoding="utf-8")
    return etl_flow(
        name="conversations",
        pipeline=_Pipeline,
        params_type=_Params,
        config_path=str(cfg),
        source_file=__file__,
    )


def _deploy_kwargs(flow: Any) -> dict[str, Any]:
    sourced = MagicMock()
    sourced.deploy = MagicMock(return_value="deployment-id")
    flow.from_source = MagicMock(return_value=sourced)
    _deploy_single(flow, getattr(flow, LOOM_ETL_META_ATTR), work_pool="default", env="prod")
    kwargs: dict[str, Any] = sourced.deploy.call_args.kwargs
    return kwargs


def _trigger_yaml(inherit: str) -> str:
    return f"trigger:\n  after: messages_daily\n  inherit_params: [{inherit}]\n"


# --- reading the block ----------------------------------------------------------


def test_trigger_is_read_into_the_settings() -> None:
    settings = flow_settings_from_mapping(
        {"trigger": {"after": "messages_daily", "inherit_params": ["updated_at_from"]}}
    )
    assert settings.trigger == FlowTrigger(
        after="messages_daily", inherit_params=("updated_at_from",)
    )


def test_inherit_params_is_optional() -> None:
    settings = flow_settings_from_mapping({"trigger": {"after": "messages_daily"}})
    assert settings.trigger == FlowTrigger(after="messages_daily")


def test_no_trigger_by_default() -> None:
    assert flow_settings_from_mapping({}).trigger is None


@pytest.mark.parametrize(
    ("after", "flow", "deployment"),
    [
        ("messages_daily", "messages_daily", "messages_daily"),
        ("messages/daily", "messages", "daily"),
    ],
)
def test_after_names_the_upstream_flow_and_deployment(
    after: str, flow: str, deployment: str
) -> None:
    trigger = FlowTrigger(after=after)
    assert (trigger.upstream_flow, trigger.upstream_deployment) == (flow, deployment)


@pytest.mark.parametrize(
    ("raw", "match"),
    [
        (["messages_daily"], "trigger: Expected `object`"),
        ({}, "trigger: Object missing required field `after`"),
        ({"after": ""}, "trigger.after"),
        ({"after": "   "}, "trigger.after"),
        ({"after": "flow/"}, "trigger.after"),
        ({"after": "a/b/c"}, "trigger.after"),
        ({"after": "a /b"}, "trigger.after"),
        ({"after": "a/ b"}, "trigger.after"),
        ({"after": " up"}, "trigger.after"),
        ({"after": "up "}, "trigger.after"),
        ({"after": 3}, "trigger: Expected `str`"),
        ({"after": "a", "inherit_params": "updated_at_from"}, r"inherit_params"),
        ({"after": "a", "inherit_params": [1]}, r"inherit_params"),
        ({"after": "a", "on": "Failed"}, "unknown field `on`"),
    ],
)
def test_an_invalid_trigger_is_refused(raw: Any, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        flow_settings_from_mapping({"trigger": raw})


def test_an_inherited_parameter_must_be_a_field(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="'run_date', not a field of _Params"):
        _flow(tmp_path, _trigger_yaml("updated_at_from, run_date"))


@pytest.mark.parametrize("field", ["batch", "until"])
def test_only_text_date_and_datetime_fields_can_be_inherited(tmp_path: Path, field: str) -> None:
    with pytest.raises(ValueError, match=rf"'{field}'.*only str, date and datetime"):
        _flow(tmp_path, _trigger_yaml(field))


def test_the_trigger_reaches_the_flow_metadata(tmp_path: Path) -> None:
    meta = getattr(_flow(tmp_path, _TRIGGER_YAML), LOOM_ETL_META_ATTR)
    assert meta.trigger == FlowTrigger(
        after="messages_daily",
        inherit_params=("updated_at_from", "updated_at_to", "run_day", "source"),
    )


# --- deploying it ---------------------------------------------------------------


def test_deploy_without_trigger_passes_the_same_kwargs(tmp_path: Path) -> None:
    assert _deploy_kwargs(_flow(tmp_path)) == {
        "name": "conversations",
        "work_pool_name": "loom-fargate",
        "build": False,
        "push": False,
        "tags": ["conversations"],
        "parameters": {
            "updated_at_from": "${today-1d}",
            "updated_at_to": "${today}",
            "run_day": "${yesterday}",
            "source": "web",
        },
        "job_variables": {"cpu": "1024"},
        "enforce_parameter_schema": False,
        "image": "sample:dev",
    }


def test_deploy_runs_after_the_upstream_flow_and_deployment_complete(tmp_path: Path) -> None:
    extra = "trigger:\n  after: messages/daily\n  inherit_params: [updated_at_from]\n"
    [trigger] = _deploy_kwargs(_flow(tmp_path, extra))["triggers"]
    assert trigger == DeploymentEventTrigger(
        expect={"prefect.flow-run.Completed"},
        match_related=[
            {"prefect.resource.role": "flow", "prefect.resource.name": "messages"},
            {"prefect.resource.role": "deployment", "prefect.resource.name": "daily"},
        ],
        parameters={
            "updated_at_from": {
                "__prefect_kind": "jinja",
                "template": "{{ flow_run.parameters['updated_at_from'] }}",
            },
        },
    )


def test_inherited_parameters_render_into_values_the_flow_decodes(tmp_path: Path) -> None:
    [trigger] = _deploy_kwargs(_flow(tmp_path, _TRIGGER_YAML))["triggers"]
    upstream = FlowRun(
        flow_id=uuid4(),
        parameters={
            "updated_at_from": "2026-09-21T00:00:00Z",
            "updated_at_to": "2026-09-22T00:00:00Z",
            "run_day": "2026-09-21",
            "source": "whatsapp",
        },
    )

    rendered = hydrate(
        trigger.parameters,
        HydrationContext(render_jinja=True, jinja_context={"flow_run": upstream}),
    )
    params = resolve_run_params(rendered, _Params, anchor=None)

    assert params == _Params(
        updated_at_from=datetime(2026, 9, 21, tzinfo=UTC),
        updated_at_to=datetime(2026, 9, 22, tzinfo=UTC),
        run_day=date(2026, 9, 21),
        source="whatsapp",
    )


def test_a_trigger_without_inherited_parameters_uses_the_defaults(tmp_path: Path) -> None:
    [trigger] = _deploy_kwargs(_flow(tmp_path, "trigger:\n  after: messages_daily\n"))["triggers"]
    assert trigger.parameters is None


def test_a_trigger_coexists_with_a_schedule(tmp_path: Path) -> None:
    extra = _TRIGGER_YAML + 'schedule:\n  cron: "0 6 * * *"\n'
    kwargs = _deploy_kwargs(_flow(tmp_path, extra))
    assert len(kwargs["schedules"]) == 1
    assert len(kwargs["triggers"]) == 1


def test_the_trigger_and_its_check_are_public() -> None:
    import loom.prefect
    import loom.prefect.flow

    assert "FlowTrigger" in loom.prefect.__all__
    assert "FlowTrigger" in loom.prefect.flow.__all__
    assert "check_inherited_params" in loom.prefect.flow.__all__
    assert loom.prefect.FlowTrigger is FlowTrigger
    assert loom.prefect.flow.FlowTrigger is FlowTrigger


def test_check_inherited_params_accepts_text_date_and_datetime_fields() -> None:
    from loom.prefect.flow import check_inherited_params

    trigger = FlowTrigger(after="upstream", inherit_params=("updated_at_from", "run_day", "source"))
    check_inherited_params("downstream", trigger, _Params)
    check_inherited_params("downstream", None, _Params)


@pytest.mark.parametrize(
    ("param", "match"),
    [
        ("run_date", "'run_date', not a field of _Params"),
        ("batch", "'batch'.*only str, date and datetime"),
        ("until", "'until'.*only str, date and datetime"),
    ],
)
def test_check_inherited_params_refuses_what_the_trigger_cannot_carry(
    param: str, match: str
) -> None:
    from loom.prefect.flow import check_inherited_params

    trigger = FlowTrigger(after="upstream", inherit_params=(param,))
    with pytest.raises(
        ValueError, match=rf"flow 'downstream': trigger.inherit_params names {match}"
    ):
        check_inherited_params("downstream", trigger, _Params)
