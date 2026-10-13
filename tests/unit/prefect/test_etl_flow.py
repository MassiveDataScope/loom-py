"""Tests for the per-ETL Prefect flow factory."""

from __future__ import annotations

import inspect
import sys
import textwrap
import time
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from loom.etl import ETLParams, ETLPipeline, ETLProcess, ETLStep, FromTable, IntoTable
from loom.prefect import etl_flow
from loom.prefect._flow_yaml import read_yaml
from loom.prefect._meta import LOOM_ETL_META_ATTR as _LOOM_ETL_META_ATTR
from loom.prefect._placeholders import resolve_placeholder
from loom.prefect.flow._assemble import flow_attribute_name, flow_settings_from_mapping
from loom.prefect.flow._factory import build_etl_flow
from loom.prefect.flow._run_name import compute_correlation_id
from loom.prefect.flow._signature import (
    normalize_datetime_fields as _normalize_datetime_fields,
)


class _SampleParams(ETLParams, frozen=True):  # type: ignore[misc]
    updated_at_from: datetime
    updated_at_to: datetime


class _SampleStep(ETLStep[_SampleParams]):
    src = FromTable("raw.events")
    target = IntoTable("staging.events").replace()

    def execute(self, params: _SampleParams, *, src):  # type: ignore[override]
        return src


class _SampleProcess(ETLProcess[_SampleParams]):
    steps = [_SampleStep]


class _SamplePipeline(ETLPipeline[_SampleParams]):
    processes = [_SampleProcess]


def _write_cfg(tmp_path: Path) -> Path:
    content = textwrap.dedent(
        """\
        etl: sample-etl
        correlation_field: updated_at_from
        schedule:
          cron: "0 6 * * *"
          timezone: Europe/Madrid
          enabled: true
        params:
          updated_at_from: ${now-1d}
          updated_at_to: ${now}
        environments:
          prod:
            work_pool: loom-fargate
            job_variables:
              task_definition_arn: arn:aws:ecs:::task-definition/sample:1
              cpu: "1024"
          local:
            work_pool: loom-docker
            job_variables:
              image: sample:dev
        """
    )
    yaml_path = tmp_path / "etl.yaml"
    yaml_path.write_text(content, encoding="utf-8")
    return yaml_path


def test_etl_flow_name_matches_argument(tmp_path: Path) -> None:
    flow_obj = etl_flow(
        name="sample-etl",
        pipeline=_SamplePipeline,
        params_type=_SampleParams,
        config_path=str(_write_cfg(tmp_path)),
        source_file=__file__,
    )
    assert flow_obj.name == "sample-etl"


def test_etl_flow_synthesizes_typed_signature(tmp_path: Path) -> None:
    flow_obj = etl_flow(
        name="sample-etl",
        pipeline=_SamplePipeline,
        params_type=_SampleParams,
        config_path=str(_write_cfg(tmp_path)),
        source_file=__file__,
    )
    sig = flow_obj.fn.__signature__
    parameters = sig.parameters
    assert list(parameters.keys()) == [
        "updated_at_from",
        "updated_at_to",
        "env",
        "correlation_id",
        "processes",
    ]
    assert parameters["updated_at_from"].annotation is datetime
    assert parameters["env"].default == "prod"
    assert parameters["correlation_id"].default is None
    assert parameters["processes"].default is None
    assert parameters["processes"].annotation == list[str] | None


def test_etl_flow_attaches_discovery_metadata(tmp_path: Path) -> None:
    cfg = _write_cfg(tmp_path)
    flow_obj = etl_flow(
        name="sample-etl",
        pipeline=_SamplePipeline,
        params_type=_SampleParams,
        config_path=str(cfg),
        source_file=__file__,
    )
    meta = getattr(flow_obj, _LOOM_ETL_META_ATTR)
    assert meta.name == "sample-etl"
    assert meta.config_path == str(cfg.resolve())
    assert meta.source_file == str(Path(__file__).resolve())
    assert meta.correlation_field == "updated_at_from"
    assert meta.schedule == {
        "cron": "0 6 * * *",
        "timezone": "Europe/Madrid",
        "enabled": True,
    }
    assert meta.raw_params == {
        "updated_at_from": "${now-1d}",
        "updated_at_to": "${now}",
    }
    assert meta.pool_config["prod"]["work_pool"] == "loom-fargate"
    assert meta.pool_config["prod"]["job_variables"]["cpu"] == "1024"
    assert meta.pool_config["local"]["work_pool"] == "loom-docker"
    assert meta.pool_config["local"]["job_variables"]["image"] == "sample:dev"


def test_etl_flow_captures_tags_when_present(tmp_path: Path) -> None:
    content = textwrap.dedent(
        """\
        etl: sample-etl
        tags:
          - mongo
          - daily
          - team:data-platform
        params: {}
        """
    )
    yaml_path = tmp_path / "tagged.yaml"
    yaml_path.write_text(content, encoding="utf-8")
    flow_obj = etl_flow(
        name="tagged-etl",
        pipeline=_SamplePipeline,
        params_type=_SampleParams,
        config_path=str(yaml_path),
        source_file=__file__,
    )
    meta = getattr(flow_obj, _LOOM_ETL_META_ATTR)
    assert meta.tags == ("mongo", "daily", "team:data-platform")


def test_etl_flow_tags_default_to_empty_tuple(tmp_path: Path) -> None:
    flow_obj = etl_flow(
        name="sample-etl",
        pipeline=_SamplePipeline,
        params_type=_SampleParams,
        config_path=str(_write_cfg(tmp_path)),
        source_file=__file__,
    )
    meta = getattr(flow_obj, _LOOM_ETL_META_ATTR)
    assert meta.tags == ()


def test_deploy_passes_meta_name_and_yaml_tags_no_loom_etl(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from unittest.mock import MagicMock

    from loom.prefect import deploy_flow

    content = textwrap.dedent(
        """\
        etl: sample-etl
        tags:
          - mongo
          - daily
        params: {}
        environments:
          prod:
            work_pool: loom-fargate
            job_variables:
              image: sample:dev
              cpu: "1024"
        """
    )
    yaml_path = tmp_path / "deploy_tags.yaml"
    yaml_path.write_text(content, encoding="utf-8")
    flow_obj = etl_flow(
        name="deploy-tags-etl",
        pipeline=_SamplePipeline,
        params_type=_SampleParams,
        config_path=str(yaml_path),
        source_file=__file__,
    )
    monkeypatch.setattr(sys.modules[__name__], "deploy_tags_etl", flow_obj, raising=False)

    sourced = MagicMock()
    sourced.deploy = MagicMock(return_value="deployment-id-abc")
    flow_obj.from_source = MagicMock(return_value=sourced)

    deployment_id = deploy_flow(flow_obj, env="prod", work_pool="default-pool")

    assert deployment_id == "deployment-id-abc"
    deploy_kwargs = sourced.deploy.call_args.kwargs
    assert deploy_kwargs["tags"] == ["deploy-tags-etl", "mongo", "daily"]
    assert "loom-etl" not in deploy_kwargs["tags"]


def test_prometheus_adapter_wired_when_env_set(monkeypatch: pytest.MonkeyPatch) -> None:
    from loom.prefect.flow._runtime import build_observers

    monkeypatch.setenv("PROMETHEUS_PUSHGATEWAY_URL", "https://pushgateway:9091")
    observers = build_observers(flow_run_id=None, manifest_store=None, manifest=None)
    assert any(type(o).__name__ == "PrometheusLifecycleAdapter" for o in observers)


def test_prometheus_adapter_absent_when_env_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    from loom.prefect.flow._runtime import build_observers

    monkeypatch.delenv("PROMETHEUS_PUSHGATEWAY_URL", raising=False)
    observers = build_observers(flow_run_id=None, manifest_store=None, manifest=None)
    assert not any(type(o).__name__ == "PrometheusLifecycleAdapter" for o in observers)


def test_etl_flow_rejects_non_string_tags(tmp_path: Path) -> None:
    content = textwrap.dedent(
        """\
        etl: sample-etl
        tags:
          - mongo
          - 42
        params: {}
        """
    )
    yaml_path = tmp_path / "bad_tags.yaml"
    yaml_path.write_text(content, encoding="utf-8")
    with pytest.raises(TypeError, match="tags"):
        etl_flow(
            name="sample-etl",
            pipeline=_SamplePipeline,
            params_type=_SampleParams,
            config_path=str(yaml_path),
            source_file=__file__,
        )


class _DateParams(ETLParams, frozen=True):  # type: ignore[misc]
    day: date


class _OptionalDtParams(ETLParams, frozen=True):  # type: ignore[misc]
    updated_at_from: datetime | None = None
    label: str = "x"
    count: int = 0


def test_naive_datetime_string_is_normalized_to_utc() -> None:
    out = _normalize_datetime_fields(
        {"updated_at_from": "2026-06-03T00:00:00", "updated_at_to": "2026-06-04T00:00:00"},
        _SampleParams,
    )
    assert isinstance(out["updated_at_from"], datetime)
    assert out["updated_at_from"].tzinfo is UTC
    assert out["updated_at_to"].tzinfo is UTC


def test_naive_datetime_object_is_normalized_to_utc() -> None:
    naive = datetime(2026, 6, 3, 0, 0, 0)
    out = _normalize_datetime_fields(
        {"updated_at_from": naive, "updated_at_to": naive},
        _SampleParams,
    )
    assert out["updated_at_from"].tzinfo is UTC
    assert out["updated_at_from"].replace(tzinfo=None) == naive


def test_offset_bearing_datetime_string_is_parsed_keeping_its_instant() -> None:
    out = _normalize_datetime_fields(
        {
            "updated_at_from": "2026-06-03T00:00:00+02:00",
            "updated_at_to": "2026-06-04T00:00:00Z",
        },
        _SampleParams,
    )
    assert isinstance(out["updated_at_from"], datetime)
    assert out["updated_at_from"].utcoffset() == timedelta(hours=2)
    assert out["updated_at_from"].astimezone(UTC) == datetime(2026, 6, 2, 22, 0, tzinfo=UTC)
    assert isinstance(out["updated_at_to"], datetime)


def test_aware_datetime_passes_through() -> None:
    aware = datetime(2026, 6, 3, 0, 0, 0, tzinfo=UTC)
    out = _normalize_datetime_fields(
        {"updated_at_from": aware, "updated_at_to": aware},
        _SampleParams,
    )
    assert out["updated_at_from"] is aware


def test_aware_datetime_with_offset_passes_through() -> None:
    offset = timezone(timedelta(hours=2))
    aware = datetime(2026, 6, 3, 0, 0, 0, tzinfo=offset)
    out = _normalize_datetime_fields(
        {"updated_at_from": aware, "updated_at_to": aware},
        _SampleParams,
    )
    assert out["updated_at_from"].tzinfo is offset
    assert out["updated_at_from"].utcoffset() == timedelta(hours=2)


def test_correlation_id_of_an_offset_bearing_string_uses_its_utc_instant() -> None:
    # The coercion feeds the correlation id as well as the params struct, so the
    # string keys the manifest store by the instant it names, not by its wall clock.
    resolved = _normalize_datetime_fields(
        {"updated_at_from": "2026-06-03T00:00:00+02:00"},
        _SampleParams,
    )
    assert compute_correlation_id("etl", "updated_at_from", resolved) == "etl-20260602T220000"


def test_a_date_on_a_datetime_field_becomes_its_midnight_in_utc() -> None:
    out = _normalize_datetime_fields(
        {"updated_at_from": date(2026, 6, 3), "updated_at_to": date(2026, 6, 4)},
        _SampleParams,
    )
    assert out["updated_at_from"] == datetime(2026, 6, 3, tzinfo=UTC)
    assert out["updated_at_to"] == datetime(2026, 6, 4, tzinfo=UTC)


def test_a_date_field_keeps_its_date(tmp_path: Path) -> None:
    # is_datetime_annotation rejects date, so a date-declared field is untouched.
    out = _normalize_datetime_fields({"day": date(2026, 6, 3)}, _DateParams)
    assert out["day"] == date(2026, 6, 3)


def test_correlation_id_of_two_offsets_distinguishes_the_instants() -> None:
    madrid = timezone(timedelta(hours=2))
    at_madrid = {"updated_at_from": datetime(2026, 6, 3, 0, 0, tzinfo=madrid)}
    at_utc = {"updated_at_from": datetime(2026, 6, 3, 0, 0, tzinfo=UTC)}
    assert compute_correlation_id("etl", "updated_at_from", at_madrid) == "etl-20260602T220000"
    assert compute_correlation_id("etl", "updated_at_from", at_utc) == "etl-20260603T000000"


def test_correlation_id_of_a_date_keeps_its_day() -> None:
    resolved = {"updated_at_from": date(2026, 6, 3)}
    assert compute_correlation_id("etl", "updated_at_from", resolved) == "etl-20260603T000000"


def test_correlation_id_of_a_naive_datetime_ignores_the_local_zone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A naive value means UTC here, so the machine's zone must not shift it.
    monkeypatch.setenv("TZ", "America/New_York")
    time.tzset()
    try:
        resolved = {"updated_at_from": datetime(2026, 6, 3, 0, 0)}
        assert compute_correlation_id("etl", "updated_at_from", resolved) == "etl-20260603T000000"
    finally:
        monkeypatch.undo()
        time.tzset()


def test_non_datetime_fields_untouched() -> None:
    out = _normalize_datetime_fields(
        {"updated_at_from": None, "label": "hello", "count": 42},
        _OptionalDtParams,
    )
    assert out["label"] == "hello"
    assert out["count"] == 42


def test_placeholder_now_produces_aware_datetime() -> None:
    resolved = {
        "updated_at_from": resolve_placeholder("${now}"),
        "updated_at_to": resolve_placeholder("${now}"),
    }
    out = _normalize_datetime_fields(resolved, _SampleParams)
    assert out["updated_at_from"].tzinfo is UTC
    # Idempotent: the aware datetime is unchanged.
    assert out["updated_at_from"] is resolved["updated_at_from"]


def test_optional_datetime_none_value() -> None:
    out = _normalize_datetime_fields(
        {"updated_at_from": None},
        _OptionalDtParams,
    )
    assert out["updated_at_from"] is None


class TestValidateProcesses:
    @pytest.fixture
    def known(self) -> frozenset[str]:
        from loom.etl.compiler import ETLCompiler
        from loom.prefect.flow._stages import known_process_names

        return known_process_names(ETLCompiler().compile(_SamplePipeline))

    def test_accepts_known_name(self, known: frozenset[str]) -> None:
        from loom.prefect.flow._stages import validate_stage_names

        assert validate_stage_names(["_SampleProcess"], known) == ("_SampleProcess",)

    def test_none_passes_through(self, known: frozenset[str]) -> None:
        from loom.prefect.flow._stages import validate_stage_names

        assert validate_stage_names(None, known) is None

    def test_empty_list_normalises_to_none(self, known: frozenset[str]) -> None:
        from loom.prefect.flow._stages import validate_stage_names

        assert validate_stage_names([], known) is None

    def test_rejects_unknown_name(self, known: frozenset[str]) -> None:
        from loom.prefect.flow._stages import validate_stage_names

        with pytest.raises(ValueError, match="unknown"):
            validate_stage_names(["DoesNotExist"], known)

    def test_rejects_non_list_value(self, known: frozenset[str]) -> None:
        from loom.prefect.flow._stages import validate_stage_names

        with pytest.raises(TypeError, match="processes"):
            validate_stage_names("not-a-list", known)

    def test_rejects_non_string_entries(self, known: frozenset[str]) -> None:
        from loom.prefect.flow._stages import validate_stage_names

        with pytest.raises(TypeError, match="processes"):
            validate_stage_names([1, 2], known)


def test_etl_flow_rejects_non_struct_params_type(tmp_path: Path) -> None:
    class NotAStruct:
        pass

    config_path = str(_write_cfg(tmp_path))
    with pytest.raises((TypeError, ValueError)):
        etl_flow(
            name="bad-etl",
            pipeline=_SamplePipeline,
            params_type=NotAStruct,  # type: ignore[arg-type]
            config_path=config_path,
            source_file=__file__,
        )


def test_build_etl_flow_from_mapping_matches_file_metadata(tmp_path: Path) -> None:
    cfg = _write_cfg(tmp_path)
    from_file = etl_flow(
        name="sample-etl",
        pipeline=_SamplePipeline,
        params_type=_SampleParams,
        config_path=str(cfg),
        source_file=__file__,
    )
    settings = flow_settings_from_mapping(read_yaml(str(cfg)))
    from_mapping = build_etl_flow(
        name="sample-etl",
        pipeline=_SamplePipeline,
        params_type=_SampleParams,
        settings=settings,
        config_path=str(cfg),
        source_file=__file__,
    )
    assert getattr(from_mapping, _LOOM_ETL_META_ATTR) == getattr(from_file, _LOOM_ETL_META_ATTR)
    assert from_mapping.name == from_file.name
    assert from_mapping.retries == from_file.retries
    assert from_mapping.retry_delay_seconds == from_file.retry_delay_seconds
    assert inspect.signature(from_mapping) == inspect.signature(from_file)


def test_build_etl_flow_keeps_cloud_uri_config_path_verbatim(tmp_path: Path) -> None:
    settings = flow_settings_from_mapping(read_yaml(str(_write_cfg(tmp_path))))
    flow_obj = build_etl_flow(
        name="sample-etl",
        pipeline=_SamplePipeline,
        params_type=_SampleParams,
        settings=settings,
        config_path="s3://b/x.yaml",
        source_file=__file__,
    )
    assert getattr(flow_obj, _LOOM_ETL_META_ATTR).config_path == "s3://b/x.yaml"


def test_flow_attribute_name_replaces_hyphens() -> None:
    assert flow_attribute_name("daily-orders") == "daily_orders"
    assert flow_attribute_name("daily_orders") == "daily_orders"


def test_dashed_name_keeps_prefect_flow_name_and_identifier(tmp_path: Path) -> None:
    flow_obj = etl_flow(
        name="daily-orders",
        pipeline=_SamplePipeline,
        params_type=_SampleParams,
        config_path=str(_write_cfg(tmp_path)),
        source_file=__file__,
    )
    assert flow_obj.name == "daily-orders"
    assert flow_obj.fn.__name__ == "daily_orders"
