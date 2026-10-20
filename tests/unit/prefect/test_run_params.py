"""Placeholders resolve against the run's scheduled start and are written back to the run."""

from __future__ import annotations

import logging
import textwrap
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import msgspec
import pytest
from prefect.runtime import flow_run as prefect_flow_run
from prefect.settings import PREFECT_CLIENT_MAX_RETRIES
from prefect.types import DateTime as PrefectDateTime

from loom.etl import ETLParams, ETLPipeline, ETLProcess, ETLStep, FromTable, IntoTable
from loom.etl.maintenance._runner import MaintenanceReport
from loom.etl.maintenance._step import MaintenanceStep
from loom.prefect import backfill_flow, etl_flow
from loom.prefect.flow import _backfill_body, _body, _maintenance, _params
from loom.prefect.flow._common import prefect_run_anchor
from loom.prefect.flow._run_name import make_run_name_callback

# A slot late in the evening: a run that starts after midnight still owns this day.
_SLOT = datetime(2026, 9, 22, 23, 30, tzinfo=UTC)
# What Prefect's runtime returns for that slot.
_PREFECT_SLOT = PrefectDateTime(2026, 9, 22, 23, 30, tzinfo=UTC)
_RUN_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")


class _Params(ETLParams, frozen=True):  # type: ignore[misc]
    run_date: date
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


class _Client:
    """Stands in for ``get_client``, noting how the call was bounded."""

    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.httpx_settings: dict[str, Any] | None = None
        self.max_retries: int | None = None
        self.update_flow_run = MagicMock(side_effect=self._update)

    def _update(self, *args: Any, **kwargs: Any) -> None:
        self.events.append("record")
        self.max_retries = PREFECT_CLIENT_MAX_RETRIES.value()

    def __call__(self, *, sync_client: bool, httpx_settings: dict[str, Any]) -> Any:
        assert sync_client is True
        self.httpx_settings = httpx_settings

        @contextmanager
        def _session() -> Iterator[_Client]:
            yield self

        return _session()


@pytest.fixture
def events() -> list[str]:
    return []


@pytest.fixture
def in_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(prefect_flow_run.FIELDS, "id", lambda: str(_RUN_ID))
    monkeypatch.setitem(prefect_flow_run.FIELDS, "scheduled_start_time", lambda: _PREFECT_SLOT)


@pytest.fixture
def stored(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """The parameters the Prefect API holds for the current run."""
    parameters: dict[str, Any] = {}
    ctx = SimpleNamespace(flow_run=SimpleNamespace(id=_RUN_ID, parameters=parameters))
    monkeypatch.setattr(_params, "FlowRunContext", SimpleNamespace(get=lambda: ctx))
    return parameters


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, events: list[str]) -> _Client:
    fake = _Client(events)
    monkeypatch.setattr(_params, "get_client", fake)
    return fake


@pytest.fixture
def invoked(monkeypatch: pytest.MonkeyPatch, events: list[str]) -> list[tuple[Any, Any]]:
    """Captures ``(params, ctx)`` of every runner invocation of ``etl_flow``."""
    calls: list[tuple[Any, Any]] = []
    monkeypatch.setattr(
        _body,
        "_invoke_runner",
        lambda _path, _pipeline, params, _pending, ctx, *_rest, **_kw: calls.append((params, ctx)),
    )
    monkeypatch.setattr(_body, "build_observers", lambda *a, **k: [])
    monkeypatch.setattr(_body, "install_log_bridge", lambda *a, **k: events.append("bridge"))
    monkeypatch.setattr(_body, "uninstall_log_bridge", lambda *a, **k: None)
    return calls


def _run(flow: Any, stored: dict[str, Any], **parameters: Any) -> None:
    """Run *flow* as Prefect does: with the parameters the API stored for it."""
    stored.clear()
    stored.update(parameters)
    flow.fn(**parameters)


def _etl(tmp_path: Path) -> Any:
    cfg = tmp_path / "etl.yaml"
    cfg.write_text(
        textwrap.dedent(
            """\
            correlation_field: run_date
            params:
              run_date: ${yesterday}
              updated_at_to: ${now}
            """
        ),
        encoding="utf-8",
    )
    return etl_flow(
        name="sample",
        pipeline=_Pipeline,
        params_type=_Params,
        config_path=str(cfg),
        source_file=__file__,
    )


# --- the anchor -----------------------------------------------------------------


def test_no_anchor_outside_a_flow_run() -> None:
    assert prefect_run_anchor() is None


def test_anchor_is_the_scheduled_start_in_utc(monkeypatch: pytest.MonkeyPatch) -> None:
    madrid = timezone(timedelta(hours=2))
    monkeypatch.setitem(prefect_flow_run.FIELDS, "id", lambda: str(_RUN_ID))
    monkeypatch.setitem(
        prefect_flow_run.FIELDS,
        "scheduled_start_time",
        lambda: _PREFECT_SLOT.astimezone(madrid),
    )
    anchor = prefect_run_anchor()
    assert anchor == _SLOT
    assert type(anchor) is datetime
    assert anchor.tzinfo is UTC


# --- etl_flow -------------------------------------------------------------------


@pytest.mark.usefixtures("in_run", "client")
def test_etl_run_resolves_against_its_slot(
    tmp_path: Path, stored: dict[str, Any], invoked: list[tuple[Any, Any]]
) -> None:
    _run(_etl(tmp_path), stored, run_date="${yesterday}", updated_at_to="${now-1h}")
    params, _ctx = invoked[0]
    assert params.run_date == date(2026, 9, 21)
    assert params.updated_at_to == _SLOT - timedelta(hours=1)


@pytest.mark.usefixtures("client")
def test_outside_a_flow_run_placeholders_use_the_clock(
    tmp_path: Path, invoked: list[tuple[Any, Any]]
) -> None:
    _etl(tmp_path).fn(run_date="${today}", updated_at_to="${now}")
    params, _ctx = invoked[0]
    assert params.run_date == datetime.now(UTC).date()
    assert abs((datetime.now(UTC) - params.updated_at_to).total_seconds()) < 5


@pytest.mark.usefixtures("in_run")
def test_etl_run_records_its_stored_parameters_with_the_values(
    tmp_path: Path, stored: dict[str, Any], client: _Client, invoked: list[tuple[Any, Any]]
) -> None:
    _run(_etl(tmp_path), stored, run_date="${yesterday}", updated_at_to="${now}")
    client.update_flow_run.assert_called_once_with(
        _RUN_ID,
        parameters={"run_date": "2026-09-21", "updated_at_to": "2026-09-22T23:30:00Z"},
    )


@pytest.mark.usefixtures("in_run")
def test_stored_control_keys_are_recorded_unchanged(
    tmp_path: Path, stored: dict[str, Any], client: _Client, invoked: list[tuple[Any, Any]]
) -> None:
    _run(
        _etl(tmp_path),
        stored,
        run_date="${yesterday}",
        updated_at_to="2026-09-22T00:00:00Z",
        env="test",
        processes=["_Process"],
    )
    client.update_flow_run.assert_called_once_with(
        _RUN_ID,
        parameters={
            "run_date": "2026-09-21",
            "updated_at_to": "2026-09-22T00:00:00Z",
            "env": "test",
            "processes": ["_Process"],
        },
    )


@pytest.mark.usefixtures("in_run")
def test_the_record_is_one_short_attempt_after_the_log_bridge(
    tmp_path: Path,
    stored: dict[str, Any],
    client: _Client,
    events: list[str],
    invoked: list[tuple[Any, Any]],
) -> None:
    _run(_etl(tmp_path), stored, run_date="${yesterday}", updated_at_to="${now}")
    assert events == ["bridge", "record"]
    assert client.max_retries == 0
    assert client.httpx_settings == {"timeout": _params._RECORD_TIMEOUT_SECONDS}
    assert _params._RECORD_TIMEOUT_SECONDS <= 10


@pytest.mark.usefixtures("in_run")
def test_a_resubmitted_run_keeps_its_correlation_id_and_makes_no_call(
    tmp_path: Path, stored: dict[str, Any], client: _Client, invoked: list[tuple[Any, Any]]
) -> None:
    flow = _etl(tmp_path)
    _run(flow, stored, run_date="${yesterday}", updated_at_to="${now}")
    recorded = client.update_flow_run.call_args.kwargs["parameters"]
    client.update_flow_run.reset_mock()

    _run(flow, stored, **recorded)

    first, retry = invoked
    assert retry[1].correlation_id == first[1].correlation_id
    assert retry[0] == first[0]
    client.update_flow_run.assert_not_called()


@pytest.mark.usefixtures("in_run")
def test_concrete_parameters_are_not_recorded(
    tmp_path: Path, stored: dict[str, Any], client: _Client, invoked: list[tuple[Any, Any]]
) -> None:
    _run(_etl(tmp_path), stored, run_date="2026-09-21", updated_at_to="2026-09-22T00:00:00Z")
    client.update_flow_run.assert_not_called()
    assert len(invoked) == 1


@pytest.mark.usefixtures("in_run")
def test_a_failed_record_is_logged_and_the_run_goes_on(
    tmp_path: Path,
    stored: dict[str, Any],
    client: _Client,
    invoked: list[tuple[Any, Any]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    client.update_flow_run.side_effect = RuntimeError("api down")
    with caplog.at_level(logging.ERROR, logger=_params.__name__):
        _run(_etl(tmp_path), stored, run_date="${yesterday}", updated_at_to="${now}")

    assert len(invoked) == 1
    [record] = [r for r in caplog.records if r.name == _params.__name__]
    assert record.levelno == logging.ERROR
    assert str(_RUN_ID) in record.getMessage()
    assert record.exc_info is not None
    assert "api down" in str(record.exc_info[1])


@pytest.mark.usefixtures("in_run")
def test_a_payload_that_cannot_be_built_is_logged_and_the_run_goes_on(
    tmp_path: Path,
    stored: dict[str, Any],
    client: _Client,
    invoked: list[tuple[Any, Any]],
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _unserialisable(_values: Any) -> Any:
        raise TypeError("cannot serialise")

    monkeypatch.setattr(_params.msgspec, "to_builtins", _unserialisable)
    with caplog.at_level(logging.ERROR, logger=_params.__name__):
        _run(_etl(tmp_path), stored, run_date="${yesterday}", updated_at_to="${now}")

    assert len(invoked) == 1
    client.update_flow_run.assert_not_called()
    [record] = [r for r in caplog.records if r.name == _params.__name__]
    assert record.levelno == logging.ERROR
    assert record.exc_info is not None
    assert "cannot serialise" in str(record.exc_info[1])


def test_nothing_is_recorded_outside_a_flow_run(
    tmp_path: Path, client: _Client, invoked: list[tuple[Any, Any]]
) -> None:
    _etl(tmp_path).fn(run_date="${yesterday}", updated_at_to="${now}")
    client.update_flow_run.assert_not_called()


# --- run name -------------------------------------------------------------------


@pytest.mark.usefixtures("in_run")
def test_run_name_resolves_against_the_slot() -> None:
    run_name = make_run_name_callback("sample", "run_date")
    assert run_name(run_date="${yesterday}").startswith("20260921T000000-")


@pytest.mark.usefixtures("in_run")
def test_scheduled_run_without_correlation_value_is_named_by_its_slot() -> None:
    run_name = make_run_name_callback("sample", None)
    assert run_name(run_date="${yesterday}") == "2026-09-22T23:30"


# --- maintenance_flow -----------------------------------------------------------


class _MaintenanceParams(msgspec.Struct, frozen=True):
    older_than: date


class _MaintenanceStep(MaintenanceStep[_MaintenanceParams]):
    operations = []


@pytest.mark.usefixtures("in_run")
def test_maintenance_run_resolves_against_its_slot_and_records_it(
    tmp_path: Path, stored: dict[str, Any], client: _Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = MagicMock()
    runner.run.return_value = MaintenanceReport()
    monkeypatch.setattr(_maintenance, "install_log_bridge", MagicMock())
    monkeypatch.setattr(_maintenance, "uninstall_log_bridge", MagicMock())
    monkeypatch.setattr(_maintenance, "_load_context", lambda path, **_kw: MagicMock())
    monkeypatch.setattr(_maintenance, "_parse_sections", lambda context: (MagicMock(), MagicMock()))
    monkeypatch.setattr(_maintenance.ObservabilityRuntime, "from_config", MagicMock())
    monkeypatch.setattr(_maintenance.MaintenanceRunner, "from_config", lambda config: runner)
    cfg = tmp_path / "maintenance.yaml"
    cfg.write_text("params: {}\n", encoding="utf-8")
    flow = _maintenance.maintenance_flow(
        name="maintenance",
        step=_MaintenanceStep,
        params_type=_MaintenanceParams,
        config_path=str(cfg),
        source_file=__file__,
    )

    _run(flow, stored, older_than="${today-7d}")

    assert runner.run.call_args.kwargs["params"].older_than == date(2026, 9, 15)
    client.update_flow_run.assert_called_once_with(_RUN_ID, parameters={"older_than": "2026-09-15"})


# --- backfill_flow --------------------------------------------------------------


class _WindowParams(ETLParams, frozen=True):  # type: ignore[misc]
    updated_at_from: datetime
    updated_at_to: datetime


class _WindowStep(ETLStep[_WindowParams]):
    src = FromTable("raw.events")
    target = IntoTable("staging.events").replace()

    def execute(self, params: _WindowParams, *, src: Any = None, **frames: Any) -> Any:
        return src


class StagingProcess(ETLProcess[_WindowParams]):
    steps = [_WindowStep]


class _WindowPipeline(ETLPipeline[_WindowParams]):
    processes = [StagingProcess]


@pytest.mark.usefixtures("in_run")
def test_backfill_window_and_start_from_resolve_against_the_slot(
    tmp_path: Path, stored: dict[str, Any], client: _Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = MagicMock()
    monkeypatch.setattr(_backfill_body.ETLRunner, "from_yaml", lambda *a, **k: runner)
    monkeypatch.setattr(_backfill_body, "build_observers", lambda *a, **k: [])
    monkeypatch.setattr(_backfill_body, "install_log_bridge", lambda *a, **k: None)
    monkeypatch.setattr(_backfill_body, "uninstall_log_bridge", lambda *a, **k: None)
    cfg = tmp_path / "backfill.yaml"
    cfg.write_text("params: {}\n", encoding="utf-8")
    flow = backfill_flow(
        name="backfill",
        pipeline=_WindowPipeline,
        params_type=_WindowParams,
        config_path=str(cfg),
        source_file=__file__,
        per_chunk_processes=["StagingProcess"],
        finalize_processes=["StagingProcess"],
        window_start_field="updated_at_from",
        window_end_field="updated_at_to",
        chunk="day",
    )

    _run(
        flow,
        stored,
        updated_at_from="${today-3d}",
        updated_at_to="${today}",
        start_from="${yesterday}",
    )

    chunk_starts = [call.args[1].updated_at_from for call in runner.run.call_args_list[:-1]]
    assert chunk_starts == [datetime(2026, 9, 21, tzinfo=UTC)]
    client.update_flow_run.assert_called_once_with(
        _RUN_ID,
        parameters={
            "updated_at_from": "2026-09-19T00:00:00Z",
            "updated_at_to": "2026-09-22T00:00:00Z",
            "start_from": "2026-09-21T00:00:00Z",
        },
    )
