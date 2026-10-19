"""Tests for Prefect maintenance-flow observability."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
from pathlib import Path
from unittest.mock import MagicMock

import msgspec
import pytest

from loom.core.environment import Environment
from loom.core.observability.event import Scope
from loom.etl.maintenance._protocol import TableMaintenanceResult
from loom.etl.maintenance._runner import MaintenanceError, MaintenanceReport
from loom.etl.maintenance._step import MaintenanceStep
from loom.prefect.flow import _maintenance


class _MaintenanceParams(msgspec.Struct, frozen=True):
    dry_run: bool = False


class _MaintenanceStep(MaintenanceStep[_MaintenanceParams]):
    operations = []


def _write_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "maintenance.yaml"
    config_path.write_text("params: {}\n", encoding="utf-8")
    return config_path


def _build_flow(tmp_path: Path) -> object:
    return _maintenance.maintenance_flow(
        name="maintenance",
        step=_MaintenanceStep,
        params_type=_MaintenanceParams,
        config_path=str(_write_config(tmp_path)),
        source_file=__file__,
    )


def test_maintenance_flow_bridges_logs_to_current_flow_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[str] = []
    flow_run_id = uuid.uuid4()
    storage_config = MagicMock()
    observability_config = MagicMock()
    runner = MagicMock()
    runner.run.side_effect = lambda *args, **kwargs: events.append("run") or MaintenanceReport()
    runtime = MagicMock()
    runtime.span.side_effect = lambda *args, **kwargs: events.append("span") or nullcontext()
    configure = MagicMock(side_effect=lambda config, **_kw: events.append("configure") or runtime)
    install = MagicMock(side_effect=lambda flow_id: events.append("install"))
    uninstall = MagicMock(side_effect=lambda: events.append("uninstall"))

    monkeypatch.setattr(_maintenance, "prefect_flow_run_id", lambda: flow_run_id)
    monkeypatch.setattr(_maintenance, "install_log_bridge", install)
    monkeypatch.setattr(_maintenance, "uninstall_log_bridge", uninstall)
    monkeypatch.setattr(_maintenance, "_load_context", lambda path, **_kw: events.append("load"))
    monkeypatch.setattr(
        _maintenance, "_parse_sections", lambda context: (storage_config, observability_config)
    )
    monkeypatch.setattr(_maintenance.ObservabilityRuntime, "from_config", configure)
    monkeypatch.setattr(_maintenance.MaintenanceRunner, "from_config", lambda config: runner)

    flow = _build_flow(tmp_path)
    flow.fn(dry_run=True)  # type: ignore[attr-defined]

    assert events == ["install", "load", "configure", "span", "run", "uninstall"]
    configure.assert_called_once_with(observability_config, environment=Environment.TEST)
    install.assert_called_once_with(flow_run_id)
    uninstall.assert_called_once_with()
    runtime.span.assert_called_once_with(Scope.MAINTENANCE, "_MaintenanceStep")
    runner.run.assert_called_once()


def test_maintenance_flow_uninstalls_log_bridge_after_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    span_events: list[str] = []
    report = MaintenanceReport(
        results=[TableMaintenanceResult(table_ref="raw.events", error=RuntimeError("boom"))]
    )
    runner = MagicMock()
    runner.run.return_value = report
    runtime = MagicMock()

    @contextmanager
    def record_span(*args: object, **kwargs: object) -> Iterator[None]:
        try:
            yield
        except MaintenanceError:
            span_events.append("error")
            raise

    runtime.span.side_effect = record_span
    uninstall = MagicMock()

    monkeypatch.setattr(_maintenance, "prefect_flow_run_id", uuid.uuid4)
    monkeypatch.setattr(_maintenance, "install_log_bridge", MagicMock())
    monkeypatch.setattr(_maintenance, "uninstall_log_bridge", uninstall)
    monkeypatch.setattr(_maintenance, "_load_context", lambda path, **_kw: MagicMock())
    monkeypatch.setattr(_maintenance, "_parse_sections", lambda context: (MagicMock(), MagicMock()))
    monkeypatch.setattr(
        _maintenance.ObservabilityRuntime,
        "from_config",
        MagicMock(return_value=runtime),
    )
    monkeypatch.setattr(_maintenance.MaintenanceRunner, "from_config", lambda config: runner)

    flow = _build_flow(tmp_path)
    with pytest.raises(MaintenanceError, match="raw.events"):
        flow.fn(dry_run=False)  # type: ignore[attr-defined]

    assert span_events == ["error"]
    uninstall.assert_called_once_with()
