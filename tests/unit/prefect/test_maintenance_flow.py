"""Tests for Prefect maintenance-flow observability."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from loom.core.environment import Environment
from loom.core.observability.event import Scope
from loom.etl.maintenance._protocol import TableMaintenanceResult
from loom.etl.maintenance._runner import MaintenanceError, MaintenanceReport
from loom.prefect.flow import _maintenance
from tests.unit.prefect.doubles import AcmeMaintenance, AcmeMaintenanceParams


def _write_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "maintenance.yaml"
    config_path.write_text("params: {}\n", encoding="utf-8")
    return config_path


def _build_flow(tmp_path: Path) -> object:
    return _maintenance.maintenance_flow(
        name="maintenance",
        step=AcmeMaintenance,
        params_type=AcmeMaintenanceParams,
        config_path=str(_write_config(tmp_path)),
        source_file=__file__,
    )


class TestMaintenanceFlow:
    def test_runs_the_step_inside_the_maintenance_span(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        events: list[str] = []
        storage_config = MagicMock()
        observability_config = MagicMock()
        runner = MagicMock()
        runner.run.side_effect = lambda *args, **kwargs: events.append("run") or MaintenanceReport()
        runtime = MagicMock()
        runtime.span.side_effect = lambda *args, **kwargs: events.append("span") or nullcontext()
        configure = MagicMock(
            side_effect=lambda config, **_kw: events.append("configure") or runtime
        )

        monkeypatch.setattr(
            _maintenance, "_load_context", lambda path, **_kw: events.append("load")
        )
        monkeypatch.setattr(
            _maintenance, "_parse_sections", lambda context: (storage_config, observability_config)
        )
        monkeypatch.setattr(_maintenance.Observability, "from_config", configure)
        monkeypatch.setattr(_maintenance.MaintenanceRunner, "from_config", lambda config: runner)

        flow = _build_flow(tmp_path)
        flow.fn(dry_run=True)  # type: ignore[attr-defined]

        assert events == ["load", "configure", "span", "run"]
        configure.assert_called_once_with(observability_config, environment=Environment.TEST)
        runtime.span.assert_called_once_with(Scope.MAINTENANCE, "AcmeMaintenance")
        runner.run.assert_called_once()

    def test_a_failed_report_ends_the_span_with_the_error(
        self,
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
        monkeypatch.setattr(_maintenance, "_load_context", lambda path, **_kw: MagicMock())
        monkeypatch.setattr(
            _maintenance, "_parse_sections", lambda context: (MagicMock(), MagicMock())
        )
        monkeypatch.setattr(
            _maintenance.Observability,
            "from_config",
            MagicMock(return_value=runtime),
        )
        monkeypatch.setattr(_maintenance.MaintenanceRunner, "from_config", lambda config: runner)

        flow = _build_flow(tmp_path)
        with pytest.raises(MaintenanceError, match="raw.events"):
            flow.fn(dry_run=False)  # type: ignore[attr-defined]

        assert span_events == ["error"]
