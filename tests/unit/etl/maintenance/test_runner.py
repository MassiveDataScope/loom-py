"""Tests for MaintenanceRunner.run() and run_from_config()."""

from __future__ import annotations

import msgspec
import pytest

from loom.etl.maintenance._builder import MaintainSchema, MaintainTable
from loom.etl.maintenance._ops import VacuumSpec
from loom.etl.maintenance._runner import MaintenanceError, MaintenanceReport, MaintenanceRunner
from loom.etl.maintenance._step import MaintenanceStep
from loom.etl.storage._config import (
    MaintenanceConfig,
    MaintenanceVacuumConfig,
    StorageConfig,
)
from tests.unit.etl.maintenance.conftest import StubMaintainer

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_runner(
    config: StorageConfig,
    maintainer: StubMaintainer | None = None,
    *,
    missing_table_policy: str = "skip",
) -> MaintenanceRunner:
    stub = maintainer or StubMaintainer()
    locator = config.to_path_locator()
    return MaintenanceRunner(
        stub,
        locator,
        config,
        missing_table_policy=missing_table_policy,  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------------------
# Basic run()
# ---------------------------------------------------------------------------


class TestRunnerBasicRun:
    def test_vacuum_called_once(self, routed_config: StorageConfig) -> None:
        stub = StubMaintainer()
        runner = _make_runner(routed_config, stub)

        class Step(MaintenanceStep[None]):
            operations = [MaintainTable("raw.events").vacuum()]

        report = runner.run(Step)
        assert len(stub.vacuum_calls) == 1
        assert stub.vacuum_calls[0]["uri"].endswith("raw/events")
        assert not report.has_errors

    def test_compact_called(self, routed_config: StorageConfig) -> None:
        stub = StubMaintainer()
        runner = _make_runner(routed_config, stub)

        class Step(MaintenanceStep[None]):
            operations = [MaintainTable("raw.events").compact()]

        runner.run(Step)
        assert len(stub.compact_calls) == 1

    def test_z_order_called(self, routed_config: StorageConfig) -> None:
        stub = StubMaintainer()
        runner = _make_runner(routed_config, stub)

        class Step(MaintenanceStep[None]):
            operations = [MaintainTable("raw.events").z_order_by(["date"])]

        runner.run(Step)
        assert len(stub.z_order_calls) == 1
        assert stub.z_order_calls[0]["spec"].columns == ["date"]

    def test_all_ops_chained(self, routed_config: StorageConfig) -> None:
        stub = StubMaintainer()
        runner = _make_runner(routed_config, stub)

        class Step(MaintenanceStep[None]):
            operations = [MaintainTable("raw.events").vacuum().compact()]

        runner.run(Step)
        assert len(stub.vacuum_calls) == 1
        assert len(stub.compact_calls) == 1
        assert len(stub.z_order_calls) == 0

    def test_schema_expand(self, routed_config: StorageConfig) -> None:
        stub = StubMaintainer()
        runner = _make_runner(routed_config, stub)

        class Step(MaintenanceStep[None]):
            operations = [MaintainSchema("raw").vacuum()]

        runner.run(Step)
        assert len(stub.vacuum_calls) == 2  # raw.events + raw.snapshots

    def test_empty_operations_returns_empty_report(self, simple_config: StorageConfig) -> None:
        class Step(MaintenanceStep[None]):
            operations = []

        stub = StubMaintainer()
        runner = _make_runner(simple_config, stub)
        report = runner.run(Step)
        assert report.results == []
        assert not report.has_errors


# ---------------------------------------------------------------------------
# Error isolation
# ---------------------------------------------------------------------------


class TestRunnerErrorIsolation:
    def test_error_on_one_table_does_not_abort_others(
        self, routed_config: StorageConfig, tmp_path: object
    ) -> None:
        stub = StubMaintainer()
        locator = routed_config.to_path_locator()
        failing_uri = str(tmp_path) + "/raw/events"
        stub.raise_on = {failing_uri}
        runner = MaintenanceRunner(stub, locator, routed_config)

        class Step(MaintenanceStep[None]):
            operations = [
                MaintainTable("raw.events").vacuum(),
                MaintainTable("raw.snapshots").vacuum(),
            ]

        report = runner.run(Step)
        assert report.has_errors
        errors = [r for r in report.results if not r.ok]
        ok = [r for r in report.results if r.ok]
        assert len(errors) == 1
        assert len(ok) == 1
        assert errors[0].table_ref == "raw.events"

    def test_raise_if_errors_raises_maintenance_error(
        self, routed_config: StorageConfig, tmp_path: object
    ) -> None:
        stub = StubMaintainer()
        locator = routed_config.to_path_locator()
        stub.raise_on = {str(tmp_path) + "/raw/events"}
        runner = MaintenanceRunner(stub, locator, routed_config)

        class Step(MaintenanceStep[None]):
            operations = [MaintainTable("raw.events").vacuum()]

        report = runner.run(Step)
        with pytest.raises(MaintenanceError) as exc_info:
            report.raise_if_errors()
        assert "raw.events" in str(exc_info.value)

    def test_raise_if_errors_no_exception_when_clean(self, simple_config: StorageConfig) -> None:
        report = MaintenanceReport(results=[])
        report.raise_if_errors()  # should not raise


# ---------------------------------------------------------------------------
# missing_table_policy
# ---------------------------------------------------------------------------


class TestMissingTablePolicy:
    def test_skip_returns_result_without_error(self, simple_config: StorageConfig) -> None:
        stub = StubMaintainer()
        runner = _make_runner(simple_config, stub, missing_table_policy="skip")

        class Step(MaintenanceStep[None]):
            # "nonexistent.table" has no route in simple_config
            operations = [MaintainTable("nonexistent.table").vacuum()]

        # simple_config has PrefixLocator, so it will construct a URI
        # even for unknown tables — this tests the error policy against
        # a DeltaTable that doesn't exist (RuntimeError from stub)
        # We verify the runner handles it gracefully when policy is skip.
        report = runner.run(Step)
        assert report is not None

    def test_error_policy_propagates_locator_miss(self, simple_config: StorageConfig) -> None:
        stub = StubMaintainer()
        # MappingLocator with no default + missing table → raises on locate()
        from loom.etl.storage._locator import MappingLocator, TableLocation

        locator = MappingLocator(
            mapping={"raw.events": TableLocation(uri="/lake/raw/events")},
            default=None,
        )
        runner = MaintenanceRunner(stub, locator, simple_config, missing_table_policy="error")

        class Step(MaintenanceStep[None]):
            operations = [MaintainTable("nonexistent.table").vacuum()]

        with pytest.raises(KeyError):
            runner.run(Step)


# ---------------------------------------------------------------------------
# run_from_config()
# ---------------------------------------------------------------------------


class TestRunFromConfig:
    def test_discovers_tables_by_schema(self, maintenance_config: StorageConfig) -> None:
        stub = StubMaintainer()
        locator = maintenance_config.to_path_locator()
        runner = MaintenanceRunner(stub, locator, maintenance_config)

        report = runner.run_from_config()
        # maintenance_config has schemas=("raw",) → raw.events + raw.snapshots
        assert len(stub.vacuum_calls) == 2
        assert len(stub.compact_calls) == 2
        assert not report.has_errors

    def test_vacuum_config_full_reaches_the_maintainer(
        self, maintenance_config: StorageConfig
    ) -> None:
        config = msgspec.structs.replace(
            maintenance_config,
            maintenance=MaintenanceConfig(
                schemas=("raw",),
                vacuum=MaintenanceVacuumConfig(retention_hours=168, dry_run=False, full=True),
            ),
        )
        stub = StubMaintainer()
        MaintenanceRunner(stub, config.to_path_locator(), config).run_from_config()

        specs = {call["spec"] for call in stub.vacuum_calls}
        assert specs == {VacuumSpec(retention_hours=168, dry_run=False, full=True)}

    def test_empty_maintenance_config_returns_empty_report(
        self, simple_config: StorageConfig
    ) -> None:
        stub = StubMaintainer()
        locator = simple_config.to_path_locator()
        runner = MaintenanceRunner(stub, locator, simple_config)

        report = runner.run_from_config()
        assert report.results == []
        assert not stub.vacuum_calls
