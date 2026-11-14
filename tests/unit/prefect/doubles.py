"""Doubles shared by the Prefect unit tests."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any
from unittest.mock import MagicMock

import msgspec

from loom.etl import ETLParams, ETLPipeline, ETLProcess, ETLStep, FromTable, IntoTable
from loom.etl.maintenance import MaintenanceStep


class AcmeSampleParams(ETLParams, frozen=True):  # type: ignore[misc]
    """Window parameters used by the sample ETL / backfill / deploy stacks."""

    updated_at_from: datetime
    updated_at_to: datetime


class AcmeWindowStep(ETLStep[AcmeSampleParams]):
    """Copies ``raw.events`` into ``staging.events`` over the sample window."""

    src = FromTable("raw.events")
    target = IntoTable("staging.events").replace()

    def execute(self, params: AcmeSampleParams, *, src: Any = None, **frames: Any) -> Any:
        return src


class AcmeWindowStagingProcess(ETLProcess[AcmeSampleParams]):
    """Single-step staging process over the sample window."""

    steps = [AcmeWindowStep]


class AcmeWindowPipeline(ETLPipeline[AcmeSampleParams]):
    """Pipeline running only the staging process."""

    processes = [AcmeWindowStagingProcess]


class AcmeMaintenanceParams(msgspec.Struct, frozen=True):
    """Maintenance parameters with a dry-run flag."""

    dry_run: bool = False


class AcmeMaintenance(MaintenanceStep[AcmeMaintenanceParams]):
    """A maintenance step with no operations."""

    operations = []


class AcmeFlowSourceRecorder:
    """Stands in for ``Flow.from_source`` and records what the deployer passes."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.deploy = MagicMock(return_value="deployment-id")

    def __call__(self, **kwargs: Any) -> AcmeFlowSourceRecorder:
        self.calls.append({**kwargs, "env_at_call": os.environ.get("LOOM_ETL_CONFIG")})
        return self
