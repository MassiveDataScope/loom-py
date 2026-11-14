"""Shared doubles for compiler tests."""

from __future__ import annotations

from datetime import date
from typing import Any

from loom.etl import ETLParams, ETLStep, IntoTable
from tests.unit.etl.doubles import RunDateETLParams


class RunDateCountryETLParams(ETLParams):
    run_date: date
    country: str


class AppendOutStep(ETLStep[RunDateETLParams]):
    target = IntoTable("t.out").append()

    def execute(self, params: RunDateETLParams) -> Any:
        return None
