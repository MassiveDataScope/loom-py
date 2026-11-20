"""Shared doubles for backend tests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from loom.etl import ETLParams


@dataclass
class RunDateParams:
    run_date: date


class NoParams(ETLParams):
    pass
