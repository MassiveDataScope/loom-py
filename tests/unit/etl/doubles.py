"""Shared doubles for etl unit tests."""

from __future__ import annotations

from datetime import date
from typing import Any

from loom.etl import ETLParams, ETLStep, FromTable, IntoTable


class RunDateETLParams(ETLParams):
    run_date: date


class RunDateIgnoredParams(ETLParams):  # type: ignore[misc]
    run_date: date


class RunDateCountriesParams(ETLParams):  # type: ignore[misc]
    run_date: date
    countries: tuple[str, ...]


class OrdersStagingAStep(ETLStep[RunDateIgnoredParams]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.a").replace()

    def execute(self, params: RunDateIgnoredParams, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class OrdersStagingBStep(ETLStep[RunDateIgnoredParams]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.b").replace()

    def execute(self, params: RunDateIgnoredParams, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class OrdersStagingCStep(ETLStep[RunDateIgnoredParams]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.c").replace()

    def execute(self, params: RunDateIgnoredParams, *, orders: Any) -> Any:  # type: ignore[override]
        return orders
