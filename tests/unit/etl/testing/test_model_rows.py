"""seed_rows, rows and assert_rows: tests written from the models, typed by them."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

pytest.importorskip("deltalake")

from loom.core.errors import InvalidUse, InvalidValue  # noqa: E402
from loom.etl.testing import PolarsStepRunner  # noqa: E402
from loom.etl.testing.spark import SparkStepRunner  # noqa: E402
from loom.testing import make  # noqa: E402
from tests.unit.etl.models import CamelEntity, Order  # noqa: E402
from tests.unit.etl.testing.models import (  # noqa: E402
    CustomerTotal,
    EchoVisitors,
    Since,
    Tier,
    TotalCustomers,
    Visitor,
)

DAY = datetime(2026, 1, 2, tzinfo=UTC)
SINCE = Since(since=DAY)


def _order(order_id: str, amount: str) -> Order:
    return make(Order, order_id=order_id, customer_id="c1", amount=Decimal(amount), placed_at=DAY)


@pytest.fixture(params=["captured", "delta"])
def runner(request: pytest.FixtureRequest, tmp_path: Path) -> PolarsStepRunner:
    return PolarsStepRunner(delta_root=tmp_path if request.param == "delta" else None)


@pytest.fixture(params=["polars", "polars-delta", "spark"])
def rows_runner(
    request: pytest.FixtureRequest, tmp_path: Path
) -> PolarsStepRunner | SparkStepRunner:
    if request.param == "spark":
        return SparkStepRunner(request.getfixturevalue("spark"))
    return PolarsStepRunner(delta_root=tmp_path if request.param == "polars-delta" else None)


class TestRowsContract:
    def test_a_model_round_trips_through_seed_run_and_rows_by_field_name(
        self, rows_runner: PolarsStepRunner | SparkStepRunner
    ) -> None:
        stop = CamelEntity(zip_code="28001", seen_at=DAY)
        visitor = make(
            Visitor, tier=Tier.GOLD, home=stop, stops=[stop], extras={"vip": True}, note=None
        )

        result = rows_runner.seed_rows(visitor).run(EchoVisitors, SINCE)

        assert result.rows(Visitor) == [visitor]


class TestOutcome:
    def test_the_total_of_the_seeded_orders_is_the_model_row(
        self, runner: PolarsStepRunner
    ) -> None:
        runner.seed_rows(_order("o1", "10"), _order("o2", "12"))

        result = runner.run(TotalCustomers, SINCE)

        result.assert_rows(make(CustomerTotal, customer_id="c1", spent=Decimal("22"), orders=2))


class TestSeedRows:
    def test_a_key_given_twice_is_refused_naming_model_and_key(
        self, runner: PolarsStepRunner
    ) -> None:
        with pytest.raises(InvalidValue, match=r"Order repeats the key \{'order_id': 'o1'\}"):
            runner.seed_rows(_order("o1", "1"), _order("o1", "2"))


class TestAssertRows:
    @pytest.mark.parametrize(
        ("expected", "message"),
        [
            pytest.param([("o1", "10")], "unexpected", id="extra-row"),
            pytest.param([("o1", "10"), ("o2", "13")], "missing", id="changed-column"),
        ],
    )
    def test_a_different_row_set_fails_naming_the_difference(
        self, tmp_path: Path, expected: list[tuple[str, str]], message: str
    ) -> None:
        runner = PolarsStepRunner(delta_root=tmp_path)
        runner.seed_rows(_order("o1", "10"), _order("o2", "12"))
        result = runner.table("sales.order")

        with pytest.raises(AssertionError, match=message):
            result.assert_rows(*(_order(*row) for row in expected))

    def test_the_order_of_the_rows_does_not_matter(self, tmp_path: Path) -> None:
        runner = PolarsStepRunner(delta_root=tmp_path)
        runner.seed_rows(_order("o1", "10"), _order("o2", "12"))

        runner.table("sales.order").assert_rows(_order("o2", "12"), _order("o1", "10"))

    def test_rows_of_two_models_are_refused(self, tmp_path: Path) -> None:
        result = (
            PolarsStepRunner(delta_root=tmp_path).seed_rows(_order("o1", "1")).table("sales.order")
        )

        with pytest.raises(InvalidUse, match="exactly one model"):
            result.assert_rows(_order("o1", "1"), make(CustomerTotal))
