"""SparkStepRunner.seed_rows and the result's rows: typed by the model, compared by the model."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from loom.etl import ETLStep, FromTable, IntoTable
from loom.etl.testing.spark import SparkStepRunner
from loom.testing import make
from tests.unit.etl.engines.frame_types import Order
from tests.unit.etl.testing.models import CustomerTotal, Since, Stop, Tier, Visitor

DAY = datetime(2026, 1, 2, tzinfo=UTC)


class SparkTotalCustomers(ETLStep[Since]):
    orders = FromTable(Order)
    target = IntoTable(CustomerTotal).upsert()

    def execute(self, params: Since, *, orders: DataFrame) -> DataFrame:  # type: ignore[override]
        return orders.groupBy(Order.c.customer_id).agg(
            F.sum(Order.c.amount).alias(CustomerTotal.c.spent),
            F.count("*").alias(CustomerTotal.c.orders),
        )


class SparkEchoVisitors(ETLStep[Since]):
    visitors = FromTable(Visitor)
    target = IntoTable(Visitor).replace()

    def execute(self, params: Since, *, visitors: DataFrame) -> DataFrame:  # type: ignore[override]
        return visitors


class TestSparkSeedRows:
    def test_the_total_of_the_seeded_orders_is_the_model_row(
        self, step_runner: SparkStepRunner
    ) -> None:
        step_runner.seed_rows(
            *(
                make(Order, order_id=id, customer_id="c1", amount=Decimal(amount), placed_at=DAY)
                for id, amount in (("o1", "10"), ("o2", "12"))
            )
        )

        result = step_runner.run(SparkTotalCustomers, Since(since=DAY))

        result.assert_rows(make(CustomerTotal, customer_id="c1", spent=Decimal("22"), orders=2))

    def test_struct_uuid_and_enum_columns_come_back_as_the_seeded_instance(
        self, step_runner: SparkStepRunner
    ) -> None:
        stop = Stop(zip_code="28001", seen_at=DAY)
        visitor = make(Visitor, tier=Tier.GOLD, home=stop, stops=[stop], extras={"a": 1}, note=None)

        result = step_runner.seed_rows(visitor).run(SparkEchoVisitors, Since(since=DAY))

        assert result.rows(Visitor) == [visitor]
