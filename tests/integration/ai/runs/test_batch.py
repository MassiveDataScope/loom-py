"""Every row of an ETL batch runs as the batch's own identity."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from loom.ai.etl._batch import Batch
from loom.ai.etl._ledger import SpendLedger
from loom.core.execution import current_execution
from loom.core.identity import Identity

from .doubles import AGENT, AcmeActingEngine


class TestAnEtlBatch:
    async def test_each_row_runs_as_the_batch_identity_in_an_execution_of_its_own(
        self, make_runtime: Any
    ) -> None:
        engine = AcmeActingEngine()
        batch = Batch(
            host=make_runtime(engine),
            name=AGENT,
            limit=2,
            ledger=SpendLedger(None),
            reservation=Decimal(0),
            to_builtins=dict,
        )

        outcomes = await batch.answer_all(["a", "b", "c"])

        assert [outcome.error for outcome in outcomes] == [None, None, None]
        assert engine.acting == [Identity(subject=f"etl:{AGENT}")] * 6
        assert engine.in_execution == [True] * 6
        assert current_execution() is None
