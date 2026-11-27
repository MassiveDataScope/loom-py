"""A JSON column that does not decode fails naming the declared column, on every engine."""

from __future__ import annotations

import pytest

from loom.core.errors import InvalidValue
from loom.etl import ETLStep, FromTable, IntoTable
from loom.etl.compiler import ETLCompiler
from loom.etl.executor import ETLExecutor
from tests.unit.etl.engines.doubles import ModelEngine, NoParams
from tests.unit.etl.models import Buyer


class _DecodeBuyer(ETLStep[NoParams]):
    customers: FromTable = FromTable("raw.customers").parse_json("buyer", Buyer)  # type: ignore[assignment]
    target = IntoTable("staging.customers").replace()

    def execute(self, params: NoParams, *, customers: object) -> object:  # type: ignore[override]
        return customers


@pytest.mark.parametrize("document", ["not json", "[1, 2]"], ids=["malformed", "shape-mismatch"])
def test_a_document_that_does_not_decode_names_the_declared_column(
    engine: ModelEngine, document: str
) -> None:
    engine.writer.write(
        engine.frame({"buyer": [document]}), IntoTable("raw.customers").append()._to_spec(), None
    )

    with pytest.raises(InvalidValue, match=r"customers\.buyer as StructType"):
        ETLExecutor(engine.reader, engine.writer).run_step(
            ETLCompiler().compile_step(_DecodeBuyer), NoParams()
        )
