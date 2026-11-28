"""Spark recognises only a FAILFAST ``from_json`` refusal as a JSON decode failure."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

pytest.importorskip("pyspark")

from py4j.protocol import Py4JJavaError  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

from loom.etl.engines.spark._json_failure import is_json_decode_failure  # noqa: E402

_ANSI = "spark.sql.ansi.enabled"
_REFUSALS = {
    "json-date": ("from_json(o, 'v date', map('mode', 'FAILFAST'))", True),
    "json-malformed": ("from_json(o, 'v int', map('mode', 'FAILFAST'))", True),
    "cast-to-int": ("cast(o as int)", False),
    "cast-to-date": ("cast(o as date)", False),
}


@pytest.fixture
def ansi_spark(spark: SparkSession) -> Iterator[SparkSession]:
    spark.conf.set(_ANSI, "true")
    yield spark
    spark.conf.set(_ANSI, "false")


@pytest.mark.parametrize(("expression", "recognised"), _REFUSALS.values(), ids=list(_REFUSALS))
def test_a_refusal_is_recognised_only_when_from_json_raised_it(
    ansi_spark: SparkSession, expression: str, recognised: bool
) -> None:
    frame = ansi_spark.createDataFrame([('{"v": "yesterday"}',)], ["o"]).selectExpr(expression)

    with pytest.raises(Py4JJavaError) as refused:
        frame.collect()

    assert is_json_decode_failure(refused.value) is recognised
