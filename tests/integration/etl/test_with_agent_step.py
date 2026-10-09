"""A WithAgent step end to end: Delta in, pydantic-ai TestModel, Delta out, anti-join on rerun."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

pytest.importorskip("polars")
pytest.importorskip("deltalake")

import polars as pl
from deltalake import write_deltalake
from pydantic_ai.exceptions import ModelHTTPError
from pydantic_ai.messages import ModelMessage, ModelResponse
from pydantic_ai.models import Model
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.models.test import TestModel

from loom.ai import AiConfig, InferenceTarget, ModelPrice
from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider
from loom.ai.etl import PolarsAgentRunner
from loom.etl import (
    AgentMapper,
    ETLParams,
    ETLPipeline,
    ETLProcess,
    ETLRunner,
    ETLStep,
    FromTable,
    IntoTable,
    WithAgent,
)
from loom.etl.storage._config import convert_storage_config, normalise_storage_section
from tests.unit.ai.etl._types import Reply

_KEYS = ("message_id",)
_AGENT = """\
spec_version: 1
name: seller_reply
description: classifies a seller reply
instructions: classify the reply
model_role: classifier
output: {kind: type_ref, ref: "tests.unit.ai.etl._types:Reply"}
policies: {retries: 0, max_usd: 0.01, on_unpriced_spend: refuse}
"""


class RunParams(ETLParams):  # type: ignore[misc]
    run_date: date


class LabelReplies(ETLStep[RunParams]):
    messages = FromTable("raw.messages")
    stored = FromTable("fact.labels")
    labeller = WithAgent("seller_reply", output=Reply, max_usd=Decimal("1"))
    target = IntoTable("fact.labels").upsert(keys=(*_KEYS, "agent_version"))

    def execute(  # type: ignore[override]
        self,
        params: RunParams,
        *,
        messages: pl.LazyFrame,
        stored: pl.LazyFrame,
        labeller: AgentMapper,
    ) -> pl.LazyFrame:
        done = stored.filter(pl.col("agent_version") == labeller.version).select(_KEYS)
        pending = messages.join(done, on=list(_KEYS), how="anti")
        return labeller.map(pending, keys=_KEYS, prompt="text").lazy()


class _Process(ETLProcess[RunParams]):
    steps = [LabelReplies]


class _Pipeline(ETLPipeline[RunParams]):
    processes = [_Process]


def _seed(lake: Path, ref: str, frame: pl.DataFrame) -> None:
    path = lake.joinpath(*ref.split("."))
    path.mkdir(parents=True, exist_ok=True)
    write_deltalake(str(path), frame, mode="overwrite")


def _labels_schema_seed() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "message_id": [-1],
            "answer": ["seed"],
            "agent_version": ["none"],
            "agent_status": ["ok"],
            "agent_error": [None],
            "agent_input_tokens": [0],
            "agent_output_tokens": [0],
            "agent_cache_read_tokens": [0],
            "agent_cache_write_tokens": [0],
            "agent_cost_usd": [0.0],
        },
        schema_overrides={"agent_error": pl.String},
    )


def _failing_model() -> Model:
    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        raise ModelHTTPError(status_code=503, model_name="down", body=None)

    return FunctionModel(respond)


def _runner(tmp_path: Path, models_built: list[Model], *, failing: bool = False) -> ETLRunner:
    (tmp_path / "agents").mkdir()
    (tmp_path / "agents" / "seller_reply.agent.yaml").write_text(_AGENT)
    ai = AiConfig(
        engine="pydantic-ai",
        specs=("agents/*.agent.yaml",),
        models={"classifier": InferenceTarget(provider="bedrock", model="eu.haiku", region="eu")},
        prices={"eu.haiku": ModelPrice(input=Decimal("1"), output=Decimal("5"))},
    )

    def build_model(target: InferenceTarget) -> Model:
        model = _failing_model() if failing else TestModel(custom_output_args={"answer": "acepta"})
        models_built.append(model)
        return model

    agents = PolarsAgentRunner(
        ai,
        root=tmp_path,
        engine_provider=lambda: PydanticAIEngineProvider(model_resolver=build_model),
    )
    storage = convert_storage_config(
        normalise_storage_section({"defaults": {"table_path": {"uri": str(tmp_path / "lake")}}})
    )
    return ETLRunner.from_config(storage, agents=agents)


def _labels(lake: Path) -> pl.DataFrame:
    return pl.read_delta(str(lake / "fact" / "labels")).filter(pl.col("message_id") >= 0)


def test_the_step_labels_every_pending_message_and_skips_them_on_rerun(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    _seed(lake, "raw.messages", pl.DataFrame({"message_id": [1, 2, 3], "text": ["a", "b", "c"]}))
    _seed(lake, "fact.labels", _labels_schema_seed())
    models_built: list[Model] = []
    runner = _runner(tmp_path, models_built)
    params = RunParams(run_date=date(2026, 10, 9))

    runner.run(_Pipeline, params)
    first = _labels(lake).sort("message_id")
    runner.run(_Pipeline, params)
    second = _labels(lake).sort("message_id")

    assert first["message_id"].to_list() == [1, 2, 3]
    assert first["answer"].to_list() == ["acepta"] * 3
    assert first["agent_status"].to_list() == ["ok"] * 3
    assert first["agent_cost_usd"].null_count() == 0
    assert second.equals(first)
    assert len(models_built) == 1


def test_a_batch_of_errors_is_upserted_into_an_existing_table(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    _seed(lake, "raw.messages", pl.DataFrame({"message_id": [1, 2], "text": ["a", "b"]}))
    _seed(lake, "fact.labels", _labels_schema_seed())
    runner = _runner(tmp_path, [], failing=True)

    runner.run(_Pipeline, RunParams(run_date=date(2026, 10, 9)))
    stored = _labels(lake).sort("message_id")

    assert stored["message_id"].to_list() == [1, 2]
    assert stored["agent_status"].to_list() == ["error", "error"]
    assert stored["agent_error"].to_list() == ["PROVIDER_UNAVAILABLE"] * 2
    assert stored["answer"].to_list() == [None, None]
    assert stored.schema["answer"] == pl.String()
