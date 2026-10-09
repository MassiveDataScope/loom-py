"""PolarsAgentRunner.map: one output row per input row, bounded, budgeted, never aborting."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest
from pydantic_ai.exceptions import ModelHTTPError
from pydantic_ai.messages import ModelMessage, ModelResponse, ToolCallPart, UserPromptPart
from pydantic_ai.models import Model
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.usage import RequestUsage

from loom.ai import AiConfig, InferenceTarget, ModelPrice
from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider
from loom.ai.etl import PolarsAgentRunner
from tests.unit.ai.etl._types import Reply

_AGENT = """\
spec_version: 1
name: seller_reply
description: classifies a seller reply
instructions: classify the reply
model_role: classifier
output: {kind: type_ref, ref: "tests.unit.ai.etl._types:Reply"}
policies: {retries: 0, max_usd: 0.01}
"""
_PRICE = ModelPrice(input=Decimal("100"), output=Decimal("200"))
_ROW_COST = Decimal("0.002")


def _prompt_of(messages: list[ModelMessage]) -> str:
    part = messages[0].parts[-1]
    assert isinstance(part, UserPromptPart)
    return str(part.content)


def _answer(prompt: str, info: AgentInfo) -> ModelResponse:
    if prompt == "boom":
        raise ModelHTTPError(status_code=503, model_name="scripted", body=None)
    return ModelResponse(
        parts=[ToolCallPart(tool_name=info.output_tools[0].name, args={"answer": prompt.upper()})],
        usage=RequestUsage(input_tokens=10, output_tokens=5),
        model_name="scripted",
    )


def _uppercasing_model() -> Model:
    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return _answer(_prompt_of(messages), info)

    return FunctionModel(respond)


class _ConcurrencyProbe:
    def __init__(self) -> None:
        self.current = 0
        self.peak = 0
        self._lock = threading.Lock()

    def model(self) -> Model:
        async def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            with self._lock:
                self.current += 1
                self.peak = max(self.peak, self.current)
            await asyncio.sleep(0.01)
            with self._lock:
                self.current -= 1
            return _answer(_prompt_of(messages), info)

        return FunctionModel(respond)


def _runner(
    tmp_path: Path, model: Callable[[], Model] = _uppercasing_model, *, concurrency: int = 4
) -> PolarsAgentRunner:
    (tmp_path / "agents").mkdir(exist_ok=True)
    (tmp_path / "agents" / "seller_reply.agent.yaml").write_text(_AGENT)
    config = AiConfig(
        engine="pydantic-ai",
        specs=("agents/*.agent.yaml",),
        models={"classifier": InferenceTarget(provider="openai", model="gpt-4o", streaming=False)},
        prices={"gpt-4o": _PRICE},
        max_concurrent_runs=concurrency,
    )
    return PolarsAgentRunner(
        config,
        root=tmp_path,
        engine_provider=lambda: PydanticAIEngineProvider(model_resolver=lambda _: model()),
    )


def _messages(*texts: str | None) -> pl.DataFrame:
    return pl.DataFrame({"id": list(range(len(texts))), "text": list(texts)})


def _map(
    runner: PolarsAgentRunner, frame: pl.DataFrame, *, max_usd: Decimal | None = None
) -> pl.DataFrame:
    return runner.map(
        "seller_reply", frame, keys=("id",), prompt="text", output_type=Reply, max_usd=max_usd
    )


class TestRows:
    def test_every_input_row_gets_its_answer_in_order(self, tmp_path: Path) -> None:
        out = _map(_runner(tmp_path), _messages("hola", "adios", "vale"))

        assert out["id"].to_list() == [0, 1, 2]
        assert out["answer"].to_list() == ["HOLA", "ADIOS", "VALE"]
        assert out["agent_status"].to_list() == ["ok", "ok", "ok"]

    def test_metadata_columns_carry_version_tokens_and_cost(self, tmp_path: Path) -> None:
        runner = _runner(tmp_path)

        out = _map(runner, _messages("hola"))

        row = out.row(0, named=True)
        assert row["agent_version"] == runner.version("seller_reply")
        assert row["agent_error"] is None
        assert (row["agent_input_tokens"], row["agent_output_tokens"]) == (10, 5)
        assert (row["agent_cache_read_tokens"], row["agent_cache_write_tokens"]) == (0, 0)
        assert row["agent_cost_usd"] == pytest.approx(float(_ROW_COST))

    def test_a_failed_row_is_returned_as_an_error_row(self, tmp_path: Path) -> None:
        out = _map(_runner(tmp_path), _messages("hola", "boom", "vale"))

        assert out["agent_status"].to_list() == ["ok", "error", "ok"]
        assert out["agent_error"].to_list() == [None, "PROVIDER_UNAVAILABLE", None]
        assert out["answer"].to_list() == ["HOLA", None, "VALE"]

    def test_a_row_without_prompt_is_not_sent(self, tmp_path: Path) -> None:
        out = _map(_runner(tmp_path), _messages("hola", None))

        assert out["agent_error"].to_list() == [None, "PROMPT_MISSING"]
        assert out["agent_input_tokens"].to_list() == [10, 0]

    def test_a_lazy_frame_and_an_expression_prompt_are_accepted(self, tmp_path: Path) -> None:
        out = _runner(tmp_path).map(
            "seller_reply",
            _messages("hola").lazy(),
            keys=("id",),
            prompt=pl.format("{}!", pl.col("text")),
            output_type=Reply,
            max_usd=None,
        )

        assert out["answer"].to_list() == ["HOLA!"]

    def test_an_empty_frame_returns_an_empty_frame_with_every_column(self, tmp_path: Path) -> None:
        out = _map(_runner(tmp_path), _messages().cast({"text": pl.String}))

        assert out.height == 0
        assert {"id", "answer", "agent_version", "agent_status"} <= set(out.columns)

    def test_a_batch_of_errors_keeps_the_declared_answer_types(self, tmp_path: Path) -> None:
        out = _map(_runner(tmp_path), _messages("boom", "boom"))

        assert out["agent_status"].to_list() == ["error", "error"]
        assert out.schema["answer"] == pl.String()


class TestConcurrency:
    def test_runs_wait_for_a_slot_instead_of_being_refused(self, tmp_path: Path) -> None:
        probe = _ConcurrencyProbe()
        runner = _runner(tmp_path, probe.model, concurrency=3)

        out = _map(runner, _messages(*(f"m{i}" for i in range(20))))

        assert out["agent_status"].to_list() == ["ok"] * 20
        assert probe.peak <= 3

    async def test_map_works_from_inside_a_running_event_loop(self, tmp_path: Path) -> None:
        out = _map(_runner(tmp_path), _messages("hola"))

        assert out["answer"].to_list() == ["HOLA"]

    def test_two_steps_mapping_at_once_do_not_share_a_runtime(self, tmp_path: Path) -> None:
        runner = _runner(tmp_path)
        results: list[pl.DataFrame] = []

        def work() -> None:
            results.append(_map(runner, _messages("a", "b", "c")))

        threads = [threading.Thread(target=work) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert [frame["agent_status"].to_list() for frame in results] == [["ok"] * 3] * 2


class TestBudget:
    def test_rows_past_the_budget_are_returned_unsent(self, tmp_path: Path) -> None:
        out = _map(
            _runner(tmp_path, concurrency=1),
            _messages(*(f"m{i}" for i in range(12))),
            max_usd=Decimal("0.025"),
        )

        assert out["agent_error"].to_list() == [None] * 8 + ["BUDGET_EXHAUSTED"] * 4
        assert out["agent_input_tokens"].to_list()[8:] == [0] * 4
        assert Decimal(str(out["agent_cost_usd"].sum())) <= Decimal("0.025")

    def test_without_a_budget_every_row_is_sent(self, tmp_path: Path) -> None:
        out = _map(_runner(tmp_path, concurrency=1), _messages(*(f"m{i}" for i in range(12))))

        assert out["agent_status"].to_list() == ["ok"] * 12


class TestVersion:
    def test_the_version_is_the_plan_fingerprint(self, tmp_path: Path) -> None:
        version = _runner(tmp_path).version("seller_reply")

        assert len(version) == 64
        assert version == _runner(tmp_path).version("seller_reply")
