"""PolarsAgentRunner.map: one output row per input row, bounded, budgeted, never aborting."""

from __future__ import annotations

import asyncio
import contextvars
import threading
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from genai_prices import calc_price
from pydantic_ai.exceptions import ModelHTTPError
from pydantic_ai.messages import ModelMessage, ModelResponse, ToolCallPart, UserPromptPart
from pydantic_ai.models import Model
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.usage import RequestUsage

from loom.ai import AgentResult, AiConfig, InferenceTarget, ModelPrice
from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider
from loom.ai.etl import PolarsAgentRunner
from loom.ai.runtime import AgentRuntime
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
_PRICEY_INPUT_TOKENS = 1000
_PRICEY_ROW_COST = Decimal("0.101")
_GPT_4O_ROW_COST = calc_price(
    RequestUsage(input_tokens=10, output_tokens=5), "gpt-4o", provider_id="openai"
).total_price


class _Crash(Exception):
    pass


@pytest.fixture
def crash_on_prompt(monkeypatch: pytest.MonkeyPatch) -> None:
    original = AgentRuntime.run

    async def run(self: AgentRuntime, name: str, prompt: str, **options: Any) -> AgentResult:
        if prompt == "crash":
            raise _Crash("the runtime broke")
        return await original(self, name, prompt, **options)

    monkeypatch.setattr(AgentRuntime, "run", run)


def _prompt_of(messages: list[ModelMessage]) -> str:
    part = messages[0].parts[-1]
    assert isinstance(part, UserPromptPart)
    return str(part.content)


def _answer(prompt: str, info: AgentInfo) -> ModelResponse:
    if prompt == "boom":
        raise ModelHTTPError(status_code=503, model_name="scripted", body=None)
    return ModelResponse(
        parts=[ToolCallPart(tool_name=info.output_tools[0].name, args={"answer": prompt.upper()})],
        usage=RequestUsage(
            input_tokens=_PRICEY_INPUT_TOKENS if prompt.startswith("pricey") else 10,
            output_tokens=5,
        ),
        model_name="scripted",
    )


def _uppercasing_model() -> Model:
    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return _answer(_prompt_of(messages), info)

    return FunctionModel(respond)


class _ConcurrencyProbe:
    def __init__(self, delay: float = 0.01) -> None:
        self.delay = delay
        self.current = 0
        self.peak = 0
        self._lock = threading.Lock()

    def model(self) -> Model:
        async def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            with self._lock:
                self.current += 1
                self.peak = max(self.peak, self.current)
            await asyncio.sleep(self.delay)
            with self._lock:
                self.current -= 1
            return _answer(_prompt_of(messages), info)

        return FunctionModel(respond)


def _runner(
    tmp_path: Path,
    model: Callable[[], Model] = _uppercasing_model,
    *,
    concurrency: int = 4,
    prices: dict[str, ModelPrice] | None = None,
    providers_built: list[object] | None = None,
    agent: str = _AGENT,
) -> PolarsAgentRunner:
    (tmp_path / "agents").mkdir(parents=True, exist_ok=True)
    (tmp_path / "agents" / "seller_reply.agent.yaml").write_text(agent)
    config = AiConfig(
        engine="pydantic-ai",
        specs=("agents/*.agent.yaml",),
        models={"classifier": InferenceTarget(provider="openai", model="gpt-4o", streaming=False)},
        prices={"gpt-4o": _PRICE} if prices is None else prices,
        max_concurrent_runs=concurrency,
    )
    built = [] if providers_built is None else providers_built

    def provider() -> PydanticAIEngineProvider:
        built.append(object())
        return PydanticAIEngineProvider(model_resolver=lambda _: model())

    return PolarsAgentRunner(config, root=tmp_path, engine_provider=provider)


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

    @pytest.mark.usefixtures("crash_on_prompt")
    def test_an_unexpected_exception_is_an_error_row(self, tmp_path: Path) -> None:
        out = _map(_runner(tmp_path), _messages("hola", "crash", "vale"))

        assert out["agent_status"].to_list() == ["ok", "error", "ok"]
        assert out["agent_error"].to_list() == [None, "UNEXPECTED_ERROR", None]
        assert out["answer"].to_list() == ["HOLA", None, "VALE"]
        assert out["agent_input_tokens"].to_list() == [10, 0, 10]

    @pytest.mark.usefixtures("crash_on_prompt")
    def test_an_unexpected_exception_keeps_its_reservation_spent(self, tmp_path: Path) -> None:
        out = _map(
            _runner(tmp_path, concurrency=1),
            _messages("crash", "a", "b"),
            max_usd=Decimal("0.021"),
        )

        assert out["agent_error"].to_list() == ["UNEXPECTED_ERROR", None, "BUDGET_EXHAUSTED"]

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


class TestColumns:
    @pytest.mark.parametrize("key", ["answer", "agent_version"])
    def test_a_key_named_like_an_answer_column_is_refused(self, tmp_path: Path, key: str) -> None:
        frame = pl.DataFrame({key: ["k"], "text": ["hola"]})

        with pytest.raises(ValueError, match=key):
            _runner(tmp_path).map(
                "seller_reply", frame, keys=(key,), prompt="text", output_type=Reply, max_usd=None
            )


_TENANT: contextvars.ContextVar[str] = contextvars.ContextVar("tenant", default="none")


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

    async def test_the_runs_see_the_context_of_the_caller(self, tmp_path: Path) -> None:
        def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            return _answer(_TENANT.get(), info)

        _TENANT.set("cuimo")

        out = _map(_runner(tmp_path, lambda: FunctionModel(respond)), _messages("hola"))

        assert out["answer"].to_list() == ["CUIMO"]

    def test_two_steps_mapping_at_once_do_not_share_a_runtime(self, tmp_path: Path) -> None:
        probe = _ConcurrencyProbe(delay=0.05)
        runner = _runner(tmp_path, probe.model, concurrency=2)
        start = threading.Barrier(2)
        results: list[pl.DataFrame] = []

        def work() -> None:
            start.wait(timeout=5)
            results.append(_map(runner, _messages(*(f"m{i}" for i in range(6)))))

        threads = [threading.Thread(target=work) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert [frame["agent_status"].to_list() for frame in results] == [["ok"] * 6] * 2
        assert probe.peak > 2


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

    @pytest.mark.parametrize("concurrency", [2, 4, 8])
    def test_concurrent_runs_never_spend_past_the_budget(
        self, tmp_path: Path, concurrency: int
    ) -> None:
        probe = _ConcurrencyProbe()
        budget = Decimal("0.025")

        out = _map(
            _runner(tmp_path, probe.model, concurrency=concurrency),
            _messages(*(f"m{i}" for i in range(30))),
            max_usd=budget,
        )

        assert Decimal(str(out["agent_cost_usd"].sum())) <= budget
        assert out["agent_error"].drop_nulls().unique().to_list() == ["BUDGET_EXHAUSTED"]
        assert probe.peak > 1

    @pytest.mark.parametrize("concurrency", [1, 4])
    def test_a_run_overshoots_only_by_its_last_response(
        self, tmp_path: Path, concurrency: int
    ) -> None:
        probe = _ConcurrencyProbe()
        budget = Decimal("0.04")

        out = _map(
            _runner(tmp_path, probe.model, concurrency=concurrency),
            _messages(*(f"pricey{i}" for i in range(12))),
            max_usd=budget,
        )

        spent = Decimal(str(out["agent_cost_usd"].sum()))
        sent = out.filter(pl.col("agent_error") != "BUDGET_EXHAUSTED")
        assert budget < spent <= budget + concurrency * _PRICEY_ROW_COST
        assert sent.height <= concurrency
        assert sent["agent_error"].unique().to_list() == ["USAGE_LIMIT_EXCEEDED"]
        assert sent["agent_cost_usd"].to_list() == [float(_PRICEY_ROW_COST)] * sent.height

    def test_a_step_budget_without_a_run_cap_is_refused(self, tmp_path: Path) -> None:
        runner = _runner(tmp_path, agent=_AGENT.replace(", max_usd: 0.01", ""))

        with pytest.raises(ValueError, match="policies.max_usd"):
            _map(runner, _messages("hola"), max_usd=Decimal("1"))

    def test_without_a_budget_every_row_is_sent(self, tmp_path: Path) -> None:
        out = _map(_runner(tmp_path, concurrency=1), _messages(*(f"m{i}" for i in range(12))))

        assert out["agent_status"].to_list() == ["ok"] * 12


class TestEmptyBatch:
    def test_an_empty_batch_neither_compiles_nor_opens_a_runtime(self, tmp_path: Path) -> None:
        providers: list[object] = []

        out = _map(
            _runner(tmp_path, providers_built=providers), _messages().cast({"text": pl.String})
        )

        assert out.height == 0
        assert out.schema["answer"] == pl.String()
        assert providers == []


def _gpt_4o() -> Model:
    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        answer = _answer(_prompt_of(messages), info)
        usage = RequestUsage(input_tokens=10, output_tokens=5)
        usage.cost = calc_price(usage, "gpt-4o", provider_id="openai").total_price
        return ModelResponse(
            parts=answer.parts, usage=usage, model_name="gpt-4o", provider_name="openai"
        )

    return FunctionModel(respond)


class TestPrices:
    def test_a_configured_price_wins_over_the_engine_price_of_a_known_model(
        self, tmp_path: Path
    ) -> None:
        own = _map(_runner(tmp_path, _gpt_4o), _messages("hola"))["agent_cost_usd"][0]
        engine = _map(_runner(tmp_path / "engine", _gpt_4o, prices={}), _messages("hola"))[
            "agent_cost_usd"
        ][0]

        assert own == pytest.approx(float(_ROW_COST))
        assert engine == pytest.approx(float(_GPT_4O_ROW_COST))
        assert _GPT_4O_ROW_COST != _ROW_COST


class TestVersion:
    def test_the_version_is_the_plan_fingerprint(self, tmp_path: Path) -> None:
        version = _runner(tmp_path).version("seller_reply")

        assert len(version) == 64
        assert version == _runner(tmp_path).version("seller_reply")

    def test_map_writes_the_version_of_the_plan_it_ran_and_version_follows(
        self, tmp_path: Path
    ) -> None:
        runner = _runner(tmp_path)
        before = runner.version("seller_reply")
        (tmp_path / "agents" / "seller_reply.agent.yaml").write_text(
            _AGENT.replace("classify the reply", "classify the seller reply")
        )

        out = _map(runner, _messages("hola"))

        assert out["agent_version"].to_list() == [runner.version("seller_reply")]
        assert runner.version("seller_reply") != before
