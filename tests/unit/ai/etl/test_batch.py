"""Batch.answer_all: bounded workers, outcomes in the order of the prompts."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast

import pytest

from loom.ai.etl._batch import PROMPT_MISSING, Batch, RowOutcome
from loom.ai.etl._ledger import SpendLedger
from loom.ai.runtime import AgentRuntime
from loom.core.identity import Identity

_LIMIT = 3


class _Runtime:
    def __init__(self) -> None:
        self.peak_tasks = 0

    async def __aenter__(self) -> _Runtime:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def run(self, name: str, prompt: str, *, identity: Identity) -> Any:
        self.peak_tasks = max(self.peak_tasks, len(asyncio.all_tasks()))
        await asyncio.sleep(0.001 * (int(prompt) % 4))
        return SimpleNamespace(output={"answer": prompt}, usage=None)


def _answer_all(runtime: _Runtime, prompts: list[str | None]) -> list[RowOutcome]:
    batch = Batch(
        runtime=cast(AgentRuntime, runtime),
        name="labeller",
        limit=_LIMIT,
        ledger=SpendLedger(None),
        reservation=Decimal(0),
        to_builtins=lambda output: output,
    )
    return asyncio.run(batch.answer_all(prompts))


def test_no_more_than_limit_tasks_run_besides_the_caller() -> None:
    runtime = _Runtime()

    _answer_all(runtime, [str(row) for row in range(200)])

    assert runtime.peak_tasks <= _LIMIT + 1


def test_outcomes_follow_the_order_of_the_prompts() -> None:
    prompts: list[str | None] = [str(row) for row in range(40)]
    prompts[7] = None

    outcomes = _answer_all(_Runtime(), prompts)

    assert [o.output["answer"] if o.output else o.error for o in outcomes] == [
        PROMPT_MISSING if prompt is None else prompt for prompt in prompts
    ]


class _Abort(BaseException):
    """Not an ``Exception``: the batch does not turn it into an error row."""


class _Failing(_Runtime):
    def __init__(self, failing: str, raised: BaseException) -> None:
        super().__init__()
        self.failing = failing
        self.raised = raised

    async def run(self, name: str, prompt: str, *, identity: Identity) -> Any:
        if prompt == self.failing:
            raise self.raised
        return await super().run(name, prompt, identity=identity)


def test_no_prompt_answers_nothing() -> None:
    assert _answer_all(_Runtime(), []) == []


def test_fewer_prompts_than_the_limit_are_all_answered() -> None:
    outcomes = _answer_all(_Runtime(), ["1", "2"])

    assert [o.output["answer"] if o.output else None for o in outcomes] == ["1", "2"]


def test_a_base_exception_in_one_row_reaches_the_caller_in_a_group() -> None:
    runtime = _Failing("5", _Abort())

    with pytest.raises(BaseExceptionGroup) as excinfo:
        _answer_all(runtime, [str(row) for row in range(10)])

    assert excinfo.group_contains(_Abort)


def test_a_keyboard_interrupt_in_one_row_reaches_the_caller_as_itself() -> None:
    runtime = _Failing("5", KeyboardInterrupt())

    with pytest.raises(KeyboardInterrupt):
        _answer_all(runtime, [str(row) for row in range(10)])


@pytest.mark.parametrize("rows", [1, 10])
def test_a_row_whose_run_is_cancelled_fails_the_batch_naming_it(rows: int) -> None:
    runtime = _Failing("0", asyncio.CancelledError())

    with pytest.raises(RuntimeError, match=r"labeller.*row\(s\) \[0\]"):
        _answer_all(runtime, [str(row) for row in range(rows)])


class _CancelledRuns(_Runtime):
    async def run(self, name: str, prompt: str, *, identity: Identity) -> Any:
        raise asyncio.CancelledError


def test_the_unanswered_rows_are_named_up_to_ten() -> None:
    with pytest.raises(RuntimeError, match=r"20 row\(s\) \[0, 1, .*, 9\] and 10 more"):
        _answer_all(_CancelledRuns(), [str(row) for row in range(20)])


@pytest.mark.parametrize("limit", [0, -1])
def test_a_limit_below_one_is_refused_at_construction(limit: int) -> None:
    with pytest.raises(ValueError, match=r"limit must be at least 1, got " + str(limit)):
        Batch(
            runtime=cast(AgentRuntime, _Runtime()),
            name="labeller",
            limit=limit,
            ledger=SpendLedger(None),
            reservation=Decimal(0),
            to_builtins=lambda output: output,
        )
