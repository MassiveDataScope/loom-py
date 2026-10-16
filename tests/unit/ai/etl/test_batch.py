"""Batch.answer_all: bounded workers, outcomes in the order of the prompts."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast

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
