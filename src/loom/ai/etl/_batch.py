"""One batch of runs of one agent on an entered runtime, bounded and budgeted."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Final

from loom.ai.abc import AgentUsage
from loom.ai.errors import AgentRunError
from loom.ai.etl._ledger import SpendLedger
from loom.ai.runtime import AgentRuntime
from loom.core.identity import Identity

PROMPT_MISSING: Final = "PROMPT_MISSING"
BUDGET_EXHAUSTED: Final = "BUDGET_EXHAUSTED"
UNEXPECTED_ERROR: Final = "UNEXPECTED_ERROR"
_SHOWN_ROWS: Final = 10

_log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RowOutcome:
    """What one row's run produced.

    Attributes:
        output: The answer as builtins, or ``None`` when the row failed.
        error: Failure code, or ``None`` when the row was answered.
        usage: What the run spent, or ``None`` when no call was made.
    """

    output: Mapping[str, Any] | None
    error: str | None
    usage: AgentUsage | None


@dataclass(frozen=True)
class Batch:
    """Runs one agent once per prompt, never more than *limit* at a time.

    Attributes:
        runtime: Runtime serving the agent, not yet entered.
        name: Agent to run.
        limit: Runs in flight at once.
        ledger: Spend of the batch against its budget.
        reservation: Worst case of one run, set aside before it starts.
        to_builtins: Projects an answer onto builtins.
    """

    runtime: AgentRuntime
    name: str
    limit: int
    ledger: SpendLedger
    reservation: Decimal
    to_builtins: Callable[[Any], Mapping[str, Any]]

    async def answer_all(self, prompts: Sequence[str | None]) -> list[RowOutcome]:
        """Enter the runtime, answer every prompt in order, and close it in the same task.

        *limit* workers take the prompts in order from one shared iterator, so
        the batch creates at most *limit* tasks whatever the number of rows.
        An exception that is not an ``Exception`` cancels the other workers
        and reaches the caller: ``KeyboardInterrupt`` and ``SystemExit`` as
        themselves, any other inside a ``BaseExceptionGroup``.

        Raises:
            RuntimeError: A run was cancelled while the batch was not, so its
                worker stopped and left rows without an outcome.
        """
        identity = Identity(subject=f"etl:{self.name}")
        outcomes: dict[int, RowOutcome] = {}
        pending = iter(enumerate(prompts))
        async with self.runtime, asyncio.TaskGroup() as workers:
            for _ in range(min(self.limit, len(prompts))):
                workers.create_task(self._work(pending, outcomes, identity))
        if missing := [row for row in range(len(prompts)) if row not in outcomes]:
            raise RuntimeError(_unanswered(self.name, missing))
        return [outcomes[row] for row in range(len(prompts))]

    async def _work(
        self,
        pending: Iterator[tuple[int, str | None]],
        outcomes: dict[int, RowOutcome],
        identity: Identity,
    ) -> None:
        for row, prompt in pending:
            outcomes[row] = await self._answer(prompt, identity)

    async def _answer(self, prompt: str | None, identity: Identity) -> RowOutcome:
        if prompt is None:
            return RowOutcome(output=None, error=PROMPT_MISSING, usage=None)
        if not self.ledger.reserve(self.reservation):
            return RowOutcome(output=None, error=BUDGET_EXHAUSTED, usage=None)
        return await self._run(prompt, identity)

    async def _run(self, prompt: str, identity: Identity) -> RowOutcome:
        usage: AgentUsage | None = None
        try:
            result = await self.runtime.run(self.name, prompt, identity=identity)
            usage = result.usage
            output = self.to_builtins(result.output)
        except AgentRunError as error:
            return self._failed(error.code.value, error.usage)
        except Exception as error:
            _log.warning(
                "agent %r failed a row with an unexpected %s", self.name, type(error).__qualname__
            )
            return self._failed(UNEXPECTED_ERROR, usage)
        self._settle(usage)
        return RowOutcome(output=output, error=None, usage=usage)

    def _failed(self, code: str, usage: AgentUsage | None) -> RowOutcome:
        self._settle(usage)
        return RowOutcome(output=None, error=code, usage=usage)

    def _settle(self, usage: AgentUsage | None) -> None:
        self.ledger.settle(self.reservation, None if usage is None else usage.cost)


def _unanswered(name: str, missing: list[int]) -> str:
    shown = missing[:_SHOWN_ROWS]
    more = "" if len(missing) <= _SHOWN_ROWS else f" and {len(missing) - _SHOWN_ROWS} more"
    return (
        f"agent {name!r} left {len(missing)} row(s) {shown}{more} without an outcome: "
        "a run was cancelled while the batch was not"
    )
