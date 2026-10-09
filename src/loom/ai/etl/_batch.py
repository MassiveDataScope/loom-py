"""One batch of runs of one agent on an entered runtime, bounded and budgeted."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping, Sequence
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
        """Enter the runtime, answer every prompt in order, and close it in the same task."""
        slots = asyncio.Semaphore(self.limit)
        identity = Identity(subject=f"etl:{self.name}")
        async with self.runtime:
            return list(await asyncio.gather(*(self._answer(p, slots, identity) for p in prompts)))

    async def _answer(
        self, prompt: str | None, slots: asyncio.Semaphore, identity: Identity
    ) -> RowOutcome:
        if prompt is None:
            return RowOutcome(output=None, error=PROMPT_MISSING, usage=None)
        async with slots:
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
