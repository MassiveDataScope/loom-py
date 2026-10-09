"""The Polars implementation of :class:`~loom.etl.runtime.contracts.AgentBatchRunner`."""

from __future__ import annotations

import asyncio
import contextvars
from collections.abc import Callable, Coroutine, Sequence
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from functools import partial
from pathlib import Path
from typing import Any, TypeVar

import polars as pl

from loom.ai.abc import AgentEngineProvider
from loom.ai.bootstrap import build_agent_runtime
from loom.ai.compiler import AgentPlan
from loom.ai.config import AiConfig
from loom.ai.etl._batch import Batch
from loom.ai.etl._columns import output_columns
from loom.ai.etl._frames import answers_frame, prompt_rows, require_free_keys
from loom.ai.etl._ledger import SpendLedger, worst_case
from loom.ai.etl._validation import validation_issues
from loom.ai.registry import resolve_engine_provider
from loom.ai.runtime import AgentRuntime
from loom.core.model import loom_type
from loom.etl.runtime.contracts import AgentIssue

ResultT = TypeVar("ResultT")


class PolarsAgentRunner:
    """Runs the agents of an ``ai:`` section over Polars frames, for ETL steps.

    Every call to :meth:`map` builds, enters and closes a runtime of its own,
    so two steps mapping at once never share one. ``map`` is synchronous: it
    runs its batch on an event loop of its own, in a dedicated thread, which
    sees the caller's context variables, when the calling thread already runs
    one. :meth:`validate` and :meth:`version` compile each agent once per
    runner.

    Args:
        config: Parsed ``ai:`` section.
        root: Directory the artifact globs are resolved against.
        engine_provider: Builds the engine provider of each runtime; resolved
            from ``config.engine`` when omitted.
    """

    def __init__(
        self,
        config: AiConfig,
        *,
        root: Path,
        engine_provider: Callable[[], AgentEngineProvider] | None = None,
    ) -> None:
        self._config = config
        self._root = root
        self._engine_provider = engine_provider or partial(resolve_engine_provider, config.engine)
        self._compiled: dict[str, tuple[AgentPlan, ...]] = {}

    def validate(
        self, name: str, output_type: type[Any], *, max_usd: Decimal | None
    ) -> tuple[AgentIssue, ...]:
        """Check that agent *name* can serve a step expecting *output_type*.

        Args:
            name: Agent name declared by the step.
            output_type: Type the step expects every answer to decode into.
            max_usd: Budget of one execution of the step, when declared.

        Returns:
            Every issue found; empty when the agent can serve the step.
        """
        return validation_issues(
            name,
            output_type,
            max_usd=max_usd,
            compile_plans=partial(self._plans, name),
            engine_provider=self._engine_provider,
        )

    def version(self, name: str) -> str:
        """Return the fingerprint of agent *name*'s compiled plan."""
        return _only_plan(self._plans(name), name).fingerprint

    def map(
        self,
        name: str,
        frame: Any,
        *,
        keys: Sequence[str],
        prompt: object,
        output_type: type[Any],
        max_usd: Decimal | None,
    ) -> pl.DataFrame:
        """Run agent *name* once per row of *frame*, never aborting on a row.

        Args:
            name: Agent to run.
            frame: Polars ``DataFrame`` or ``LazyFrame``; collected here.
            keys: Columns identifying a row, copied to the output.
            prompt: Polars expression, or column name, giving each row's prompt.
            output_type: Type every answer decodes into; its fields become columns.
            max_usd: Budget of this call; rows past it are returned unsent.

        Returns:
            The keys, one column per field of *output_type*, and
            ``agent_version``, ``agent_status`` (``ok`` or ``error``),
            ``agent_error``, the four token counts and ``agent_cost_usd``.

        Raises:
            ValueError: When a key is named like a column the agent writes.
        """
        columns = output_columns(output_type)
        require_free_keys(keys, columns)
        rows, prompts = prompt_rows(frame, keys, prompt)
        runtime = self._runtime(name)
        plan = _only_plan(runtime.plans, name)
        projection = loom_type(output_type)
        batch = Batch(
            runtime=runtime,
            name=name,
            limit=self._config.max_concurrent_runs,
            ledger=SpendLedger(max_usd),
            reservation=worst_case(plan) or Decimal(0),
            to_builtins=projection.to_builtins,
        )
        outcomes = _run_blocking(partial(batch.answer_all, prompts))
        return answers_frame(rows, outcomes, columns, plan.fingerprint)

    def _runtime(self, name: str) -> AgentRuntime:
        return build_agent_runtime(
            self._config,
            root=self._root,
            names={name},
            engine_provider=self._engine_provider(),
        )

    def _plans(self, name: str) -> tuple[AgentPlan, ...]:
        plans = self._compiled.get(name)
        if plans is None:
            plans = self._compiled[name] = self._runtime(name).plans
        return plans


def _only_plan(plans: tuple[AgentPlan, ...], name: str) -> AgentPlan:
    if not plans:
        raise LookupError(f"no agent artifact declares an agent named {name!r}")
    return plans[0]


def _run_blocking(work: Callable[[], Coroutine[Any, Any, ResultT]]) -> ResultT:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(work())
    context = contextvars.copy_context()
    with ThreadPoolExecutor(max_workers=1) as thread:
        return thread.submit(context.run, lambda: asyncio.run(work())).result()
