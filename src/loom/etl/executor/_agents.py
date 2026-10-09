"""The object a step receives for each of its :class:`~loom.etl.WithAgent` declarations."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from loom.etl.compiler._plan import AgentBinding
from loom.etl.runtime.contracts import AgentBatchRunner


class AgentMapper:
    """Runs one declared agent over a frame from inside ``execute()``.

    Built by the executor for every run of the step, bound to the step's
    declaration; ``execute()`` stays synchronous.

    Args:
        runner: Agent runner configured for the pipeline.
        binding: Compiled declaration of the step.

    Example::

        def execute(self, params, *, messages, labeller: AgentMapper):
            return labeller.map(messages, keys=("message_id",), prompt="text")
    """

    __slots__ = ("_binding", "_runner")

    def __init__(self, runner: AgentBatchRunner, binding: AgentBinding) -> None:
        self._runner = runner
        self._binding = binding

    @property
    def version(self) -> str:
        """Version of the agent, the value of the ``agent_version`` column."""
        return self._runner.version(self._binding.name)

    def map(self, frame: Any, *, keys: Sequence[str], prompt: object) -> Any:
        """Answer every row of *frame*, one output row per input row.

        Args:
            frame: Rows to answer.
            keys: Columns identifying a row, copied to the output.
            prompt: Expression, or column name, giving each row's prompt.

        Returns:
            The keys, the declared output's fields and the run's metadata
            columns, in the backend's frame type.
        """
        return self._runner.map(
            self._binding.name,
            frame,
            keys=tuple(keys),
            prompt=prompt,
            output_type=self._binding.output_type,
            max_usd=self._binding.max_usd,
        )
