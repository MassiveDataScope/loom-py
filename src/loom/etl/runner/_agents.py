"""Agent runner of an engine no agent runner serves.

Internal module — not part of the public API.
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from typing import Any, NoReturn

from loom.etl.runtime.contracts import AgentIssue, AgentIssueKind


class UnservedAgents:
    """Refuses every ``WithAgent`` at compile time: agents map Polars frames only.

    Args:
        engine: Engine the runner was configured with.
    """

    __slots__ = ("_reason",)

    def __init__(self, engine: str) -> None:
        self._reason = (
            f"agents map Polars frames only, and this runner executes on {engine!r}; "
            "run the step with storage.engine 'polars'"
        )

    def validate(
        self, name: str, output_type: type[Any], *, max_usd: Decimal | None
    ) -> tuple[AgentIssue, ...]:
        """Return the one issue every declaration has on this engine."""
        return (AgentIssue(kind=AgentIssueKind.UNSUPPORTED_ENGINE, message=self._reason),)

    def version(self, name: str) -> NoReturn:
        """Refuse; a compiled plan never reaches here."""
        raise RuntimeError(self._reason)

    def map(
        self,
        name: str,
        frame: Any,
        *,
        keys: Sequence[str],
        prompt: object,
        output_type: type[Any],
        max_usd: Decimal | None,
    ) -> NoReturn:
        """Refuse; a compiled plan never reaches here."""
        raise RuntimeError(self._reason)
