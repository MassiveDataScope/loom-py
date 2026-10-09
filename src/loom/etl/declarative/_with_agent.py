"""Agent declaration for ETL steps.

:class:`WithAgent` names an agent of the runner's ``ai:`` section. The
executor injects an :class:`~loom.etl.AgentMapper` bound to it into
``execute()``, as a keyword argument named like the class attribute. It is
not a frame source: no reader is involved and it never becomes a
:class:`~loom.etl.declarative.source.SourceSpec`.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from loom.etl.declarative.source._from_config import type_label


class WithAgent:
    """Declare an agent injected into ``execute()`` to answer rows of a frame.

    Args:
        name: Agent name, as its artifact declares it.
        output: Type every answer decodes into; must be the artifact's own
            ``type_ref`` output.
        max_usd: Budget of one execution of the step, in USD. ``None``
            spends without a ceiling beyond each run's own
            ``policies.max_usd``.

    Raises:
        ValueError: When *name* is empty or *max_usd* is not positive.

    Example::

        class LabelReplies(ETLStep[DailyParams]):
            replies = FromTable("prep.seller_reply")
            labeller = WithAgent("seller_reply", output=SellerReply, max_usd=Decimal("2"))
            target = IntoTable("fact.seller_reply").upsert(keys=("message_id", "agent_version"))

            def execute(self, params, *, replies, labeller: AgentMapper) -> pl.DataFrame:
                return labeller.map(replies, keys=("message_id",), prompt="text")
    """

    __slots__ = ("_max_usd", "_name", "_output")

    def __init__(self, name: str, *, output: type[Any], max_usd: Decimal | None = None) -> None:
        if not name:
            raise ValueError("WithAgent name must not be empty")
        if max_usd is not None and max_usd <= 0:
            raise ValueError(f"WithAgent max_usd must be positive, got {max_usd}")
        self._name = name
        self._output = output
        self._max_usd = max_usd

    @property
    def name(self) -> str:
        """Agent name."""
        return self._name

    @property
    def output(self) -> type[Any]:
        """Type every answer decodes into."""
        return self._output

    @property
    def max_usd(self) -> Decimal | None:
        """Budget of one execution of the step, or ``None``."""
        return self._max_usd

    def __repr__(self) -> str:
        return f"WithAgent({self._name!r}, {type_label(self._output)})"
