"""Agent declaration for ETL steps.

:class:`WithAgent` names an agent of the runner's ``ai:`` section. The
executor injects an :class:`~loom.etl.AgentMapper` bound to it into
``execute()``, as a keyword argument named like the class attribute. It is
not a frame source: no reader is involved and it never becomes a
:class:`~loom.etl.declarative.source.SourceSpec`.
"""

from __future__ import annotations

import numbers
import operator
import warnings
from decimal import Decimal
from typing import Any

from loom.etl.declarative.source._from_config import type_label


class WithAgent:
    """Declare an agent injected into ``execute()`` to answer rows of a frame.

    Args:
        name: Agent name, as its artifact declares it.
        output: Type every answer decodes into; must be the artifact's own
            ``type_ref`` output.
        max_usd: Budget of one execution of the step, in USD, as a
            :class:`~decimal.Decimal` or an integral number (an ``int`` or
            another :class:`numbers.Integral`, such as ``numpy.int64``),
            which is converted exactly. A ``float`` is deprecated until loom 3.0: it emits a
            :class:`DeprecationWarning` and is converted through its
            ``str()``, so ``0.1`` becomes ``Decimal("0.1")``. ``None`` spends
            without a ceiling beyond each run's own ``policies.max_usd``.

    Raises:
        TypeError: When *max_usd* is not ``None``, a :class:`~decimal.Decimal`,
            an integral number or a ``float``; a ``bool`` is refused.
        ValueError: When *name* is empty or *max_usd* is not a finite
            amount greater than zero.

    Example::

        class LabelReplies(ETLStep[DailyParams]):
            replies = FromTable("prep.seller_reply")
            labeller = WithAgent("seller_reply", output=SellerReply, max_usd=Decimal("2"))
            target = IntoTable("fact.seller_reply").upsert(keys=("message_id", "agent_version"))

            def execute(self, params, *, replies, labeller: AgentMapper) -> pl.DataFrame:
                return labeller.map(replies, keys=("message_id",), prompt="text")
    """

    __slots__ = ("_max_usd", "_name", "_output")

    def __init__(
        self,
        name: str,
        *,
        output: type[Any],
        max_usd: Decimal | int | float | None = None,
    ) -> None:
        if not name:
            raise ValueError("WithAgent name must not be empty")
        self._name = name
        self._output = output
        self._max_usd = None if max_usd is None else _budget(max_usd)

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


def _budget(max_usd: object) -> Decimal:
    """Return *max_usd* as a finite :class:`~decimal.Decimal` above zero."""
    if isinstance(max_usd, Decimal):
        amount = max_usd
    elif isinstance(max_usd, bool):
        raise TypeError("WithAgent max_usd must be a Decimal or an int, got bool")
    elif isinstance(max_usd, numbers.Integral):
        amount = Decimal(operator.index(max_usd))
    elif isinstance(max_usd, float):
        warnings.warn(
            "WithAgent(max_usd=<float>) is deprecated and will be removed in loom 3.0; "
            'pass a Decimal, such as Decimal("2.50"), or an int instead.',
            DeprecationWarning,
            stacklevel=3,
        )
        amount = Decimal(str(max_usd))
    else:
        raise TypeError(
            f"WithAgent max_usd must be a Decimal or an int, got {type(max_usd).__name__}"
        )
    if not amount.is_finite() or amount <= 0:
        raise ValueError(f"WithAgent max_usd must be a finite amount above zero, got {max_usd}")
    return amount
