"""Spend of one batch against its optional budget."""

from __future__ import annotations

from decimal import Decimal

from loom.ai.compiler import AgentPlan


def worst_case(plan: AgentPlan) -> Decimal | None:
    """Return the reservation of one run of *plan*, ``None`` when nothing caps it.

    ``policies.max_usd``, which the engine checks against the run's spend over
    every attempt after each response: a run may only exceed it by the cost of
    its last response.
    """
    return plan.policies.max_usd


class SpendLedger:
    """Reserves the worst case of a call before it starts and settles it after.

    A batch stops launching calls the first time a reservation does not fit:
    every later reservation is refused too, so the rows left unsent are the
    tail of the batch. Not thread-safe; one batch runs on one event loop.

    Args:
        budget: USD the batch may spend, or ``None`` for no ceiling.
    """

    __slots__ = ("_budget", "_exhausted", "_reserved", "_spent")

    def __init__(self, budget: Decimal | None) -> None:
        self._budget = budget
        self._spent = Decimal(0)
        self._reserved = Decimal(0)
        self._exhausted = False

    @property
    def spent(self) -> Decimal:
        """USD settled so far."""
        return self._spent

    def reserve(self, amount: Decimal) -> bool:
        """Set *amount* aside for one call, if the budget still allows it.

        Returns:
            Whether the call may start.
        """
        if self._budget is not None and (
            self._exhausted or self._spent + self._reserved + amount > self._budget
        ):
            self._exhausted = True
            return False
        self._reserved += amount
        return True

    def settle(self, reserved: Decimal, cost: Decimal | None) -> None:
        """Replace a call's reservation with what it cost; an unknown cost keeps the reservation."""
        self._reserved -= reserved
        self._spent += reserved if cost is None else cost
