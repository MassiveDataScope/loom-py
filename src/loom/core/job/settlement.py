"""The port that settles what job runs came to on the transport that delivered them."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, TypeVar

from loom.core.job.outcome import JobOutcome

MessageT = TypeVar("MessageT")


class JobSettlement(Protocol[MessageT]):
    """Maps each delivered message's outcome to an action of its transport.

    The decision is per message, from its outcome alone: the settlement owns
    no retry rule. Only the calls to the platform are grouped.
    """

    def settle_all(self, settled: Sequence[tuple[MessageT, JobOutcome]]) -> list[MessageT]:
        """Settle every message by its outcome.

        Args:
            settled: Each message that ran and what its run came to.

        Returns:
            The messages the transport must deliver again.
        """
        ...


__all__ = ["JobSettlement"]
