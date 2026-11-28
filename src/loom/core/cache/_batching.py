"""Chunking shared by the cache read path.

Both round trips the cached list path performs grow with the page size, and
each is bounded for its own reason, so the two limits are declared separately.

``itertools.batched`` would do the job but requires Python 3.12; this package
supports 3.11.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TypeVar

from loom.core.errors import InvalidValue

T = TypeVar("T")

COUNTER_BATCH_SIZE = 500
"""Maximum number of generation counters read with a single multi-key command.

Redis is single-threaded: one ``MGET`` covering a whole page would block every
other client for as long as it runs.
"""

REFILL_BATCH_SIZE = 500
"""Maximum number of ids sent in one ``id IN (...)`` refill query.

The repositories build a ``PageParams`` from the query, whose limit must stay
within ``1..1000``, so a refill batch must never exceed that ceiling.
"""


def batched(values: Sequence[T], size: int) -> Iterator[Sequence[T]]:
    """Split *values* into consecutive slices of at most *size* elements.

    Args:
        values: Sequence to split.  An empty sequence yields nothing.
        size: Maximum length of each slice.  Must be positive.

    Yields:
        Slices of ``values`` in order.

    Raises:
        InvalidValue: If *size* is not positive.
    """
    if size <= 0:
        raise InvalidValue("size must be >= 1")
    for start in range(0, len(values), size):
        yield values[start : start + size]
