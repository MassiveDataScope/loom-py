"""Deterministic in-memory load generator.

Every generated event is a pure function of ``(seed, partition, offset)``, so a
resumed run regenerates exactly the records the crashed run would have
produced, and every configuration processes the same bytes.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass

from benchmarks.streaming.params import LoadParams

_MASK64 = (1 << 64) - 1
_BLOB_POOL = 64
DRAINED_LANE = "z"
"""Lane the reference flow drops with ``Drain``; never reaches a sink."""


def _mix(value: int) -> int:
    """Return the SplitMix64 finaliser of *value* (a cheap, stable hash)."""
    value = (value + 0x9E3779B97F4A7C15) & _MASK64
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & _MASK64
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & _MASK64
    return value ^ (value >> 31)


@dataclass(frozen=True, slots=True)
class Event:
    """Engine-neutral description of one generated event."""

    partition: int
    offset: int
    key: str
    lane: str
    amount: int
    blob: bytes


class LoadGenerator:
    """Generate the events of one load shape, partition by partition."""

    def __init__(self, load: LoadParams) -> None:
        self._load = load
        rng = random.Random(load.seed)
        self._blobs = tuple(rng.randbytes(load.payload_bytes) for _ in range(_BLOB_POOL))

    @property
    def params(self) -> LoadParams:
        """Load shape this generator was built for."""
        return self._load

    def partition_names(self) -> list[str]:
        """Return stable partition names."""
        return [f"p{index}" for index in range(self._load.partitions)]

    def partition_size(self, partition: int) -> int:
        """Return how many events *partition* holds."""
        base, extra = divmod(self._load.messages, self._load.partitions)
        return base + (1 if partition < extra else 0)

    def event(self, partition: int, offset: int) -> Event:
        """Return the event at *offset* of *partition*."""
        h = _mix((self._load.seed << 40) ^ (partition << 32) ^ offset)
        bucket = h % 100
        lane = "a" if bucket < 45 else "b" if bucket < 90 else DRAINED_LANE
        return Event(
            partition=partition,
            offset=offset,
            key=f"k{(h >> 8) % self._load.keys}",
            lane=lane,
            amount=int((h >> 24) % 10_000),
            blob=self._blobs[(h >> 40) % _BLOB_POOL],
        )

    def expected_outputs(self) -> int:
        """Return how many events reach a sink (every lane but the drained one)."""
        return sum(
            1
            for partition in range(self._load.partitions)
            for offset in range(self.partition_size(partition))
            if self.event(partition, offset).lane != DRAINED_LANE
        )


class Pacer:
    """Decide how many records one partition may emit now.

    With ``rate == 0`` a partition emits ``source_batch`` records per pull.
    Otherwise it follows a fixed schedule of ``rate / partitions`` records per
    second from its first pull.
    """

    def __init__(self, load: LoadParams) -> None:
        self._batch = load.source_batch
        self._per_second = load.rate / load.partitions if load.rate > 0 else 0.0
        self._start_ns = 0

    def due(self, emitted: int, now_ns: int) -> int:
        """Return how many records may be emitted at *now_ns*."""
        if self._per_second == 0.0:
            return self._batch
        if self._start_ns == 0:
            self._start_ns = now_ns
        scheduled = int((now_ns - self._start_ns) * self._per_second / 1e9) + 1
        return max(0, min(self._batch, scheduled - emitted))

    def next_awake_ns(self, emitted: int) -> int | None:
        """Return when the next record is due, or ``None`` to pull right away."""
        if self._per_second == 0.0 or self._start_ns == 0:
            return None
        return self._start_ns + int(emitted * 1e9 / self._per_second)


def now_ns() -> int:
    """Return the wall clock used for every cross-process timestamp."""
    return time.time_ns()
