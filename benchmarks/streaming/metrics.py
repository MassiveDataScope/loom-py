"""Measurement primitives: latency histogram, sink recorder, resource usage, statistics.

Everything here is engine-neutral. An engine adapter calls :meth:`Recorder.mark_emit`
from its source and :meth:`Recorder.record_batch` from its sink; the recorder
owns counting, latency, crash simulation and the recovery target.
"""

from __future__ import annotations

import json
import math
import os
import resource
import statistics
import sys
import threading
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from benchmarks.streaming.load import now_ns
from benchmarks.streaming.params import Mode, RunSpec

CRASH_EXIT_CODE = 86
"""Exit code of a child that simulated a crash on purpose."""

_SUB_BUCKETS = 64
"""Histogram buckets per power of two: about 1.1 % relative resolution."""


class LatencyHistogram:
    """Log-bucketed histogram of nanosecond latencies; mergeable across workers.

    A fixed-size histogram instead of a list keeps the memory the benchmark
    itself uses independent of the number of messages, so it does not pollute
    the RSS being measured.
    """

    def __init__(self, buckets: Mapping[int, int] | None = None) -> None:
        self._buckets: dict[int, int] = dict(buckets or {})

    def add(self, value_ns: int) -> None:
        """Count one latency sample."""
        index = int(math.log2(value_ns) * _SUB_BUCKETS) if value_ns > 1 else 0
        self._buckets[index] = self._buckets.get(index, 0) + 1

    def merge(self, other: LatencyHistogram) -> None:
        """Add every sample of *other* to this histogram."""
        for index, count in other._buckets.items():
            self._buckets[index] = self._buckets.get(index, 0) + count

    @property
    def count(self) -> int:
        """Number of samples."""
        return sum(self._buckets.values())

    def percentile(self, fraction: float) -> float:
        """Return the latency in nanoseconds at *fraction* (``0.99`` for p99)."""
        total = self.count
        if total == 0:
            return float("nan")
        rank = max(1, math.ceil(fraction * total))
        seen = 0
        for index in sorted(self._buckets):
            seen += self._buckets[index]
            if seen >= rank:
                return float(2 ** ((index + 0.5) / _SUB_BUCKETS))
        raise AssertionError("rank beyond histogram")

    def to_json(self) -> dict[str, int]:
        """Return a JSON-serialisable mapping."""
        return {str(index): count for index, count in self._buckets.items()}

    @classmethod
    def from_json(cls, data: Mapping[str, int]) -> LatencyHistogram:
        """Rebuild a histogram written by :meth:`to_json`."""
        return cls({int(index): int(count) for index, count in data.items()})


@dataclass(frozen=True, slots=True)
class SinkItem:
    """What a sink reports for one output: where it came from and when it was made."""

    partition: int
    offset: int
    emitted_ns: int


class Recorder:
    """Process-wide sink observer shared by every worker thread.

    Args:
        spec: Spec of the run, for crash and resume behaviour.
    """

    def __init__(self, spec: RunSpec) -> None:
        self._spec = spec
        self._lock = threading.Lock()
        self._histogram = LatencyHistogram()
        self._count = 0
        self._first_emit_ns = 0
        self._first_output_ns = 0
        self._last_output_ns = 0
        self._reached_ns = 0
        self._replayed = 0
        self._max_offsets: dict[str, int] = {}
        self._pending_target = dict(spec.resume_target)
        self._cpu_at_first_emit = 0.0
        self._rss_at_first_emit = 0

    def mark_emit(self, at_ns: int) -> None:
        """Record a source emission time; the earliest one starts the clocks.

        CPU time is counted from here too, so imports, compilation and engine
        startup stay out of the per-message cost; the peak RSS reached so far
        is kept to tell the interpreter's footprint from the flow's.
        """
        with self._lock:
            if self._first_emit_ns == 0:
                self._cpu_at_first_emit = cpu_seconds()
                self._rss_at_first_emit = peak_rss_bytes()
            if self._first_emit_ns == 0 or at_ns < self._first_emit_ns:
                self._first_emit_ns = at_ns

    def record_batch(self, items: Iterable[SinkItem]) -> None:
        """Record outputs written by one sink ``write_batch`` call."""
        arrived = now_ns()
        with self._lock:
            for item in items:
                self._record(item, arrived)
            if self._first_output_ns == 0:
                self._first_output_ns = arrived
            self._last_output_ns = arrived
            if self._spec.mode is Mode.CRASH and self._count >= self._spec.crash_after:
                self._crash()

    def _record(self, item: SinkItem, arrived: int) -> None:
        self._count += 1
        self._histogram.add(max(1, arrived - item.emitted_ns))
        name = f"p{item.partition}"
        if item.offset > self._max_offsets.get(name, -1):
            self._max_offsets[name] = item.offset
        target = self._spec.resume_target.get(name)
        if target is None:
            return
        if item.offset <= target:
            self._replayed += 1
        if name in self._pending_target and item.offset >= target:
            del self._pending_target[name]
            if not self._pending_target:
                self._reached_ns = arrived

    def _crash(self) -> None:
        """Write progress and die without any cleanup, like a killed process."""
        self.write(self._spec.result_path)
        sys.stdout.flush()
        os._exit(CRASH_EXIT_CODE)

    def result(self) -> dict[str, Any]:
        """Return this process's raw measurements."""
        return {
            "count": self._count,
            "first_emit_ns": self._first_emit_ns,
            "first_output_ns": self._first_output_ns,
            "last_output_ns": self._last_output_ns,
            "reached_ns": self._reached_ns,
            "replayed": self._replayed,
            "max_offsets": dict(self._max_offsets),
            "histogram": self._histogram.to_json(),
            "cpu_s": cpu_seconds() - self._cpu_at_first_emit if self._first_emit_ns else 0.0,
            "peak_rss_bytes": peak_rss_bytes(),
            "startup_rss_bytes": self._rss_at_first_emit,
            "loadavg": list(os.getloadavg()),
        }

    def write(self, path: str) -> None:
        """Write :meth:`result` as JSON to *path*."""
        Path(path).write_text(json.dumps(self.result()), encoding="utf-8")


def cpu_seconds() -> float:
    """Return user plus system CPU seconds used by this process so far."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime + usage.ru_stime


def peak_rss_bytes() -> int:
    """Return the peak resident set size of this process in bytes.

    ``ru_maxrss`` is in bytes on macOS and in KiB on Linux.
    """
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(peak if sys.platform == "darwin" else peak * 1024)


# t-distribution 97.5 % quantiles for small samples; the normal value beyond.
_T975 = {
    1: 12.706,
    2: 4.303,
    3: 3.182,
    4: 2.776,
    5: 2.571,
    6: 2.447,
    7: 2.365,
    8: 2.306,
    9: 2.262,
    10: 2.228,
    12: 2.179,
    15: 2.131,
    20: 2.086,
    30: 2.042,
}


def t975(dof: float) -> float:
    """Return the two-sided 95 % Student-t quantile for *dof* degrees of freedom."""
    if dof < 1:
        return _T975[1]
    for bound in sorted(_T975):
        if dof <= bound:
            return _T975[bound]
    return 1.96


@dataclass(frozen=True, slots=True)
class Summary:
    """Location and spread of one metric across repetitions."""

    n: int
    mean: float
    median: float
    stdev: float

    @property
    def cv_pct(self) -> float:
        """Coefficient of variation in percent."""
        return 100.0 * self.stdev / self.mean if self.mean else float("nan")

    def to_json(self) -> dict[str, float]:
        """Return a JSON-serialisable mapping."""
        return {
            "n": self.n,
            "mean": self.mean,
            "median": self.median,
            "stdev": self.stdev,
            "cv_pct": self.cv_pct,
        }


def summarize(values: Sequence[float]) -> Summary:
    """Summarise *values* (at least one)."""
    clean = [v for v in values if not math.isnan(v)]
    if not clean:
        return Summary(n=0, mean=float("nan"), median=float("nan"), stdev=float("nan"))
    stdev = statistics.stdev(clean) if len(clean) > 1 else 0.0
    return Summary(
        n=len(clean), mean=statistics.fmean(clean), median=statistics.median(clean), stdev=stdev
    )


def relative_difference(candidate: Summary, baseline: Summary) -> tuple[float, float, float]:
    """Return ``(diff %, CI low %, CI high %)`` of *candidate* against *baseline*.

    The interval is a Welch 95 % interval of the ratio of means, propagated with
    the delta method.
    """
    if candidate.n == 0 or baseline.n == 0 or baseline.mean == 0:
        return float("nan"), float("nan"), float("nan")
    ratio = candidate.mean / baseline.mean
    var_c = candidate.stdev**2 / candidate.n
    var_b = baseline.stdev**2 / baseline.n
    se = math.sqrt(var_c / baseline.mean**2 + (candidate.mean**2) * var_b / baseline.mean**4)
    dof = _welch_dof(var_c, candidate.n, var_b, baseline.n)
    half = t975(dof) * se
    return 100.0 * (ratio - 1), 100.0 * (ratio - half - 1), 100.0 * (ratio + half - 1)


def _welch_dof(var_a: float, n_a: int, var_b: float, n_b: int) -> float:
    numerator = (var_a + var_b) ** 2
    denominator = 0.0
    if n_a > 1:
        denominator += var_a**2 / (n_a - 1)
    if n_b > 1:
        denominator += var_b**2 / (n_b - 1)
    return numerator / denominator if denominator else float(n_a + n_b - 2)
