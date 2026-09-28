"""Compare benchmark result files against a baseline.

Prints, per scenario and metric, the median and standard deviation of every
file, the relative difference against the baseline with its 95 % interval, and
a verdict against the regression threshold::

    python -m benchmarks.streaming.compare BASELINE.json OTHER.json [...]

The default estimator is the ratio of medians with a percentile bootstrap
interval, using a fixed seed. ``--method mean`` uses the ratio of means with
a Welch interval instead.

Verdicts apply only to the gate metrics of each scenario (:data:`GATES`), with
*worsening* measured in the metric's bad direction:

* ``ok``: the whole interval worsens less than the threshold;
* ``worse``: the whole interval worsens more than the threshold;
* ``?``: the interval straddles the threshold. The result is not demonstrated
  either way; more repetitions or a quieter machine are needed.

Every other metric is printed as ``info``.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import statistics
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from benchmarks.streaming.metrics import relative_difference, summarize

HIGHER_IS_BETTER = frozenset({"msgs_per_s", "msgs_per_cpu_s"})
"""Metrics where a lower value is the regression; every other one is lower-is-better."""

REPORTED = (
    "msgs_per_s",
    "msgs_per_cpu_s",
    "instructions_per_msg",
    "p50_ms",
    "p99_ms",
    "peak_rss_mb",
    "startup_rss_mb",
    "flow_rss_mb",
    "startup_s",
    "recovery_s",
    "resume_first_output_s",
    "replayed",
)
"""Metrics printed, in this order, when a scenario has them."""

_THROUGHPUT_GATES = frozenset({"msgs_per_s", "msgs_per_cpu_s", "peak_rss_mb"})
GATES: dict[str, frozenset[str]] = {
    "throughput-w1": _THROUGHPUT_GATES,
    "throughput-w4": _THROUGHPUT_GATES,
    "throughput-p2": _THROUGHPUT_GATES,
    "latency-w1": frozenset({"peak_rss_mb"}),
    "latency-b1-w1": frozenset({"p99_ms", "peak_rss_mb"}),
    "recovery-w1": frozenset({"recovery_s", "peak_rss_mb"}),
}
"""Metrics that decide the gate verdict, per scenario."""

_BOOTSTRAP_RESAMPLES = 4_000
_BOOTSTRAP_SEED = 15

Interval = tuple[float, float, float]


def load(path: Path) -> dict[str, Any]:
    """Read one result document."""
    data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return data


def samples(document: dict[str, Any], scenario: str, metric: str) -> list[float]:
    """Return the measured (non-warm-up) values of *metric* in *scenario*."""
    block = document["scenarios"].get(scenario)
    if block is None:
        return []
    values = []
    for sample in block["samples"]:
        if sample["warmup"]:
            continue
        if metric == "flow_rss_mb" and "startup_rss_mb" in sample:
            values.append(float(sample["peak_rss_mb"] - sample["startup_rss_mb"]))
        elif metric in sample and not math.isnan(float(sample[metric])):
            values.append(float(sample[metric]))
    return values


def median_ratio(candidate: Sequence[float], baseline: Sequence[float]) -> Interval:
    """Return ``(diff %, low %, high %)`` of the ratio of medians, bootstrapped."""
    base_median = statistics.median(baseline)
    if base_median == 0:
        return float("nan"), float("nan"), float("nan")
    rng = random.Random(_BOOTSTRAP_SEED)
    ratios = sorted(
        statistics.median(rng.choices(candidate, k=len(candidate)))
        / statistics.median(rng.choices(baseline, k=len(baseline)))
        for _ in range(_BOOTSTRAP_RESAMPLES)
    )
    low = ratios[int(0.025 * _BOOTSTRAP_RESAMPLES)]
    high = ratios[int(0.975 * _BOOTSTRAP_RESAMPLES) - 1]
    point = statistics.median(candidate) / base_median
    return 100 * (point - 1), 100 * (low - 1), 100 * (high - 1)


def mean_ratio(candidate: Sequence[float], baseline: Sequence[float]) -> Interval:
    """Return ``(diff %, low %, high %)`` of the ratio of means, Welch interval."""
    return relative_difference(summarize(candidate), summarize(baseline))


METHODS: dict[str, Callable[[Sequence[float], Sequence[float]], Interval]] = {
    "median": median_ratio,
    "mean": mean_ratio,
}


def worsening(metric: str, diff_pct: float) -> float:
    """Return how much *diff_pct* worsens *metric*, positive meaning worse."""
    return -diff_pct if metric in HIGHER_IS_BETTER else diff_pct


def verdict(metric: str, low: float, high: float, threshold: float) -> str:
    """Classify an interval of relative differences against *threshold*."""
    if math.isnan(low) or math.isnan(high):
        return "-"
    worst = max(worsening(metric, low), worsening(metric, high))
    best = min(worsening(metric, low), worsening(metric, high))
    if worst <= threshold:
        return "ok"
    if best > threshold:
        return "worse"
    return "?"


def _cell(values: Sequence[float]) -> str:
    summary = summarize(values)
    return f"{summary.median:.4g} ± {summary.stdev:.2g} (cv {summary.cv_pct:.0f}%)"


def rows(
    baseline: dict[str, Any],
    others: Sequence[dict[str, Any]],
    threshold: float,
    method: str,
) -> list[list[str]]:
    """Return the table rows comparing *others* with *baseline*."""
    estimate = METHODS[method]
    table: list[list[str]] = []
    for scenario in baseline["scenarios"]:
        gates = GATES.get(scenario, frozenset())
        for metric in REPORTED:
            base = samples(baseline, scenario, metric)
            if not base:
                continue
            row = [scenario, metric, _cell(base)]
            for other in others:
                candidate = samples(other, scenario, metric)
                if not candidate:
                    row += ["-", "-", "-"]
                    continue
                diff, low, high = estimate(candidate, base)
                judged = verdict(metric, low, high, threshold) if metric in gates else "info"
                row += [_cell(candidate), f"{diff:+.1f}% [{low:+.1f}, {high:+.1f}]", judged]
            table.append(row)
    return table


def render_markdown(headers: Sequence[str], table: Sequence[Sequence[str]]) -> str:
    """Render *table* as a Markdown table."""
    lines = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    lines += ["| " + " | ".join(row) + " |" for row in table]
    return "\n".join(lines)


def render_text(headers: Sequence[str], table: Sequence[Sequence[str]]) -> str:
    """Render *table* as aligned plain text."""
    widths = [max(len(h), *(len(r[i]) for r in table)) for i, h in enumerate(headers)]
    out = ["  ".join(h.ljust(w) for h, w in zip(headers, widths, strict=True))]
    out += ["  ".join(c.ljust(w) for c, w in zip(row, widths, strict=True)) for row in table]
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    """Entry point."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("baseline", type=Path)
    parser.add_argument("others", type=Path, nargs="+")
    parser.add_argument("--threshold", type=float, default=5.0, help="allowed worsening, %%")
    parser.add_argument("--method", choices=sorted(METHODS), default="median")
    parser.add_argument("--markdown", action="store_true")
    args = parser.parse_args(argv)
    baseline = load(args.baseline)
    others = [load(path) for path in args.others]
    name = baseline["config"]
    headers = ["scenario", "metric", f"{name} median ± sd"]
    for other in others:
        headers += [f"{other['config']} median ± sd", f"Δ vs {name} [95% CI]", "verdict"]
    table = rows(baseline, others, args.threshold, args.method)
    render = render_markdown if args.markdown else render_text
    print(render(headers, table))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
