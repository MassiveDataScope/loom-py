"""Compare benchmark result files against a baseline.

Prints, per scenario and metric, the median and standard deviation of every
file, the difference of the means against the baseline with its 95 % interval,
and a verdict against the regression threshold (SC-003: 5 %)::

    python -m benchmarks.streaming.compare BASELINE.json OTHER.json [...]

Verdicts, with *worsening* measured in the metric's bad direction:

* ``ok``: the whole interval worsens less than the threshold;
* ``worse``: the whole interval worsens more than the threshold;
* ``?``: the interval straddles the threshold; more repetitions or a quieter
  machine are needed before calling it.
"""

from __future__ import annotations

import argparse
import json
import math
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from benchmarks.streaming.metrics import Summary, relative_difference

HIGHER_IS_BETTER = frozenset({"msgs_per_s", "msgs_per_cpu_s"})
"""Metrics where a lower value is the regression; every other one is lower-is-better."""

REPORTED = (
    "msgs_per_s",
    "msgs_per_cpu_s",
    "p50_ms",
    "p99_ms",
    "peak_rss_mb",
    "startup_rss_mb",
    "startup_s",
    "recovery_s",
    "resume_first_output_s",
    "replayed",
)
"""Metrics printed, in this order, when a scenario has them."""


def load(path: Path) -> dict[str, Any]:
    """Read one result document."""
    data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return data


def _summary(document: dict[str, Any], scenario: str, metric: str) -> Summary | None:
    block = document["scenarios"].get(scenario, {}).get("summary", {}).get(metric)
    if block is None:
        return None
    return Summary(
        n=int(block["n"]),
        mean=float(block["mean"]),
        median=float(block["median"]),
        stdev=float(block["stdev"]),
    )


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


def rows(
    baseline: dict[str, Any], others: Sequence[dict[str, Any]], threshold: float
) -> list[list[str]]:
    """Return the table rows comparing *others* with *baseline*."""
    table: list[list[str]] = []
    for scenario in baseline["scenarios"]:
        for metric in REPORTED:
            base = _summary(baseline, scenario, metric)
            if base is None:
                continue
            row = [scenario, metric, _cell(base)]
            for other in others:
                candidate = _summary(other, scenario, metric)
                if candidate is None:
                    row.append("-")
                    continue
                diff, low, high = relative_difference(candidate, base)
                row.append(
                    f"{_cell(candidate)} | {diff:+.1f}% [{low:+.1f}, {high:+.1f}] "
                    f"{verdict(metric, low, high, threshold)}"
                )
            table.append(row)
    return table


def _cell(summary: Summary) -> str:
    return f"{summary.median:.4g} ± {summary.stdev:.2g} (cv {summary.cv_pct:.1f}%)"


def render_markdown(headers: Sequence[str], table: Sequence[Sequence[str]]) -> str:
    """Render *table* as a Markdown table."""
    lines = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    lines += ["| " + " | ".join(cell.replace("|", "·") for cell in row) + " |" for row in table]
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
    parser.add_argument("--markdown", action="store_true")
    args = parser.parse_args(argv)
    baseline = load(args.baseline)
    others = [load(path) for path in args.others]
    headers = [
        "scenario",
        "metric",
        f"{baseline['config']} median ± sd",
        *(f"{o['config']} median ± sd | Δ vs {baseline['config']} [95% CI]" for o in others),
    ]
    table = rows(baseline, others, args.threshold)
    render = render_markdown if args.markdown else render_text
    print(render(headers, table))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
