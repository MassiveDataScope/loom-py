"""Run the reference streaming benchmark against one or more environments.

Each ``--config NAME=PYTHON`` names a virtual environment by its interpreter.
Repetitions are interleaved across configurations (A, B, C, A, B, C, ...) so
drift in the load of a shared machine spreads evenly over all of them; the first
``--warmup`` repetitions are recorded but left out of the summaries. Every
measurement runs in a fresh child process. One JSON file per configuration is
written to ``--out`` as ``<date>-<config>.json``.

Example::

    python -m benchmarks.streaming.run \\
        --config A=/venvs/a/bin/python --config C=/venvs/c/bin/python \\
        --repetitions 7 --warmup 1
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any

from benchmarks.streaming.load import LoadGenerator, now_ns
from benchmarks.streaming.metrics import (
    CRASH_EXIT_CODE,
    LatencyHistogram,
    summarize,
)
from benchmarks.streaming.params import FlowParams, LoadParams, Mode, RunSpec

REPO_ROOT = Path(__file__).resolve().parents[2]
_CHILD = "benchmarks.streaming.child"
_RUN_TIMEOUT_S = 900


@dataclass(frozen=True, slots=True)
class Scenario:
    """One measured situation.

    Args:
        name: Stable name used in the result files.
        kind: ``throughput`` (as fast as possible), ``latency`` (fixed offered
            rate) or ``recovery`` (crash half-way, then resume).
        workers: Worker threads per process.
        processes: Cluster processes.
    """

    name: str
    kind: str
    workers: int = 1
    processes: int = 1


SCENARIOS: dict[str, Scenario] = {
    s.name: s
    for s in (
        Scenario("throughput-w1", "throughput"),
        Scenario("throughput-w4", "throughput", workers=4),
        Scenario("throughput-p2", "throughput", processes=2),
        Scenario("latency-w1", "latency"),
        Scenario("recovery-w1", "recovery"),
    )
}


@dataclass(frozen=True, slots=True)
class Config:
    """One environment under test."""

    name: str
    python: str


class ChildFailed(RuntimeError):
    """A child process exited with an unexpected status."""


def free_port() -> int:
    """Return a TCP port that is free right now on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def probe(config: Config, engine: str) -> dict[str, Any]:
    """Describe the environment of *config* by asking its interpreter."""
    completed = subprocess.run(
        [config.python, "-m", _CHILD, "--probe", engine],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    data: dict[str, Any] = json.loads(completed.stdout.strip().splitlines()[-1])
    return data


def _spawn(config: Config, spec: RunSpec, workdir: Path) -> subprocess.Popen[bytes]:
    spec_path = workdir / f"spec-{spec.process_id}.json"
    spec_path.write_text(json.dumps(spec.to_json()), encoding="utf-8")
    return subprocess.Popen(
        [config.python, "-m", _CHILD, "--spec", str(spec_path)],
        cwd=REPO_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )


def run_cluster(
    config: Config, spec: RunSpec, workdir: Path, expected_exit: int = 0
) -> list[dict[str, Any]]:
    """Run *spec* on ``spec.flow.processes`` child processes and return their results."""
    workdir.mkdir(parents=True, exist_ok=True)
    processes = spec.flow.processes
    addresses = tuple(f"127.0.0.1:{free_port()}" for _ in range(processes)) if processes > 1 else ()
    spawn_ns = now_ns()
    specs = [
        spec.with_changes(
            process_id=index,
            addresses=addresses,
            spawn_ns=spawn_ns,
            result_path=str(workdir / f"result-{index}.json"),
        )
        for index in range(processes)
    ]
    children = [_spawn(config, child_spec, workdir) for child_spec in specs]
    for child in children:
        _, stderr = child.communicate(timeout=_RUN_TIMEOUT_S)
        if child.returncode != expected_exit:
            tail = stderr.decode("utf-8", errors="replace")[-2_000:]
            raise ChildFailed(f"{config.name}: exit {child.returncode}\n{tail}")
    results = []
    for child_spec in specs:
        path = Path(child_spec.result_path)
        data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
        data["spawn_ns"] = spawn_ns
        results.append(data)
    return results


def _merge(results: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Merge the per-process results of one cluster run."""
    histogram = LatencyHistogram()
    for result in results:
        histogram.merge(LatencyHistogram.from_json(result["histogram"]))
    emits = [r["first_emit_ns"] for r in results if r["first_emit_ns"]]
    firsts = [r["first_output_ns"] for r in results if r["first_output_ns"]]
    return {
        "count": sum(r["count"] for r in results),
        "first_emit_ns": min(emits) if emits else 0,
        "first_output_ns": min(firsts) if firsts else 0,
        "last_output_ns": max(r["last_output_ns"] for r in results),
        "spawn_ns": results[0]["spawn_ns"],
        "cpu_s": sum(r["cpu_s"] for r in results),
        "peak_rss_mb": max(r["peak_rss_bytes"] for r in results) / 2**20,
        "startup_rss_mb": max(r["startup_rss_bytes"] for r in results) / 2**20,
        "histogram": histogram,
        "loadavg_1m": max(r["loadavg"][0] for r in results),
    }


def _flow_metrics(merged: dict[str, Any], messages: int) -> dict[str, float]:
    elapsed_s = (merged["last_output_ns"] - merged["first_emit_ns"]) / 1e9
    histogram: LatencyHistogram = merged["histogram"]
    return {
        "msgs_per_s": messages / elapsed_s,
        "msgs_per_cpu_s": messages / merged["cpu_s"] if merged["cpu_s"] else float("nan"),
        "p50_ms": histogram.percentile(0.50) / 1e6,
        "p99_ms": histogram.percentile(0.99) / 1e6,
        "peak_rss_mb": merged["peak_rss_mb"],
        "startup_rss_mb": merged["startup_rss_mb"],
        "startup_s": (merged["first_output_ns"] - merged["spawn_ns"]) / 1e9,
    }


def measure_flow(config: Config, spec: RunSpec, expected: int) -> dict[str, Any]:
    """Run one throughput or latency sample."""
    with tempfile.TemporaryDirectory(prefix="loom-bench-") as tmp:
        merged = _merge(run_cluster(config, spec, Path(tmp)))
    if merged["count"] != expected:
        raise ChildFailed(f"{config.name}: {merged['count']} outputs, expected {expected}")
    return {**_flow_metrics(merged, spec.load.messages), "loadavg_1m": merged["loadavg_1m"]}


def measure_recovery(config: Config, spec: RunSpec, expected: int) -> dict[str, Any]:
    """Crash a run half-way, resume it from the recovery store and time the catch-up.

    ``recovery_s`` is the time from spawning the resumed process until its sink
    has again reached the highest offset of every partition seen before the
    crash; ``cold_same_point_s`` is the time the fresh run took to reach that
    point from its own spawn, so the two compare like for like.
    """
    with tempfile.TemporaryDirectory(prefix="loom-bench-") as tmp:
        workdir = Path(tmp)
        recovery_dir = workdir / "recovery"
        crash_spec = spec.with_changes(
            mode=Mode.CRASH, recovery_dir=str(recovery_dir), crash_after=expected // 2
        )
        crashed = run_cluster(config, crash_spec, workdir / "crash", CRASH_EXIT_CODE)[0]
        target = crashed["max_offsets"]
        resume_spec = spec.with_changes(
            mode=Mode.RESUME, recovery_dir=str(recovery_dir), resume_target=target
        )
        resumed = run_cluster(config, resume_spec, workdir / "resume")[0]
        shutil.rmtree(recovery_dir, ignore_errors=True)
    if not resumed["reached_ns"]:
        raise ChildFailed(f"{config.name}: resumed run never reached the crash point")
    return {
        "recovery_s": (resumed["reached_ns"] - resumed["spawn_ns"]) / 1e9,
        "resume_first_output_s": (resumed["first_output_ns"] - resumed["spawn_ns"]) / 1e9,
        "cold_same_point_s": (crashed["last_output_ns"] - crashed["spawn_ns"]) / 1e9,
        "replayed": float(resumed["replayed"]),
        "crash_outputs": float(crashed["count"]),
        "peak_rss_mb": resumed["peak_rss_bytes"] / 2**20,
        "loadavg_1m": resumed["loadavg"][0],
    }


def scenario_spec(base: RunSpec, scenario: Scenario, latency_rate: int) -> RunSpec:
    """Return the spec of *scenario* derived from the *base* parameters."""
    flow = replace(base.flow, workers=scenario.workers, processes=scenario.processes)
    load = replace(base.load, rate=latency_rate) if scenario.kind == "latency" else base.load
    return base.with_changes(flow=flow, load=load)


def measure(config: Config, scenario: Scenario, spec: RunSpec, expected: int) -> dict[str, Any]:
    """Take one sample of *scenario* in *config*."""
    if scenario.kind == "recovery":
        return measure_recovery(config, spec, expected)
    return measure_flow(config, spec, expected)


def git_state() -> dict[str, Any]:
    """Return the commit the benchmark code was run from."""

    def git(*args: str) -> str:
        return subprocess.run(
            ["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, check=False
        ).stdout.strip()

    return {"commit": git("rev-parse", "HEAD"), "dirty": bool(git("status", "--porcelain"))}


def config_label(config: Config, environment: dict[str, Any]) -> str:
    """Return the file-name label of *config*: name, Python and engine version."""
    python = ".".join(str(environment["python_version"]).split(".")[:2])
    return (
        f"{config.name}-py{python}-{environment['engine_distribution']}"
        f"-{environment['engine_version']}"
    )


def parse_config(value: str) -> Config:
    """Parse ``NAME=PYTHON``."""
    name, sep, python = value.partition("=")
    if not sep or not name or not python:
        raise argparse.ArgumentTypeError("expected NAME=/path/to/python")
    return Config(name=name, python=python)


def build_parser() -> argparse.ArgumentParser:
    """Return the command-line parser."""
    defaults_load, defaults_flow = LoadParams(), FlowParams()
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--config", type=parse_config, action="append", required=True)
    parser.add_argument("--engine", default="loom-bytewax")
    parser.add_argument("--scenarios", default=",".join(SCENARIOS))
    parser.add_argument("--repetitions", type=int, default=7, help="measured repetitions")
    parser.add_argument("--warmup", type=int, default=1, help="extra leading repetitions")
    parser.add_argument("--messages", type=int, default=defaults_load.messages)
    parser.add_argument("--partitions", type=int, default=defaults_load.partitions)
    parser.add_argument("--keys", type=int, default=defaults_load.keys)
    parser.add_argument("--payload-bytes", type=int, default=defaults_load.payload_bytes)
    parser.add_argument("--source-batch", type=int, default=defaults_load.source_batch)
    parser.add_argument("--seed", type=int, default=defaults_load.seed)
    parser.add_argument("--latency-rate", type=int, default=20_000, help="msgs/s offered")
    parser.add_argument("--batch-max", type=int, default=defaults_flow.batch_max)
    parser.add_argument("--batch-timeout-ms", type=int, default=defaults_flow.batch_timeout_ms)
    parser.add_argument("--epoch-interval-ms", type=int, default=defaults_flow.epoch_interval_ms)
    parser.add_argument("--out", type=Path, default=REPO_ROOT / "benchmarks" / "results")
    parser.add_argument("--note", default="", help="free text stored with the results")
    return parser


def _base_spec(args: argparse.Namespace) -> RunSpec:
    return RunSpec(
        engine=args.engine,
        load=LoadParams(
            messages=args.messages,
            partitions=args.partitions,
            keys=args.keys,
            payload_bytes=args.payload_bytes,
            source_batch=args.source_batch,
            seed=args.seed,
        ),
        flow=FlowParams(
            batch_max=args.batch_max,
            batch_timeout_ms=args.batch_timeout_ms,
            epoch_interval_ms=args.epoch_interval_ms,
        ),
    )


def _log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def run_all(
    args: argparse.Namespace, log: Callable[[str], None] = _log
) -> dict[str, dict[str, Any]]:
    """Run every repetition and return one result document per configuration."""
    base = _base_spec(args)
    scenarios = [SCENARIOS[name] for name in args.scenarios.split(",")]
    expected = LoadGenerator(base.load).expected_outputs()
    configs: list[Config] = args.config
    documents: dict[str, dict[str, Any]] = {}
    for config in configs:
        environment = probe(config, args.engine)
        documents[config.name] = {
            "schema": 1,
            "config": config.name,
            "label": config_label(config, environment),
            "created": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
            "environment": environment,
            "code": git_state(),
            "note": args.note,
            "params": {
                "load": asdict(base.load),
                "flow": asdict(base.flow),
                "latency_rate": args.latency_rate,
                "repetitions": args.repetitions,
                "warmup": args.warmup,
                "expected_outputs": expected,
            },
            "host_loadavg_start": list(os.getloadavg()),
            "scenarios": {s.name: {"scenario": asdict(s), "samples": []} for s in scenarios},
        }
        log(f"{config.name}: {documents[config.name]['label']}")
    total = args.warmup + args.repetitions
    for repetition in range(total):
        warmup = repetition < args.warmup
        for scenario in scenarios:
            spec = scenario_spec(base, scenario, args.latency_rate)
            for config in configs:
                sample = measure(config, scenario, spec, expected)
                sample["repetition"] = repetition
                sample["warmup"] = warmup
                documents[config.name]["scenarios"][scenario.name]["samples"].append(sample)
                log(f"[{repetition + 1}/{total}] {scenario.name} {config.name}: {_brief(sample)}")
    for document in documents.values():
        document["host_loadavg_end"] = list(os.getloadavg())
        for block in document["scenarios"].values():
            block["summary"] = summarize_samples(block["samples"])
    return documents


def _brief(sample: dict[str, Any]) -> str:
    keys = ("msgs_per_s", "p99_ms", "recovery_s", "peak_rss_mb", "loadavg_1m")
    return " ".join(f"{k}={sample[k]:.4g}" for k in keys if k in sample)


def summarize_samples(samples: Sequence[dict[str, Any]]) -> dict[str, dict[str, float]]:
    """Summarise every numeric metric over the non-warm-up samples."""
    measured = [s for s in samples if not s["warmup"]]
    metrics = [k for k, v in measured[0].items() if isinstance(v, float)] if measured else []
    return {metric: summarize([s[metric] for s in measured]).to_json() for metric in metrics}


def write_documents(documents: dict[str, dict[str, Any]], out: Path) -> list[Path]:
    """Write one ``<date>-<label>.json`` per configuration."""
    out.mkdir(parents=True, exist_ok=True)
    date = dt.date.today().isoformat()
    paths = []
    for document in documents.values():
        path = out / f"{date}-{document['label']}.json"
        path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        paths.append(path)
    return paths


def main(argv: list[str] | None = None) -> int:
    """Entry point."""
    args = build_parser().parse_args(argv)
    unknown = set(args.scenarios.split(",")) - SCENARIOS.keys()
    if unknown:
        _log(f"unknown scenarios: {', '.join(sorted(unknown))}")
        return 2
    for path in write_documents(run_all(args), args.out):
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
