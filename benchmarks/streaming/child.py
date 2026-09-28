"""Child process: run one measurement, or describe the environment.

Invoked by :mod:`benchmarks.streaming.run` with the interpreter of the virtual
environment under test::

    python -m benchmarks.streaming.child --probe loom-bytewax
    python -m benchmarks.streaming.child --spec spec.json
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
from pathlib import Path
from typing import Any

from benchmarks.streaming import gc_policy
from benchmarks.streaming.engines import load_engine
from benchmarks.streaming.load import LoadGenerator
from benchmarks.streaming.metrics import Recorder
from benchmarks.streaming.params import RunSpec


def cpu_model() -> str:
    """Return a human-readable CPU model name."""
    if sys.platform == "darwin":
        completed = subprocess.run(
            ["sysctl", "-n", "machdep.cpu.brand_string"],
            capture_output=True,
            text=True,
            check=False,
        )
        return completed.stdout.strip() or platform.processor()
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text(encoding="utf-8").splitlines():
            if line.lower().startswith(("model name", "cpu model")):
                return line.split(":", 1)[1].strip()
    return platform.processor() or platform.machine()


def probe(engine_name: str) -> dict[str, Any]:
    """Describe the interpreter, the platform and the engine of this environment."""
    return {
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "python_executable": Path(sys.executable).name,
        "gil_enabled": getattr(sys, "_is_gil_enabled", lambda: True)(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_model": cpu_model(),
        "cpu_count": os.cpu_count(),
        **gc_policy.describe(),
        **load_engine(engine_name).describe(),
    }


def run(spec: RunSpec) -> None:
    """Run one measurement and write the recorder's result to ``spec.result_path``."""
    recorder = Recorder(spec)
    load_engine(spec.engine).run(spec, LoadGenerator(spec.load), recorder)
    recorder.write(spec.result_path)


def main(argv: list[str] | None = None) -> int:
    """Entry point."""
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--probe", metavar="ENGINE", help="print the environment as JSON")
    group.add_argument("--spec", type=Path, help="run the RunSpec stored in this JSON file")
    args = parser.parse_args(argv)
    if args.probe is not None:
        print(json.dumps(probe(args.probe)))
        return 0
    run(RunSpec.from_json(json.loads(args.spec.read_text(encoding="utf-8"))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
