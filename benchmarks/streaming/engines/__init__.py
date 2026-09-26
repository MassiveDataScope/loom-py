"""Engines the reference benchmark can drive.

An engine is a module exposing ``ENGINE``, an object satisfying :class:`Engine`.
It builds the reference flow with its own API, feeds it from
:class:`~benchmarks.streaming.load.LoadGenerator` and reports every output to
the :class:`~benchmarks.streaming.metrics.Recorder`. Register it in
:data:`ENGINES`; see ``benchmarks/streaming/README.md``.
"""

from __future__ import annotations

import importlib
from typing import Protocol, cast

from benchmarks.streaming.load import LoadGenerator
from benchmarks.streaming.metrics import Recorder
from benchmarks.streaming.params import RunSpec

ENGINES: dict[str, str] = {
    "loom-bytewax": "benchmarks.streaming.engines.loom_bytewax",
}
"""Engine name to the module that defines its ``ENGINE``."""


class Engine(Protocol):
    """One streaming engine behind the reference flow."""

    def describe(self) -> dict[str, str]:
        """Return the engine's distribution name and version, and anything else relevant."""
        ...

    def run(self, spec: RunSpec, generator: LoadGenerator, recorder: Recorder) -> None:
        """Run the reference flow for *spec* until the load is exhausted.

        Args:
            spec: Parameters of this run, including recovery and cluster layout.
            generator: Deterministic source of the input events.
            recorder: Observer every sink output must be reported to.
        """
        ...


def load_engine(name: str) -> Engine:
    """Import and return the engine registered as *name*.

    The import is deferred on purpose: each engine needs its own optional
    dependencies, and only the child process that measures it has them.

    Raises:
        KeyError: If *name* is not registered.
    """
    module = importlib.import_module(ENGINES[name])
    return cast(Engine, module.ENGINE)
