"""Parameters of one benchmark run, shared by the runner and the child process."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from enum import StrEnum
from typing import Any


class Mode(StrEnum):
    """What a child process does with the flow."""

    RUN = "run"
    """Run the flow to completion."""
    CRASH = "crash"
    """Run with recovery and exit abruptly once ``crash_after`` outputs arrived."""
    RESUME = "resume"
    """Resume from the recovery store left by a ``CRASH`` run."""


@dataclass(frozen=True, slots=True)
class LoadParams:
    """Shape of the deterministic in-memory load.

    Args:
        messages: Total messages generated across all partitions.
        partitions: Source partitions (``FixedPartitionedSource.list_parts``).
        keys: Distinct business keys carried in the Kafka record key.
        payload_bytes: Size of the opaque blob carried by every payload.
        rate: Target messages per second across all partitions; ``0`` means
            as fast as the engine pulls.
        source_batch: Maximum records a partition returns per ``next_batch``.
        seed: Seed of the deterministic generator.
    """

    messages: int = 200_000
    partitions: int = 4
    keys: int = 1_000
    payload_bytes: int = 256
    rate: int = 0
    source_batch: int = 256
    seed: int = 15


@dataclass(frozen=True, slots=True)
class FlowParams:
    """Knobs of the reference flow and of the engine runtime.

    Args:
        batch_max: ``CollectBatch.max_records``.
        batch_timeout_ms: ``CollectBatch.timeout_ms``.
        epoch_interval_ms: Engine epoch (snapshot) interval.
        workers: Worker threads per process.
        processes: Cluster processes (``-i``/``-a`` style).
    """

    batch_max: int = 64
    batch_timeout_ms: int = 50
    epoch_interval_ms: int = 100
    workers: int = 1
    processes: int = 1


@dataclass(frozen=True, slots=True)
class RunSpec:
    """Everything a child process needs to run one measurement.

    Args:
        engine: Engine name registered in :mod:`benchmarks.streaming.engines`.
        load: Load shape.
        flow: Flow and runtime knobs.
        mode: Run, crash or resume.
        result_path: File the child writes its JSON result to.
        spawn_ns: Wall-clock ``time.time_ns()`` taken by the parent just before
            spawning the child; the origin of startup and recovery times.
        process_id: Index of this process in a multi-process cluster.
        addresses: Cluster addresses, one per process; empty for one process.
        recovery_dir: Recovery store directory; empty disables recovery.
        crash_after: Outputs after which a ``CRASH`` run exits abruptly.
        resume_target: Highest source offset per partition seen at the sink
            before the crash; a ``RESUME`` run reports when it reaches it again.
    """

    engine: str
    load: LoadParams
    flow: FlowParams
    mode: Mode = Mode.RUN
    result_path: str = ""
    spawn_ns: int = 0
    process_id: int = 0
    addresses: tuple[str, ...] = ()
    recovery_dir: str = ""
    crash_after: int = 0
    resume_target: dict[str, int] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        """Return a JSON-serialisable mapping."""
        return asdict(self)

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> RunSpec:
        """Rebuild a spec written by :meth:`to_json`."""
        return cls(
            engine=str(data["engine"]),
            load=LoadParams(**data["load"]),
            flow=FlowParams(**data["flow"]),
            mode=Mode(data["mode"]),
            result_path=str(data["result_path"]),
            spawn_ns=int(data["spawn_ns"]),
            process_id=int(data["process_id"]),
            addresses=tuple(data["addresses"]),
            recovery_dir=str(data["recovery_dir"]),
            crash_after=int(data["crash_after"]),
            resume_target={str(k): int(v) for k, v in data["resume_target"].items()},
        )

    def with_changes(self, **changes: Any) -> RunSpec:
        """Return a copy with *changes* applied."""
        return replace(self, **changes)
