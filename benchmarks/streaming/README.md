# Reference streaming benchmark

A reproducible benchmark of the flow loom runs on its streaming engine
(spec 015, FR-007). It answers one question: does changing the engine build, or
the Python it runs on, make loom's streaming slower or heavier? The threshold is
SC-003: no metric may get more than 5 % worse than bytewax 0.21.1 on Python 3.12.

It is not part of the test suite. `pytest` never collects it, because
`benchmarks/` is outside `testpaths`.

## What it measures

**Reference flow** (`engines/loom_bytewax.py`) is built with loom's streaming
DSL and wired through the same path as production: `compile_flow`, then the
runner's `_prepare_run`. That is a private entry point, the same one
`loom.streaming.testing.StreamingTestRunner` uses. Only the Kafka source and
sinks are replaced:

```
FromTopic(bench.in, BenchEvent)      FixedPartitionedSource / StatefulSourcePartition,
                                     KafkaRecord[bytes] decoded by loom (map + branch)
  -> Normalize        RecordStep     map
  -> CollectBatch(64, 50 ms)         key_on + collect (grouped by topic:partition)
  -> ScoreBatch       BatchExpandStep  map + flat_map back to records
  -> Fork.by(lane)                   branch
       a -> IntoTopic(bench.a)       DynamicSink (recorder)
       b -> IntoTopic(bench.b)       DynamicSink (recorder)
       * -> Drain                    flat_map (10 % of the load)
```

loom's `CollectBatch` groups by `topic:partition` whenever the record carries a
partition, as every Kafka record does. That means the number of `collect` groups
equals `--partitions`, not `--keys`. `--keys` only sets the cardinality of the
record key.

**Load** (`load.py`) is generated in memory and is deterministic: every event is
a pure function of `(seed, partition, offset)`. A resumed run therefore
regenerates exactly the records a crashed run would have produced. Records are
msgpack-encoded when a partition emits them, and the emission time is stamped
into the payload at that moment, so latency does not include the backlog of a
pre-generated queue.

**Scenarios** (`run.py`, `SCENARIOS`):

| scenario | what it runs | main metrics |
|---|---|---|
| `throughput-w1` | 1 worker, as fast as the engine pulls | `msgs_per_s`, `msgs_per_cpu_s`, `peak_rss_mb` |
| `throughput-w4` | 4 worker threads in one process | same; this is where cloning objects across workers under the GIL would show |
| `throughput-p2` | 2 processes × 1 worker (`cli_main` with `process_id`/`addresses`) | same, across a cluster exchange |
| `latency-w1` | 1 worker, fixed offered rate (`--latency-rate`, default 20 000 msg/s) | `p50_ms`, `p99_ms` |
| `recovery-w1` | `RecoveryConfig`; the process dies abruptly (`os._exit`) after half the outputs, then resumes from the recovery store | `recovery_s`, `resume_first_output_s`, `replayed` |

**Metrics:**

- `msgs_per_s`: messages generated, divided by the time from the first source
  emission to the last sink write. Startup is excluded.
- `msgs_per_cpu_s`: messages per CPU-second (user + system, all threads, all
  processes), counted from the first emission. This is the per-core figure. It
  is also the most robust one on a busy machine.
- `p50_ms`, `p99_ms`: end-to-end latency, from source emission to sink write,
  taken from a log-bucketed histogram with about 1 % resolution. In the
  throughput scenarios these numbers mostly measure queueing at saturation, so
  compare latency with `latency-w1`, where every configuration receives the
  same offered load.
- `peak_rss_mb`: `ru_maxrss` of the process, normalised to bytes on macOS and
  Linux. `startup_rss_mb` is the peak reached before the first emission
  (interpreter, imports, compilation); the difference is what the flow adds.
- `startup_s`: time from spawning the process to the first output.
- `recovery_s`: time from spawning the resumed process until its sink has again
  reached, in every partition, the highest offset seen before the crash.
  `cold_same_point_s` is how long the fresh run took to reach the same point.
  `replayed` counts the outputs the resumed run re-emitted below that point
  (at-least-once replay since the last epoch snapshot; `--epoch-interval-ms`
  bounds it).

Every sample checks that the sink received exactly the expected number of
outputs, and fails otherwise.

## Running it

Create one virtual environment per configuration, install loom in editable mode
plus the engine build under test, and pass each interpreter to the runner.
Install the streaming dependencies explicitly rather than with the `streaming`
extra. On 3.12 the extra pulls PyPI's `bytewax`, which would clobber a
`loom-bytewax` wheel that installs the same `bytewax` package. The engine
refuses to run when more than one distribution provides `bytewax`.

```bash
DEPS=("anyio>=4,<5" "confluent-kafka>=2.6,<3" "uvloop>=0.21,<1"
      "opentelemetry-api>=1.27,<2" "opentelemetry-sdk>=1.27,<2"
      "opentelemetry-exporter-otlp-proto-http>=1.27,<2"
      "opentelemetry-exporter-otlp-proto-grpc>=1.27,<2")
uv venv -p 3.12 /tmp/bench/A && VIRTUAL_ENV=/tmp/bench/A uv pip install -e . "${DEPS[@]}" "bytewax==0.21.1"
uv venv -p 3.12 /tmp/bench/B && VIRTUAL_ENV=/tmp/bench/B uv pip install -e . "${DEPS[@]}" loom_bytewax-*-cp312-*.whl
uv venv -p 3.14 /tmp/bench/C && VIRTUAL_ENV=/tmp/bench/C uv pip install -e . "${DEPS[@]}" loom_bytewax-*-cp314-*.whl

python -m benchmarks.streaming.run \
    --config A=/tmp/bench/A/bin/python \
    --config B=/tmp/bench/B/bin/python \
    --config C=/tmp/bench/C/bin/python \
    --repetitions 10 --warmup 2 --note "what else was running"
python -m benchmarks.streaming.compare \
    benchmarks/results/<date>-A-*.json benchmarks/results/<date>-B-*.json \
    benchmarks/results/<date>-C-*.json --markdown
```

Run both commands from the repository root. The runner itself needs only the
standard library, so any Python 3.11+ can drive it. Each measurement runs in a
fresh child process started with the configuration's interpreter
(`python -m benchmarks.streaming.child`). Repetitions are interleaved
(A, B, C, A, B, C, …), so drift in the load of a shared machine spreads over
every configuration instead of penalising whichever ran last. The first
`--warmup` repetitions are stored with `"warmup": true` and left out of the
summaries. `--scenarios` selects a subset, and the other flags change the load
(`--messages`, `--partitions`, `--keys`, `--payload-bytes`, `--seed`) or the
flow (`--batch-max`, `--batch-timeout-ms`, `--epoch-interval-ms`).

Each run writes `benchmarks/results/<date>-<config>-py<X.Y>-<distribution>-<version>.json`.
The file holds the Python version and executable, whether the GIL is enabled,
the distribution and version providing `bytewax`, the loom version, platform,
CPU model and count, the commit of the benchmark code, all parameters, the host
load average, every sample, and a summary per metric (n, mean, median, standard
deviation, coefficient of variation).

`compare.py` prints the median ± standard deviation of every file, the
difference of the means against the first file with a Welch 95 % interval, and a
verdict:

- `ok`: the whole interval worsens by less than `--threshold` (5 %).
- `worse`: the whole interval worsens by more than the threshold.
- `?`: the interval straddles the threshold. More repetitions or a quieter
  machine are needed, and the result is not a pass.

## Adding another engine

B009 evaluates Quix Streams with this benchmark. An engine is a module under
`engines/` that exposes `ENGINE`, an object satisfying `engines.Engine`:

```python
class Engine(Protocol):
    def describe(self) -> dict[str, str]: ...
    def run(self, spec: RunSpec, generator: LoadGenerator, recorder: Recorder) -> None: ...
```

1. Build the same reference flow with the engine's own API: record step,
   per-partition batches of `spec.flow.batch_max` or `batch_timeout_ms`, the
   batch step fanning back out to records, a split on `lane` with `a` and `b`
   going to a sink and every other lane dropped. Reuse the payload semantics
   of `engines/loom_bytewax.py` so the work per message is the same.
2. Feed it from `generator`: one source partition per
   `generator.partition_names()`, each emitting `generator.event(p, offset)`
   paced by `load.Pacer`. Call `recorder.mark_emit(now_ns())` on a partition's
   first emission, and stamp `now_ns()` into each record when it is emitted.
   Resume from the engine's own checkpoint when `spec.recovery_dir` is set.
3. Report outputs from the sink with
   `recorder.record_batch(SinkItem(partition, offset, emitted_ns) for ...)`.
   The recorder handles counting, latency, the crash in `CRASH` mode and the
   catch-up target in `RESUME` mode.
4. Honour `spec.flow.workers` and `spec.flow.processes` where the engine has
   an equivalent. Scenarios it cannot run should fail loudly rather than
   silently run something else.
5. Return the engine's distribution and version from `describe()`, then
   register the module in `engines.ENGINES`.

Run it with `--engine <name>` and compare its results with the bytewax
baseline, as above.

## Not covered yet

- **Kafka lane.** The load is in memory on purpose, so the numbers measure loom
  and the engine rather than a broker. A lane that reads from a real broker,
  enabled only when a broker address is given through an environment variable,
  has not been written yet. No broker was available where the first results
  were taken, and an untested lane would be worse than none. To add one, give
  the engine a source mode that uses loom's `KafkaPartitionedSource` over a
  pre-filled topic, keeping the recorder sinks.
- **`workflow_dispatch`.** This is not trivial yet. The `loom-bytewax` wheels
  live in a private repository and are not on PyPI (B003), and a shared CI
  runner is at least as noisy as a busy laptop. Once the wheels are published,
  a manual workflow can create the three environments above, run the benchmark
  and upload `benchmarks/results/` as an artefact.
