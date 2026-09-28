# Reference streaming benchmark

A reproducible benchmark of the flow loom runs on its streaming engine. It
answers one question: does changing the engine build, or the Python it runs
on, make loom's streaming slower or heavier? No metric may get more than 5 %
worse than bytewax 0.21.1 on Python 3.12.

It is not part of the test suite; `benchmarks/` is outside `testpaths`, so
`pytest` never collects it.

## What it measures

**Reference flow** (`engines/loom_bytewax.py`) is built with loom's streaming
DSL and wired through the same path as production: `compile_flow`, then the
runner's `_prepare_run`. Only the Kafka source and sinks are replaced:

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
msgpack-encoded and the emission time is stamped into the payload at the
moment a partition emits them.

**Scenarios** (`run.py`, `SCENARIOS`):

| scenario | what it runs | main metrics |
|---|---|---|
| `throughput-w1` | 1 worker, as fast as the engine pulls | `msgs_per_s`, `msgs_per_cpu_s`, `peak_rss_mb` |
| `throughput-w4` | 4 worker threads in one process | same |
| `throughput-p2` | 2 processes × 1 worker (`cli_main` with `process_id`/`addresses`) | same, across a cluster exchange |
| `latency-w1` | 1 worker, fixed offered rate (`--latency-rate`, default 20 000 msg/s) | `p50_ms`, `p99_ms` |
| `latency-b1-w1` | the same with `CollectBatch(max_records=1)` | `p99_ms` of the engine itself |
| `recovery-w1` | `RecoveryConfig`; the process dies abruptly (`os._exit`) after half the outputs, then resumes from the recovery store | `recovery_s`, `resume_first_output_s`, `replayed` |

**Metrics:**

- `msgs_per_s`: messages generated, divided by the time from the first source
  emission to the last sink write. Startup is excluded.
- `msgs_per_cpu_s`: messages per CPU-second (user + system, all threads, all
  processes), counted from the first emission. This is the per-core figure.
- `p50_ms`, `p99_ms`: end-to-end latency, from source emission to sink write,
  taken from a log-bucketed histogram with about 1 % resolution. In the
  throughput scenarios these numbers mostly measure queueing at saturation;
  compare latency on the fixed-rate lanes, where every configuration receives
  the same offered load. In `latency-w1` a record mostly waits for its batch
  to fill: about `batch_max * partitions / rate` (12.8 ms by default) at
  p99, whatever the engine. `latency-b1-w1` removes that wait.
- `instructions_per_msg` (macOS only): instructions retired by the whole
  process tree, as reported by `/usr/bin/time -l`, divided by the messages.
  Startup is included, so the figure is comparable only between
  configurations on the same interpreter, such as A and B.
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
(`python -m benchmarks.streaming.child`). Repetitions are interleaved,
and the order rotates with each repetition (A B C, B C A, C A B, …). Each
sample records its `position` in that order. The first
`--warmup` repetitions are stored with `"warmup": true` and left out of the
summaries. `--gc NAME=POLICY` runs one configuration under a garbage-collector
policy, applied once the flow is built and before the first record: `freeze`
or `threshold:T0[,T1[,T2]]` (see `gc_policy.py`). Give the same interpreter
twice under two names to compare a policy with the defaults. `--max-load L` waits, up to `--max-wait-s`, before each repetition
until the 1-minute load average is at most `L`; the load is stored with every
sample either way. `--tag` adds a suffix to the file names, so two runs on the
same day do not overwrite each other. `--scenarios` selects a subset, and the other flags change the load
(`--messages`, `--partitions`, `--keys`, `--payload-bytes`, `--seed`) or the
flow (`--batch-max`, `--batch-timeout-ms`, `--epoch-interval-ms`).

Each run writes `benchmarks/results/<date>-<config>-py<X.Y>-<distribution>-<version>.json`.
The file holds the Python version and executable, whether the GIL is enabled,
the distribution and version providing `bytewax`, the loom version, platform,
CPU model and count, the commit of the benchmark code, all parameters, the host
load average, every sample, and a summary per metric (n, mean, median, standard
deviation, coefficient of variation).

`compare.py` prints the median ± standard deviation of every file and the
difference against the first file. By default that difference is the ratio of
medians with a 95 % percentile-bootstrap interval, using a fixed seed.
`--method mean` switches to the ratio of means with a Welch interval. Only the
gate metrics of each scenario (`GATES` in `compare.py`) get a verdict; the rest
are printed as `info`. The gate metrics are `msgs_per_s`, `msgs_per_cpu_s` and
`peak_rss_mb` for the throughput scenarios, `p99_ms` for `latency-b1-w1`, and
`recovery_s` and `peak_rss_mb` for recovery:

- `ok`: the whole interval worsens by less than `--threshold` (5 %).
- `worse`: the whole interval worsens by more than the threshold.
- `?`: the interval straddles the threshold. More repetitions or a quieter
  machine are needed, and the result is not a pass.

## Adding another engine

An engine is a module under `engines/` that exposes `ENGINE`, an object
satisfying `engines.Engine`:

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

- **Kafka lane.** The load is generated in memory; there is no lane that
  reads from a real broker. To add one, give the engine a source mode that
  uses loom's `KafkaPartitionedSource` over a pre-filled topic, keeping the
  recorder sinks.
