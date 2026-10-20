# Delivery, Scaling, and Recovery

How the partitioned Kafka source delivers messages, how a streaming service
scales, and how offsets interact with Bytewax recovery.

## Delivery semantics

Every Kafka consumer declares its delivery mode explicitly:

```yaml
kafka:
  consumer:
    brokers: ["broker:9092"]
    group_id: orders-service        # stable per service — never per pod
    topics: ["orders.in"]
    delivery: at_least_once         # or at_most_once
```

- **`at_least_once`** — offsets are committed to the consumer group only
  after every downstream branch confirmed the record: sink flush, DLQ
  delivery, error routes, and drops all count. A crash mid-processing
  re-delivers (duplicates are possible — make steps idempotent); messages are
  never lost. This is what generated services should use.
- **`at_most_once`** — librdkafka auto-commits on a timer, decoupled from
  processing. Simpler (no duplicates, no tracker), but a crash loses whatever
  was polled and not yet processed. Fine for metrics/telemetry-class data.
- **Unset** — legacy resolution from the deprecated `enable_auto_commit`
  flag (`false` → at-least-once; `true` or unset → at-most-once). Configs
  written before `delivery` existed keep their exact behavior. Setting both
  fields to contradicting values is a compile error (`DELIVERY_CONFLICT`).

Under `at_least_once` two structural rules are enforced at compile/startup:

- A terminal `Fork` must declare a `default` branch
  (`FORK_UNMATCHED_UNROUTED`): a message no branch claims would otherwise be
  committed without being written anywhere. Add a default route that writes
  it, or `default=Process(Drain())` to discard it explicitly.
- Keyed nodes (`CollectBatch`) are rejected on a **multi-process** Bytewax
  cluster (`DELIVERY_KEYED_MULTIPROCESS`): keyed operators route records to a
  primary worker by key hash, which may be another process, where completions
  cannot reach the source's commit tracker. Scale those flows with
  `workers_per_process` (threads share the tracker) or use Bytewax recovery.

Whatever the delivery mode, the `default` branch of a `Fork` is shape-validated
like every keyed or predicate branch and must reach a terminal
(`FORK_BRANCH_NO_TERMINAL`). `default=Process(step)` is a compile error; write
`default=Process(step, Drain())` to discard the record explicitly.

### What at-least-once requires of a flow

The guarantee rests on one rule: **every record is completed exactly once,
after the work it represents is durably written**. Loom enforces the parts it
can see.

- Nodes that change the record count (`Explode`, `Expand`, `BatchExpand`,
  `ExpandRoutes`) declare the fan-out they actually produced, so an offset is
  committed only once *all* its outputs are written — and a record that
  produced nothing is released instead of stalling its partition.
- A path ends in one place. A sink that ends it (`IntoTopic`, `IntoTable` or
  any `IntoSink`, `Drain`) completes what it writes and nothing is wired after
  it. A branch still live at its end goes to the sink compiled for it, such as
  a Broadcast route's `output`, else to a drop sink that completes its record,
  so an unfinished `Broadcast` route or `Fork` branch cannot freeze commits.
  Only the flow's own live end goes to the flow `output`, else to a drop sink.
- A `Router` selects one branch per message and runs it inline, so the
  messages that continue after it keep their source order. A branch that ends
  in a sink hands the message to that sink at its compiled path; a message no
  branch claims continues after the Router.
- An inline `IntoTopic` inside `With(process=...)` or `WithAsync(process=...)`
  completes nothing: the message goes on and completes where its path ends.
  A caller-supplied sink written inline must expose `mark_inline_partition()`,
  or the flow is rejected at assembly (`INLINE_SINK_CANNOT_SKIP_COMMITS`).
- A storage partition reports from `write_batch` and `close` how many buffered
  events it made durable, and only those complete. Events still buffered when
  a worker dies are consumed again, and after a failed write nothing more
  completes. Bytewax calls `write_batch` only when items arrive, not on idle
  epochs, so the buffer of an idle partition is checked against its thresholds
  when the next item arrives and flushed at `close` at the latest; until then
  its offsets stay uncommitted.
- A Kafka sink batch whose delivery fails and whose `IntoTopic` declares a
  `dlq` topic is sent to the DLQ topic and completes only once the producer
  flushed it. A failed DLQ send or flush raises `KafkaDeliveryError` and
  completes nothing, so the batch is consumed again.
- Custom sinks must expose `bind_commit_tracker(tracker)`. A sink without it
  would never complete what it writes, so under `at_least_once` the flow is
  rejected at assembly with `RuntimeConfigurationError` naming the sink.
- Asynchronous commit failures are logged with their topic-partition-offsets.
  They do not break the guarantee — the group offset simply does not advance
  and those records are reprocessed — but they are never silent.

### How commits work

The source registers every record before emitting it; each terminal branch
completes it. A **gap-tolerant watermark** per partition advances past
completed offsets — offsets that never arrive (transactional control records,
compacted-topic gaps) are never waited on, and compacted-topic tombstones
(`value = None`) are skipped entirely. Commits are **coalesced**: one
asynchronous group commit per partition per poll cycle, plus a synchronous
final commit on close. Idle partitions re-commit their current offset every
`commit_keepalive_ms` (default 30 min) so the broker's offset retention
(`offsets.retention.minutes`, default 7 days — the clock always runs for
member-less groups) never expires a live service's position.

## Scaling model

The source creates **one Bytewax input partition per Kafka partition**, with
one assign-mode consumer each — no group membership, no rebalancing: the
consumer group is purely an offset store, so standard Kafka lag tooling keeps
working and pods stay stateless.

Bytewax distributes those partitions across the workers of **one cluster**
(`-i`/`-a` / `addresses`). Two rules follow:

- **Scale by growing the Bytewax cluster, never by adding free replicas.**
  Independent replicas of the same dataflow each consume *all* partitions —
  with `assign()` the group does not arbitrate, so replicas duplicate work.
- **`group_id` must be stable per service** (never derived from the pod
  hostname): it is the durable home of the service's committed offsets.

Partition discovery is static per execution: partitions added to a topic are
picked up on the next restart.

### Batching across processes

Keyed operators route records by key hash, which is a **different** distribution
from the source's partition assignment. A record read by one process is
therefore batched on whichever process owns its batch key — often another one.
Its completion then reaches a commit tracker that does not own the partition,
and that partition stops committing.

Loom refuses this combination at startup rather than letting it drift:
`delivery=at_least_once` plus a keyed node (`CollectBatch`) on a multi-process
cluster raises `RuntimeConfigurationError`. Within a single process the workers
share one tracker, so **scale batching flows with `workers_per_process`
(threads), not with processes** — or declare `at_most_once`.

Verified on a 3-partition topic with a 2-process cluster: the source assigned
partitions `[0, 2]` and `[1]` to the two processes with no overlap and no loss,
while the batch stage grouped `[0]` and `[1, 2]` — a different split, which is
exactly the hazard the guard rejects.

## Recovery and snapshots

Bytewax recovery is **optional** for stateless Kafka→Kafka flows: the
committed group offset is the source of truth and pods need no volumes. Flows
with stateful operators (`CollectBatch` windows) need recovery to restore
that state; each partition then snapshots its read position.

Start-offset precedence at partition build:

```
resume_state (Bytewax recovery)  >  committed group offset  >  auto_offset_reset
```

Two protections keep the two offset stores consistent:

- **Commit floor** — the committed group offset observed at startup is a
  floor: a recovery replay re-processes internally but never rewinds the
  group's committed offset (lag alerts stay sane).
- A **loud warning** is logged when `resume_state` lags the committed offset
  (stale recovery store) — recovery directories must not be mixed across
  service generations.

If the coordinator cannot answer the committed-offset fetch at startup, the
partition fails to build (never a silent fallback to `auto_offset_reset`).

## Runtime tuning

```yaml
kafka:
  consumer:
    batch_size: 500            # records per poll cycle
    poll_backoff_ms: 50        # sleep after an empty poll
    commit_keepalive_ms: 1800000
```

`poll_timeout_ms` is deprecated for the partitioned source (it was the old
source's emission interval) and ignored with a warning; it still applies to
the standalone `KafkaConsumerClient.poll()`.
