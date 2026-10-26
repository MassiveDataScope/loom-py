# Message lifecycle tracing

A streaming message is traceable from ingestion, through every node, to its
death, under one trace id, continuous across services.

## The trace id is the message's

The id arrives in the Kafka `x-trace-id` header, is carried on
`MessageMeta.trace_id` through every node, survives the error snapshot, and
crosses the Celery broker as a task kwarg. Loom installs an OpenTelemetry
`IdGenerator` that returns that id whenever a **root** span is opened, so the
OTEL trace id *is* the message trace id. A span with a real parent still
inherits its parent's trace: the generator is consulted for roots only.

Any 32-character hexadecimal id works. An id of another shape (a foreign
producer's opaque token) is rejected and the span falls back to a random trace
rather than emitting an unusable one.

## The spans of one message

| Span | Where it comes from |
| --- | --- |
| `transport:kafka_consume` | The consumer, from the inbound header — the message's birth |
| `node:<flow>:<idx>` | Each node the message traverses |
| `terminal:sink_write` | Written to a storage sink or an outbound Kafka topic |
| `terminal:error_envelope` | Turned into an error envelope or routed to a DLQ |
| `terminal:dropped_no_route` | Expanded or routed to zero rows |

A message has exactly one terminal span. `TerminalReason` is a closed enum:
"some other ending" is not an ending anybody can act on.

`parent_trace_id` and `causation_id` name a *different* message's trace. They
are emitted as the `loom.parent_trace_id` and `loom.causation_id` attributes,
never as the OTEL parent.

## Batches: the N+1 rule

A batch has N parents, and a trace is a tree, so fan-in is expressed with span
links. Every batch operation — a sink flush, a batch-shaped node — produces:

- one **participation** span per message, in that message's own trace, carrying
  `loom.batch_id`. For a sink write, that span is the message's death.
- one **batch** span, in a trace of its own, with one link per participation
  span.

Navigable both ways: message to batch through `loom.batch_id`, batch to
messages through the links. A link is added only for a participation span that
was actually recorded, so the batch span never advertises an edge to a span the
sampler dropped. The link count is bounded by `max_span_links`, and the batch
span carries `loom.links_truncated=true` when the bound bites.

## Configuration

```yaml
observability:
  otel:
    enabled: true
    config:
      endpoint: "http://collector:4318/v1/traces"
      sampler: parentbased_traceidratio
      sampler_ratio: 0.01
      max_span_links: 128
```

`endpoint` chooses where spans go; `adopt_host_id_generator` chooses what trace
ids they get when the provider is not Loom's. The two are independent.

| Setting | Tracer source | Per-message trace ids |
| --- | --- | --- |
| `enabled: false` | injected no-op tracer | no spans at all |
| `endpoint` set | Loom's private `TracerProvider` | yes — the default path |
| `endpoint: ""`, no adoption | the host's provider | random ids, logged once at startup |
| `endpoint: ""`, `adopt_host_id_generator: true` | the host's provider | yes |

Adoption is opt-in because mutating another library's provider behind its
owner's back would be a hidden side effect. It is behaviour-preserving: the
generator delegates to the default random generator whenever no Loom trace id
is active, so host spans keep independent trace ids. When the installed
provider is a `ProxyTracerProvider` there is nothing to install onto; Loom logs
a warning naming the setting and carries on.

### Sampling

The ratio samplers decide on the trace id's low bits, and that id is the
message's — identical at every hop and across services. The decision is
therefore the same everywhere: **complete traces for a sampled subset, never
partial traces for all**.

Under `endpoint: ""` the host's sampler decides; Loom does not force
`always_on`.

## Known limits

These are signals, not oversights. Read them before concluding a trace is
broken.

- **A message in flight when the process is killed emits no terminal span.**
  There is nowhere to emit one, and inventing an `abandoned` reason at shutdown
  would be a lie for messages already written but not yet flushed. *Absence of
  a terminal span in an otherwise complete trace means the message was in
  flight at process death.*
- **Spans of one message are flat roots.** No parent span id crosses a process
  boundary, so `Broadcast` and `ExpandRoutes` — which derive several messages
  from one — produce descendants you cannot tell apart from the span tree
  alone. Correlate on `loom.message_id`.
- **Celery shares a trace but has no parent edge.** `trace_id` crosses the
  broker as a task kwarg, so the worker's `JOB` span lands in the originating
  trace. No remote span id crosses, so the dispatcher's span and the worker's
  span are two roots of one trace. W3C `traceparent` propagation would add the
  causal edge.
- **The batch span is judged by the sampler on its own trace id.** At a low
  ratio, most batch spans are dropped along with most messages. What holds at
  every ratio is that an exported batch span links to exactly the participation
  spans that were also exported.
- **Two death paths are still untraced.** `_DropSinkPartition` (an unrouted
  branch or error kind) and `Drain` discard a message without a terminal span,
  so `TerminalReason.DROPPED_NO_ROUTE` is not emitted on either. Three of the
  five death paths — outbound topic, storage sink, error envelope — are traced.
- **A wire decode failure has no terminal span, and cannot have one.** A
  `DecodeError` never became a message: it carries no `MessageMeta`, so there is
  no trace id, no message id and no lineage to open a span in. Fabricating a
  synthetic meta would invent a message that never existed.
- **A failed outbound batch fails every message in it.** The Kafka producer
  keeps one pending delivery error and `flush()` raises it for the whole batch,
  so a single rejected record closes all N terminal spans failed. Those spans
  carry `terminal.failure_scope="batch"` to say the attribution is batch-wide,
  not per record. Success carries no such attribute: `flush()` waited for every
  record, so a successful death is per-message truth.
- **A batch diverted to a DLQ closes failed, with `terminal.dlq_topic` set.**
  The batch never reached its outbound topic. It commits only after the DLQ
  flush confirmed every record; a DLQ send or flush failure raises instead, and
  the spans close failed without `terminal.dlq_topic`.
- **A delivery error can be attributed to the wrong batch.** The producer's
  pending error is consumed by whichever `flush()` observes it, which may be the
  next batch's. The terminal spans that close failed are then one batch late.
- **Epoch replay produces duplicate terminal spans.** Bytewax replays an epoch
  from the last snapshot after a failure, so a message written twice records two
  `terminal:sink_write` spans under one trace id. That is at-least-once delivery
  being visible, not a tracing fault; deduplicate on `loom.batch_id`.
