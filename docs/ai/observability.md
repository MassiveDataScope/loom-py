# Observability and Logfire

Agent runs emit OpenTelemetry spans through the same `ObservabilityRuntime` the
rest of loom uses. This page covers the packaged Logfire setup, the one
configuration rule that makes it work, and — with no varnish — what the result
does **not** give you today.

## The `logfire` extra

```bash
pip install "loom-kernel[logfire]"
```

[Logfire](https://logfire.pydantic.dev/) is an OpenTelemetry distribution, not a
second telemetry stack. It ships an OTel SDK, an exporter and a large set of
instrumentations, and — this is the part that matters — `logfire.configure()`
installs a **global `TracerProvider`**.

## Bootstrap

Two lines, and one configuration rule that ties them together.

```python
# main.py — before create_app(), before anything emits a span
import logfire

logfire.configure(send_to_logfire=False)     # installs the global TracerProvider
logfire.instrument_pydantic_ai()             # LLM calls and tool calls
logfire.instrument_fastapi(app)              # HTTP handlers
```

In `config/api.yaml`:

```yaml
app:
  observability:
    otel:
      enabled: true
      config:
        service_name: incidents-api
        endpoint: ""
```

`endpoint: ""` is empty on purpose: share Logfire's provider.

`send_to_logfire=False` keeps the data out of Logfire's SaaS and leaves you with
a plain, locally-configured OTel pipeline — point it wherever you already send
traces. Drop the argument if you *do* want Logfire's backend.

```{admonition} `endpoint` and Logfire are mutually exclusive
:class: important

`OtelConfig.endpoint` decides whether loom builds a tracer provider **of its
own**:

- **`endpoint: ""`** — loom calls `trace.get_tracer(...)` and therefore uses
  the **global** provider. When Logfire installed one, loom shares it. This
  is the setting that makes the integration work.
- **`endpoint: "https://collector…"`** — loom constructs its own
  `TracerProvider` with its own `BatchSpanProcessor` and its own exporter.

Set both and you get **two providers in one process**. The trace itself stays
intact — the OTel *context* is global even though the provider is not, so the
spans still nest and still share one trace id — but each provider exports only
its own spans, so loom's half lands in loom's collector and Logfire's half in
Logfire's. Nothing raises; you simply have to query two backends to see one
trace.

**Pick one**, unless both exporters point at the same collector. Using Logfire
(or any other OTel distribution that configures the global provider) means
`endpoint` stays empty and that distribution owns the export. Configuring
`endpoint` means loom owns the export.
```

Without Logfire nothing changes: leave `endpoint` set and loom builds and
exports its own provider exactly as it always has.

## What you actually get today

This is the honest part, and it is the reason this section exists rather than a
screenshot.

**What works.** Loom's lifecycle spans and Logfire's instrumented spans (the LLM
call, the tool calls, the FastAPI handler) end up in the same backend and
**share a trace id**. You can retrieve everything belonging to one request, and
correlate a slow agent run with the model call inside it.

Spans **nest**: loom opens each of its spans as the *current* span for the
duration of the work it covers, so an LLM call instrumented by Logfire hangs
off the agent run that made it, and a tool span hangs off the same run. A
waterfall view shows one tree.

The same holds in the other direction, and across pillars: a loom span opened
inside a Logfire-instrumented FastAPI handler is a child of that handler's
span, and ETL's `PIPELINE` / `PROCESS` / `STEP` and streaming's `POLL_CYCLE` /
`NODE` spans form trees of their own.

```{admonition} One exception: parallel ETL groups
:class: warning

Processes and steps inside a `ParallelProcessGroup` or `ParallelStepGroup` are
submitted to a thread pool without copying the OTel context, so their spans
are roots rather than children of the pipeline span. Sequential runs are
unaffected.
```

Sharing a provider gives a shared trace id; nesting comes from the shared
*active context*, which is global whether or not the provider is. That is why
loom never calls `set_tracer_provider`: it takes ownership of nothing, and
still parents correctly in both directions.

## What the spans carry

| Scope | Emitted for |
|---|---|
| `Scope.TOOL` | one capability call — a use case, a SQL query, an MCP tool, a remote agent |

A `Scope.TOOL` span of an **MCP call** names the side that reached the
server, and only one of the two:

| Attribute | Present when |
|---|---|
| `agent` | the call came from an agent — its own capability, or an `AgentHandle.mcp()` view |
| `mcp_server` | the call came from a use case's `Mcp()` marker |

They never appear together. Anything that groups `Scope.TOOL` spans by
`agent` alone will not see marker calls.

The agent span of a run — streamed or not — closes with what the run spent,
under the OpenTelemetry GenAI names:

| Attribute | Meaning |
|---|---|
| `gen_ai.usage.input_tokens` | input tokens, cached ones included |
| `gen_ai.usage.output_tokens` | output tokens |
| `gen_ai.usage.cache_read.input_tokens` | input tokens served from the prompt cache |
| `gen_ai.usage.cache_creation.input_tokens` | input tokens written to the prompt cache |
| `gen_ai.usage.requests` | model round trips |
| `gen_ai.usage.tool_calls` | tool invocations the model completed |
| `gen_ai.usage.cost` | run cost, **absent** when the engine could not price the model |
| `gen_ai.usage.details.*` | every other counter the engine reported, under its own name |

They land on the closing event, not the opening one: nothing is spent when a
run starts. `gen_ai.usage.cost` is missing rather than zero for a model with no
price entry — a Bedrock inference profile, say — because a zero would win a
cost comparison the model never entered. `gen_ai.usage.cost_known` is always
present, so `SUM(cost) WHERE cost_known` is a complete total and
`WHERE NOT cost_known` counts what the total is missing; summing the cost
without checking it gives a lower bound that looks like a total.

**Failed runs are in the number.** A run that made three model round trips and
then failed its output schema, was refused by a capability, or lost its output
hook still publishes what it burned: a model that fails often must not rank
better on cost than one that answers. The same attributes ride the `ERROR`
closing event.

Two gaps, stated rather than hidden. A run killed by its own
`run_timeout_ms`, `tool_timeout_ms` or `max_iterations` publishes **no** usage:
the supervisor terminates the run from outside the engine, and the counters of
the attempt it cancelled never leave it. And a caller that abandons a stream
mid-run closes the span without a terminal event, so that run reports nothing
either. Both cases are absences, never zeros — `gen_ai.usage.requests` is
simply not there.

Attributes are chosen so a trace can be shared without leaking a deployment:
an MCP span carries the server **host**, never the full URL and never the
resolved headers. The same containment applies everywhere the compiled plan is
rendered — an `InferenceTarget` redacts its `credentials_ref` and `options` in
`repr`, and refuses to serialise at all when it carries one.

## Health

Each mounted agent serves `GET /agents/{name}/health`.

```json
{ "status": "ok" }
```

Unauthenticated callers get the **aggregate only** — dependency identifiers are
internal topology, and a health endpoint is not an inventory of your services.
Authenticated callers get the breakdown:

```json
{ "status": "ok", "checks": { "model": "ok", "mcp:runbooks": "ok" } }
```

`status` is `ok`, `degraded` or `unavailable`; the endpoint returns `503` when
`unavailable`. The value is **cached and refreshed by a background probe** — a
liveness probe that amplified into network I/O on every scrape would be a
self-inflicted outage under a Kubernetes readiness loop.

```{note}
The per-dependency breakdown is currently less informative than it looks:
several entries are fixed at start-up rather than re-checked by the probe, so
`"model"` is the entry to trust. Treat the aggregate `status` as the
contract and the breakdown as a hint until the probe is completed.
```
