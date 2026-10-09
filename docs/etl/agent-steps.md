# Agents in ETL steps (`WithAgent`)

A step that needs a model to answer something about each row (classify a
message, extract a field, label a reply) declares the agent with `WithAgent`,
the way it declares a config value with `FromConfig`. The agent itself is an
[agent artifact](../ai/artifacts.md) in the repository; the model, its region
and its price are deployment config under `ai:`. The step stays synchronous
and Polars: it receives an `AgentMapper` and calls `map` on a frame.

```bash
pip install "loom-kernel[etl-polars,ai-bedrock]"
```

## A complete example

The output type, next to the step that uses it. It must reject unknown fields,
as every `type_ref` output does:

```python
# src/pipelines/respondio/agents/seller_reply.py
from typing import Annotated, Literal

import msgspec


class SellerReply(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    respuesta: Literal["acepta", "contraoferta", "evalua", "rechaza", "otro"]
    motivo: Literal["ya_vendida", "precio", "competencia", "sin_motivo"] | None
    confianza: Annotated[float, msgspec.Meta(ge=0, le=1)]
```

The artifact, reviewed in pull requests like any other code:

```yaml
# src/pipelines/respondio/agents/seller_reply.agent.yaml
spec_version: 1
name: seller_reply
description: Classifies the seller's answer to our appraisal.
model_role: classifier
instructions:
  - name: guide
    text: |
      Classify the seller's message as one of acepta, contraoferta, evalua,
      rechaza or otro. Give a reason only when the answer is rechaza.
output: {kind: type_ref, ref: "pipelines.respondio.agents.seller_reply:SellerReply"}
policies: {retries: 2, run_timeout_ms: 60000, max_requests: 3, max_usd: 0.01, on_unpriced_spend: refuse}
```

The pipeline config, next to `storage:`. `specs` resolve against the
directory holding the YAML and may not leave it (see below for a config kept
in a subfolder):

```yaml
# etl.yaml, at the repository root
storage:
  engine: polars
  # ...

ai:
  engine: pydantic-ai
  specs: ["src/pipelines/respondio/agents/*.agent.yaml"]
  max_concurrent_runs: 8
  models:
    classifier:
      provider: bedrock
      model: eu.anthropic.claude-haiku-5-5
      region: eu-west-1
      output_mode: tool
      options: {max_tokens: 3000}
  prices:                           # USD per million tokens
    eu.anthropic.claude-haiku-5-5:
      input: 0.11
      output: 0.55
      cache_read: 0.011
      cache_write: 0.1375
      source: AWS Pricing API eu-west-1
      as_of: 2026-10-09
```

### A config in a subfolder

`specs` resolve against `ai.root`, a path relative to the directory holding
the YAML; without `ai.root`, against that directory itself. A glob may not
leave the directory it resolves against, so a config kept under
`config/pipelines/` names the repository root with `ai.root` and its globs
from there:

```yaml
# config/pipelines/respondio.yaml
ai:
  engine: pydantic-ai
  root: ../..                       # the repository root
  specs: ["src/pipelines/respondio/agents/*.agent.yaml"]
  # models, prices, ...
```

`ai.root` is trusted config and may point anywhere; `..` in a `specs` glob is
still refused (`AGENT_COMPILATION_FAILED` carrying `AGENT_SPECS_ESCAPE_ROOT`).

The step reads the messages and a bounded window of what is already labelled,
keeps the pending ones with an anti-join, and maps them:

```python
from decimal import Decimal

import polars as pl

from loom.etl import AgentMapper, ETLStep, FromTable, IntoTable, WithAgent, col, params
from pipelines.respondio.agents.seller_reply import SellerReply

KEYS = ("workspace", "message_id")
TARGET = "fact.seller_reply"


class SellerReplyStep(ETLStep[DailyParams]):
    messages = FromTable("prep.seller_reply").where(col("message_date") == params.run_date)
    stored = FromTable(TARGET).where(col("message_date") == params.run_date)
    agent = WithAgent("seller_reply", output=SellerReply, max_usd=Decimal("2.00"))
    target = IntoTable(TARGET).upsert(keys=(*KEYS, "agent_version"))

    def execute(
        self,
        params: DailyParams,
        *,
        messages: pl.LazyFrame,
        stored: pl.LazyFrame,
        agent: AgentMapper,
    ) -> pl.LazyFrame:
        done = stored.filter(pl.col("agent_version") == agent.version).select(KEYS)
        pending = messages.join(done, on=list(KEYS), how="anti")
        prompt = pl.format(
            "<our_message>\n{}\n</our_message>\n<seller_message>\n{}\n</seller_message>",
            pl.col("our_text").fill_null("(none)"),
            pl.col("seller_text"),
        )
        return agent.map(pending, keys=KEYS, prompt=prompt).lazy()
```

```python
runner = ETLRunner.from_yaml("etl.yaml")
runner.run(RespondioPipeline, DailyParams(run_date=date(2026, 10, 9)))
```

## What `map` returns

One row per input row, in the input order, failures included:

| Column | Content |
|---|---|
| the `keys` | copied from the input row |
| one per field of `output`, named as the field is on the wire (its `rename` or alias) | the answer, null on an error row |
| `agent_version` | the agent's fingerprint (see below) |
| `agent_status` | `ok` or `error` |
| `agent_error` | null, a run error code (`PROVIDER_UNAVAILABLE`, `OUTPUT_SCHEMA_VIOLATION`, ...), `BUDGET_EXHAUSTED`, `PROMPT_MISSING`, `OUTPUT_UNREPRESENTABLE` or `UNEXPECTED_ERROR` |
| `agent_input_tokens`, `agent_output_tokens`, `agent_cache_read_tokens`, `agent_cache_write_tokens` | what the run consumed, `0` when nothing was sent |
| `agent_cost_usd` | what the run cost, null when it could not be priced |

`prompt` is a Polars expression or a column name; a null prompt is not sent.
A key named like a column `map` writes is refused with a `ValueError` before
anything runs; an output field named like an `agent_*` column fails
compilation (`AGENT_COLUMN_COLLISION`).

The answer columns take their Polars type from the field, never from the rows,
so a batch without rows or with errors only has the same schema as any other
and upserts into the same table:

| Field type | Polars type |
|---|---|
| `str`, a `Literal` or `Enum` of strings | `String` |
| `int`, a `Literal` or `Enum` of integers | `Int64` |
| `float` | `Float64` |
| `bool` | `Boolean` |
| `date` | `Date` |
| `datetime` | `Datetime("us", "UTC")`; a naive value is read as UTC. `Datetime("us")` when the field requires naive values (`Meta(tz=False)`) |
| `Decimal` | `Decimal(38, 9)`, keeping 9 decimal places |
| `list[T]`, `set[T]`, `tuple[T, ...]` | `List` of the type of `T` |
| a nested struct | `Struct` of its fields' types |
| `T \| None` | the type of `T` |
| anything else (`dict`, a union of several types, ...) | `String` holding the value's JSON |

An answer holding a value its column cannot (an `int` past `Int64`, a
`Decimal` past `Decimal(38, 9)`, ...) becomes an error row with
`agent_error = OUTPUT_UNREPRESENTABLE`, its answer columns null and its tokens
and cost kept; the other rows of the batch are unaffected.

A failed row never aborts the batch, whatever failed: an exception that is not
a run error becomes `UNEXPECTED_ERROR`, keeps its reservation as spent and is
logged by its type. Whether to write error rows, drop them or
count them against a threshold is the step's decision. Writing them with
`agent_version` in the upsert keys and filtering `agent_status == "ok"` in the
anti-join retries them on the next execution; leaving them in the `done` side
does not.

## How it runs

- **One runtime per call.** Each `map` builds, enters and closes a runtime of
  its own on an event loop of its own (a dedicated thread when the calling
  thread already runs one, as under an async orchestrator; that thread sees
  the caller's context variables). Two steps mapping at once share nothing. A
  frame without rows compiles nothing and opens no runtime. `version` and the
  compile-time checks compile each agent once per runner; each `map`
  recompiles it and `version` then answers that compilation, so it always
  equals the `agent_version` the last `map` wrote.
- **Concurrency waits.** At most `ai.max_concurrent_runs` runs are in flight;
  the others wait for a slot instead of being refused with `TOO_MANY_RUNS`.
- **Budget per execution.** `WithAgent(..., max_usd=...)` bounds what one
  `map` call may spend, whatever `max_concurrent_runs`. Before each run its
  `policies.max_usd` is set aside, and replaced by the real cost when the run
  ends (the reservation itself when the cost is unknown or the run failed
  unexpectedly). The first run that does not fit stops the batch: it and every
  row after it are returned with `agent_error = BUDGET_EXHAUSTED` and nothing
  spent, and the anti-join picks them up on the next execution. A step budget
  therefore needs `policies.max_usd` on the artifact, and one run's
  `policies.max_usd` must fit in it; otherwise the step does not compile
  (`AGENT_BUDGET_UNENFORCEABLE`), and `map` called directly with a budget
  raises `ValueError`.

  What the step spends never exceeds its budget except by the elastic excess
  of the last response of each run in flight, so by at most
  `max_concurrent_runs` responses. `policies.max_usd` is
  [elastic](../ai/artifacts.md): the engine checks it against the run's spend,
  summed over all its attempts, after each response, so a run stops with
  `USAGE_LIMIT_EXCEEDED` once a response takes it past the cap, and that
  response is billed and counted. Retries do not multiply the reservation: the
  attempts of one run share the same cap.
- **Writes happen once**, after `execute` returns. A process killed mid-batch
  loses what that batch paid for, so a large backfill goes in bounded chunks
  (one day per execution, for instance).
- **Polars only.** `map` takes a Polars `DataFrame` or `LazyFrame` and returns
  a `DataFrame`. On a Spark runner every `WithAgent` fails compilation
  (`AGENT_UNSUPPORTED_ENGINE`) and `loom.ai.etl` is never loaded.

## Versions

`agent.version`, and the `agent_version` column, is the agent's
[fingerprint](../ai/artifacts.md#the-agents-version--agentplanfingerprint): it
changes when the instructions, the output schema, the output check, the
policies, the tools (the kind and name of each capability) or the model
binding change, and stays when the region, the credentials, the price, the
budget or where a tool is served from change. Keeping `agent_version` in the
upsert keys and in the anti-join relabels every row the day the agent changes,
and leaves the old labels in place for comparison.

## What is checked before anything is spent

`ETLRunner.run` compiles the pipeline and validates every `WithAgent` before
any step runs:

| Code | Cause |
|---|---|
| `AGENT_NOT_FOUND` | no artifact under `ai.specs` declares the name, or the config has no `ai:` section |
| `AGENT_COMPILATION_FAILED` | the artifact does not compile; the message carries every compiler issue with its code |
| `AGENT_OUTPUT_MISMATCH` | `output=` is not the artifact's `type_ref` output (a `json_schema` output never matches) |
| `AGENT_UNPRICED_BUDGET` | a budget is declared (`WithAgent(max_usd=...)` or the artifact's `policies.max_usd`) for a model neither `ai.prices` nor the engine can price |
| `AGENT_BUDGET_UNENFORCEABLE` | `WithAgent(max_usd=...)` is declared but the artifact has no `policies.max_usd`, or that `policies.max_usd` exceeds it |
| `AGENT_COLUMN_COLLISION` | a field of `output` is named like an `agent_*` column `map` writes |
| `AGENT_UNSUPPORTED_ENGINE` | the runner executes on Spark; agents map Polars frames only |

`ETLCompiler()` alone checks the shape, with the codes `FromConfig` uses: a
keyword-only parameter named like the attribute (`MISSING_CONFIG_PARAMS`), no
name shared with a source or `client` (`CONFIG_ALIAS_CONFLICT`), and no
`WithAgent` on a `StepSQL` (`UNSUPPORTED_CONFIG_VALUE`).

## Without `from_yaml`

`ETLRunner`, `ETLRunner.from_config`, `ETLCompiler` and `ETLExecutor` take
`agents=`, any implementation of `loom.etl.runtime.AgentBatchRunner`.
`loom.ai.etl.PolarsAgentRunner(ai_config, root=...)` is the one loom ships;
its `engine_provider=` lets a test serve the agent from a pydantic-ai
`TestModel` or `FunctionModel`:

```python
from pydantic_ai.models.test import TestModel

from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider
from loom.ai.etl import PolarsAgentRunner

agents = PolarsAgentRunner(
    ai_config,
    root=repo_root,
    engine_provider=lambda: PydanticAIEngineProvider(model_resolver=lambda _: TestModel()),
)
runner = ETLRunner.from_config(storage_config, agents=agents)
```

`loom.etl` never imports `loom.ai`: `from_yaml` loads `loom.ai.etl` by module
path only when the config has an `ai:` section.
