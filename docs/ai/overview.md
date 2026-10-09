# Agents Overview

`loom.ai` turns a declarative, vendor-neutral **agent artifact** into a running,
governed HTTP service. An agent is authored as a YAML file, validated offline
with no credentials and no network, compiled into an immutable plan at start-up,
and served under the authentication the rest of your application already uses.

The pillar is deliberately narrow about what belongs where:

| Concern | Lives in | Why |
|---|---|---|
| What the agent *is* — its name, instructions, answer shape, granted capabilities | the **artifact** (`agent.yaml`) | Portable. The same file moves between environments unchanged. |
| Where things *are* — model provider, endpoints, credentials, server URLs | the **`ai:` config section** | Deployment facts. They differ per environment and must never travel in the artifact. |
| Which engine actually runs it | an **entry point** (`loom.ai.engines`) | Swappable. The compiler never imports an engine. |

That split is the whole design. An artifact names a model *role* (`reasoning`),
never a model id; it names an MCP *server* (`runbooks`), never a URL; it names a
skill *library* (`./skills`), never an absolute path. Nothing in the artifact can
locate a resource or carry a secret, because those fields do not exist in the
format.

## Stability

Two different promises, and conflating them is the mistake this section exists
to prevent.

```{admonition} The artifact format is stable; the Python API is not
:class: important

**The programmatic contracts are experimental** (FR-056). The classes,
protocols and functions exported from `loom.ai` may change within a major
line. Pin your loom version if you import them.

**The artifact format is not experimental** (FR-056a). An agent definition
declaring `spec_version: 1` keeps validating and compiling for the whole
major line. The version-1 corpus is append-only: an existing entry may never
be edited to make a change pass, only added to.

Author against the artifact. Pin the Python API.
```

This is why the [JSON Schema](artifacts.md#the-published-json-schema) is shipped
inside the distribution and the artifact reference is the longest page in this
section: the artifact is the surface we ask you to build on.

## Install

The base wheel imports no engine and no vendor SDK — `import loom.ai` works on a
bare install. Capabilities arrive as extras:

```bash
pip install "loom-kernel[rest,ai-pydantic]"          # the engine
pip install "loom-kernel[ai-bedrock]"                # + AWS Bedrock models
pip install "loom-kernel[ai-openai]"                 # + OpenAI and every compatible endpoint
pip install "loom-kernel[ai-anthropic]"              # + Anthropic models
pip install "loom-kernel[ai-a2a]"                    # + the A2A interoperability surface
pip install "loom-kernel[ai-harness]"                # + the `skills` capability
pip install "loom-kernel[logfire]"                   # + the Logfire OTel distribution
```

A missing extra is reported at start-up, by name, with the extra to install —
never as an `ImportError` on the first request.

## The shape of a deployment

```yaml
# config/api.yaml
ai:
  engine: pydantic-ai
  specs: ["ai/agents/*/agent.yaml"]

  models:
    default:
      provider: openai
      model: gpt-4o-mini
      credentials_ref: OPENAI_API_KEY
    reasoning:
      provider: bedrock
      model: anthropic.claude-sonnet-4-20250514-v1:0
      region: eu-west-1

  mcp_servers:
    runbooks:
      url: https://runbooks.internal.example.com/mcp
      headers_ref: ${secrets:/loom/runbooks/api-key}
      timeout_ms: 20000

  endpoints:
    incident-triage:
      enabled: true
      auth: jwt
```

`specs` globs resolve against the application root (the YAML's own directory
for an ETL pipeline) and may not leave it: an absolute pattern, a `..` segment
or a match that resolves outside the root through a symlink fails with
`AGENT_SPECS_ESCAPE_ROOT` before any artifact is read.

Every agent matched by `specs` is compiled. Only the agents named in
`endpoints`, with `enabled` **and** a named `auth`, are mounted on the HTTP
and A2A doors. An agent absent from `endpoints` is still reachable — from
application code, through an
[`Agent()` marker](../rest/use-case-dsl.md#agent-marker--reaching-a-named-agent)
declared on a use case's own `execute` signature, bound to that use case's
own verified caller. That is a separate door with its own default: a
marker-filled handle always refuses an anonymous caller, with no
`allow_anonymous` to relax it, unlike the HTTP door an operator explicitly
opts an agent into. Exposure over the network is always an explicit opt-in,
never a default; the code door is a distinct decision, not derived from it.

### Building a runtime outside an HTTP app

`loom.ai.build_agent_runtime(config, *, root, names=None, ...)` is the
assembly `create_app` itself uses: it resolves the engine, loads the artifacts
under `root`, compiles them and returns the `AgentRuntime` unentered. `names`
restricts it to some agents; `registry`, `container`, `deps`, `sql` and
`use_case_mcp` default to empty, and `engine_provider` replaces the provider
resolved from `ai.engine`.

```python
from loom.ai import build_agent_runtime
from loom.core.identity import Identity

runtime = build_agent_runtime(ai_config, root=repo_root, names={"seller_reply"})
async with runtime:
    result = await runtime.run("seller_reply", prompt, identity=Identity(subject="batch"))
```

ETL steps reach agents declaratively instead, through
[`WithAgent`](../etl/agent-steps.md).

### Nesting: how deep an agent may call another

```yaml
ai:
  max_agent_depth: 1   # default
```

`max_agent_depth` bounds the longest chain of nested agent runs one task may
open, **counting the top-level run itself**. A use case reaches a named
agent through an
[`Agent()` marker](../rest/use-case-dsl.md#agent-marker--reaching-a-named-agent),
and that handle's `run` counts as one more entry in the same chain the run
that reached the use case already opened.

Defaults to `1`: the top-level run alone already consumes the whole budget,
so an `on_output` hook — or any other use case an agent's run invokes — that
itself declares an `Agent()` marker finds no depth left, and the whole class
of agent-calls-agent cycles is unreachable without deliberately raising this
value. Raising it buys deliberate nesting (a hook that escalates by asking a
second agent, say) at the cost of a chain that can grow that much deeper
before `AGENT_CALL_TOO_DEEP` catches a runaway one — see
[what the marker refuses](../rest/use-case-dsl.md#what-the-agent-marker-refuses).

### Conversation memory belongs to the application

An artifact may declare a `conversation` use case beside `on_output`: for
every run that carries a `conversation_id`, the runtime executes it before the
model starts and it returns the prior turns as opaque, engine-native bytes —
or `None` on the first turn. The engine replays them and hands the turn's new
messages to the `on_output` use case as `messages`, so the application
persists them under its own `conversation_id`, in its own store, under its own
tenancy rules. Loom stores nothing, caches nothing and never reads a message;
no history travels on the wire in either direction. See
[`conversation`](artifacts.md#conversation--loading-the-prior-turns).

## An application that is only agents

An application does not need a model, a use case or a REST interface to run an
agent. Its manifest module declares `AGENTS` and nothing else, discovery runs in
`manifest` mode, and persistence is switched off:

```python
# myapp/manifest.py
AGENTS = ["agents/*.yaml"]          # globs resolve against app.code_path
```

```yaml
# config/api.yaml
app:
  name: incident-triage
  discovery:
    mode: manifest
    manifest:
      module: myapp.manifest

persistence:
  backend: none

ai:
  engine: pydantic-ai
  models:
    default:
      provider: bedrock
      model: <model id>
      region: eu-west-1
      output_mode: native          # optional, see Model providers
  endpoints:
    incident-triage:
      enabled: true
      auth: jwt
```

```bash
pip install "loom-kernel[rest,ai-pydantic,ai-bedrock]"   # swap the provider extra for yours
```

The `sqlalchemy` extra is not needed: `create_app` imports the SQLAlchemy backend
only inside the `sqlalchemy` wiring, so an agents-only process never loads it.
(`loom.celery` still imports it at module level; this recipe is about the REST
application, not every loom module.)

What `persistence.backend: none` means:

- No unit of work and no repositories. A granted `usecase` capability still
  runs through the kernel executor, just without a transaction around it.
- Discovered models are accepted but **not compiled**, and they get no
  repository. Declaring them under `none` is harmless and pointless.
- A `database:` section is ignored, silently.
- Deferred job dispatch does not fire. `JobService.dispatch` queues the call to
  run after the unit of work commits, and with no unit of work nothing flushes
  the queue, so a job dispatched from a use case is dropped without an error.
  An agents-only application that needs background jobs keeps a real backend.

Only `manifest` discovery can describe this application: the `interfaces` and
`modules` engines require at least one module path, and their errors — like the
`RuntimeError` raised when discovery finds no use case, no interface and no
agent — say so by naming `app.discovery.mode: manifest` and `AGENTS`.

`persistence.backend` still defaults to `sqlalchemy`; loom never infers `none`
from the absence of models or of a `database:` section. Leaving the default in
place with no models starts the application with a WARNING that names
`persistence.backend: none` as the way out, and still requires a reachable
database.

## When loom is not enough: the escape hatch

Loom is a wrapper over pydantic-ai, and a wrapper you cannot step around is a
ceiling. `native_agent` is the step around it: an async context manager that
hands back the very `Agent` this deployment runs, the dependency bundle of
the calling identity, and the artifact's spend caps already projected, so
calling code can drive pydantic-ai with its own API — `iter()`,
`run(event_stream_handler=...)`, `capture_run_messages()`, a capability of
its own.

```python
from loom.ai.engines.pydantic_ai import native_agent

async def execute(self, triage: AgentHandle[Assessment] = Agent("incident-triage")):
    async with (
        native_agent(triage) as access,
        access.agent.iter(
            "classify this page", deps=access.deps, usage_limits=access.usage_limits
        ) as run,
    ):
        async for node in run:
            ...  # your own decision between nodes
```

Loom's chain-cycle, depth and admission bounds hold for the whole block —
there is no supported way to drive the hatch outside them, so skipping
`max_concurrent_runs` is never the default path. Pass `access.usage_limits`
explicitly — pydantic-ai enforces no cap you do not pass it yourself. To add
a tool for this one run only, pass `toolsets=`/`capabilities=` to `iter()`
rather than registering it on the shared agent (see the rule below).

It lives in `loom.ai.engines.pydantic_ai`, not in `loom.ai`: importing an engine
is the explicit act of leaving loom's neutral surface, and the neutral
`AgentHandle.native()` returns `object` precisely so the engine package is the
one that names the type.

The full account of what still holds and what is lost is written once, on
`AgentHandle.native()`'s own docstring — read it before relying on this hatch
for anything beyond the sketch above.

**One rule.** The `agent` you receive is the single object serving every run of
that agent in this worker. Read it, run it, never mutate it: registering a tool,
a validator or a capability on it changes every other run, including the ones
loom itself drives. `access.shaped_agent` — the same spec with no output
validator registered — stands in for it when the plan declares `output_check`
and this run overrides the output shape: pydantic-ai refuses a run-level
`output_type` on an agent holding one.

## Where to go next

- **[Artifact reference](artifacts.md)** — the complete `spec_version: 1`
  format, field by field, and the folder layout an agent lives in. It includes
  [`on_output`](artifacts.md#on_output--a-use-case-run-once-per-completed-run),
  the use case the runtime executes once per completed run with the validated
  output, and
  [`conversation`](artifacts.md#conversation--loading-the-prior-turns), the
  use case that loads the prior turns of a `conversation_id` before the model
  starts.
- **[Model providers](providers.md)** — the provider matrix, and why an
  OpenAI-compatible endpoint is one provider covering many vendors.
- **[A2A surface](a2a.md)** — publishing an agent to other agents, and exactly
  what the card does and does not say.
- **[MCP deployment](mcp.md)** — where an MCP server goes in your topology, and
  the rule that decides `mcp` versus `usecase`.
- **[Observability](observability.md)** — OTel and Logfire, including an honest
  account of what the traces do not yet give you.
