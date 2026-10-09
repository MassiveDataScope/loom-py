# Agent Artifact Reference (`spec_version: 1`)

The artifact is the file a human or a generator writes. It is **engine-agnostic
and vendor-agnostic**: no engine, provider, model identifier, URL or credential
is representable in it. Those fields do not exist in the format, which is what
lets one artifact move from a laptop to staging to production unchanged.

Unknown keys are a **decoding failure**, not a silently dropped value. In a file
that governs permissions, ignoring a key you do not understand is fail-open.

```{admonition} Format stability
:class: important
`spec_version: 1` is **not** experimental. A definition that validates today
keeps validating and compiling for the whole major line (FR-056a). The
programmatic API around it is experimental — see
[Stability](overview.md#stability).
```

## Folder layout

An agent is a **directory**, not a loose file. The artifact sits at its root and
everything that travels with it sits beside it:

```
myapp/
├── ai/
│   └── agents/
│       ├── incident-triage/
│       │   ├── agent.yaml
│       │   └── skills/                  # this agent's own skill library
│       │       └── severity-rubric/
│       │           └── SKILL.md
│       └── market-analyst/
│           └── agent.yaml               # no skills of its own
└── config/
    └── api.yaml
```

Point the deployment at the directories with a glob:

```yaml
ai:
  specs: ["ai/agents/*/agent.yaml"]
```

or declare them on the application manifest's `AGENTS` attribute instead. The
two are **mutually exclusive**: declaring both is a compilation error, because
an application must have exactly one answer to "which agents do I run".

Why a directory per agent: a `skills` capability written `library: ./skills`
resolves **beside the artifact**, so the prompt material travels with the agent
that uses it. A bare name (`library: rubrics`) resolves instead against
`ai.skills_root`, for libraries several agents share. `..` is not representable
in the pattern, so a library can never escape its own directory.

## A complete artifact

Every field of version 1, in the order the schema declares them:

```yaml
spec_version: 1
name: incident-triage
description: Investigates production incidents by combining warehouse data, tools and a remote agent.
instructions: >-
  Investigate the reported incident, gather the supporting evidence and propose the next
  remediation step. State explicitly when the evidence is inconclusive.
model_role: reasoning
output:
  kind: type_ref
  ref: myapp.domain.incidents:IncidentReport
on_output:
  usecase: incidents.record_report
conversation:
  usecase: incidents.load_conversation
capabilities:
  - kind: usecase
    keys:
      - incidents.get_incident
      - incidents.append_timeline_entry
  - kind: sql
    connection: observability_readonly
    max_rows: 1000
    max_result_bytes: 2097152
  - kind: mcp
    server: runbooks
    include: ["search_*"]
    exclude: ["execute_*"]
  - kind: skills
    library: ./skills
  - kind: python
    factory: myapp.tools.metrics:build_metrics_toolset
  - kind: a2a
    agent: oncall
    include: ["page_oncall"]
policies:
  retries: 3
  tool_timeout_ms: 30000
  max_iterations: 20
  run_timeout_ms: 300000
metadata:
  owner: platform-reliability
  tier: critical
  runbook: incident-response
```

## Top-level fields

| Field | Required | Default | Notes |
|---|---|---|---|
| `spec_version` | yes | — | Always `1`. Read first, before any other field. |
| `name` | yes | — | `^[a-z][a-z0-9_-]{0,62}$`. Unique in the application. **Published** in the A2A card. |
| `description` | yes | — | Non-empty. **Published** in the A2A card. |
| `instructions` | yes | — | A literal string, or a non-empty sequence of named blocks. **Never published.** Never a place to encode authorization. See below. |
| `deps_type` | no | — | Declares the agent's state shape: `dict`, or a `module:Symbol` reference. Sugar over `deps_schema`. Mutually exclusive with `deps_schema`. See below. |
| `deps_schema` | no | — | Declares the agent's state shape directly, as a JSON Schema object. Mutually exclusive with `deps_type`. See below. |
| `model_role` | no | `default` | `^[a-z][a-z0-9_-]{0,31}$`. A logical role, never a vendor name or a model id. |
| `output` | yes | — | The declared answer shape. See below. |
| `on_output` | no | — | Use case executed once per completed run with the validated output; see below. |
| `conversation` | no | — | Use case executed before a run that carries a `conversation_id`; returns the prior history; see below. |
| `capabilities` | no | `[]` | Explicit grants. Empty means the agent can only talk. |
| `policies` | no | see below | Execution limits. |
| `metadata` | no | `{}` | Free-form `string: string` labels. **Never published**, never read by the runtime. |

### `instructions` are not authorization

The single most important rule on this page. Instructions steer behaviour; they
never grant or restrict access. An agent's blast radius is the intersection of
its **capability grants** and the **caller's identity** — never what its prompt
says. A prompt that says "only read the orders table" is a hint to a model that
may ignore it; a `sql` grant on a read-only connection is enforced by the
database.

### `instructions` — a literal string, or named blocks

The short form is one literal block with no name:

```yaml
instructions: "Investigate the reported incident and propose the next step."
```

The long form is an ordered, non-empty sequence of blocks, each with its own
`text`, an optional `name` and an optional `template`:

```yaml
instructions:
  - name: tone
    text: "You are the appraiser. Never invent a figure."
  - name: context
    text: "Appraising a {{marca}}, {{anios}} years old, {{km}} km."
    template: handlebars
```

A block reaches the model in authored order. `name` is optional; it never
contains `:` and is never the literal `agent`, because the engine reserves
both. With `template` absent, `{{` is **literal text** and reaches the model
unchanged — it is never inferred from the presence of `{{` in the prose.
`template: handlebars` is the only value accepted today, and it renders the
block against the run's state (see below): declaring it on a block while the
artifact declares no state is a compilation error, because there is nothing
to render against.

A templated block's `name` never becomes an addressable id on the engine's
own side — it names the block in compilation issues and start-up
diagnostics only. The engine can name a callable through its own
`@agent.instructions(name=...)` route, but that route appends after every
block passed at construction, which would push every templated block behind
every literal one and destroy the order an artifact authored. loom takes the
order and accepts the nameless template.

### The artifact's state — `deps_type` and `deps_schema`

An artifact may declare a **state** a caller supplies per invocation, so a
templated instruction block can render facts specific to one call rather than
only the agent's fixed prose. Declaring neither field means the agent has no
state, which is what every artifact means today.

There is exactly **one mechanism**, spelled three ways:

```yaml
deps_schema:                            # canonical: JSON Schema, directly
  type: object
  properties:
    marca: {type: string}
    km:    {type: integer}
  required: [marca]
```

```yaml
deps_type: myapp.agents:AppraisalDeps   # sugar over the schema above
```

```yaml
deps_type: dict                         # open: an explicit waiver of validation
```

`deps_type: <symbol>` resolves the reference at compile time and calls
`msgspec.json.schema()` on it; from that point the artifact is
indistinguishable from one that wrote the schema by hand. Declaring both
`deps_type` and `deps_schema` is a compilation error.

A strict `pydantic.BaseModel` subclass (`model_config["extra"] == "forbid"`)
is accepted for `deps_type` exactly as a `msgspec.Struct` is: decoding is
strict, the state a run renders when no `state` is sent comes from the
model's own field defaults, and a request `state` with an undeclared field
fails with `422 INVALID_STATE`, unchanged from the Struct path. State is
always decoded strictly regardless of library — `"5"` for a declared `int`
is rejected, never coerced — whereas a pydantic `type_ref` *output* is
validated with pydantic-ai's own coercion (see "Output" below).

The consequences an author needs, stated rather than left implicit:

* **`deps_type: <symbol>` buys validation at start-up — every templated
  marker is checked against the derived schema before the artifact ever
  serves a request — and validation plus normalisation at the request
  boundary. It does not buy a typed object in `ctx.deps`: a capability
  reading the run's dependencies (`RunContext.deps.state`) always sees a
  plain mapping, never an instance of the declared symbol.
* **`deps_type: dict` waives marker validation.** No schema exists to check
  a template against, so a misspelt marker survives the build and renders as
  the empty string on every request — the documented cost of the open form.
* **A templated block's `name` is not an addressable engine id** — see
  above. It exists for diagnostics only.
* **A stateful artifact cannot be published over A2A.** The A2A handler runs
  an agent with no state and the message shape it reads carries a prompt and
  a conversation id, nothing else; rendering a declared state that never
  arrives would degrade silently. An artifact declaring `deps_type` or
  `deps_schema` and listed in `ai.a2a.expose` fails **start-up**, naming both
  halves of the conflict. Resolve it by removing the state declaration or by
  removing the artifact from `expose`.
* **A required field with no default, like `marca` above, must be supplied.**
  Omitting `state` on a call fills every other field with its own declared
  default, but a field the schema marks `required` without one has no
  default to fill in, so the call fails with `STATE_REQUIRED` instead of
  silently rendering an incomplete state.
* **`ai.max_state_bytes`** bounds the raw `state` a caller may send in one
  request body, the same way `ai.max_prompt_bytes` bounds the prompt — a
  `state` over its cap is refused with its own `413`, naming `state`. State
  is spent as tokens on every request of a run, since it is rendered into
  the instructions, so this cap matters even for a deployment that never
  worried about request size before.

```{admonition} Security: caller-supplied state renders into instructions
:class: danger
The `state` a caller sends is rendered into the agent's instructions with
**no escaping**. Declaring a template is an explicit opt-in by the artifact's
author, and it is prompt-injection surface: whatever the caller puts in
`state` can shape the prose the model reads. What loom guarantees instead is
that **`identity`, the application container and the caller-bound invoker are
unreachable from a template** — a template renders against the state mapping
alone, never against the wider dependency bundle a capability call runs
with, so injected text can shape a prompt and can never reach a credential
or an application service.
```

### `model_role`

The artifact names a role; the deployment binds it:

```yaml
# agent.yaml               # config/api.yaml
model_role: reasoning      # ai.models.reasoning: {provider: bedrock, model: ..., region: ...}
```

A role an artifact declares but the deployment does not bind fails **start-up**
with `MODEL_ROLE_UNBOUND`. See [Model providers](providers.md).

## `output` — the declared answer shape

Structured output is mandatory: there is no "just give me text" mode, because a
declared shape is what makes an agent's answer consumable by the code that
called it.

**`json_schema`** — the canonical form, and what a generator emits:

```yaml
output:
  kind: json_schema
  schema:
    type: object
    required: [answer, confidence]
    properties:
      answer: {type: string}
      confidence: {type: number, minimum: 0, maximum: 1}
```

**`type_ref`** — the shortcut for hand-written applications. The reference is
resolved and validated at compile time:

```yaml
output:
  kind: type_ref
  ref: myapp.domain.incidents:IncidentReport
```

The reference is `module:Symbol`. Filesystem paths are not representable by the
pattern.

A strict `pydantic.BaseModel` subclass (`model_config["extra"] == "forbid"`) is
accepted alongside a strict `msgspec.Struct`. The two are validated
differently: a pydantic `type_ref` is handed to pydantic-ai itself as the
run's `output_type`, so pydantic-ai validates the answer with its own
coercion, feeds a rejection back to the model and retries within
`policies.retries`; a `msgspec.Struct` `type_ref` is decoded strictly by loom
and fails fast on the first rejection.

### `output_check` — demanding the shape of the answer

`output_check` names an `OutputCheck` (`loom.ai.OutputCheck`): a pure,
synchronous predicate over the mapping the engine parsed from the model's
answer. It returns `None` to accept, or the text the model must read to
correct itself:

```yaml
output_check: "myapp.agents.checks:incident_answer_complete"
```

```python
from typing import Any
from collections.abc import Mapping

def incident_answer_complete(answer: Mapping[str, Any]) -> str | None:
    if not answer.get("root_cause"):
        return "The answer is missing a root cause; consult the incident timeline."
    return None
```

A rejection drives a real retry, bounded by `policies.retries`, inside the
engine's own run — loom's outer retry loop is not involved, so the check
still works for an agent holding a capability.

The check receives the mapping the engine parsed, not loom's decoded output,
and its return value is never substituted for the answer: loom decodes the
model's own bytes independently, so a check that builds and returns a
different mapping has that mapping discarded. Work that needs to read data
belongs in `on_output`, which runs outside the engine.

Declaring `output_check` changes how the run streams: loom withholds the
answer's deltas until it passes the check, then emits them, rather than
relaying them token by token. An artifact with no check streams exactly as
it does today; the change is per artifact, never a deployment-wide switch.

## `on_output` — a use case run once per completed run

```yaml
output:
  kind: type_ref
  ref: myapp.domain.triage:TriageReport
on_output:
  usecase: incidents.record_triage   # a use-case key of the registry; must not also be a grant
```

For every run that **completes** with an output validated against the declared
shape, the runtime executes that use case **exactly once**, as the caller,
through the normal use-case path — executor, rules, unit of work. The use
case's return value comes back to the caller as `hook_result`.

It is **not a tool: the model never sees it.** It never enters the
instructions, it is never offered to the model, and the model cannot call it,
skip it or choose its arguments. Whether a triage is recorded is decided by the
deployment that wrote the artifact, not by the model on each run — which is
what makes the record deterministic. Granting the same operation as a
`kind: usecase` tool gives you the opposite: a record that exists only when the
model felt like calling it, with whatever arguments it wrote.

Only a completed run fires the hook. A run that ends in an `error`, breaches a
declared limit (`run_timeout_ms`, `max_iterations`, …) or whose client leaves
the stream before the `final` event never runs it.

### What the use case receives

The command nests the validated output under `output` and offers the run's
context beside it:

| Name | Value |
|---|---|
| `output` | The validated answer, as **one nested value**, whatever the artifact's `output` block declares. |
| `messages` | This run's **new** messages in the engine's serialised form, as `bytes`; `None` on a run that carried no conversation. See [`conversation`](#conversation--loading-the-prior-turns). Never return `cmd.messages` or the stored thread from the hook: `hook_result` is encoded as-is. |
| `interaction_id` | Identifier the runtime mints for every admitted run. |
| `conversation_id` | The request's `conversation_id`, verbatim; `None` when the request carried none; over A2A it is never `None`: loom mints a `contextId` when the client sends none. |
| `subject`, `mechanism` | The caller's identity. |
| `agent`, `provider`, `model` | The agent's name, and the provider and model its `model_role` resolved to. |

**The Command declares what it wants** and receives only that: the runtime
filters the offered names down to the ones the Input declares, so a strict
Command (`forbid_unknown_fields=True`) decodes without listing every context
name. Because `output` is always nested, a field called `subject` inside the
model's answer can never shadow the caller's.

```python
class RecordTriageCommand(Command):
    output: TriageReport                # the type_ref type itself; dict[str, Any] also works
    interaction_id: str
    conversation_id: str | None = None
    agent: str
    model: str
    # `subject`, `mechanism`, `provider` are offered but not declared: filtered out.


class TriageRecorded(msgspec.Struct, frozen=True):
    triage_id: str


@use_case_key("incidents.record_triage")
class RecordTriage(UseCase[Triage, TriageRecorded]):
    def __init__(self, triages: TriageRepository) -> None:
        self._triages = triages

    async def execute(
        self,
        cmd: RecordTriageCommand = Input(),
        caller: Identity = Caller(),      # the agent's caller
    ) -> TriageRecorded:
        report = cmd.output
        await self._triages.save(
            Triage(
                id=cmd.interaction_id, conversation_id=cmd.conversation_id,
                subject=caller.subject, agent=cmd.agent, model=cmd.model,
                incident_ref=report.incident_ref, severity=report.severity,
                confidence=report.confidence,
            )
        )
        return TriageRecorded(triage_id=cmd.interaction_id)
```

The verdict the on-call engineer gives later ("wrong severity") is an ordinary
use case of your application, called with the `interaction_id` the app already
holds. Loom stores nothing.

### The compile-time rule

The compiler proves that the run can feed the use case, so a missing field is
never discovered at the end of a paid run. The key is resolved against the
same registry as a `kind: usecase` grant, and the use case's `execute` must
take an `Input()` whose required fields are all among `output` and the context
names above. Four coded issues:

| Code | When |
|---|---|
| `ON_OUTPUT_USECASE_UNKNOWN` | The key is not registered. |
| `ON_OUTPUT_INPUT_UNSATISFIED` | The use case cannot be fed from a run: it is not compiled, its `execute` declares primitive parameters, declares no `Input()`, or its Input requires a name the run does not offer. `internal` and `calculated` command fields and `Caller()` are never demanded. |
| `ON_OUTPUT_USECASE_ALSO_GRANTED` | The same key also appears in a `kind: usecase` grant. A hook use case must not be callable by the model. |
| `ON_OUTPUT_INVOKER_MISSING` | Start-up: an agent declares a hook but the dependency bundle carries no `invoker` bound to the caller. Probed before any client opens, so a misconfigured deployment fails at start-up rather than after every run. |

The offline validator (`python -m loom.ai.validate`) accepts the field but
does not resolve the key: it runs only the configuration-independent phases,
and a use-case registry is deployment state. The four issues above surface
when the application compiles its agents.

### `on_output` versus a `kind: usecase` grant

Same vocabulary, same registry, same caller identity, same executor — and
opposite owners. A **grant** is a tool the *model* may call, when and how it
decides. A **hook** is a use case the *runtime* calls, once, with the
validated output. That is why one key cannot be both. Deciding whether an
operation is a tool at all follows
[the rule](mcp.md#the-rule-your-own-tools-are-a-usecase-grant) on the MCP page;
`on_output` is for the operation that must happen after every answer,
regardless of what the model did.

### When the hook fails, the run fails

The model's answer is withheld and the caller gets a coded error. The hook is
**never retried**: it runs outside the engine's retry loop, and `HOOK_FAILED`
is not a retriable code.

| The hook… | The caller gets |
|---|---|
| raises | `500` `HOOK_FAILED` with a fixed message — `the output hook failed; the detail is recorded server-side`. The exception never reaches the caller; the server log carries it under the `interaction_id`. |
| raises `Forbidden`, `Unauthenticated`, `RoleNotAllowedError` or `RolesNotBoundError` | `403` `UNAUTHORIZED`, exactly as a `kind: usecase` tool would. |
| exceeds `tool_timeout_ms` | Cut at the bound and reported as `HOOK_FAILED`. |
| is cancelled internally | `HOOK_FAILED`: a hook that cancels itself is a hook failure, not a client exit. |

On `/stream` the failure is a single `error` frame and no `final`.

An anonymous caller — an endpoint declaring `allow_anonymous` — runs the hook
with `Caller()` bound to `ANONYMOUS`, so the command's `subject` is `""`.
Recording those is the deployment's choice: a use case whose rules refuse an
anonymous caller answers `403 UNAUTHORIZED` like any other denial.

### Timing and the concurrency permit

The hook runs after the model has finished, but **inside** the run: the
`max_concurrent_runs` permit is held until it ends. It is bounded by the same
`tool_timeout_ms` as a capability call, so the worst-case duration of an
admitted run is `run_timeout_ms + tool_timeout_ms + 1 s` of grace given to a
cut hook to observe its cancellation — and
`tool_timeout_ms + 1 s + run_timeout_ms + tool_timeout_ms + 1 s` when the
artifact also declares a [`conversation`](#conversation--loading-the-prior-turns)
loader, bounded the same way before the model starts. A hook that ignores
that grace runs detached afterwards; the permit is released regardless.

A client that disconnects while the hook is running does not interrupt it:
the hook is shielded, so a record that has begun finishes or fails cleanly and
its unit of work is committed or rolled back as usual.

A hook cut at its bound is rolled back; on a non-transactional backend such
as DynamoDB, partial writes may remain, so a hook use case should be
idempotent on `interaction_id`, and `HOOK_FAILED` means "unknown", not "not
recorded".

### What the caller receives

Every result carries `interaction_id`, hook or no hook: `AgentResult`, the
`final` and `error` SSE frames, and every HTTP error body. It is `null` only
when the failure happened before a run was admitted — a `422` body, a `429`
`TOO_MANY_RUNS` — because a fixed shape beats a conditional one. `hook_result`
is on `final` and on `AgentResult`, `null` when the artifact declares no hook.
The hook's return value is a client-facing DTO delivered verbatim to the caller
— public on an `allow_anonymous` mount — so return a purpose-built struct,
never a domain entity.

In process, `AgentAnswer` and `AgentResult` also carry `provider_details`: what the provider
reported about the final answer beyond the answer itself, verbatim and opaque
to loom — a decision model's confidence and probabilities per field (see
[`typesafe`](providers.md#typesafe-fills-an-output-it-never-writes-text)),
another provider's finish reason — or `None` when it reported nothing. Like
`messages`, it never reaches the wire: the `/run` body and the `final` frame
keep their four keys.

The request body accepts an optional `conversation_id`: a string of 1 to 128
characters that loom never reads and never keys anything on. It selects the
conversation the loader receives when the artifact declares
[`conversation`](#conversation--loading-the-prior-turns), and it is copied
verbatim into the hook's command; loom stores nothing under it. An
out-of-range value is a `422`.

`POST /agents/incident-triage/run` with
`{"prompt": "Checkout latency doubled since 09:40…", "conversation_id": "c-42"}`:

```json
{
  "output": {"incident_ref": "INC-1", "severity": "high", "confidence": 0.71, "alerts": ["A-7", "A-9"]},
  "usage": {
    "input_tokens": 1840, "output_tokens": 412, "requests": 3, "duration_ms": 5210,
    "cache_read_tokens": 1200, "cache_write_tokens": 64, "tool_calls": 2,
    "cost": "0.0413", "details": {"reasoning_tokens": 96}
  },
  "interaction_id": "7f3c9a0e4b2d4c1e9a7b5d6e8f0a1b2c",
  "hook_result": {"triage_id": "7f3c9a0e4b2d4c1e9a7b5d6e8f0a1b2c"}
}
```

`usage` is the whole accounting the engine reported, not a selection of it:
the counters any engine would report are named fields, and every other field it
returned — the audio counters, a provider's extras, a counter a newer engine
release adds — rides under its own name in `details`. `cache_read_tokens` is
already included in `input_tokens`, so a model with a warm prompt cache is
compared on the split, not on the total.

Three properties to code against:

- **`cost` is a decimal *string*, or `null` — never a JSON number.** A JSON
  float would round money, so it travels as `"0.0413"`. `usage.cost * runs` is
  `NaN` in JavaScript; parse it with a decimal type. `null` means the engine
  could not price the model, and must not be read as free.
- **`details` is not disjoint from the named counters.** Providers report their
  own entry beside the normalised one — OpenAI's `cached_tokens` next to
  `cache_read_tokens` — and loom does not decide which of the two is
  redundant, so summing `details.*` double-counts.
- **`usage` is open for extension.** A future engine release adds keys to it
  without a major version of loom; decode it into a struct that tolerates
  unknown fields.

Over `/stream`, the last frame is:

```
event: final
data: {"output":{...},"usage":{...},"interaction_id":"7f3c...","hook_result":{"triage_id":"7f3c..."}}
```

Had `RecordTriage` raised, the app would instead get
`500 {"code":"HOOK_FAILED","message":"the output hook failed; the detail is recorded server-side","interaction_id":"7f3c..."}`
— or an `error` frame with the same three fields — and no answer.

## `conversation` — loading the prior turns

```yaml
on_output:
  usecase: incidents.record_turn            # persists the answer and the turn's messages
conversation:
  usecase: incidents.load_conversation      # returns the prior messages, or None
```

For every run that carries a `conversation_id`, the runtime executes that use
case **exactly once, before the engine starts**, as the caller, through the
same path as `on_output` — executor, rules, unit of work. The use case returns
the prior turns of that conversation as opaque `bytes`, or `None` on the first
turn. The engine replays them to the model before the new prompt and hands
back the turn's **new** messages, which reach the `on_output` command as
`messages`; the application persists them under its own `conversation_id`.

Loom stores nothing, caches nothing and never reads a message. The history
lives in one run's locals and nowhere else, so two runs with the same
`conversation_id` load twice, each as its own caller. Like `on_output`, the
loader is **not a tool: the model never sees it**, it never enters the
instructions or the capabilities, and the model cannot choose which
conversation to load. An artifact that declares no `conversation` behaves
exactly as before; a `conversation_id` sent to such an agent reaches no use
case and the engine runs single-shot.

### The loader contract

The runtime offers the loader's Input five names and, exactly as for the
hook, filters them down to the ones the Input declares:

| Name | Value |
|---|---|
| `conversation_id` | The request's `conversation_id`, verbatim. **The Input must declare it**: a loader that cannot know which conversation it loads is refused at compile time. |
| `interaction_id` | Identifier the runtime mints for every admitted run, so an auditing loader can correlate with the run's own log line. |
| `subject`, `mechanism` | The caller's identity. |
| `agent` | The agent's name. |

`provider` and `model` are not offered: a loader keys on the thread and the
caller, never on the model.

The return value is `bytes | None`, checked at run time: `None` on a first
turn, otherwise **one JSON array holding every prior turn** in the format the
engine produces. For the pydantic-ai engine that is pydantic-ai's
`ModelMessagesTypeAdapter` JSON — exactly what `new_messages_json()` produced
on the earlier turns, and exactly what the hook received as `messages`. A
`str`, a `bytearray`, a list or a dict is a failure, never coerced.

**The merge contract.** `messages` is one JSON array per turn, and the loader
must return a single JSON array. The application merges the per-turn arrays
itself — decode each with `json.loads`, extend, re-encode — or stores the
running array and replaces it on every turn. Byte-concatenating two arrays is
not JSON and fails the next turn with `CONVERSATION_LOAD_FAILED`.

```python
class LoadConversationCommand(Command):
    conversation_id: str                    # required: the loader must know the thread
    agent: str                              # scope: a thread belongs to one agent
    # `interaction_id`, `subject`, `mechanism` are offered but not declared: filtered out.


@use_case_key("incidents.load_conversation")
class LoadConversation(UseCase[Thread, bytes | None]):
    def __init__(self, threads: ThreadRepository) -> None:
        self._threads = threads

    async def execute(
        self,
        cmd: LoadConversationCommand = Input(),
        caller: Identity = Caller(),        # tenancy: the thread must belong to this caller
    ) -> bytes | None:
        thread = await self._threads.get(
            owner=caller.subject, agent=cmd.agent, id=cmd.conversation_id
        )
        return None if thread is None else thread.messages    # one JSON array, stored verbatim


class RecordTurnCommand(Command):
    output: TriageReport
    interaction_id: str
    agent: str
    conversation_id: str | None = None
    messages: bytes | None = None           # this run's new messages; None on a single-shot run


@use_case_key("incidents.record_turn")
class RecordTurn(UseCase[Thread, TurnRecorded]):
    def __init__(self, threads: ThreadRepository) -> None:
        self._threads = threads

    async def execute(
        self, cmd: RecordTurnCommand = Input(), caller: Identity = Caller()
    ) -> TurnRecorded:
        if cmd.conversation_id is not None and cmd.messages is not None:
            await self._threads.append(
                owner=caller.subject, agent=cmd.agent, id=cmd.conversation_id, messages=cmd.messages
            )
        return TurnRecorded(interaction_id=cmd.interaction_id)
```

A hook Command may declare `messages: bytes` as required; like
`conversation_id: str`, that fails the run with `HOOK_FAILED` when it carried
no conversation.

The engine trusts the bytes only after validating them, and it applies no
trimming: a long history costs what the model charges for it, and the
request-size bound of the endpoint does not apply to it. Trimming or
summarising is the application's decision, in the loader.

### Tenancy is the application's

The loader runs as the caller: `Caller()` is bound to the run's identity, or
to `ANONYMOUS` under `allow_anonymous`. Whether *this* caller may read *this*
conversation is decided by the use case's rules — the repository lookup by
`owner` and `agent` above — not by loom. Three consequences:

- A loader that looks up by `conversation_id` alone makes every thread
  readable by anyone holding the id. The loader **must** scope by the caller
  — `Caller()` or `subject` — **and** by `agent`, and raise `Forbidden` on a
  mismatch: a thread owned by another subject is then a `403 UNAUTHORIZED`,
  like any other denial.
- Under `allow_anonymous` every caller shares one subject, so a loader keyed
  on `subject` gives every anonymous caller every anonymous thread. Ids must
  then be unguessable, because the id alone is the credential.
- The history is sent to the model verbatim, system-level parts included, so
  the thread store is part of the prompt trust boundary. Keying by
  `(owner, agent, conversation_id)` also prevents replaying agent A's thread
  into agent B; `instructions` are re-applied on every turn regardless.

### The compile-time rule

The compiler proves the run can feed the loader with the rule `on_output`
uses — the key resolves against the registry, the use case is compiled, its
`execute` takes an `Input()` and declares no primitive parameters, every
required Input field is among the five offered names — plus one rule of its
own: the Input declares `conversation_id` (FR-057). Nothing about the return
type is inspected at compile time; the return value is checked on every run.
Four coded issues:

| Code | When |
|---|---|
| `CONVERSATION_USECASE_UNKNOWN` | The key is not registered. |
| `CONVERSATION_INPUT_UNSATISFIED` | The use case cannot be fed from a run — the same reasons as `ON_OUTPUT_INPUT_UNSATISFIED` — or its Input does not declare `conversation_id`. The reason is named in the message. |
| `CONVERSATION_USECASE_ALSO_GRANTED` | The same key also appears in a `kind: usecase` grant. The model would then choose the `conversation_id` argument — a tenancy hole. |
| `CONVERSATION_INVOKER_MISSING` | Start-up: an agent declares a loader but the dependency bundle carries no `invoker` bound to the caller. Probed once with the hook's check; an agent declaring both reports both codes. |

As with `on_output`, the offline validator accepts the field but does not
resolve the key; the issues surface when the application compiles its agents.

Nothing refuses the *same* key on both `on_output` and `conversation`. The
feedability rule catches the common case — a hook Input requiring `output`
cannot be fed by the loader, which offers no `output` — but an Input whose
fields are all optional compiles as both and runs as both. Write two use
cases.

### When the loader fails, the run fails

The loader runs before the model, so a failed load costs no tokens and the
hook never runs. The caller gets a coded error carrying the run's
`interaction_id` and no `usage`: nothing was spent. The loader is **never
retried** — the engine's retry loop never sees it — and
`CONVERSATION_LOAD_FAILED` is an `APPLICATION` error, not a retriable one
(FR-058); only a loader that times out — cut at `tool_timeout_ms`, or raising
`TimeoutError` from its own I/O — is reported as `CONVERSATION_LOAD_TIMEOUT`
(`INFRASTRUCTURE`, retriable, `504`; FR-063). The hook keeps `HOOK_FAILED` on
the same timeout: it runs after the model spent tokens, so a retry is not free.

| The loader… | The caller gets |
|---|---|
| raises anything but `TimeoutError` | `500` `CONVERSATION_LOAD_FAILED` with a fixed message — `the conversation could not be loaded; the detail is recorded server-side`. The exception never reaches the caller; the server log carries it under the `interaction_id`. |
| raises `Forbidden`, `Unauthenticated`, `RoleNotAllowedError` or `RolesNotBoundError` | `403` `UNAUTHORIZED`, exactly as the hook and a `kind: usecase` tool. |
| times out — cut at `tool_timeout_ms`, or a `TimeoutError` of its own | `504` `CONVERSATION_LOAD_TIMEOUT` with a fixed message — `the conversation loader exceeded its time limit`. Retriable; the origin is in the server log. |
| returns anything but `bytes` or `None` — a `str`, a list, a dict | `CONVERSATION_LOAD_FAILED`; the value is never coerced and never logged. |
| returns more than `policies.max_history_bytes` bytes | `CONVERSATION_LOAD_FAILED`; the length and the bound are logged under the `interaction_id`, never returned. loom never trims. |
| returns bytes the engine cannot decode — not JSON, or not one array of messages | `CONVERSATION_LOAD_FAILED`, raised by the engine before any model call. The data is the application's; the engine's `health()` probe is unaffected. |

On `/run` the body is the usual three fields —
`{"code": "CONVERSATION_LOAD_FAILED", "message": "…", "interaction_id": "…"}` —
and on `/stream` a single `error` frame with the same fields and no `final`.

### Timing: the loader holds the permit too

The loader is bounded by the same `tool_timeout_ms` as a capability call and
shielded like the hook, so a client that disconnects while it runs does not
interrupt it: the unit of work is committed or rolled back as usual. The
`max_concurrent_runs` permit is held throughout; the worst-case duration of an
admitted run is given in
[`on_output`'s timing paragraph](#on_output--a-use-case-run-once-per-completed-run).
On `/stream` a slow loader is up to `tool_timeout_ms` of silence on an
already-committed `200`, exactly as admission is today.

### What the engine receives

The runtime hands the engine one neutral value, `Conversation(conversation_id,
history)` — exported from `loom.ai` — only when the artifact declares a loader
and the run carries a `conversation_id`; otherwise it receives `None` and
follows the single-shot path exactly, serialising nothing. `Conversation`,
`AgentResult.messages` and `FinalEvent.messages` are the whole neutral
contract: loom defines no message model, and a second engine defines its own
byte format without any change to the artifact (FR-059). The bytes are the
engine's own format, so switching `ai.engine` invalidates stored histories:
store an engine/format tag beside the bytes, or start new ids.

The pydantic-ai engine passes the decoded history as `message_history=`
together with loom's `conversation_id=`, so pydantic-ai stamps every new
message with the application's own id, and returns `new_messages_json()` of
the run. Retries re-send the same decoded
history; `messages` holds only the successful attempt's new messages. For an
agent answering through an output tool, one turn is three messages — the
request, the response with the output-tool call, and the tool-return request
that closes it — never the prior turns.

One literal to avoid: pydantic-ai reserves `'new'` as a sentinel, so a
`conversation_id` of `"new"` gets its messages stamped with a fresh UUID
instead. Loom does not guard it, because correctness does not depend on the
stamp — the hook's command carries loom's `conversation_id` beside `messages`
— and a refusal would leak one engine's vocabulary into the neutral contract.

```{admonition} Breaking for third-party engines
:class: warning
`AgentEngine.run` and `run_stream` gained a keyword-only parameter,
`conversation: Conversation | None = None`, and the runtime passes
`conversation=` on **every** run. The handshake version is bumped so a
mismatch is refused at load rather than failing every run: an engine built
for this release must declare `LOOM_AI_ENGINE_API = 2`. The engine surface is
experimental (FR-056); the artifact format is unchanged. `FakeAgentEngine`
accepts the parameter and ignores it, so scripted tests stay byte for byte.
```

### Nothing conversational on the wire

Clients keep sending `{"prompt", "conversation_id"}`. The `/run` body has
exactly `output`, `usage`, `interaction_id` and `hook_result`; the `final`
frame has the same four keys; `messages` and `provider_details` appear in
neither, nor in any A2A frame. Over A2A the thread travels as the message's `contextId`, which the
runtime treats as the `conversation_id` (see [the A2A surface](a2a.md#conversations)).

### Two turns

Turn 1 — `POST /agents/incident-triage/run` with
`{"prompt": "Checkout latency doubled since 09:40", "conversation_id": "c-42"}`:
the loader receives `conversation_id: "c-42"` and returns `None`; the model
sees one request; `RecordTurn` receives `messages` — this run's messages,
stamped `"c-42"` — and stores them. The body is the same four keys as any
other run:

```text
{"output": {...}, "usage": {...}, "interaction_id": "7f3c...", "hook_result": {"interaction_id": "7f3c..."}}
```

Turn 2 — same `conversation_id`, prompt `"and the payments queue?"`: the
loader returns the stored array; the model sees the prior turn and then the
new request; `RecordTurn` receives only this turn's messages and appends them.
No message ever appears in either body.

## `capabilities` — the seven kinds

A capability is a **grant**, never a discovery. Nothing is expanded
automatically, and an agent with no capabilities can do nothing but answer from
the prompt.

Every **local** capability runs under the **caller's** identity, not a service
identity. That is the property that makes a grant safe to give: the agent cannot
reach anything the human who invoked it could not reach directly. Remote kinds
(`mcp`, `a2a`) reach their endpoint with the deployment's credential, and
`native` runs inside the model provider — neither is bounded by who called.

### `usecase` — this application's own operations

```yaml
- kind: usecase
  keys: [incidents.get_incident, incidents.append_timeline_entry]
```

Explicitly listed use-case keys, resolved against the registry at compile time.
An unknown key fails compilation with `USECASE_KEY_UNKNOWN`.

**This is the right kind for your own tools.** See
[the rule](mcp.md#the-rule-your-own-tools-are-a-usecase-grant) on the MCP page.

### `sql` — read-only warehouse access

```yaml
- kind: sql
  connection: observability_readonly
  max_rows: 1000
  max_result_bytes: 2097152
```

The connection must be **read-only**; a writable one fails compilation with
`SQL_CONNECTION_NOT_READONLY`. Both result bounds are **mandatory** — an
unbounded query is not representable (FR-046b). Queries run under the roles
bound to the caller's identity; no path reaches a shared default role.

### `mcp` — tools from a remote MCP server

```yaml
- kind: mcp
  server: runbooks           # named in ai.mcp_servers
  include: ["search_*"]      # empty means all
  exclude: ["execute_*"]     # applied after include
```

The artifact **names** the server; it never locates it. Where it lives, how to
authenticate and how long to wait are deployment facts. A filter matching no
tool the server actually offers fails **start-up**, not the first request — see
[MCP deployment](mcp.md).

### `skills` — packaged prompt material

```yaml
- kind: skills
  library: ./skills          # or a bare name resolved against ai.skills_root
  include: []
  exclude: []
```

Requires the `ai-harness` extra. Two libraries granted to one agent that expose
the same skill name fail compilation with `SKILLS_NAME_COLLISION`.

(python-application-owned-toolsets)=
### `python` — application-owned toolsets

```yaml
- kind: python
  factory: myapp.tools.metrics:build_metrics_toolset
  params:
    max_results: 3
    radius_km: 25
```

A **factory**, never a constructed object: the reference must be callable and is
invoked once at start-up as `factory(context, **params)`.

The first positional is a `ToolsetContext` (exported from `loom.ai`) with three
members:

- `agent` — the name of the agent being built;
- `container` — the application container, to resolve services from;
- `remote(server)` — the agent's shared MCP session for one of **its own**
  `mcp` grants, by server name; the same connection the agent's `mcp` toolset
  runs over, so a tool that wraps a remote query does not open a second one.

The context is a **build-time** object. Resolve what you need in the factory
body and keep it on the toolset you return; do not call `remote()` from a tool
at run time. `remote()` is bounded to the agent's own `mcp` grants: a server the
artifact did not declare is refused at start-up with `PYTHON_REMOTE_NOT_GRANTED`
naming the agent, the factory and the server, whatever else the deployment
knows about that server. Calls made through the session go to the shared
connection directly and are **not** filtered by that grant's `include`/`exclude`;
the toolset already sits behind the authenticated call boundary. See
[MCP](mcp.md#reuse-the-agents-connection-from-python).

`params` is a nested block (never a sibling of `factory:`) passed to the factory
as **keyword arguments**. The factory declares its own named parameters with
defaults:

```python
from typing import Any

from pydantic_ai.toolsets import AbstractToolset

from loom.ai import ToolsetContext


def build_metrics_toolset(
    context: ToolsetContext, *, max_results: int = 3, radius_km: int = 25
) -> AbstractToolset[Any]:
    session = context.remote("metrics")  # one of this agent's mcp grants
    ...
```

The parameter **names** are validated at compile time against the factory's
signature: an unknown key, or a required parameter the artifact does not
supply, fails with `PYTHON_FACTORY_PARAMS_REJECTED` naming the factory and
Python's binding reason. A factory declaring `**kwargs` accepts any key; a
callable whose signature cannot be inspected is accepted as-is. The **values**
are decoded YAML and are not validated: a wrong type surfaces as a Python error
when the factory runs at start-up. `params` carries settings, never secrets; the
artifact's self-description lists the parameter names only.

Compilation refuses a reference that cannot be imported
(`PYTHON_FACTORY_UNRESOLVABLE`), one that is not callable
(`PYTHON_FACTORY_NOT_CALLABLE`) and a `params` block the signature cannot bind
(`PYTHON_FACTORY_PARAMS_REJECTED`). Three failures are raised at start-up, when
the factory runs: `PYTHON_FACTORY_NOT_CALLABLE` when what it returns is not a
toolset, `PYTHON_REMOTE_NOT_GRANTED` when it asks for a server the agent was not
granted, and `PYTHON_FACTORY_FAILED` when the factory itself raises — the
exception class is reported, its message is not.

A toolset caches through `cached_calls`, never by writing its own keys: a
factory typed `(ctx: ToolsetContext) -> AbstractToolset[Any]` returns
`FunctionToolset(cached_calls(ctx.container).bind(MyTools()))`, and every method
marked `@cache_call` is served from the application's `cache:` section. What is
cached, what is refused and why a cached call is never invalidated are in
[Caching any coroutine](../rest/cache.md#caching-any-coroutine).

```{admonition} Breaking change
:class: warning
The factory used to be called as `factory(container)`. It now receives the
`ToolsetContext` as its first positional; a factory written against the old
shape gets a `TypeError` at start-up. `ToolsetFactory` is now the alias
`Callable[..., object]`; the contract above is what the compiler checks.
```

### `a2a` — delegation to a remote agent

```yaml
- kind: a2a
  agent: oncall              # named in ai.a2a_agents
  include: ["page_oncall"]
```

The remote agent's skills become callable capabilities under exactly the same
rules as every other kind. Its output is **untrusted input** — see
[A2A](a2a.md#untrusted-input).

### `native` — tools the model provider runs

```yaml
- kind: native
  tool: web_search           # web_search | web_fetch | code_execution
```

The tool runs in the **model provider's** infrastructure, not in this process:
loom neither implements it nor sees its calls. A grant is checked at compile
time against the model bound to the agent's `model_role`, so a tool the binding
cannot run fails with `NATIVE_TOOL_UNSUPPORTED` naming the provider, the model,
the role and what that binding does admit — never on the first request. The same
tool granted twice is `NATIVE_TOOL_DUPLICATE` — loom refuses it even though the
engine would collapse identical grants, the same rule `skills` follows.

What each binding admits comes from the model class of the installed engine, not
from a table in loom. As of pydantic-ai 2.36 (`Model.supported_native_tools()`;
re-check after upgrading):

| `tool` | `bedrock` | `openai` / `gateway` | `anthropic` |
|---|---|---|---|
| `web_search` | no | yes¹ | yes |
| `web_fetch` | no | no | yes |
| `code_execution` | yes | no | yes |

¹ The class admits it, but the provider narrows it again per model name when the
request is made — OpenAI chat models only run web search on `*-search-preview`
models. A binding that passes compilation can still be refused by the provider
on its first call, and that refusal reaches you as the engine's own error, not
as a loom error code.

What a `native` grant does not get:

- **no `tool_timeout_ms`**: there is no call in this process to bound;
- **no stream events**: the provider's tool calls do not appear in `/stream`;
- **no options**: `allowed_domains`, `max_uses` and the rest are not expressible
  yet; the grant is the tool name and nothing else;
- **no retries**: an agent holding any capability, `native` included, does not
  retry a failed run.

For tools loom itself should call, use `mcp` or `python` instead.

## `policies` — execution limits

| Field | Default | Min | Max |
|---|---|---|---|
| `retries` | `2` | `0` | `10` |
| `tool_timeout_ms` | `20000` | `100` | `600000` |
| `max_iterations` | `12` | `1` | `100` |
| `run_timeout_ms` | `120000` | `1000` | `1800000` |
| `max_history_bytes` | `1048576` | `1024` | `67108864` |
| `max_usd` | *(none)* | `0.01` | `100000` |
| `max_total_tokens` | *(none)* | `1` | `50000000` |
| `max_input_tokens_per_request` | *(none)* | `1` | `10000000` |
| `max_tool_calls` | *(none)* | `1` | `10000` |
| `max_requests` | `50` | `1` | `1000` |
| `on_unpriced_spend` | `serve` | — | — |

`run_timeout_ms` bounds the **whole run**, not one capability call.
`tool_timeout_ms` bounds a single call. `max_history_bytes` caps the serialised
history a `conversation` loader may return; it is a limit, not trimming — loom
never edits the history. An out-of-range value is reported as a
coded issue (`POLICY_OUT_OF_RANGE`) rather than a decoding failure, so it
accumulates with the other problems in the file instead of hiding them.

### `retries` — two axes, one knob

One authored number governs two independent retry loops, and honouring
either loop's outcome for both would double-invoke a granted use case:

- `AgentSpec.retries` (`loom.ai.engines.pydantic_ai._spec`) retries a failed
  tool call, and an answer `output_check` rejects, inside one run — always,
  regardless of what the plan grants.
- `PydanticAIEngine`'s own attempt loop (`_engine.py`, `_may_retry`) retries a
  failed **provider** call across runs — only when the plan holds no
  capability at all.

The second loop stops as soon as the plan grants any capability
(`PydanticAIEngine._may_retry`): by the time a provider call fails, the model
may already have invoked an application operation, and nothing about a
granted use case is idempotent or keyed, so replaying the run would invoke it
again. That is the behaviour `docs/ai/artifacts.md` and
`engines/pydantic_ai/_spec.py`'s own module docstring already document; this
section exists so the field an author reads says the same thing, not a
different rule.

Two things this field does **not** do, on purpose:

- It does not raise a compile-time warning when a capability-bearing plan
  keeps the default. `RETRIES_DEFAULT` is `2`, so every capability-bearing
  artefact that never touched the field would warn — a warning firing on the
  common case trains an operator to ignore warnings, which is worse than no
  warning at all.
- Loom does not "honour" the plan's `retries` for the provider loop once a
  capability is granted. Doing so is exactly the double invocation the guard
  above exists to prevent.

### Spend caps

```yaml
policies:
  max_usd: 2.00
  max_total_tokens: 50000
  max_input_tokens_per_request: 12000
  max_tool_calls: 30
  max_requests: 50
  on_unpriced_spend: serve  # serve (default) | refuse
```

Each of the five projects onto the engine's own `UsageLimits`
(`cost_limit`, `total_tokens_limit`, `per_request_input_tokens_limit`,
`tool_calls_limit`, `request_limit`). A cap left undeclared is `None`, which
the engine treats as "disable that limit" — an artifact declaring no
`policies` behaves exactly as it does today.

`UsageLimits` also carries `input_tokens_limit` and `output_tokens_limit`,
which loom deliberately does not project: `max_total_tokens` and
`max_input_tokens_per_request` already give an operator the two angles —
cumulative and per-request — that bound cost, and this artifact does not
take a position on generation length.

**`max_requests` has always been in force, at `50`.** loom passes no explicit
usage limits to the engine, and the engine substitutes its own default
whenever none is given — a `request_limit` of `50`. Every run this pillar has
ever served has therefore carried this bound; declaring `max_requests` does
not introduce a new cap, it publishes the number that was already deciding
outcomes. The default stays `50` so declaring no `policies` block changes
nothing.

**`max_usd` is elastic, not a hard failsafe: `on_unpriced_spend` decides what
a run does when its cost cannot be fully computed.** The cost `max_usd`
measures is computed by `genai-prices`, not reported by the provider, and is
unavailable for some model/provider combinations, permanently for some and
only for one request's reported identifiers (a gateway prefix, a Bedrock
inference profile, an Azure deployment name) for others. Failing a *completed*
run over that gap destroys work the provider already billed for: the model
answered, the charge stands either way, and refusing to hand back the answer
only costs the caller twice. So the default, `on_unpriced_spend: serve`,
answers anyway and records the gap — in the run's own returned usage
(`details.unpriced_requests`) and in the engine's health, `degraded`, once
observed — rather than in a log line nobody watching the caller would ever
read. `on_unpriced_spend: refuse` is there for an operator who would rather
lose the answer than risk an unenforced cap; it fails the run coded
`COST_NOT_MEASURABLE`, distinct from `USAGE_LIMIT_EXCEEDED` — the cap was
never *evaluated*, not exceeded. `on_unpriced_spend` is inert without
`max_usd`: there is no cap for it to change the behaviour of.

A YAML artifact parses `max_usd` through a binary double before loom converts
it to the `Decimal` it enforces, so a literal with more than about 15
significant digits loses precision; a JSON artifact decodes the same literal
straight to a `Decimal`, with no such rounding — the same literal can
therefore mean a different value depending on which format the artifact is
written in.

**Why a model's cost can be permanently unpriceable.** The cost `max_usd`
measures is computed by `genai-prices`, not reported by the provider, and is
`None` when the model or provider cannot be priced. pydantic-ai's own
response to that is a warning that does not enforce anything
(`CostNotFoundWarning`), so a run with `max_usd` declared could spend without
ever being told it was unbounded if nothing watched for it — which is what
`policies.on_unpriced_spend` and the engine's start-up notice
(`warn_if_model_not_priceable`) are for. A missing catalogue entry is the
typical cause, and it is permanent until the catalogue changes; but
`genai-prices`' own best-effort pricing can also degrade on one response's
inconsistent cache-token counts, a property of that response, not of the
model, that a later response from the same model need not repeat. Neither
the start-up probe nor the per-run guard distinguishes the two causes —
both are treated the same way.

**A configured price closes the gap.** `ai.prices` gives a model its rates,
in USD per million tokens, keyed by the vendor model id the binding names:

```yaml
ai:
  models:
    classifier: {provider: bedrock, model: eu.anthropic.claude-haiku-5-5, region: eu-west-1}
  prices:
    eu.anthropic.claude-haiku-5-5:
      input: 0.11          # uncached input token
      output: 0.55
      cache_read: 0.011    # optional; billed as input when absent
      cache_write: 0.1375  # optional; billed as input when absent
      source: AWS Pricing API eu-west-1   # optional, for the audit trail
      as_of: 2026-10-09                   # optional
```

The compiler carries the bound model's price on the plan (`AgentPlan.price`),
and the engine prices every response with it before `genai-prices` is
consulted, replacing whatever the catalogue would have said: `AgentUsage.cost`
is `(input - cache_read - cache_write) * input + output * output +
cache_read * cache_read + cache_write * cache_write`, per million, since
`input_tokens` already counts the cached tokens. With a price configured,
`max_usd` is enforced, `on_unpriced_spend` never fires and the start-up notice
is not logged. A negative rate is refused when the config is read.


serve` into `refuse`, for every deployment that sets it.** Both
`CostNotFoundWarning` and `CostCalculationFailedWarning` subclass `Warning`
directly, not `UserWarning` — unlike pydantic-ai's own deprecation warnings,
which choose `UserWarning` specifically to stay visible under Python's
*default* filter. A bare `Warning` is exactly what `-W error` elevates to an
exception, so under that flag pydantic-ai raises instead of warning, the
provider call that already returned an answer fails, and
`loom.ai.engines.pydantic_ai._errors.classify` maps it to
`COST_NOT_MEASURABLE` (`_errors.py:_EXCEPTION_CODES`) — the same code
`on_unpriced_spend: refuse` produces on purpose. A strict deployment is
therefore fail-closed on an unpriced response no matter what
`on_unpriced_spend` declares: `serve`'s whole point — answer anyway, since the
provider already billed for it — is silently unavailable, and the caller gets
a discarded answer and a `500` instead. This is not a bug in `classify`, which
must recognise the exception once it is raised (see the totality test in
`tests/unit/ai/engines/test_pydantic_ai_errors.py`, which fails the day
pydantic-ai adds a third bare `Warning` it does not cover); it is a property
of running under `-W error` at all. An operator who wants `on_unpriced_spend:
serve` to mean what it says under a strict interpreter must exempt both
classes explicitly, narrower than blanket `-W error`, for example
`-W error -W default::pydantic_ai.exceptions.CostNotFoundWarning -W
default::pydantic_ai.exceptions.CostCalculationFailedWarning`.

The start-up probe is fed the identifiers the *built* pydantic-ai model
reports — its `model_name` and its provider's `name` — never loom's own
`InferenceTarget.provider`, because billing itself prices on the built
model's reported name; those two diverge for `gateway` and `bedrock`
bindings. The probe only prices the built model's own identifiers once, at
start-up, while every run's bill prices whatever the provider's response
reports for that one request — so the probe is a notice, never a boot
refusal, and a start-up pass finding nothing does not guarantee every run
will price cleanly.

**The `degraded` health this sets never clears itself; only restarting the
worker does.** `PydanticAIEngine._unpriced_spend_observed` is set the first
time any response in any run prices incompletely and is never reset by a
later clean run — deliberately, because the gap is usually permanent for the
bound model (a missing catalogue entry), not a property of one run. It is not
always the model, though: `genai-prices` can also raise on a single
response's own inconsistent cache-token counts, which a later response from
the same model would not repeat — `health()`'s `degraded` detail therefore
does not name the model as the cause, only that a response was seen unpriced.
`details.unpriced_requests`, on a *served* run's own returned usage, says how
many of that run's responses priced incompletely; it does not say which of
the two causes produced that count, and a *refused* run carries no
`details.unpriced_requests` at all — neither cause is distinguishable at
this layer, for either kind of run.

**A spend cap bounds one run, not a conversation, and the whole retried run,
not one attempt.** Every run builds a fresh accounting object, so a
multi-turn thread has no cap on what it spends across its turns — only on
what any one of them does. Within one run, loom's provider-retry loop hands
the same accumulating usage object to every retried attempt, so a cumulative
cap such as `max_usd` or `max_total_tokens` is checked against everything the
run has spent so far, across every attempt — which is what an operator
writing `max_usd` means.

**`RunUsage.cost` staying set does not mean every response priced.** It only
*accumulates* the responses that did; a run of twelve requests where eleven
price and one does not still reports a cost. `on_unpriced_spend` and
`details.unpriced_requests` read the run's own responses, not that
accumulator, to catch exactly that case — and "the run's own" is literal: a
multi-turn conversation's injected history is excluded, so a prior turn's
already-billed, already-evaluated response never counts a second time against
this run's `max_usd`.

`details.unpriced_requests` is counted from the *winning* attempt's messages
alone. Because loom's provider-retry loop starts every retried attempt from
the same conversation, an unpriced response made by an attempt that later
failed and was retried never reaches the winning attempt's history and is
not counted — a known gap, because loom does not wrap each attempt in
pydantic-ai's own `capture_run_messages`, which is built for exactly this
case and captures even a failed or interrupted attempt's partial messages;
this is a loom choice, not a pydantic-ai limitation.

**`max_input_tokens_per_request` is enforced after the fact, not
preemptively.** The engine checks it against the provider's reported input
token count once the response has already been sent and billed — the request
that first exceeds it still runs and still costs, and the run fails right
there, at that same request, rather than reaching a next one. The engine has
a preemptive mode
(`count_tokens_before_request`) that counts tokens ahead of the request, but
it is a per-model opt-in most providers this release binds do not implement
(notably `OpenAIChatModel`, which serves both `openai` and `gateway`), so
loom does not enable it: doing so unconditionally would raise on the very
first request for every model that does not support it.

**`max_usd` and `max_total_tokens` are enforced after the fact too, not
preemptively.** The engine checks `cost_limit` and `total_tokens_limit`
against what the run has already spent or already used *before* sending
each request, and again against the updated total once the response
arrives — in both cases, the request that first pushes the run over the cap
has already been sent and billed, and the run fails right there, at that
same request, rather than reaching a next one. The overspend either cap allows
is therefore bounded by the cost
or token count of one request, not by zero. `max_tool_calls` does not share
this mechanic: the engine checks its projected count against the tool calls
a step is about to run *before* running any of them, so it is genuinely
preemptive — none of that step's tool calls execute if the batch would
cross the cap.

### `max_iterations` versus `max_tool_calls`

The two count different things, and an operator who only knows about one of
them will eventually trip on the other:

- **`max_iterations`** (default `12`) is counted by loom's own supervisor,
  over the event stream — it increments once per `ToolCallEvent` the
  supervisor observes, never once per model response. A step that answers
  without calling a tool does not increment it at all, and a step whose
  model calls several tools in parallel increments it once per call, all
  within that one model request.
- **`max_tool_calls`** is counted by the engine, over *successful* tool
  calls (`UsageLimits.tool_calls_limit`), checked before a step's tool
  calls run — the two caps overlap in what they watch but are not the same
  count, and a breach of either surfaces as a different error code:
  `MAX_ITERATIONS_EXCEEDED` for `max_iterations`, `USAGE_LIMIT_EXCEEDED`
  for `max_tool_calls`.

Reach for `max_iterations` to bound how many tool calls loom's own
supervisor lets a run make before giving up; reach for `max_tool_calls` (or
the other spend caps above) to bound what a run may cost the provider.

## The agent's version — `AgentPlan.fingerprint`

Every compiled plan carries a stable sha256 of what decides its answers: the
instructions as compiled (text and template engine), the output schema, the
output check, the policies, `spec_version`, the kind and name of every
capability (the use case keys, the SQL connection, the MCP server, the skill
library and its skill names, the Python factory, the A2A agent, the native
tool) and the bound provider, model, `output_mode` and `options`. The name,
description, metadata, artifact path, configured price, where a capability is
served from (URL, directory, credentials, timeouts, tool filters) and the
binding's region, endpoint, credentials and `streaming` flag are left out, so
moving a deployment keeps the version and changing the prompt, the model or
the tools does not. ETL steps write it as
`agent_version` (see [Agents in ETL steps](../etl/agents.md)).

The output check enters by its import path (`module:qualname`): editing the
body of the check keeps the fingerprint, renaming or moving it changes it.

## Validating offline

The validator needs no configuration, no credentials and no network. This is
what a generator runs between emitting a file and deploying it:

```bash
python -m loom.ai.validate 'ai/agents/*/agent.yaml'
```

Exit `0` with empty stderr when every artifact is valid. Otherwise every issue is
printed as one line carrying its stable error code, and issues **accumulate** —
a file with three faults produces three lines, not the first one repeatedly, and
a broken file does not hide the files after it:

```console
$ python -m loom.ai.validate 'ai/agents/*/agent.yaml'
OUTPUT_TYPE_REF_UNRESOLVABLE ai/agents/incident-triage/agent.yaml: output type reference 'myapp.domain:Missing' cannot be imported
POLICY_OUT_OF_RANGE ai/agents/incident-triage/agent.yaml: policy 'max_iterations' value 500 is outside the allowed range 1..100
POLICY_OUT_OF_RANGE ai/agents/incident-triage/agent.yaml: policy 'run_timeout_ms' value 10 is outside the allowed range 1000..1800000
$ echo $?
1
```

One class of fault is reported alone: a **structural** failure — an unknown key,
a missing required field, a bad `spec_version` — stops that file at the decoding
step, because there is no valid struct left to run the later phases against:

```console
$ python -m loom.ai.validate 'ai/agents/*/agent.yaml'
SPEC_UNKNOWN_FIELD ai/agents/incident-triage/agent.yaml: unknown field 'engine'; unknown fields are rejected
```

Fix the structure, re-run, and the remaining issues appear together.

## The published JSON Schema

The full format is published as a JSON Schema document so an editor, a linter or
a generator in **any** language can validate an artifact:

```python
from loom.ai.declarative import agent_spec_json_schema, agent_spec_schema_path

agent_spec_json_schema(1)      # the document, as a dict
agent_spec_schema_path(1)      # the path of the file shipped in the distribution
```

The file ships inside the wheel and the sdist at
`loom/ai/declarative/schemas/agent-spec-v1.schema.json`. Extract it and hand it
to any validator — **validating an artifact does not require installing loom**,
which is the point of publishing it at all.

The shipped file and `agent_spec_json_schema()` are the same document byte for
byte, and a test asserts it. Regenerate the file from the structs when the
format legitimately changes; never edit it by hand to make that test pass.
