# The A2A Surface

A2A (Agent-to-Agent) is how an agent talks to agents outside this application.
Two independent directions, both optional. With no `ai.a2a` section nothing is
published and no endpoint exists.

| Direction | What it means | Status |
|---|---|---|
| **Outbound** | your agent delegates to a remote agent (`kind: a2a` capability) | wired end to end |
| **Inbound** | other agents call *your* agent, and read its card | the surface exists; mounting it is a manual `bind_a2a_endpoints` call |

```{admonition} Inbound mounting is not automatic yet
:class: warning
`create_app` wires the HTTP surface (`/run`, `/stream`, `/health`) but does
**not** yet call `bind_a2a_endpoints`. Publishing an agent over A2A today
means calling it yourself against the app you built. This is tracked work,
not a design decision — the projection, the card and the server are complete
and tested; only the automatic wiring in `auto.py` is outstanding.
```

## Outbound — delegating to a remote agent

The artifact **names** a remote agent; the deployment says where it is.

In `ai/agents/incident-triage/agent.yaml`:

```yaml
capabilities:
  - kind: a2a
    agent: oncall
    include: ["page_oncall"]
```

`include` empty means every skill it advertises.

In `config/api.yaml`:

```yaml
ai:
  a2a_agents:
    oncall:
      url: https://oncall.partner.example.com/a2a/rota
      headers_ref: ${secrets:/loom/oncall/api-key}
```

`headers_ref` stores e.g. `X-API-Key=abc123`.

At start-up the remote card is fetched and validated. An unreachable agent fails
start-up with `A2A_AGENT_UNREACHABLE` — by name, not as a hung lifespan and not
as a surprise on the first user request.

A delegation is a capability like any other, with none of the exemptions a
"trusted partner" framing would invite:

- the **caller's** identity governs whether it is permitted;
- `policies.tool_timeout_ms` bounds the call;
- it counts against `policies.max_iterations`;
- it appears in the trace as a capability call under `Scope.TOOL`;
- failure maps to `TOOL_UNAVAILABLE` or `TOOL_TIMEOUT` — infrastructure-class,
  and therefore retriable.

The remote URL must be `https://`, with no credentials in its userinfo and no
query string. Compilation refuses anything else, and the offending URL is
redacted in the error message.

### Authenticating to the remote agent

A remote agent declares its credential exactly as an MCP server does, and loom
resolves it through the same registry — see
[mcp.md § Authentication](mcp.md#authentication) for the full reference:

```yaml
ai:
  a2a_agents:
    oncall:
      url: https://oncall.partner.example.com/a2a/rota
      headers_ref: ${secrets:/loom/oncall/api-key}
    market:
      url: https://market.partner.example.com/a2a
      auth:
        kind: bearer
        token_ref: ${secrets:/loom/market/token}
    orders:
      url: https://orders.internal.example.com/a2a
      auth:
        kind: agent-session
        session_url: https://orders.internal.example.com/auth/agent/session
        bootstrap_ref: ${secrets:/agents/prod/agent-sales}
```

| Key | Meaning |
|---|---|
| `headers_ref` | one `Name=value` pair |
| `kind: bearer` | `Authorization: Bearer <token>` |
| `kind: agent-session` | a strategy you register |

The credential is set on the HTTP client, not on one request, so **every** call
carries it — including the card fetch, which is the *first* request of the
session. An agent that authenticates its card endpoint would otherwise fail
start-up before a skill was ever called.

`kind` names an entry point in the group `loom.ai.remote_auth`, the same group
MCP servers resolve against: the strategy contract is the HTTP client's own
[`Auth`](https://www.python-httpx.org/advanced/authentication/), which knows
nothing of either protocol, so a deployment registers its strategy once and
grants it to either transport. The A2A client is loom's own, built with `httpx`,
while an MCP server is reached through a client using `httpx2` — a strategy
registered as a class is refused by the other flavour, and a strategy returning
a plain callable serves both: see
[mcp.md § Two HTTP libraries, one callable](mcp.md#two-http-libraries-one-callable).

The one strategy an A2A agent cannot use is `kind: oauth`, which delegates to the MCP client library's own flow; naming it
here is refused with `MCP_AUTH_STRATEGY_INVALID` rather than connecting without
the credential the deployment asked for.

What compilation guarantees is what it guarantees for an MCP server:
`headers_ref` and `auth` are **mutually exclusive** (`MCP_AUTH_CONFLICT`); a
`kind` nobody registers fails at compile time naming what is installed
(`MCP_AUTH_STRATEGY_UNKNOWN`); and no literal secret is accepted anywhere in the
block (`MCP_CREDENTIALS_INLINE`, and the rejection never repeats the value).

The authentication object is built **once per configured agent and shared by
every agent granted it**: the credential belongs to the deployment, so a
renewing strategy renews once rather than once per caller.

## Inbound — publishing your agent

```yaml
ai:
  a2a:
    base_url: https://api.example.com
    expose: [incident-triage]
```

`expose` must be non-empty. Empty means **none**, never all. An empty list fails start-up with
`A2A_EXPOSE_EMPTY`, because "publish everything by default" is the wrong
direction for a decision this consequential.

### What the card publishes

Served per agent at `{prefix}/{name}/.well-known/agent-card.json` — `/a2a` by
default. The path is **per agent** rather than at the deployment root, because
`expose` is a list and one root path cannot serve N agents. It is also what lets
the authentication exclusion match the card alone.

```json
{
  "protocolVersion": "1.0.0",
  "name": "incident-triage",
  "description": "Investigates production incidents by combining warehouse data, tools and a remote agent.",
  "url": "https://api.example.com/a2a/incident-triage",
  "version": "1",
  "capabilities": {
    "streaming": true,
    "pushNotifications": false,
    "stateTransitionHistory": false
  },
  "defaultInputModes": ["text/plain"],
  "defaultOutputModes": ["application/json"],
  "skills": [
    {
      "id": "incident-triage",
      "name": "incident-triage",
      "description": "Investigates production incidents by combining warehouse data, tools and a remote agent.",
      "tags": ["agent"]
    }
  ],
  "securitySchemes": {
    "bearer": {"type": "http", "scheme": "bearer", "bearerFormat": "JWT"}
  }
}
```

### The projection, field by field

The card is what a stranger sees. It says **what** the agent does and **never
how it is built**. Every row below is a unit test, not a convention — and the
projection imports neither an A2A library nor a web framework, so the redaction
guarantee is provable in the base wheel:

| Plan field | Card field | Rule |
|---|---|---|
| `name` | `name`, `skills[].id`, `skills[].name` | published |
| `description` | `description`, `skills[].description` | published |
| `spec_version` | `version` | published, as a string |
| `instructions` | — | **never published** |
| `inference` | — | **never published**: no model, no provider, no region, no endpoint, no credentials |
| `capabilities[]` | — | **never published**: no use-case key, no SQL connection, no MCP URL, no remote agent leaks |
| `policies` | — | not published |
| `metadata` | — | **never published** — it carries owner, cost centre and ticket references |
| `A2AConfig.base_url` | `url` | deployment fact |
| what the runtime actually serves | `capabilities` | must match reality |
| the constant `("agent",)` | `skills[].tags` | **fixed**, never derived from `metadata` |
| the authenticator actually in use | `securitySchemes` | derived, never a hardcoded guess |

`skills[].tags` being a constant is the clearest example of the principle: it
would be tempting to derive tags from `metadata`, and `metadata` is exactly
where owner, cost centre and ticket references live. A convenient derivation
would have published all three.

### Security schemes are derived, not assumed

| Authenticator | Published scheme |
|---|---|
| `jwt` | `{"bearer": {"type": "http", "scheme": "bearer", "bearerFormat": "JWT"}}` |
| `api-key` | `{"apiKey": {"type": "apiKey", "in": "header", "name": "X-API-Key"}}` |
| `mtls` | `{"mutualTLS": {"type": "mutualTLS"}}` |
| anything else, or none | `{}` |

A mechanism with no A2A representation publishes **no scheme at all**, never a
bearer guess. A client that acts on a guessed scheme sends a credential the
wrong way.

### Conversations

The A2A `contextId` **is** the `conversation_id`. When an agent declares
`conversation` (see [`conversation` — loading the prior
turns](artifacts.md#conversation--loading-the-prior-turns)), the loader and the
`on_output` hook receive the `contextId` the client sent; when the client sends
none — or an empty string — loom mints one. Either way the id is echoed as
`contextId` on the returned `Task` and on every stream frame, so a client
continues a thread by sending back what it received.

| The message carries | Answer |
|---|---|
| `contextId` absent or `""` | A fresh id is minted and echoed. |
| `contextId` that is not a string, or longer than `CONVERSATION_ID_MAX_LENGTH` (128) | `-32602`, naming the field and the bound — never the value. No run, no span. |
| `taskId` non-empty | `-32001` `Task not found`: loom retains no task, so there is none to continue. Omit it. No run, no span. |
| `taskId` `""` | Treated as absent. |
| `taskId` that is not a string | `-32602`, naming the field. No run, no span. |
| `referenceTaskIds`, `metadata` | Accepted and ignored. |

`taskId` and `contextId` are checked before the message parts, so a message
that continues a task answers `-32001` even without a text part.

Two consequences follow from the id being the same value on both surfaces.
For the same caller, the HTTP `conversation_id` and the A2A `contextId` of the
same agent select the **same** loader conversation: a thread started over one
surface continues over the other. And over A2A the hook's `conversation_id` is
**never `None`**, because a `contextId` is minted before the run starts.

With authenticated callers, isolation is the loader's: it receives `subject`
beside the id and must scope its lookup by both, refusing a thread that belongs
to someone else (see [Tenancy is the application's](artifacts.md#tenancy-is-the-applications)).
A `contextId` is client-controlled input: treat it as untrusted when the loader
or the hook logs or persists it.

Under `allow_anonymous` every caller shares one subject, so nothing but the
`contextId` separates their threads: **the id is the credential**. Whoever
presents an id reads that thread. The start-up WARNING of such a mount says so;
if that is not acceptable, authenticate the callers or leave `conversation`
undeclared on that agent.

## Methods

Transport is HTTPS + JSON-RPC 2.0, streaming over SSE.

| Method | v1 | Notes |
|---|---|---|
| `SendMessage` | **yes** | Synchronous. Returns a `Message`, or a `Task` already terminal. |
| `SendStreamingMessage` | **yes** | SSE, mapped one-for-one from the internal event union. |
| `GetTask` | no | Needs persisted task state. |
| `ListTasks` | no | idem |
| `CancelTask` | no | Cancellation is expressed by disconnecting the stream. |
| `SubscribeToTask` | no | Needs task persistence. |
| push-notification config CRUD | no | idem |

Unsupported methods return a JSON-RPC error naming the method. The card already
advertises their absence, so a conformant client never calls them.

**"No tasks in v1" does not mean no `Task` objects on the wire.** A2A v1.0 is
task-centric even when streaming, so task-shaped events do appear. It means **no
persisted task state and no out-of-band retrieval**: those events live for the
duration of the request and nothing is retained. That is precisely why this
feature needs no storage.

## Event mapping

Both the HTTP/SSE surface and A2A streaming are projections of the *same*
internal event union. There is no second event set to keep in sync.

| Internal event | A2A |
|---|---|
| `text_delta` | `TaskArtifactUpdateEvent` (append text part) |
| `tool_call` | `TaskStatusUpdateEvent` (`working`, **opaque ordinal only** — `step 3/12`) |
| `tool_result` | `TaskStatusUpdateEvent` (`working`, no summary, no payload) |
| `final` | `TaskArtifactUpdateEvent` (output) + `TaskStatusUpdateEvent` (`completed`, final) |
| `error` | `TaskStatusUpdateEvent` (`failed`, final, code in metadata) |

The `tool_call` row is the one that matters. On the internal HTTP surface a
`tool_call` event carries the capability key and its arguments, because that
surface serves authenticated callers inside your own application. Outward, it
projects to an **opaque ordinal**. Redacting the card while publishing the same
information event by event would have been a leak sitting right beside a
guarantee.

## Security

The card advertises the scheme; **enforcement is the REST authentication layer
you already run**, unchanged. The agent layer defines no authentication of its
own. An A2A caller is an identity like any other, and an anonymous one is
refused unless that specific agent opted out.

**Only the card is unauthenticated.** `bind_a2a_endpoints` registers the
well-known card path as the sole authentication exclusion, and start-up fails if
the deployment excludes any other path under the A2A or agents prefix.
Exclusions are matched as **exact strings**, so a hand-written `/a2a` exclusion
would silently open the entire invocation surface — which is why the guard
exists rather than trusting the deployment to write the narrow one.

### Untrusted input

The prompt from an external caller, and the output of any remote agent this
agent delegates to, are **untrusted input**. Treat a remote agent's answer the
way you would treat a form field from the internet, not the way you would treat
a return value from your own function.

This does not, on its own, make an agent dangerous. The agent's blast radius is
the **intersection of its capability grants and the caller's identity** — never
its instructions. A prompt injection can make an agent *try* anything; it cannot
widen a grant, and it cannot borrow an identity the caller does not have. That
is why the grant model and the identity propagation matter far more on this
surface than anywhere else, and why `instructions` are explicitly not an
authorization mechanism.

**Publication is announced.** Start-up logs exactly which agents are externally
published, with their security state, so "we did not realise that was public"
is not reachable from a clean log.
