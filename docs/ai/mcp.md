# MCP Deployment

MCP (Model Context Protocol) is how an agent reaches tools that belong to
**someone else's** service. This page is about where such a server goes in your
topology, and — more importantly — when you should not be using MCP at all.

## The shape: a sideways call, never a gateway

An MCP server is a **separate service the agent calls sideways**. It sits beside
your application, not in front of it.

```
                    ┌──────────────────────────┐
   HTTP / A2A  ────► │  your loom application   │
   (authenticated)   │  ┌────────────────────┐  │
                     │  │    AgentRuntime    │  │
                     │  └─────────┬──────────┘  │
                     │            │             │
                     │   usecase / sql grants   │
                     │            │             │
                     │  ┌─────────▼──────────┐  │
                     │  │ your use cases, DB │  │
                     │  └────────────────────┘  │
                     └────────────┬─────────────┘
                                  │  mcp grant: an outbound https call
                                  ▼
                     ┌──────────────────────────┐
                     │  MCP server (separate    │
                     │  service, someone else's │
                     │  tools)                  │
                     └──────────────────────────┘
```

```{admonition} An MCP server is never a gateway in front of your API
:class: warning
If a diagram shows traffic entering through an MCP server and continuing into
your application, that diagram is wrong. Your application's front door is its
authenticated HTTP surface. An MCP server placed in front of it terminates
the caller's identity, replaces your authentication with its own, and turns
every governed capability into an anonymous one. MCP is an **outbound** edge
from the agent, always.
```

## Configuring a server

The artifact **names** a server; it never locates it:

```yaml
# ai/agents/incident-triage/agent.yaml
capabilities:
  - kind: mcp
    server: runbooks
    include: ["search_*"]      # empty means all
    exclude: ["execute_*"]     # applied after include
```

```yaml
# config/api.yaml
ai:
  mcp_servers:
    runbooks:
      url: https://runbooks.internal.example.com/mcp
      headers_ref: ${secrets:/loom/runbooks/api-key}   # resolved, never literal
      timeout_ms: 20000
```

That separation is what lets the same artifact run against a staging server and
a production one with no edit.

### The handshake deadline vs. the call deadline

Three deadlines govern one MCP connection: one for the handshake, and two
that race each other on every call an agent makes.

| Deadline | Answers | Configured by |
|---|---|---|
| Handshake | How long to wait for the connection and the `initialize` exchange | `ai.startup_timeout_ms` (default `10000`) |
| Call (transport) | How long to wait for one tool call's response | the server's own `timeout_ms` (default `20000`) |
| Call (supervisor) | How long an agent's own tool call may run before loom's supervisor cuts it off, coded `TOOL_TIMEOUT` | the plan's `policies.tool_timeout_ms` (default `20000`) |

`ai.startup_timeout_ms` is the same published budget that already bounds
opening every live client concurrently at start-up; it now reaches the MCP
client's own handshake deadline too, so the number an operator configures is
the number a slow `initialize` is actually bound by. The server's `timeout_ms`
now governs **both** paths that call it: the use-case path, which it always
governed, and the agent's own tool calls' transport-level wait, which used to
run on the engine's undocumented 300-second default instead.

That transport wait is not the only deadline an agent's own call was ever
under, though: `guarded_toolset` already wrapped every MCP tool call an agent
makes in `asyncio.timeout(policies.tool_timeout_ms / 1000)` (`capability_call`,
in `loom/ai/engines/pydantic_ai/_guards.py`), before and after this change,
raising `TOOL_TIMEOUT` on expiry. This is the table's third row, "Call
(supervisor)" — the *model-facing* MCP path, contrasted below with the
marker's own `Mcp()` grant (`loom/ai/runtime/_grants.py`), whose own
`asyncio.timeout` guard raises the same `TOOL_TIMEOUT` directly to the caller,
never a refusal value the model reads: a use case invoked outside a run has no
model to hand the refusal to. With both deadlines at their default of `20000`
ms, the two races to the same number with different outcomes: if the
supervisor's `asyncio.timeout` wins, the call fails `TOOL_TIMEOUT` and the
run ends; if the transport wins first, `guarded` turns the failure into a
refusal value the model reads and may retry on its own. Whichever deadline a
deployment cares about winning should be set strictly shorter than the
other — in general, `policies.tool_timeout_ms` at or below the server's
`timeout_ms` gives the model a chance to see and react to the refusal, rather
than ending the run outright.

Under `transport: http` (the default) the URL must be `https://`, carry no
credentials in its userinfo and no query string — compilation refuses anything
else and redacts the URL in the error, so the message cannot leak the credential
it just rejected. `headers_ref` is a reference the deployment's secret resolver
looks up; a literal secret is rejected fail-closed.

**Under the SSE transport, the server's `timeout_ms` governs a second thing as
well.** Whenever a server declares `headers_ref` or `auth`, loom builds its
transport explicitly rather than letting the client infer one, and the same
`timeout_ms` value that bounds one tool call's response also reaches
`SSETransport.sse_read_timeout` — the deadline of the server's *idle event
stream*, not of one call. A server whose SSE stream today survives 300 seconds
of silence between events can therefore be cut off at the shorter `timeout_ms`
a deployment names for "one call". A server declaring neither `headers_ref`
nor `auth` keeps FastMCP's own inferred transport and is unaffected. This is a
real, documented behaviour change, not a hidden one — raise `timeout_ms` for
an SSE server whose event stream legitimately idles longer than one call
should ever take.

### stdio: a subprocess in your container

The MCP specification also defines a stdio transport, where the client launches
the server as a **subprocess** and speaks to it over its standard input and
output. Most published servers ship that way, so loom accepts it — in the
deployment's configuration, never in the artifact:

```yaml
# config/api.yaml
ai:
  mcp_servers:
    runbooks:
      transport: stdio
      command: uvx
      args: [mcp-server-runbooks]
      env:
        RUNBOOKS_TOKEN: ${secrets:/loom/runbooks/token}
```

Know what you are choosing. The server runs **inside this container, as this
process's child**: it shares the identity, the file system, the network and the
instance credentials of the worker, and there is no connection to authenticate —
which is why `headers_ref` and `auth` are refused under `transport: stdio`. Its
lifetime, its crashes and its memory are the worker's problem, it scales with
your workers rather than with the tool's own load, and it cannot be deployed,
rolled back or monitored on its own. A server you operate yourself is better off
behind an address; stdio is for the servers you only consume.

What stdio does not do:

- **it does not supervise the child.** A dead subprocess fails the call that
  finds it, exactly as a dead HTTP server does. A call made through `Mcp()`
  after that one spawns a new child (see "A session that died after it opened
  is replaced by the next call"); an agent's own capability does not;
- **it spawns one process per server, for the whole worker.** Start-up spawns
  it and every agent granted that server speaks to that one child. It dies with
  the last holder of the connection — normally the runtime, briefly a straggler
  run that outlived it — so none survives the worker. Two entries of
  `ai.mcp_servers` are two children even when they run the same command;
- **it does not inherit your environment.** The child receives only `HOME`,
  `LOGNAME`, `PATH`, `SHELL`, `TERM` and `USER` plus what `env` declares, so a
  secret in the worker's environment cannot leak into the tool by accident;
- **it does not precompile your command.** The handshake budget is
  `ai.startup_timeout_ms` (see [The handshake deadline
  vs. the call deadline](#the-handshake-deadline-vs-the-call-deadline)
  below), so a cold `uvx`/`npx` download must complete inside it: raise
  `startup_timeout_ms`, or install the server in the image and let `command`
  run it.

Values in `env` reach loom already resolved — `${secrets:…}` is an OmegaConf
resolver that runs before this configuration is validated — so loom cannot tell a
resolved secret from a literal one. It rejects only what cannot be a value at
all (spaces, braces, quotes, userinfo), which is what catches a broken
interpolation; keeping real secrets out of the file is the deployment's job.

### Failures happen at start-up

- an unreachable server fails start-up **by name**, under `startup_timeout_ms`,
  rather than hanging the ASGI lifespan — the MCP client's own handshake
  deadline is now derived from this same setting (see [The handshake
  deadline vs. the call deadline](#the-handshake-deadline-vs-the-call-deadline)
  above), so a server that is merely *slow* to connect gets the whole budget
  you configured, not a fixed five seconds nobody could adjust;
- an `include`/`exclude` filter that matches **no tool the server actually
  offers** fails start-up — a filter that silently matches nothing is how an
  agent quietly loses a capability it was granted;
- every client opens **concurrently** under one shared start-up deadline, so the
  budget does not scale with the number of servers.

Tool filters are matched against the tools the server really advertises, not
against what the artifact hoped for.

### Booting without the network: `ai.remote_clients`

Granting `kind: mcp` makes every server the grant names a start-up dependency.
That is right in production and awkward everywhere else — a laptop, a CI job, an
environment brought up before its side-cars. `ai.remote_clients` says how a
client that will not open is treated:

```yaml
ai:
  remote_clients: optional     # required (default) | optional
```

| Value | A client that fails to *connect* |
|---|---|
| `required` (default) | Aborts start-up, by name, with `MCP_SERVER_UNREACHABLE` — or `A2A_AGENT_UNREACHABLE` for a remote agent. |
| `optional` | Logged at WARNING and dropped; the runtime starts without it. |

The switch is process-wide and covers the remote agents of `ai.a2a_agents` as
well as the servers of `ai.mcp_servers`. An unknown value fails configuration
load with `REMOTE_CLIENTS_UNKNOWN`, naming the key and both accepted values.

Under `optional`, the WARNING carries the error code and the registered name
only: the transport's own reason is an arbitrary library's exception text and
can name a URL, so it goes to DEBUG, where an operator asks for it deliberately,
rather than into routine logs on every boot. The health probe reports the
dependency `unavailable` once its first pass has run — for every server **an
agent declares**. A server named only by a use case's `Mcp()` marker is not
covered; see [The health probe does not cover a server reached only this
way](#the-health-probe-does-not-cover-a-server-reached-only-this-way).

A slow-but-live server now spends real budget under `optional`, not a fixed
five seconds. Because the handshake deadline is `ai.startup_timeout_ms`
(above), a server that is merely slow — not down — is given the whole
configured budget before it is dropped, exactly as it would be under
`required`. A deployment relying on the old five-second ceiling to fail fast
and move on now waits up to `startup_timeout_ms` for that one server before
tolerating it, and the concurrent open of every other declared server shares
that same clock: one slow server can consume the group's whole budget, and
when the shared deadline does expire, every server whose connection had not
completed yet is reported `MCP_SERVER_UNREACHABLE` — named individually, but
the cause may be a single slow neighbour rather than a fault of its own.
Lower `startup_timeout_ms` if failing fast matters more than giving a slow
server room to connect.

**The per-server handshake deadline never fires first.** Each MCP toolset
carries its own `init_timeout`, and the group of concurrent connection
attempts is wrapped in its own deadline — but both are the *same*
`ai.startup_timeout_ms`. The group's deadline is not armed when the
connection attempts themselves start; it is computed once, as an absolute
clock reading (`AgentRuntime._startup_deadline`), at the very top of
`__aenter__`, before `_verify_sql_readonly`, `_verify_invoker` and
`_verify_mcp_connections` run — three synchronous checks with no timeout of
their own — and only then handed to `_open_clients`, which arms
`asyncio.timeout_at` on that same absolute reading. So the group's deadline is
always armed strictly *before* any per-server handshake even begins, and
by the time a connection attempt starts, part of the shared budget is already
spent, which is what makes the group's clock always at least as tight as —
in practice, strictly tighter than — any one server's own handshake budget.
The same absolute reading is reused, unless start-up already tolerated an
unreachable server, by `_verify_tool_filters` right after `_open_clients`
returns, so one shared clock covers the whole of start-up, not only the
handshake. The per-server deadline exists in the code (`SharedMcpToolsets`'s
`init_timeout`, applied to every toolset it builds) but is structurally
dominated by the group's: the diagnostic a slow server produces is always the
aggregated `MCP_SERVER_UNREACHABLE` list the group timeout raises for every
connection still in flight, never a per-server timeout that singles it out. A
future change that wants a genuinely slower server to self-identify would
need the per-server budget to carry a margin below the group's, not merely
equal it.

`optional` tolerates a network that is not there. It tolerates nothing else, and
three carve-outs are deliberate:

**A missing client factory is still fatal.** A plan declaring an `mcp` grant in a
deployment that wired no MCP client factory is a wiring bug, not an offline
network. It is collected apart, where the factory is found missing, never told
from a connection failure by reading its message, and aborts start-up under
**both** values.

**Tool-filter verification still fails closed for a server that did open.** The
waiver covers only servers that never connected: a server with no session is
skipped, and a filter on it does not fail start-up. A server that opened has its
declared filters verified as usual, and a listing that times out still aborts
start-up under `optional`. Because a tolerated connection failure has already
spent the shared start-up budget, the verification pass is given a fresh
`startup_timeout_ms` rather than the exhausted one — otherwise one hanging
server would fail the filters of every server that answered.

**Nothing becomes lazy.** A start-up client that never opened is not reconnected
by the runtime; reconnecting it needs a supervisor task that does not exist yet.
The run path holds the *same* toolset the runtime opened, so when that toolset
never connected it is simply not open, and the run finds out for itself.

Be clear about what that costs. The engine enters the toolset once per run,
before it sends anything to the model, so a server that is *still* unreachable
fails the run as a whole with a provider error — not as a per-tool refusal the
model could work around. And once the network returns, each run opens the
connection and closes it again when it ends, because nothing outside the run
holds it: that is one connection, and on a server that registers clients
dynamically one registration, **per run** rather than per worker.

That recovery belongs to the agent path alone. A server reached through an
`Mcp()` marker does not recover: its grant is resolved once, when the runtime
is entered, so a tolerated outage keeps every marker call failing
`TOOL_UNAVAILABLE` until the worker restarts.

`optional` is for a laptop, a CI job or a side-car that has not come up yet;
it is not a production posture.

## Authentication

The MCP specification standardises OAuth 2.0 for HTTP transports, so an
authenticated server is the expected case. Loom ships **no login flow of its
own** and hard-codes no vendor: a server names a strategy, and the deployment
supplies it.

The artifact never changes. It keeps saying `server: runbooks` whether that
server needs no credential, a fixed key, or a token exchange.

### Which one to reach for

| Your server wants | Use |
|---|---|
| A key in a custom header, e.g. `X-API-Key` | `headers_ref` |
| `Authorization: Bearer <token>` | `auth: {kind: bearer}` |
| The standard OAuth 2.0 flow | `auth: {kind: oauth}` |
| Anything else — a token exchange, an identity provider, renewal logic | a strategy you register |

### A fixed key: `headers_ref`

`${secrets:...}` is an OmegaConf resolver, so the value that reaches loom is
already the resolved payload. That payload is **one `Name=value` header pair**:

```yaml
ai:
  mcp_servers:
    knowledge:
      url: https://kb.internal.example.com/mcp
      headers_ref: ${secrets:/loom/kb/api-key}     # stores e.g. X-API-Key=abc123
```

Anything richer — several headers, a value carrying spaces, a credential that
must be renewed — belongs in a strategy. A payload that is not one `Name=value`
pair is refused at start-up with `MCP_HEADERS_REF_INVALID` rather than silently
sending nothing. Note that `Authorization: Bearer <token>` is *not* expressible
here, deliberately: the space is what the inline-credential check refuses. Use
`kind: bearer`, below.

### A strategy: `auth`

```yaml
ai:
  mcp_servers:
    catalog:
      url: https://catalog.internal.example.com/mcp
      auth:
        kind: bearer                               # Authorization: Bearer <token>
        token_ref: ${secrets:/loom/catalog/token}
    directory:
      url: https://directory.internal.example.com/mcp
      auth:
        kind: oauth                                # the client's own flow
    orders:
      url: https://orders.internal.example.com/mcp
      auth:
        kind: agent-session                        # a deployment's own
        session_url: https://orders.internal.example.com/auth/agent/session
        bootstrap_ref: ${secrets:/agents/prod/agent-sales}
```

`kind` names an entry point in the group `loom.ai.remote_auth`; every other key in
the block is passed to it as a **keyword argument**. Loom registers three, all
thin delegations to what the libraries already provide:

| `kind` | Settings | What it does |
|---|---|---|
| `oauth` | — | Runs the MCP client library's own standard OAuth flow. Loom implements no part of it. |
| `bearer` | `token_ref` | Sends `Authorization: Bearer <token>`. |
| `static` | `headers_ref` | Fixed headers, from the same payload as the shorthand above. |

`bearer` exists because the strategy must **compose the header itself**. The
composed value carries a space, and configuration refuses a space precisely so
that no literal credential can hide in one; a token on its own — a JWT is
base64url with dots — passes that test. So the deployment stores the token and
loom writes the header.

`headers_ref` and `auth` are **mutually exclusive** on one server: two ways to
set credentials on one connection is ambiguous, and compilation refuses it with
`MCP_AUTH_CONFLICT`.

### Two HTTP libraries, one callable

A strategy is handed to an HTTP client, and the two outbound transports do not
use the same one.

An `mcp` grant becomes a pydantic-ai `MCPToolset`, which connects through one of
fastmcp's HTTP transports — `StreamableHttpTransport`, or `SSETransport` when the
URL ends in `/sse`. Those transports special-case OAuth and pass any other auth
object straight through to their client, and their client is **`httpx2`**. An
`a2a` grant goes to a client loom builds itself, with **`httpx`**.

Each library accepts an auth object only when it is an instance of *its own*
`Auth` class, a two-tuple, or a callable. So an `httpx.Auth` subclass is refused
by the MCP transport's client, and an `httpx2.Auth` subclass by the A2A one.
**Loom adapts neither**, and publishes no recipe for a class that satisfies both
at once: nothing in its test suite would keep such a recipe honest.

| Grant | Client library | A class must subclass |
|---|---|---|
| `kind: mcp` | `httpx2`, reached through fastmcp's transport | `httpx2.Auth` |
| `kind: a2a` | `httpx`, in the client loom builds | `httpx.Auth` |

**A plain callable is the supported answer for both.** Each library wraps a
callable in a `FunctionAuth` of its own, so one function serves either transport
— and whatever flavour either library moves to next. It takes the outgoing
request, sets its headers, and **returns** it:

```python
def incident_api_key(*, key_ref: str):
    """Register this as a strategy: it returns the callable both clients wrap."""

    def add_key(request):
        request.headers["X-API-Key"] = key_ref
        return request

    return add_key
```

Returning the request is not optional: the wrapper's flow is
`yield self._func(request)`, so a callable returning `None` sends `None`.
Nothing in the inner signature names a library, which is exactly the point — the
request it receives is of whichever flavour drove it. The two strategies loom
ships, `bearer` and `static`, are this shape.

A callable is a single-shot flow: it never sees the response, so it cannot
inspect a `401` or renew. A strategy that needs the response is a class.

### Writing your own

The contract is the HTTP client's own
[`Auth`](https://www.python-httpx.org/advanced/authentication/), not an
abstraction of loom's — nobody has to learn one of ours, and an existing `Auth`
class works with no adapter, provided it is the flavour of the transport that
will use it (above). This example authenticates an MCP server, so it subclasses
`httpx2.Auth`; the identical class written for an A2A agent subclasses
`httpx.Auth`. Register it from **your own package**; loom does not change:

```toml
# pyproject.toml of your own distribution
[project.entry-points."loom.ai.remote_auth"]
agent-session = "my_package.auth:AgentSessionAuth"
```

A worked example — a server exposing a session endpoint, where the agent
presents a long-lived bootstrap secret and receives short-lived tokens:

```python
import httpx2       # the flavour the MCP transport's client uses


class AgentSessionAuth(httpx2.Auth):
    """Exchange a bootstrap secret for a token, renewed when rejected."""

    def __init__(self, *, session_url: str, bootstrap_ref: str) -> None:
        self._url = session_url
        self._ref = bootstrap_ref
        self._token: str | None = None

    def auth_flow(self, request):
        if self._token is None:
            self._token = yield from self._mint()
        request.headers["Authorization"] = f"Bearer {self._token}"
        response = yield request
        if response.status_code == 401:            # expired or revoked
            self._token = yield from self._mint()  # one renewal, not a loop
            request.headers["Authorization"] = f"Bearer {self._token}"
            yield request

    def _mint(self):
        response = yield httpx2.Request(
            "POST", self._url, json={"secret_path": self._ref}
        )
        return response.json()["access_token"]
```

Retry-with-a-refreshed-credential is the library's standard generator shape;
loom does not reimplement it.

```{warning}
`requires_response_body` is honoured by `Auth`'s **own** base flow, not by the
client. A strategy that overrides `async_auth_flow` — which any asynchronous
token exchange must — replaces the very code that reads the flag, and has to
`await response.aread()` itself before touching the body. Setting the attribute
and overriding the flow leaves the body unread, silently.
```

If you are designing such an endpoint: this is OAuth 2.0 `client_credentials`
by another name — `secret_path` is the client id and the bootstrap secret is the
client secret. Using the standard grant means every MCP client works with no
custom code on either side. Two properties are easy to add early and painful
later: the bootstrap secret is a long-lived bearer credential, so plan rotation
and per-agent revocation; and if the client names the secret path, scope the
server's read permissions to that prefix and log failed attempts per path.

### What compilation guarantees

- A `kind` that resolves to **no installed entry point** fails at compile time
  with `MCP_AUTH_STRATEGY_UNKNOWN`, naming the strategy and listing what is
  registered — not at the first message in production.
- **No literal secret anywhere in the block.** Every setting is held to the same
  fail-closed reference test as `headers_ref`, and the rejection never repeats
  the value it rejected (`MCP_CREDENTIALS_INLINE`).
- A strategy that cannot be built from its settings fails at start-up with
  `MCP_AUTH_STRATEGY_INVALID`.

The group is `loom.ai.remote_auth`, not `loom.ai.mcp_auth`: one registry serves
the MCP servers of `ai.mcp_servers` **and** the remote agents of `ai.a2a_agents`
(see [a2a.md](a2a.md)), because the contract is the HTTP client's and knows
neither protocol. A strategy returning a callable is registered once and granted
to either; one registered as a class is granted to the transports of its own
flavour. The one exception is `kind: oauth`, which delegates to the MCP client
library's own flow: an A2A agent naming it is refused with
`MCP_AUTH_STRATEGY_INVALID` rather than connected without a credential.

### One instance per server

The authentication object is built **once per server and shared by every agent
granted it**. The credential belongs to the deployment, not to the agent: a
renewing strategy holds the live token, so sharing means one renewal instead of
one per agent, and no burst of simultaneous logins when several agents start
together.

What is shared is what the strategy returned. A class instance is shared as
itself, so a strategy that must renew once, for everybody, stays a class and
keeps that identity. A callable is shared as itself too, but each client wraps
it in a `FunctionAuth` of its own, so two clients built from one credential no
longer hold the same `client.auth` object — they hold two wrappers around one
function. For a fixed header that is a distinction without a difference, and it
is the reason a stateful strategy is a class.

```{admonition} Public contract
:class: note
`shared_mcp_auth` and `shared_a2a_auth` are exported, and their return type has
widened from `httpx.Auth | str` to what a client accepts, which now includes a
callable. Code doing `isinstance(value, httpx.Auth)` on the result stops
matching for the built-in `bearer` and `static` strategies.
```

## Reuse the agent's connection from Python

A `kind: python` factory of the same agent can wrap a remote tool — run one
canonical query instead of letting the model dictate it — without opening a
second connection. Its `ToolsetContext.remote(server)` returns the worker's
shared session for one of the agent's own `mcp` grants, so the registration and
the credential above are still resolved once per worker. The context is
build-time only, the reach is bounded to that agent's grants, and calls through
the session bypass the grant's `include`/`exclude`. Details and failure codes in
[the `python` capability](#python-application-owned-toolsets).

## Reach a server directly, with no agent in the middle

A use case can declare `Mcp(server, include=[...])` in its own `execute`
signature and reach a configured server directly — no agent, no `kind: mcp`
capability, nothing compiled for a model to call. The deployment still
declares `ai.engine` and installs its extra — the MCP client factory comes
from that provider — so what this sheds is the filler agent artifact, not the
engine. The client is the same one
the worker already opened for `ai.mcp_servers`, so this costs no second
connection: it is a second, independently declared filter over the same
shared session, checked at start-up against the server's real tool list, the
same way an agent's own `mcp` capability is checked. See [the `Mcp()`
marker](../rest/use-case-dsl.md#mcp-marker--reaching-an-mcp-server-directly)
for the complete example, anchored by
[`tests/integration/ai/test_use_case_mcp_marker_test_double.py`](https://github.com/MassiveDataScope/loom-py/blob/master/tests/integration/ai/test_use_case_mcp_marker_test_double.py).

The rule below — your own tools are a `usecase` grant, not an MCP one — is
unchanged by this: `Mcp()` is for reaching *someone else's* server directly
from application code, the same ownership boundary the rule already draws.

### The health probe does not cover a server reached only this way

Under `ai.remote_clients: optional` (above), the health probe reports a
server `unavailable` only for a server **an agent declared** — it walks
compiled agent plans, and a server named solely by a use case's `Mcp()`
marker is outside its reach. Concretely: a deployment whose only MCP server
is reached through `Mcp()`, never through an agent, **boots successfully and
reports `ok`** even when that server never connected, and the failure
surfaces only on the first business request that reaches it, as
`TOOL_UNAVAILABLE`. Extending the probe to a use-case-only server is future
work, not something this version does.

### A session that died after it opened is replaced by the next call

A server that connected at start-up can still go away later: a transport error
under an in-flight call, a redeploy of the server, a stdio child that exits.
The worker's one session is held open for its whole life, and that holder is
also what stops the underlying client from ever being re-entered — so before
this was handled, every call through `Mcp()` failed from then on, in a
millisecond each, until the worker restarted, while the health probe kept
answering `ok`.

`_ToolsetSession` now replaces the session itself. Nothing can be asked of the
client: once its transport is gone it still reports itself connected. The
evidence is the call. A tool's own error comes back as a result flagged
`is_error`; **an exception out of the round trip is the transport failing**.
So:

- **the call that found it dead is raised as it is, and never repeated.** It
  may have run on the server, and a tool with side effects is not run twice on
  your behalf. Retrying it is your decision;
- **the next call opens a new session first**, against the same connection —
  over stdio, that is a new child process. Concurrent calls that find it dead
  renew it once;
- **a renewal that cannot connect raises**, and the call after it tries again:
  an outage costs one connection attempt per call, bounded by the handshake
  deadline, and ends with the first call made after the server is back;
- **suspecting a healthy session costs one reconnection.** A single failed call
  is not proof the session died; opening another is cheap and harmless, so it
  is not weighed further.

This covers the `Mcp()` path and whatever else calls through the runtime's
session. **An agent's own `mcp` capability does not recover yet**: the compiled
agent holds the toolset it was built with, and swapping that needs the engine
build to go through an indirection it does not have. Two other things are
unchanged: a server that never connected at start-up under `optional` is still
not reconnected (above), and the health probe still does not see a
use-case-only server.

### A use case's call runs in parallel with other grant views

A use case's tool call goes through `_ToolsetSession`
(`loom/ai/engines/pydantic_ai/_mcp.py`) — the same reference-counted
`MCPToolset` the model's own tool calls and every other grant view over
that server share: another use case's `Mcp()`, and an agent handle's own
`handle.mcp(server)`. None of them takes a lock over the others, so they
all run concurrently, the same way the model's own calls always have.

`timeout_ms` bounds the wait exactly where you would expect on the
unwrapped, concurrent path: `_ToolsetSession.call_tool` holds no lock over
the shared `MCPToolset`, so a caller cancelled by its own deadline returns
immediately — the round trip it started is simply abandoned, not waited
out, and the underlying JSON-RPC client keeps every neighbour's own
in-flight response matched to its own request id regardless.

Only the serialised fallback changes this: a session this engine did not
open falls back to `SharedMcpSession`'s single lock
(`loom/ai/runtime/_mcp.py`), which shields and drains the call it is
currently holding — a caller cancelled by its own deadline still waits out
that in-flight round trip before the lock is released and its cancellation
reaches it (`shield_and_drain`, `loom/ai/_concurrency.py`). That drain
exists only because a locked, single-framed session would otherwise leave
its next holder desynchronised; it is the cost of the lock, not of the
timeout.

## The rule: your own tools are a `usecase` grant

Here is the decision that actually comes up, and the one this page exists for.

You have an operation in your own application — `incidents.get_incident` — and
you want the agent to call it. You *could* stand up an MCP server in front of
your own API and grant `kind: mcp`. **Don't.** Use a `usecase` grant:

```yaml
capabilities:
  - kind: usecase
    keys: [incidents.get_incident]
```

Reaching your own application over MCP costs you three things, concretely:

**1. A localhost hop.** The call leaves the process, crosses the loopback
interface (or worse, a load balancer), gets serialised, authenticated,
deserialised, and comes back. You pay a full HTTP round trip, a second
serialisation of the same payload, and a second set of timeouts and retries — to
reach code that was one function call away.

**2. The caller's identity.** This is the real cost. The `usecase` path carries
the **caller's** `Identity` into the executor: a use case declaring `Caller()`
runs as the human who invoked the agent, and every rule keyed on that identity
applies. Go out through MCP and that identity terminates at the boundary. What
arrives on the other side is whatever service credential the MCP client was
configured with — which means the agent now reaches things the caller could not,
and your audit log records a service account instead of a person. That is not a
performance regression; it is a different security model, arrived at by
accident.

**3. The unit of work and the rules.** `ApplicationInvoker` gives you the
transaction boundary, the rule evaluation, the error taxonomy and the
observability span **for free**, because that is what invoking a use case
already means in this framework. Over MCP you are outside all of it: a separate
transaction, no shared unit of work, rules re-evaluated under a different
identity or not at all, and an error taxonomy flattened into an HTTP status.

So the rule is simple:

| The tool belongs to… | Use |
|---|---|
| **this application** | `kind: usecase` |
| this application's read-only warehouse | `kind: sql` |
| this application's own Python, with no use-case key | `kind: python` |
| **another service** | `kind: mcp` |
| **another agent** | `kind: a2a` |

MCP is for crossing an ownership boundary. If you own both sides, you are not
crossing one, and the protocol is buying you nothing while costing you the three
things above.

```{note}
The reverse direction is a legitimate and different question: **publishing**
your own tools as an MCP server for *other* people's agents. That is a real
use case — it just is not how *your* agent should reach *your* tools.
```

## Operational notes

**One connection per server per worker.** The connection is shared by the whole
worker: start-up and every agent granted the server work over one `MCPToolset`,
whose entries are reference-counted, so ten agents naming one server are one
session and not eleven. That matters beyond sockets — a server that registers
clients dynamically sees one registration and one credential resolution per
worker, not one per agent. A server a use case names with `Mcp()` joins that
same one — it is folded into the set the worker opens under the very same key,
so a deployment where an agent and a use case both name a server still opens
it once.

Sharing the toolset is deliberate and is *not* the same as sharing a serialised
session: concurrent runs keep issuing their calls in parallel, so one agent's
`tool_timeout_ms` bounds only its own call and never leaves a neighbour waiting
for a remote that has not answered.

**One server name, one connection.** Because the worker keeps a single client
per name, two agents naming one server must resolve it to the same transport,
address, credential and deadline. Two grants that disagree abort start-up with
`MCP_CONNECTION_CONFLICT`, naming the server and both agents; `include` and
`exclude` are per-agent views over the shared connection and never conflict.

**Sessions are lifespan-scoped, and the last holder closes them.** The runtime
opens every client inside a single `AsyncExitStack` and releases them in strict
reverse order from the task that entered it. That release is a reference-count
decrement, not necessarily the close: if a run is still in flight it holds the
last reference and the actual close happens there. This is safe for a stronger
reason than task affinity — the client keeps its session in an `asyncio.Task`
of its own, created precisely so it outlives the individual context-manager
scopes that enter and leave it, so the closing task is not the one the session
is bound to.

**Every result is untrusted input.** A tool result from a remote server is
data from another system, exactly like a remote agent's answer. The agent's
blast radius remains the intersection of its grants and the caller's identity —
a malicious tool result can steer the model, but it cannot widen a grant.
