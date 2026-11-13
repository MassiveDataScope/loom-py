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

The artifact **names** a server; it never locates it.

In `ai/agents/incident-triage/agent.yaml`:

```yaml
capabilities:
  - kind: mcp
    server: runbooks
    include: ["search_*"]
    exclude: ["execute_*"]
```

`include` empty means all. `exclude` is applied after `include`.

In `config/api.yaml`:

```yaml
ai:
  mcp_servers:
    runbooks:
      url: https://runbooks.internal.example.com/mcp
      headers_ref: ${secrets:/loom/runbooks/api-key}
      timeout_ms: 20000
```

`headers_ref` is resolved, never literal.

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
ms, the two race to the same number, and either one ends the run: if the
supervisor's `asyncio.timeout` wins, the call fails `TOOL_TIMEOUT`; if the
transport wins first, the MCP client raises its own request timeout, which
proves nothing about whether the tool ran, so the run fails
`MCP_CALL_INTERRUPTED` (see [A failed call is retried only when it never
ran](#a-failed-call-is-retried-only-when-it-never-ran)). Neither reaches the
model as a refusal it could retry. Set the deadline whose code you want to
see strictly shorter than the other; `policies.tool_timeout_ms` below the
server's `timeout_ms` reports a slow tool as `TOOL_TIMEOUT`.

Under `transport: http` (the default) the URL must be `https://`, carry no
credentials in its userinfo and no query string — compilation refuses anything
else and redacts the URL in the error, so the message cannot leak the credential
it just rejected. `headers_ref` is a reference the deployment's secret resolver
looks up; a literal secret is rejected fail-closed.

A server reached only inside a private network, behind an internal load
balancer without TLS, opts in with `allow_private_plaintext: true`. It admits
`http://` when the host is a private, loopback or link-local address, or a name
under `.internal`, `.local`, `.localdomain`, `.lan` or `.home.arpa`; any other
host is still refused, and start-up logs a warning naming the host:

```yaml
ai:
  mcp_servers:
    gateway:
      url: http://mcp.production.example.internal:8081/mcp
      allow_private_plaintext: true
```

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

In `config/api.yaml`:

```yaml
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
  finds it, exactly as a dead HTTP server does. The next call through the
  server's session spawns a new child, whichever path makes it (see [A failed
  call is retried only when it never
  ran](#a-failed-call-is-retried-only-when-it-never-ran));
- **it does not run on AWS Lambda.** A frozen function keeps no subprocess, so
  a Lambda function refuses `transport: stdio` at start-up (see [MCP on AWS
  Lambda](#mcp-on-aws-lambda));
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
  remote_clients: optional
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

**A server that never opened connects on each use.** The runtime does not
reconnect a start-up client that failed; that would need a supervisor task that
does not exist. The agent path and a `kind: python` factory's `remote()` still
reach the server through the connection's one session, but that session holds
nothing open: each use connects, does its work and closes again. A use is a
run listing the server's tools, or one tool call.

Be clear about what that costs. While the server is still unreachable:

- an agent run granted it fails at the tool listing, before the model is
  called, with `PROVIDER_UNAVAILABLE` (retriable; `503` on an agent endpoint).
  Every run fails this way until the server is back;
- a tool call through the session, such as one from `remote()`, fails with
  `MCP_CALL_INTERRUPTED` and the message "may have run", although nothing was
  sent: the connection itself failed. A renewal of such a session only swaps
  in another toolset that is not connected either, so it never connects
  ahead of the next call.

Once the network returns, every use opens a connection and closes it again,
because nothing holds it between uses: that is one connection, and on a server
that registers clients dynamically one registration, **per listing or call**
rather than per worker.

That recovery belongs to the agent's own `mcp` capability and `remote()`. A
server reached through an `Mcp()` marker does not recover: its grant is
resolved once, when the runtime is entered, so a tolerated outage keeps every
marker call failing `TOOL_UNAVAILABLE` ("the server never connected at
start-up") until the worker restarts. `handle.mcp(server)` does not recover
either: the agent's grants hold only servers that opened, so it raises
`MCP_GRANT_UNKNOWN` for that server until the worker restarts.

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
      headers_ref: ${secrets:/loom/kb/api-key}
```

`headers_ref` stores e.g. `X-API-Key=abc123`.

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
        kind: bearer
        token_ref: ${secrets:/loom/catalog/token}
    directory:
      url: https://directory.internal.example.com/mcp
      auth:
        kind: oauth
    orders:
      url: https://orders.internal.example.com/mcp
      auth:
        kind: agent-session
        session_url: https://orders.internal.example.com/auth/agent/session
        bootstrap_ref: ${secrets:/agents/prod/agent-sales}
```

| Key | Meaning |
|---|---|
| `kind: bearer` | `Authorization: Bearer <token>` |
| `kind: oauth` | the client's own flow |
| `kind: agent-session` | a deployment's own |

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

Credentials are static for the life of the connection, so every caller can
share one session per server (below); an internal guard refuses per-run `auth`
or `headers` callables, which no configuration produces.

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
shared session for one of the agent's own `mcp` grants, the one the model's own
calls and every `Mcp()` marker use, so the registration and the credential
above are still resolved once per worker, and a renewal any of them makes
serves them all. The context is
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

### A use case's call runs in parallel with other grant views

A use case's tool call goes through `_ToolsetSession`
(`loom/ai/engines/pydantic_ai/_mcp.py`) — the same session the model's own
tool calls and every other caller of that server share: another use case's
`Mcp()`, an agent handle's own `handle.mcp(server)` and a `kind: python`
factory's `remote()`. None of them takes a lock over the others, so they
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

## When a call fails

Every caller of a server goes through the connection's one session: a use
case's `Mcp()` marker, an agent handle's `handle.mcp(server)`, a `kind:
python` factory's `remote()` and the model's own tool calls. They share one
renewal, one count of the calls in flight and one rule for what a failure
proves. The rule is that loom never sends a call twice unless the server
proved the first one never ran.

### A failed call is retried only when it never ran

The MCP client raises one exception class, `MCPError`, both for an error the
server sent and for one the client made itself, such as a closed connection or
its own request timeout. Only the JSON-RPC code tells them apart. On a tool
call, the client (mcp 2.3.0) makes these six codes and no other:

| Code | Name |
|---|---|
| `-32000` | connection closed |
| `-32001` | request timeout |
| `-32600` | invalid request |
| `-32601` | method not found |
| `-32603` | internal error |
| `-32700` | parse error |

A failed call is read this way:

| What the call raised | What it proves | What loom does |
|---|---|---|
| A result flagged `is_error`, or the tool's own error | The server answered | Passes it through unchanged. The model receives it as a tool error it may react to |
| An `MCPError` whose code is outside the six above, such as `-32043` | The server answered | Passes it through unchanged |
| `-32600` with the message `Session not found` or `Session terminated` | The server no longer knows the session, so nothing ran | Renews the session and sends the call **once** more |
| Any other code of the six, **even when the server sent it**, or any other exception | The call may have run | Raises `MCP_CALL_INTERRUPTED`, never repeats the call, and renews before the next one |

The six codes are a fixed set in `loom/ai/engines/pydantic_ai/_failures.py`. A
server that sends one of them itself, for example a middleware answering
`-32603`, is treated as an interruption: the code cannot prove the server
answered. An exception that is not the client's `MCPError` is an
interruption too. This includes an exception group, even one that holds a
server's own error.

`MCP_CALL_INTERRUPTED` is the same code on every path: a use case's `Mcp()`
marker, `handle.mcp(server)`, a python tool's `remote()` and the model's own
call.

- It is not retriable: `is_retriable` returns `False`, because the tool may
  already have run. An agent endpoint answers `500` with the code
  `MCP_CALL_INTERRUPTED`. A REST use case that lets it through answers `500`
  `internal_error` with a `trace_id`; the code appears in the log only.
  Whether to send the call again is your decision, made knowing the tool may
  have run.
- A call refused before it was sent carries the same code. This happens when
  the session is closed, or when the renewal before the call cannot connect.
  Its message says the call "was not sent".
- When the one retry also fails, the call raises `MCP_CALL_INTERRUPTED` (or
  the server's own answer, if that is what came back). There is never a third
  attempt, and never the raw transport exception.
- The caller's tool deadline bounds the first attempt, the renewal and the
  retry together. That is the plan's `policies.tool_timeout_ms` for the
  model's own calls, `handle.mcp(server)` and `remote()`, and the server's
  `timeout_ms` for an `Mcp()` marker.

**An agent run fails rather than guessing.** When the model's own call is
interrupted, the run fails with `MCP_CALL_INTERRUPTED`. The failure is not
turned into a refusal the model reads, the model is not called again for that
step, and the run is not replayed, so the tool runs at most once. Before this
release such a failure reached the model as a refusal, and the model could
issue the same call again. A server that sends one of the client's own codes
now fails the run the same way instead of reaching the model, for example
`-32603`. The exception is a server-sent `-32600` with "Session not found" or
"Session terminated", which is renewed and retried once.

### A renewal serves every caller

- **The next call after an interruption opens a new session first**, against
  the same connection. Over stdio, that is a new child process. Concurrent
  calls that find the session suspect renew it once.
- **A renewal lists the server's tools once, before it serves anyone.** The
  client caches that list, so later runs do not list again. An agent run's
  listing never renews: it reads the cached list, even from a suspect session,
  so a run that calls no tool of that server is never held up by a renewal.
  The renewal happens on the next call instead, bounded by that call's
  deadline.
- **A renewal does not cut calls that are still running.** The replaced
  session is retired and closed when its last call returns. At most two are
  open at once: a renewal that finds an earlier one still draining closes it
  first, and the calls left on it fail with `MCP_CALL_INTERRUPTED`. A renewal
  also waits for every close still in progress before it connects.
- **A renewal that cannot connect raises**, with "was not sent", and the call
  after it tries again. An outage costs one connection attempt per call,
  bounded by the handshake deadline, and ends with the first call made after
  the server is back.
- **Suspecting a healthy session costs one reconnection.** A single failed
  call does not prove the session died. Opening another is cheap and safe, so
  loom does not try to tell the cases apart.

loom does not ask the client whether it is connected before a call. Over
streamable HTTP the client still reports itself connected after the server has
restarted, so the answer would not catch a dead connection. Two other things
are unchanged. A server that never connected at start-up under `optional` is
not reconnected by the runtime, and is connected on each use (above). The
health probe still does not see a server named only by an `Mcp()` marker.

### An idle gap replaces pooled connections, not the session

A server reached directly over streamable HTTP gets an HTTP connection pool of
its own, which every caller of its session goes through. Before each request,
the pool reads the wall clock. When more than 5 seconds have passed since a
request last started or finished, and none of the pool's own requests is in
flight, it closes its pooled connections and opens fresh ones. The MCP session
is kept: there is no new `initialize` and no new `tools/list`.

- 5 seconds is httpcore2's own keep-alive expiry. On a running clock httpcore2
  would not reuse those connections either, so on FastAPI an eviction costs
  nothing extra. On AWS Lambda it is what keeps the first call after a freeze
  off a dead connection (see [MCP on AWS Lambda](#mcp-on-aws-lambda)).
- The check costs two wall-clock reads per request: the check itself, and the
  timestamp taken when the response body closes. loom reads no monotonic clock
  for it.
- A `GET` request does not count as in flight. In the handshake era of the
  protocol, a `GET` is the server's event stream, which stays open for the
  whole session. An eviction cuts that stream, and the MCP client reopens it
  on its own.

**Behind a proxy there is no eviction.** When the environment
(`HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`) or the system's proxy settings name a
proxy for the server URL's scheme, or for all schemes, loom keeps the MCP
client's stock transport. That transport applies httpx2's proxy and `NO_PROXY`
rules, which a pool of loom's own would ignore. This holds even when `NO_PROXY`
exempts the server. A stale connection is then left to the rules above. The
first call that hits it fails `MCP_CALL_INTERRUPTED` (nothing ran), and the
next call renews the session. An SSE URL and a stdio server also keep their
stock transport.

## MCP on AWS Lambda

A Lambda function hosts the `ai` section on every trigger: `http`, `jobs`,
and the event triggers (`eventbridge`, `s3`, `sns`, `schedule`). The agent runtime is opened once, during Init, together with the
session of every server the function can reach. The sessions stay open across
warm invocations and close at shutdown, as on FastAPI. Every rule of [When a
call fails](#when-a-call-fails) applies unchanged.

### What a Lambda function builds

- **Only the agents a use case can reach.** A Lambda function serves no agent
  endpoint, so it builds only the agents named by the `Agent()` markers of the
  use cases it compiles, including agents a manifest declares under `AGENTS`.
  Any other agent is not compiled, and Init sends no request to the servers
  that only it uses. An `Agent("missing")` marker still fails start-up. An
  `a2a` capability names a remote agent, so it adds no local one.
- **The servers of those agents and of every `Mcp()` marker.** Init opens them
  under `ai.startup_timeout_ms`, as on FastAPI.
- **No agent endpoints.** A function serves no agent over HTTP or A2A. Init
  logs one warning for each `ai.endpoints.<name>.enabled` that is set and for
  `ai.a2a.expose`, saying it is ignored on Lambda. Serving agents over HTTP
  from a Lambda function is future work.
- **No stdio.** A frozen function keeps no subprocess, so
  `ai.mcp_servers.<name>.transport: stdio` raises `LambdaUnservedConfigError`
  naming that key. It is caught before discovery from the configuration as
  written, and again during Init from the resolved configuration, so a
  transport given by an interpolation is refused too.
- **`remote_clients: optional`** works as described above. A server that did
  not open at Init connects on each use, which on Lambda means one connection
  per listing or call.

Two limits shape a function that hosts agents:

- **Init has about 10 seconds**, and `ai.startup_timeout_ms` defaults to
  `10000`. Set it lower on Lambda, so that a slow server fails Init by name
  with `MCP_SERVER_UNREACHABLE` before Lambda stops Init on its own, and the
  rest of Init has time to run.
- **API Gateway allows about 29 seconds per request.** A long agent run
  belongs on a queue (`jobs`) trigger rather than behind an `http` route.

### Freezes and thaws

Lambda freezes the process between invocations. While it is frozen, the server
or a gateway may close idle connections, and httpcore2's monotonic clock may not
move, so without help the first call after a thaw would be written to a dead
connection. The idle-gap eviction above handles it: the first call after a
wall-clock gap of more than 5 seconds evicts the pool and goes out over a new
connection, with the same session.

What else happens depends on the protocol era the server negotiates:

- **Modern era** (`server/discover`, as a FastMCP 4 server answers): the
  session is stateless, with no session id, so there is no session for the
  server to forget. Eviction is the whole fix.
- **Handshake era** (`initialize` and an `Mcp-Session-Id`): if the server
  forgot the session during the freeze, the first call gets "Session not
  found". That is the safe retry: one renewal, and the call succeeds.
  - With mcp 2.3.0, every `POST` answered as an event stream opens its own TCP
    connection and closes it once the response is read. A warm call therefore
    costs one TCP (and TLS) connect, and no `POST` connection stays pooled to
    go stale.
  - The `GET` event stream cut by an eviction reopens only during an
    invocation, because the event loop runs only then. The client waits one
    second and tries twice. If both attempts fail, the session keeps working
    without an event stream, and only server-to-client notifications are lost.

Known limits:

- **The wall clock may slew instead of jump.** If Lambda restores the wall
  clock after a thaw gradually, the first call may see a gap under 5 seconds.
  It then reuses a stale connection and fails `MCP_CALL_INTERRUPTED`, with
  nothing run, and the next call renews and works. The A3 measurement below
  records both clocks' gaps to show whether this happens.
- **A server that closes idle connections in under 5 seconds** races
  httpcore2's own expiry, freeze or not. uvicorn's default `timeout_keep_alive`
  is 5 seconds.
- **Behind a proxy there is no eviction** (see above). The first call after a
  thaw may fail `MCP_CALL_INTERRUPTED`, and the next one renews.
- **Plan B, not built:** a pool that keeps no idle connection
  (`httpx2.Limits(max_keepalive_connections=0)`) is correct by construction
  but costs a TCP and TLS handshake per call. It would replace eviction only
  if eviction proved fragile.

### An interrupted call on a queue

On a `jobs` trigger, a job whose MCP call is interrupted fails. Its record is
reported in `batchItemFailures` and SQS delivers it again. With
`report_batch_item_failures: false`, the whole batch fails instead. loom never
repeats the call itself, but a redelivery runs the job again, so the tool may
run a second time. Until idempotent delivery exists, make such a tool
idempotent on the server, or keep it off a queue.

### What it costs, and how it is measured (A3)

These request counts are tested on every pull request, against a real
`LambdaAdapter` function on both triggers and a real streamable-HTTP FastMCP
server behind a local TCP proxy
([`tests/integration/ai/freeze/test_cost.py`](https://github.com/MassiveDataScope/loom-py/blob/master/tests/integration/ai/freeze/test_cost.py)):

| Situation | MCP requests | TCP connects |
|---|---|---|
| Warm invocation | the tool calls only | modern: 0; handshake era: 1 per call |
| First call after a thaw, session still valid | the tool call only | 1 (the eviction) |
| First call after a thaw, session expired (handshake era) | the refused call, `server/discover`, `initialize`, `notifications/initialized`, `tools/list`, the retried call | 6, with the new event stream |
| Interrupted call | the failed call, once; the next call renews (modern: `server/discover`, `tools/list`) and then calls | - |
| Init, per reachable server | modern: `server/discover`, `tools/list`; handshake era: also `initialize`, `notifications/initialized` and the event stream | modern: 1; handshake era: 4 |
| Init, server of an agent the function does not build | none | 0 |

A credential is fetched once at Init and not again while it is valid.

The deployed measurement is separate and does not gate a release. The probe
application
([`tests/integration/ai/freeze/probe.py`](https://github.com/MassiveDataScope/loom-py/blob/master/tests/integration/ai/freeze/probe.py))
is deployed unchanged, at 1024 MB, against the real MCP gateway. The harness
([`tests/integration/ai/freeze/measure.py`](https://github.com/MassiveDataScope/loom-py/blob/master/tests/integration/ai/freeze/measure.py))
invokes it and reports p50, p95 and p99 for:

- the warm per-invocation MCP time, over 500 invocations each of a marker and
  of an agent use case. The pass rule is p95 at or below 150 ms, against a 2 s
  endpoint target;
- the first call after an expired session, over 30 samples, with the session
  expired by the gateway's TTL or by a server-side drop;
- the MCP part of a cold Init, over 30 cold starts, against the Init budget.

It also records the wall-clock and monotonic gaps of each invocation, the TCP
connects per call, the gateway's protocol era and its idle timeout. The method
and the results are kept with the specification
(`.specify/specs/010-usecase-entrypoints/a3.md`). A missed target opens a
follow-up task. It does not block a release.

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
worker: start-up and every agent granted the server work over one session,
which speaks through one `MCPToolset` at a time (a renewal swaps it, see [When
a call fails](#when-a-call-fails)), so ten agents naming one server are one
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

**Sessions are lifespan-scoped.** The runtime opens every client inside a
single `AsyncExitStack` and releases them in strict reverse order from the task
that entered it. Releasing a session closes every toolset it holds, each in a
task of its own, and waits for them all; a call still in flight at that moment
is cut. A toolset retired by a renewal is closed the same way, by whichever
task sees its last call return. Closing from another task is safe: the client
keeps its session in an `asyncio.Task` of its own, created precisely so it
outlives the individual context-manager scopes that enter and leave it, so the
closing task is not the one the session is bound to.

**Every result is untrusted input.** A tool result from a remote server is
data from another system, exactly like a remote agent's answer. The agent's
blast radius remains the intersection of its grants and the caller's identity —
a malicious tool result can steer the model, but it cannot widen a grant.
