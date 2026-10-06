# Use-case DSL

The use-case DSL lets you declare inputs, validations, derived values, and
pre-conditions in a declarative, composable style. The engine resolves everything
before `execute()` runs — keeping the body focused on the business outcome.

---

## Anatomy of a use case

```python
from loom.core.use_case import Compute, Exists, F, Input, LoadById, OnMissing, Rule
from loom.core.use_case.use_case import UseCase

from app.product.model import Product
from app.product.schemas import CreateProduct


class CreateProductUseCase(UseCase[Product, Product]):
    #            ↑ model   ↑ return type

    computes = [CREATE_NORMALIZE_NAME, CREATE_NORMALIZE_PRICE]   # run first
    rules    = [CREATE_NAME_RULE, CREATE_PRICE_RULE]             # run after computes

    async def execute(
        self,
        cmd: CreateProduct = Input(),           # body ← JSON-decoded command
    ) -> Product:
        return await self.main_repo.create(cmd)
```

Execution order for every request:

```
1. Computes   — derive / normalise command fields in declaration order
2. Rules      — validate; any failure raises 422 before execute() is called
3. execute()  — your business logic, guaranteed clean inputs
```

`self.main_repo` is the repository for the model declared in `UseCase[Model, ...]` — no
injection boilerplate needed.

---

## Commands

A `Command` is a typed, immutable input object. Declare it with `msgspec.Struct`
conventions — the framework decodes the HTTP body into it automatically:

```python
from loom.core.command import Command, Patch


class CreateProduct(Command, frozen=True):
    name: str
    price: float
    sku: str


class UpdateProduct(Command, frozen=True):
    # Patch[T] marks a field as optional in partial updates.
    # None means "not provided" — not "set to null".
    name:  Patch[str]   = None
    price: Patch[float] = None
    sku:   Patch[str]   = None
```

> `Patch[T]` is a union type alias for `T | None` with framework-level tracking
> of which fields were actually sent. Rules can gate on whether a `Patch` field
> is present using `.when_present(...)`.

---

## F — typed field reference

`F(CommandClass).field` creates a typed reference to a command field. It is used
everywhere the DSL needs to identify a specific field — in `Compute`, `Rule`, and
predicate conditions:

```python
from loom.core.use_case import F

# reference to the 'name' field of CreateProduct
F(CreateProduct).name

# reference to the 'price' field of UpdateProduct
F(UpdateProduct).price
```

Field references are type-safe. Accessing a field that does not exist on the
command raises an error at bootstrap time, not at runtime.

---

## Input markers

`Input()` in the `execute()` signature tells the engine to inject the decoded
command body as that parameter:

```python
async def execute(self, cmd: CreateProduct = Input()) -> Product: ...
```

`LoadById` fetches an entity by ID from a path or command parameter before
`execute()` runs — the entity is available in rules and the body:

```python
from loom.core.use_case import LoadById

async def execute(
    self,
    product_id: int,                                    # path parameter
    cmd: UpdateProduct = Input(),
    product: Product = LoadById(Product, by="product_id"),  # loaded before execute
) -> Product | None:
    # product is guaranteed to exist here if OnMissing.RAISE (default)
    return await self.main_repo.update(product_id, cmd)
```

`LoadById` parameters:

| Parameter | Description |
|-----------|-------------|
| `model` | Entity class to load |
| `by` | Name of the path/command param that holds the ID |
| `on_missing` | `OnMissing.RAISE` (default) → 404; `OnMissing.NONE` → `None` |

`Exists` checks a DB condition and injects a `bool` — without loading the entity:

```python
from loom.core.use_case import Exists, OnMissing

async def execute(
    self,
    user_id: int,
    cmd: CreateAddress = Input(),
    _user_exists: bool = Exists(
        User,
        from_param="user_id",    # check User.id == user_id
        against="id",
        on_missing=OnMissing.RAISE,  # auto-404 if False
    ),
) -> Address:
    return await self.main_repo.create(...)
```

`Exists` parameters:

| Parameter | Description |
|-----------|-------------|
| `model` | Entity class to check |
| `from_param` | Path parameter whose value is compared |
| `from_command` | Command field whose value is compared |
| `against` | Entity field to compare against |
| `on_missing` | `RAISE` → 404 immediately; `NONE` → inject False |

---

## Agent marker — reaching a named agent

`Agent(name)` declares a handle to one compiled agent, bound to the caller
already verified for this execution — the same identity
[`Caller()`](identity.md) fills. The executor resolves the marker before
`execute()` runs; nothing but a reviewed use case's own code chooses which
agent runs or which caller it runs as.

The example below is complete and runnable, uses `Caller()` alongside
`Agent()`, and is the exact use case exercised — with no network, no model
and no database — by
[`tests/integration/ai/test_agent_marker_test_double.py`](https://github.com/MassiveDataScope/loom-py/blob/master/tests/integration/ai/test_agent_marker_test_double.py):
editing one without the other is a gap the next review will catch. An
example that binds identity through a plain argument instead of `Caller()`
would teach a forgeable pattern — a parameter with no marker is filled from
whatever the caller supplies, and it ends up in the schema the model sees.

```python
import msgspec

from loom.ai.abc import AgentHandle
from loom.core.identity import Identity
from loom.core.use_case import Agent, Caller, UseCase


class SeverityAssessment(msgspec.Struct, frozen=True):
    """The artefact's own declared output shape."""

    severity: int


class IncidentReport(msgspec.Struct, frozen=True):
    incident_id: str
    caller: str
    runbook_title: str
    severity: int
    escalated: bool


class TriageIncidentUseCase(UseCase[object, IncidentReport]):
    async def execute(
        self,
        incident_id: str,
        caller: Identity = Caller(),
        triage: AgentHandle[SeverityAssessment] = Agent("incident-triage"),
    ) -> IncidentReport:
        obs = triage.sql("observability_readonly")             # the artefact's own granted view
        deploys = await obs.query(
            "select service, status from deploys where incident = :id",
            parameters={"id": incident_id},
        )

        runbooks = triage.mcp("runbooks")                       # the artefact's own filtered view
        runbook = await runbooks.call(
            "search_incident",
            {"incident_id": incident_id, "recent_deploy": deploys[0]["service"]},
            expect=dict,
        )

        assessment = await triage.run(f"Assess {incident_id}.")  # the artefact's declared shape

        return IncidentReport(
            incident_id=incident_id,
            caller=caller.require_subject(),
            runbook_title=str(runbook["title"]),
            severity=assessment.output.severity,
            escalated=assessment.output.severity >= 4,
        )
```

The type argument on `AgentHandle[SeverityAssessment]` — not a parameter to
`Agent()` — is what the compiler checks at start-up against the named
agent's own declared output. `Agent()` itself carries only the name, exactly
like `Caller()` carries no configuration.

### What the handle offers

| Member | Returns | Notes |
|---|---|---|
| `run(prompt)` | `AgentAnswer[T]` | Decodes into the artefact's own declared output — the handle's type argument. |
| `run(prompt, expect=X)` | `AgentAnswer[X]` | This run only; the artefact's own output check does not run for it. |
| `run_text(prompt)` | `AgentAnswer[str]` | Open prose — no declared or overridden shape, so nothing can fail to decode. |
| `mcp(server)` | `McpHandle` | The artefact's own filtered view of one `mcp` grant. |
| `sql(connection)` | `SqlGrantHandle` | The artefact's own bounded view of one `sql` grant. |
| `grants()` | `tuple[str, ...]` | Every grant name reachable through `mcp()` / `sql()` — pin a name in a test in one line. |

### Three run modes

```python
assessment = await triage.run(prompt)                       # the artefact's declared shape
plan       = await triage.run(prompt, expect=RollbackPlan)  # this run only
summary    = await triage.run_text(f"Write it up: {facts}") # open prose, decodes nothing
```

### Two grant views, reached through the agent — never re-declared

`triage.mcp("runbooks")` and `triage.sql("observability_readonly")` return
the very same composed view — over the same shared session or connection,
filtered or bounded by the same predicate — that the model's own toolset
runs over. The grant reached this way is never re-declared: it is declared
once, on the artefact's `mcp` / `sql` capability, and the use case reaches
it as-is.

That does not mean no wider view can exist. A use case may declare its own,
independent view of an MCP server with [`Mcp()`](#mcp-marker--reaching-an-mcp-server-directly),
whose filter belongs to the use case, not the agent — written in the
signature and checked at start-up against the server's real tool list, the
same way an agent's own `mcp` capability is checked.

### Testing it

`loom.testing.runner.AgentHandleDouble` stands in for the handle: no
network, model or database call is ever made, and a call the test never
scripted fails closed with an `AssertionError` instead of silently
succeeding with the wrong data.

```python
from loom.testing.runner import AgentHandleDouble, UseCaseTest

double = AgentHandleDouble("incident-triage").on_run(SeverityAssessment(severity=5))
double.sql("observability_readonly").on_query([{"service": "checkout", "status": "unhealthy"}])
double.mcp("runbooks").on_call("search_incident", {"title": "checkout rollback runbook"})

result = await (
    UseCaseTest(TriageIncidentUseCase())
    .with_caller(identity)
    .with_agent("incident-triage", double)
    .with_params(incident_id="INC-100")
    .run()
)
```

A use case declaring `Agent(name)` with no matching `.with_agent(name, ...)`
fails closed with a `RuntimeError` naming the use case and the parameter —
the same fail-closed default `Caller()` already has when no `.with_caller()`
is registered.

---

## What the Agent() marker refuses

Every refusal below raises `loom.ai.errors.AgentRunError`, whose `.code` is
one of `loom.ai.errors.AgentRunErrorCode`, checked before any model or
network call where the table says so. Each row is pinned by a test — follow
the file to see the exact assertion.

| Refusal | Code | Message (from the code) | What to do |
|---|---|---|---|
| Anonymous caller | `UNAUTHORIZED` | `agent 'incident-triage' requires an authenticated caller` | A marker-filled handle never accepts an anonymous identity, with no configuration to relax it — the caller must already be authenticated at the use case's own route. In a test, register one with `.with_caller(identity)`. See `tests/unit/ai/runtime/test_agent_handle.py::TestIdentidadAnonima::test_el_mensaje_es_el_que_muestra_use_case_dsl_md`. |
| A cycle: the same agent already in the chain (agent names `outer`/`inner` below, as the pinning test names them) | `AGENT_CALL_CYCLE` | `agent call cycle detected: outer -> outer` | Break the cycle: an `on_output` hook or any use case an agent's run invokes must not call the same agent back. Route to a different named agent instead. See `tests/integration/ai/test_agent_call_chain.py::TestProfundidadPorDefecto::test_una_corrida_anidada_del_mismo_agente_es_un_ciclo`. |
| A depth past `ai.max_agent_depth` | `AGENT_CALL_TOO_DEEP` | `agent call chain outer -> inner exceeds ai.max_agent_depth=1` | Raise `ai.max_agent_depth` deliberately if the nesting is intended — see [the nesting bound](../ai/overview.md#nesting-how-deep-an-agent-may-call-another) for what that costs — or remove the nested `Agent()` call. See `tests/integration/ai/test_agent_call_chain.py::TestProfundidadPorDefecto::test_una_corrida_anidada_de_otro_agente_excede_la_profundidad`. |
| A shape override (`expect=` or `run_text`) on an artefact whose `on_output` hook declares the `output` field | `AGENT_RUN_SHAPE_WITH_HOOK` | `agent 'incident-triage' declares an output hook that reads the run's output, so this run cannot use a per-run shape; call run(prompt) for the artefact's own declared output instead` | Call `run(prompt)` with no `expect` so the hook receives the artefact's declared shape, or narrow the hook's `Input` to conversation-bookkeeping fields only (drop `output`) so any mode is accepted. See `tests/unit/ai/runtime/test_agent_handle.py::TestRechazoPorHookDeSalida::test_el_mensaje_es_el_que_muestra_use_case_dsl_md`. |
| A tool outside the artefact's own `mcp` grant filter | `TOOL_UNKNOWN` | `mcp server 'runbooks' grants no tool named 'delete_incident'; tools this grant admits: search_incident` | Call one of the tools `handle.mcp(server).tools()` lists, or widen the artefact's `include` / `exclude` filter for that server. See `tests/unit/ai/runtime/test_grants.py::TestElFiltroDelPermisoMcp::test_el_mensaje_de_tool_fuera_del_permiso_es_el_que_muestra_use_case_dsl_md`. |
| A tool that publishes no output schema, called through `call(..., expect=X)` | `TOOL_UNTYPED` | `tool 'legacy_lookup' of mcp server 'runbooks' publishes no output schema; call it with call_untyped() instead` | Call `call_untyped(tool, arguments)` instead, and treat the result as the server's own, undecoded JSON. See `tests/unit/ai/runtime/test_grants.py::TestLlamadaTipada::test_el_mensaje_de_tool_sin_forma_es_el_que_muestra_use_case_dsl_md`. |

---

## Mcp marker — reaching an MCP server directly

`Mcp(server, *, include)` declares a handle to one configured MCP server,
bound to the caller already verified for this execution — the same identity
`Agent()` binds. Unlike `triage.mcp(server)`, reached *through* an agent's
own grant, `Mcp()` needs no agent in the middle: the use case is the
capability's only owner, and its `include` is the *only* filter checked for
it.

`include` is mandatory, for the same reason it is required on every
include/exclude filter this framework has: an empty sequence would mean
"every tool this server publishes", not "the handful this signature names".
Passing one glob is not enough of a decision to make by omission, so `Mcp()`
raises `ValueError` rather than widening a grant no one asked for.

The example below is complete and runnable, uses `Caller()` alongside
`Mcp()`, and is the exact use case exercised — with no network and no MCP
server — by
[`tests/integration/ai/test_use_case_mcp_marker_test_double.py`](https://github.com/MassiveDataScope/loom-py/blob/master/tests/integration/ai/test_use_case_mcp_marker_test_double.py):
editing one without the other is a gap the next review will catch. As with
`Agent()`, an example that skipped `Caller()` would teach a forgeable
pattern.

```python
import msgspec

from loom.ai.abc import McpHandle
from loom.core.identity import Identity
from loom.core.use_case import Caller, Mcp, UseCase


class RunbookLookup(msgspec.Struct, frozen=True):
    incident_id: str
    requested_by: str
    runbook_title: str


class LookUpRunbookUseCase(UseCase[object, RunbookLookup]):
    async def execute(
        self,
        incident_id: str,
        caller: Identity = Caller(),
        runbooks: McpHandle = Mcp("runbooks", include=("search_incident",)),
    ) -> RunbookLookup:
        result = await runbooks.call_untyped(
            "search_incident", {"incident_id": incident_id}
        )
        return RunbookLookup(
            incident_id=incident_id,
            requested_by=caller.require_subject(),
            runbook_title=str(result["title"]),
        )
```

`server` is validated at start-up against `ai.mcp_servers`, naming the
declaring use case and the parameter when it is not configured. `include`
is checked at start-up too, against the server's *real* tool list — not a
promise that no wider view exists (an `include` with broad globs admits
everything, and nothing caps how broad it may be), but a guarantee that
whatever it admits is written down in the signature and verified before the
first request ever reaches it.

### What the handle offers

| Member | Returns | Notes |
|---|---|---|
| `tools()` | `tuple[str, ...]` | Tool names already narrowed by this marker's own `include`. |
| `call(tool, arguments, expect=X)` | `X` | Decodes the tool's structured result into `X`; the tool must publish an output schema. |
| `call_untyped(tool, arguments)` | `Mapping[str, Any]` | The server's own structured content, undecoded — for a tool that publishes no schema. |

### Testing it

`loom.testing.runner.McpHandleDouble` stands in for the handle, exactly as
`AgentHandleDouble` does for `Agent()`: no network or server is ever
reached, and a call the test never scripted fails closed. Unlike the
published double, the resolver wraps it in a private filter carrying this
marker's own `include`, so a test that calls a tool outside it gets the same
refusal production would give — see [What the Mcp() marker
refuses](#what-the-mcp-marker-refuses) below.

```python
from loom.testing.runner import McpHandleDouble, UseCaseTest

double = McpHandleDouble("runbooks").with_tools("search_incident")
double.on_call_untyped("search_incident", {"title": "checkout rollback runbook"})

result = await (
    UseCaseTest(LookUpRunbookUseCase())
    .with_caller(identity)
    .with_mcp("runbooks", double)
    .with_params(incident_id="INC-100")
    .run()
)
```

A use case declaring `Mcp(server, ...)` with no matching `.with_mcp(server,
...)` fails closed with a `RuntimeError` naming the use case and the server —
the same fail-closed default `Agent()` already has.

---

## What the Mcp() marker refuses

Every refusal below raises either `loom.ai.errors.AgentRunError` (a call, at
run time) or `loom.ai.errors.AgentCompilationError` (start-up). Each row is
pinned by a test — follow the file to see the exact assertion.

| Refusal | Code | When | What to do | Test |
|---|---|---|---|---|
| A tool outside the marker's own `include` | `TOOL_UNKNOWN` (`AgentRunError`) | Call time | Call one of `handle.tools()`, or widen the marker's `include`. | `tests/unit/testing/test_runner.py::TestMcpHandleDoubleEnforcesInclude::test_a_use_case_cannot_widen_its_own_include_by_calling_outside_it` and, over the real chain, `tests/integration/ai/test_use_case_mcp_end_to_end.py::TestElDobleYElCaminoRealCoincidenEnElRechazo::test_ambos_caminos_rechazan_la_misma_llamada_fuera_de_include` |
| A tolerated-unreachable server (`ai.remote_clients: optional`, the server never connected) | `TOOL_UNAVAILABLE` (`AgentRunError`) | Call time | Treat it like any other dependency outage: the server, not the filter, is the problem. | `tests/integration/ai/test_use_case_mcp_resolver.py::TestServidorInalcanzableTolerado::test_el_arranque_pasa_y_la_primera_llamada_es_tool_unavailable` |
| `server` names no server configured under `ai.mcp_servers` | `MCP_MARKER_UNKNOWN` (`AgentCompilationError`) | Start-up | Fix the typo, or add the server to `ai.mcp_servers`. | `tests/unit/rest/test_fastapi_auto_mcp_markers.py::TestServidorDesconocido::test_un_typo_aborta_nombrando_caso_de_uso_parametro_servidor_y_configurados` |
| `include` matches no tool the server actually publishes | `TOOL_FILTER_MATCHES_NOTHING` (`AgentCompilationError`) | Start-up | Fix the glob, or confirm the server still publishes the tool you expect. | `tests/integration/ai/test_use_case_mcp_end_to_end.py::TestElIncludeQueNoCasaNadaAbortaPorElCaminoReal::test_un_include_que_no_casa_nada_aborta_con_tool_filter_matches_nothing` |

A tool that publishes no output schema, called through `call(..., expect=X)`,
is refused the same way `Agent()`'s own grant refuses it (`TOOL_UNTYPED`) —
but production is the only side that can know: a tool's published schema is
not something the test double tracks, so `McpHandleDouble` cannot reproduce
this particular refusal.

Under `ai.remote_clients: optional`, a server tolerated as down never has its
`include` checked at all — a tolerated outage means the filter goes
**unverified**, not that it verified clean.

---

## Compute — derive and normalise fields

`Compute` derives or normalises a command field before rules run. This keeps
normalisation logic out of both the command and the execute body:

```python
from loom.core.use_case import Compute, F


def _normalize_name(value: str) -> str:
    return value.strip()


def _normalize_price(value: float) -> float:
    return round(value, 2)


# Simple normalisation — takes one field, returns the normalised value
CREATE_NORMALIZE_NAME = Compute.set(F(CreateProduct).name).from_command(
    F(CreateProduct).name, via=_normalize_name
)

CREATE_NORMALIZE_PRICE = Compute.set(F(CreateProduct).price).from_command(
    F(CreateProduct).price, via=_normalize_price
)
```

Computes can read multiple fields:

```python
def _compute_subtotal(unit_price: float, quantity: int) -> float:
    return unit_price * quantity

# Derives subtotal from unit_price × quantity
CREATE_SUBTOTAL = Compute.set(F(PricingCommand).subtotal).from_command(
    F(PricingCommand).unit_price,
    F(PricingCommand).quantity,
    via=_compute_subtotal,
)
```

Computes can also read path parameters via `.from_params(...)`:

```python
# Normalise name — but only if product_id != "1" (system product)
UPDATE_NORMALIZE_NAME = (
    Compute.set(F(UpdateProduct).name)
    .from_command(F(UpdateProduct).name, via=_normalize_name_with_context)
    .from_params("product_id")
    .when_present(F(UpdateProduct).name)   # skip if name was not sent
)
```

Computes run in **declaration order** — a later compute can reference a field
already set by an earlier one (e.g. subtotal → tax_amount).

### Apply computes conditionally

`.when_present(field)` skips the compute when the `Patch` field was not provided.
Essential for partial-update commands:

```python
UPDATE_NORMALIZE_PRICE = (
    Compute.set(F(UpdateProduct).price)
    .from_command(F(UpdateProduct).price, via=_normalize_price)
    .when_present(F(UpdateProduct).price)   # skip when price is absent from PATCH body
)
```

---

## Rule — validate before execute

Rules run after all computes. A failing rule raises a structured 422 response
before `execute()` is called: `code: "rule_violations"` and a `violations` list
of `{field, message}` pairs, one per failing rule. A body that fails to convert
into the `Command` in the first place — a wrong field type, a missing required
field — never reaches a rule: it answers its own 422 with
`code: "boundary_validation"` and a `violations` list shaped the same way
(`field` being the wire name the client sent). A body that is not JSON at all
— a syntax error, bytes that are not UTF-8, or an empty body on a route that
takes input — answers `400` with `code: "bad_request"` and the fixed message
`Request body is not valid JSON`, never echoing what was sent. Well-formed
JSON that is not an object — an array, a string, a number, `null` — is a
`boundary_validation` 422 whose message and single `body` violation read
`Request body must be a JSON object`. The body is decoded as JSON whatever its
`Content-Type` says.

A single `RuleViolation(field, message)` raised from `execute()` also answers
422, with `code: "rule_violation"`, its `message` and its `field` at the top
level of the body, so a client tells two such 422s apart by `field` instead of
by the message text:

```json
{"detail": {"code": "rule_violation", "message": "no tenant is selectable",
            "field": "tenant", "trace_id": "abc-123"}}
```

### Rule.check — field validation

`Rule.check` validates a single field. The `via=` function returns an error
string when the value is invalid, or `None` when it is valid:

```python
from loom.core.use_case import Rule, F


def _name_must_not_be_blank(name: str) -> str | None:
    return None if name.strip() else "name must not be blank"


def _price_must_be_positive(price: float) -> str | None:
    return None if price > 0 else "price must be positive"


CREATE_NAME_RULE  = Rule.check(F(CreateProduct).name,  via=_name_must_not_be_blank)
CREATE_PRICE_RULE = Rule.check(F(CreateProduct).price, via=_price_must_be_positive)
```

### Rule.forbid — invariant enforcement

`Rule.forbid` declares a condition that **must not be true**. The predicate
receives the arguments you attach; it returns `True` when the forbidden condition
holds (triggering the error):

```python
def _patch_payload_is_empty(_cmd: UpdateProduct, fields: frozenset[str]) -> bool:
    """Forbid empty PATCH bodies."""
    return len(fields) == 0


# from_command() with no arguments injects (cmd, fields_set_frozenset)
UPDATE_NOT_EMPTY_RULE = Rule.forbid(
    _patch_payload_is_empty,
    message="at least one field must be provided",
).from_command()
```

### Conditional rules with .when_present

`.when_present(field)` skips the rule entirely when a `Patch` field was not
provided. Use this to avoid running patch validations when the field was not sent:

```python
UPDATE_NAME_RULE = Rule.check(
    F(UpdateProduct).name,
    via=_name_must_not_be_blank,
).when_present(F(UpdateProduct).name)

UPDATE_PRICE_RULE = Rule.check(
    F(UpdateProduct).price,
    via=_price_must_be_positive,
).when_present(F(UpdateProduct).price)
```

### Rules with path parameters

Use `.from_params(...)` when validation needs a value from the request path
(e.g. to check a system-protected entity by ID):

```python
def _is_system_product_name_update_forbidden(
    _cmd: UpdateProduct,
    _fields_set: frozenset[str],
    product_id: str,
) -> bool:
    """Prevent renaming the system product (id=1)."""
    return str(product_id) == "1"


UPDATE_SYSTEM_NAME_IMMUTABLE_RULE = (
    Rule.forbid(
        _is_system_product_name_update_forbidden,
        message="system product name cannot be changed",
    )
    .from_command(F(UpdateProduct).name)
    .from_params("product_id")
    .when_present(F(UpdateProduct).name)
)
```

### Rules with multiple fields

`.from_command(field1, field2, ...)` passes multiple values to the predicate:

```python
def _name_cannot_match_price(name: str | None, price: float | None) -> bool:
    if name is None or price is None:
        return False
    return name.strip() == str(price)


UPDATE_NAME_PRICE_MISMATCH_RULE = (
    Rule.forbid(
        _name_cannot_match_price,
        message="name cannot be equal to price",
    )
    .from_command(F(UpdateProduct).name, F(UpdateProduct).price)
    .when_present(F(UpdateProduct).name & F(UpdateProduct).price)
)
```

---

## Predicate composition

`.when_present(...)` accepts composed predicates using `&` (AND) and `|` (OR):

```python
# Run only when BOTH name AND price are present in the request
.when_present(F(UpdateProduct).name & F(UpdateProduct).price)

# Run when EITHER field is present
.when_present(F(UpdateProduct).name | F(UpdateProduct).price)
```

---

## Full use case example — update with all DSL features

```python
class UpdateProductUseCase(UseCase[Product, Product | None]):
    computes = [
        UPDATE_NORMALIZE_NAME,    # strip whitespace — skipped if name absent
        UPDATE_NORMALIZE_PRICE,   # round to 2 dp  — skipped if price absent
    ]
    rules = [
        UPDATE_NOT_EMPTY_RULE,              # reject empty PATCH body
        UPDATE_NAME_RULE,                   # name not blank (if present)
        UPDATE_PRICE_RULE,                  # price positive (if present)
        UPDATE_SYSTEM_NAME_IMMUTABLE_RULE,  # block rename of id=1 (if name present)
        UPDATE_NAME_PRICE_MISMATCH_RULE,    # name ≠ str(price) (if both present)
    ]

    async def execute(
        self,
        product_id: str,
        cmd: UpdateProduct = Input(),
    ) -> Product | None:
        return await self.main_repo.update(int(product_id), cmd)
```

---

## Declaring use cases on a REST interface

Attach use cases to routes via `RestRoute`:

```python
from loom.rest.model import RestInterface, RestRoute

class ProductInterface(RestInterface[Product]):
    prefix = "/products"
    tags = ("Products",)
    routes = (
        RestRoute(use_case=CreateProductUseCase, method="POST",   path="/",              status_code=201),
        RestRoute(use_case=GetProductUseCase,    method="GET",    path="/{product_id}"),
        RestRoute(use_case=ListProductsUseCase,  method="GET",    path="/"),
        RestRoute(use_case=UpdateProductUseCase, method="PATCH",  path="/{product_id}"),
        RestRoute(use_case=DeleteProductUseCase, method="DELETE", path="/{product_id}"),
    )
```

Or let the framework generate all five routes automatically:

```python
class ProductInterface(RestInterface[Product]):
    prefix = "/products"
    tags = ("Products",)
    auto = True   # generates GET, POST, PATCH, DELETE, list automatically
```

See [Auto-CRUD guide](autocrud.md) for the full options reference.

---

## Declaring routes in configuration (`app.rest.interfaces`)

Every key above has a YAML twin under `app.rest.interfaces.<name>`, named and
defaulted identically — the same vocabulary, so nothing here is new to learn.
Three keys are required per route: `use_case`, `method`, `path`. Everything
else inherits the same default the Python route does.

```yaml
app:
  rest:
    interfaces:
      products:
        prefix: /products
        tags: [Products]
        routes:
          - use_case: myapp.application.products:CreateProductUseCase
            method: POST
            path: /
            status_code: 201
          - use_case: myapp.application.products:GetProductUseCase
            method: GET
            path: /{product_id}
```

`use_case` is a `module:Symbol` reference, resolved with the same offline
reader the AI artifact format already uses in its own references — a bad
reference fails the same way, at startup, naming the interface.

### Auto-CRUD from configuration

`auto: true` works exactly like the Python `auto = True` attribute, with one
addition: YAML cannot express `RestInterface[Model]`'s generic parameter, so
a `model` reference stands in for it. `model` is required only when `auto`
is set and the interface declares no explicit `routes`:

```yaml
app:
  rest:
    interfaces:
      gadgets:
        prefix: /gadgets
        auto: true
        model: myapp.domain.gadget:Gadget
```

The generated routes come from the same auto-CRUD generator a Python
`auto = True` interface calls — same routes, same status codes, same
defaults.

### Coexistence with Python interfaces

A YAML interface is converted into the same `RestRoute` and `RestInterface`
objects a Python subclass produces, and handed to the same compiler — there
is no second compilation path. Python interfaces compile first, YAML ones
after, deterministically; the order decides nothing about behaviour, only
which origin an error names first.

A `(method, path)` declared twice aborts startup, naming both declarations.
This is not a new rule: the compiler already refuses a route duplicated
within one Python interface; a same- or cross-origin collision is that same
refusal, extended. Nothing takes precedence — to change a Python-declared
route per environment, disable it (below) and redeclare it in YAML. Two
`app.rest.interfaces` entries sharing a route have no such override: remove
the duplicate declaration instead.

> **Behaviour change:** before `app.rest.interfaces` existed, two Python
> `RestInterface` subclasses declaring the same `(method, path)` mounted two
> handlers, and FastAPI silently served only the first one — the duplicate
> was never rejected. An application that relied on that (almost certainly
> by accident) will now fail to start, naming both interfaces, until the
> duplicate is removed. This applies whether or not any YAML interface is in
> use.

> **Breaking change:** `create_fastapi_app`'s second parameter used to be
> `interfaces: Sequence[type[RestInterface]]`; it is now
> `routes: RouteSources`. Code that called it as documented —
> `create_fastapi_app(result, interfaces=[...])`, keyword form, every
> published example — still works: a deprecated `interfaces=` keyword wraps
> the list in `RouteSources(python=interfaces)` and emits a
> `DeprecationWarning` naming `routes` as the replacement (the keyword will
> be removed in 2.0). A call that passed the list *positionally* now binds
> it to `routes`, which requires a `RouteSources` instance:
> `create_fastapi_app` raises `TypeError` immediately, naming
> `RouteSources(python=[...])` as the replacement — it does not wait for
> compilation to fail on a missing attribute. Passing both `routes` and
> `interfaces`, or neither, also raises `TypeError`. Migrate by replacing
> `interfaces=[...]` with `routes=RouteSources(python=[...])`.

### Disabling a route per environment (`app.rest.disable_routes`)

`app.rest.disable_routes` is a subtractive-only list, and it only targets
routes declared by a **Python** `RestInterface` — it can remove one, never
add one, and it has no effect on a YAML-declared route or on a route mounted
outside interface compilation (such as the health check). A route declared
in `app.rest.interfaces` needs no such mechanism: remove it from the YAML
instead.

```yaml
app:
  rest:
    disable_routes:
      - method: DELETE
        path: /products/{product_id}
```

Each entry names the route exactly as it is published — the full path,
prefix included. Disablement is applied to the Python-declared routes
**before** they are merged with `app.rest.interfaces` entries and checked
for collisions. That ordering is what makes the override flow work: disable
the Python route, declare the same `(method, path)` in YAML, and there is no
collision because the Python route is no longer in the set by the time the
check runs.

An entry matching no Python-declared route aborts startup instead of doing
nothing: a silent no-op would leave an operator believing a route is gone
when it is still being served.

> **Security note:** the disable-and-redeclare override flow lets a
> `app.rest.interfaces` entry drop a `requires_roles` the Python route
> declared, since the redeclaration is a fresh route with its own policy,
> not a patch of the old one. That is consistent with treating configuration
> as trusted — the same trust an operator already has to remove or rewrite
> any other route — but it means "per environment" includes the route's
> authorization, not just its shape.

A runnable version of every example above lives in
[`tests/integration/rest/test_yaml_interfaces.py`](https://github.com/MassiveDataScope/loom-py/blob/master/tests/integration/rest/test_yaml_interfaces.py),
exercised end to end against a real FastAPI app on every test run.

---

## Cross-use-case calls with ApplicationInvoker

Use `ApplicationInvoker` to call another use case by type without tight coupling.
The engine resolves it from the DI container:

```python
from loom.core.use_case.invoker import ApplicationInvoker


class RestockWorkflowUseCase(UseCase[Product, RestockWorkflowResponse]):
    def __init__(self, app: ApplicationInvoker, job_service: JobService) -> None:
        self._app = app
        self._jobs = job_service

    async def execute(
        self,
        product_id: str,
        cmd: DispatchRestockEmailCommand = Input(),
    ) -> RestockWorkflowResponse:
        # Call another use case by type — no import of its instance
        summary = await self._app.invoke(
            BuildProductSummaryUseCase,
            params={"product_id": int(product_id)},
        )
        handle = self._jobs.dispatch(
            SendRestockEmailJob,
            params={"product_id": int(product_id)},
            payload={"product_id": int(product_id), "recipient_email": cmd.recipient_email},
        )
        return RestockWorkflowResponse(
            summary=summary.summary,
            restock_job_id=handle.job_id,
            queue=handle.queue,
        )
```

`app.entity(Model)` gives a CRUD-focused facade when you know the entity:

```python
# Equivalent to invoke_name("product:get", params={"id": product_id})
product = await self._app.entity(Product).get(params={"id": product_id})
await self._app.entity(Product).update(params={"id": product_id}, payload={"stock": 0})
```

---

## Execution lifecycle

`RuntimeExecutor.execute` owns one execution end to end — from opening the
unit of work to running the post-commit actions it queued. REST use cases
and Celery jobs run through the same lifecycle; the Celery worker drives it
through the async bridge, not through a lifecycle of its own.

```
EXEC_START
  → unit of work __aenter__      (skipped when read_only or no uow_factory)
  → pipeline                     (binds, loads, computes, rules, execute())
  → unit of work __aexit__       (commit on success, rollback on failure/cancellation, always closes)
→ one terminal event: EXEC_DONE or EXEC_ERROR
→ post-commit actions drained   (job dispatch, on_transaction_committed hooks)
```

The executor drives the unit of work through its context-manager protocol
only (`__aenter__` / `__aexit__`); `begin()` / `commit()` / `rollback()` stay
on the `UnitOfWork` protocol for adapters and hand-driven use — the executor
never calls them directly.

### What runs after commit

Post-commit actions — job dispatches queued through `JobService` and
`on_transaction_committed` hooks enqueued by the `@transactional` decorator,
and the cache invalidations a [cached repository](cache.md) publishes — are enqueued on a
`PostCommitChannel` bound to the execution and drained only after the unit
of work has closed, outside its transaction, in enqueue order:

- **With a unit of work**: actions run after `commit`, never inside the
  transaction — a broker failure cannot roll back a write that already
  committed.
- **Without a unit of work, or `read_only=True`**: the same actions run at
  the end of the execution — there is nothing to commit, but they still run
  once, not per repository call.
- **On failure or cancellation**: the owner discards its queue instead of
  running it — nothing partially dispatched leaks into a later execution
  sharing the same async context.

A failure during `drain()` does not replace the terminal event already
emitted: it surfaces as `PostCommitError(failures=(...))` raised by
`execute()` after `EXEC_DONE`. Every action still runs — failures are
collected, not short-circuited.

`PostCommitError.committed` says whether the owner that drained had
committed a unit of work of its own:

- `committed=True` — the execution owned a unit of work and it committed.
  A retry would repeat a write that already stands.
- `committed=False` — the execution owned no unit of work (`read_only=True`,
  no `uow_factory`), or the drain came from a hand-driven
  `flush_pending_dispatches()` outside any execution. Nothing was written,
  so the whole operation is safe to retry.

### Nesting

An execution owns the post-commit channel **iff** it owns the unit of work,
or no channel is bound yet:

- A nested call that joins the outer unit of work (or finds none active)
  enqueues on the outer channel, drained once at the outer execution's end.
- A nested call that opens its own unit of work — typically a read-only
  outer execution calling a writing inner one — owns and drains its own
  channel right after its own close. An outer failure afterward cannot
  discard the actions of an inner transaction that already committed.
- An execution started **from** a post-commit action (inside `drain()`)
  finds no channel bound and opens its own lifecycle.

### Cancellation

Cancelling an execution mid-flight is a terminal outcome, not a special
case: the unit of work still closes exactly once. Adapters shield their own
driver I/O (`session.rollback()`, session close, `end_session`) with
`asyncio.shield`, so a cancellation arriving while closing lets the close
finish instead of cutting it off, and reset their `ContextVar` tokens in the
*caller's* context — a shielded coroutine runs in a copied context, where
`ContextVar.reset` raises. The executor itself
never shields; only the adapters do, around their own I/O. `EXEC_ERROR`
carries `error_kind="cancelled"` for a cancelled execution.

### Events

| Event | When | Carries |
|---|---|---|
| `EXEC_START` | Before the unit of work opens (or before the pipeline, when none is configured) | `use_case_name`, `trace_id` |
| `EXEC_DONE` | The only success terminal event — after commit, or after the pipeline when there is no unit of work | `duration_ms`, `pipeline_ms`, `commit_ms` |
| `EXEC_ERROR` | The only failure terminal event | `error_kind` (`begin`, `business`, `commit`, `cancelled`, `post_commit`), `duration_ms`, `pipeline_ms`, `commit_ms` |

`error_kind` names the step that failed: `begin` for `__aenter__`,
`business` for the pipeline, `commit` for the unit-of-work exit, `cancelled`
for a cancellation, and `post_commit` when the propagating exception is a
`PostCommitError` — an inner execution that owned its own unit of work
committed and then failed to drain, so the outer failure is not its own
business logic.

`duration_ms` measures start → terminal event; `pipeline_ms` measures the
pipeline stage alone; `commit_ms` measures the unit-of-work exit (`None`
when no unit of work was opened). A post-commit failure raises no new event
kind — it surfaces only as `PostCommitError`, after `EXEC_DONE`.

### The `transactional` declaration

Every unit-of-work adapter declares `transactional: ClassVar[bool]`: `True`
when `commit` / `rollback` are real (SQLAlchemy, Mongo with
`transactions: true`), `False` for adapters whose writes autocommit
(DynamoDB, Mongo with `transactions: false`). The protocol is
runtime-checkable, so an adapter that omits the declaration no longer
satisfies `isinstance(x, UnitOfWork)`. Nothing reads the flag yet: the
bootstrap check that refuses a use case requiring a transaction on a
non-transactional backend, and the cache rules keyed on it, arrive in a
later release; see [Persistence backends](persistence-backends.md).

### `PostCommitError` in REST and Celery

`PostCommitError` carries `committed`, and both transports read it:

- **REST**: `HttpErrorMapper` maps it to `500` and copies the flag into the
  response body's `detail` as `committed` — `true` means the write stands
  even though the request failed, `false` means nothing was written.
- **Celery**: the worker does **not** retry a `committed=True` failure (a
  retry would repeat the write) and retries a `committed=False` one like any
  other failure — see [Post-commit failures](celery.md#post-commit-failures).

### Hand-driven units of work

Constructing a unit of work directly (e.g. `SQLAlchemyUnitOfWork`) and
calling `begin()` / `commit()` / `rollback()` by hand keeps working — those
methods stay on the protocol. Closing is the context manager's job
(`__aexit__`), not `commit()`'s: a hand-driven caller that never enters the
`async with` block leaves the session open.

### Upgrade notes

- **Metrics timing changed**: `EXEC_DONE` now fires after commit, not
  before — a consumer computing latency from `EXEC_DONE.duration_ms` alone
  now sees the commit included; `pipeline_ms` and `commit_ms` split it back
  out.
- **`PostCommitError` where a rollback used to be reported**: a broker or
  cache-bump failure after a successful commit used to roll back an
  already-committed transaction; it now raises
  `PostCommitError(committed=True)` instead — the write stands, the failure
  is reported separately. The same failure in an execution that owned no
  unit of work raises `PostCommitError(committed=False)`, which REST reports
  as `committed: false` and the Celery worker retries.
- **`flush_pending_dispatches()` now raises `PostCommitError`** instead of
  letting an individual dispatch's exception propagate directly, and
  **raises `RuntimeError` when called inside an execution**: the executor
  owns the post-commit channel and drains it after the unit of work closes.
  Call it only outside an execution, as `InlineJobService` documents.
  `clear_pending_dispatches()` discards the executor's channel when one is
  bound, so a hand-driven caller cancelling dispatches inside an execution
  now really cancels them.
- **`UnitOfWork` is a runtime-checkable protocol with a data member**:
  `isinstance(x, UnitOfWork)` is `False` for an adapter that does not
  declare `transactional`, and `issubclass(X, UnitOfWork)` raises
  `TypeError` — Python cannot check a data member on a class. Third-party
  adapters must declare `transactional: ClassVar[bool]`; code that used
  `issubclass` must switch to `isinstance` on an instance.
- **`error_kind="commit"` means the commit itself failed**: the outcome is
  undetermined — the driver may have committed before the failure surfaced.
  Treat it as "unknown", not as "rolled back"; the adapter still rolled back
  and closed on its side (SQLAlchemy) or ended the session (Mongo). A failure
  while *closing* after a successful commit is logged as `UoWCloseFailed` and
  does not fail the execution on either adapter: the write landed, so the
  terminal event is `EXEC_DONE` and the post-commit actions run.
- **`error_kind="post_commit"`** is new: an outer execution whose inner one
  committed and then failed to drain no longer reports `business`.
- **Hand-driven `begin()`/`commit()` callers must close through the context
  manager** (`async with uow:` or an explicit `__aexit__` call) — calling
  `commit()` alone no longer closes the session.

---

## DSL quick-reference

| Primitive | Purpose |
|-----------|---------|
| `F(Cmd).field` | Typed field reference |
| `Input()` | Inject decoded request body as command |
| `LoadById(Model, by=...)` | Fetch entity by path/command param; 404 on missing |
| `Exists(Model, from_param=..., against=...)` | Check existence; 404 on missing if `RAISE` |
| `Compute.set(F).from_command(...)` | Derive/normalise a command field |
| `Compute.set(F).from_params(...)` | Include path params in derive computation |
| `Rule.check(F, via=fn)` | Validate a field; `fn` returns error string or `None` |
| `Rule.forbid(predicate, message=...)` | Enforce invariant; predicate returns `True` to fail |
| `.from_command(F1, F2, ...)` | Pass command fields to rule predicate |
| `.from_params("name")` | Pass path parameter to rule predicate |
| `.when_present(F)` | Skip rule/compute when `Patch` field absent |
| `F1 & F2` | AND predicate — both must be present |
| `F1 \| F2` | OR predicate — either must be present |
| `ApplicationInvoker.invoke(UseCase, ...)` | Call another use case by type |
| `ApplicationInvoker.entity(Model)` | CRUD facade for a model entity |
| `Agent(name)` → `AgentHandle[T]` | Handle to a named agent, bound to the verified caller |
| `handle.run(prompt)` / `run(prompt, expect=X)` / `run_text(prompt)` | The three run modes — declared shape, per-run shape, open text |
| `handle.mcp(server)` / `handle.sql(connection)` | The artefact's own granted views, never re-declared |
| `Mcp(server, include=[...])` → `McpHandle` | Handle to a configured MCP server, reached directly, bound to the verified caller |
