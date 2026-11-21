# Ports and adapters

A use case depends on ports: protocols it declares in its constructor. An
adapter implements a port and declares, in its own constructor, what it needs
in turn, including the agents and MCP servers it calls. A DI module binds each
port to its adapter. loom builds everything inside the execution that runs the
use case, and checks the whole graph when the application starts.

Every example on this page is a file under `tests/docs/examples/`, run by
`tests/docs/test_ports_and_adapters.py`.

## Ports in use-case constructors

Declare the port as a `Protocol` and take it as a constructor parameter. The
use case imports no adapter and no infrastructure.

```{literalinclude} ../../tests/docs/examples/invoicing.py
:language: python
:pyobject: InvoiceNotifier
```

```{literalinclude} ../../tests/docs/examples/invoicing.py
:language: python
:pyobject: IssueInvoice
```

`Caller()` injects the identity the execution acts as (see
[Call-time identity](#call-time-identity)). `invoice_id` has no marker, so the
caller passes it by name in `params`.

## Adapters

An adapter is a plain class. Its required constructor parameters are resolved
from the container (autowiring): a bound key, a `RepoFor[Model]` through the
repository mapping, or an execution kind (`Caller()`, `Agent()`, `Mcp()`). A
parameter with a default that is not a marker is left to Python.

```{literalinclude} ../../tests/docs/examples/invoicing.py
:language: python
:pyobject: EmailInvoiceNotifier
```

An installer binds the port to the adapter, and the adapter's own dependencies:

```{literalinclude} ../../tests/docs/examples/invoicing.py
:language: python
:pyobject: invoicing_module
```

`register(Port, Adapter)` binds at `Scope.REQUEST` by default: one instance per
execution, shared by everything that execution resolves. `register_instance`
binds an object you built as an application singleton. A factory function
works like a class: `container.register(Gateway, gateway)` resolves the
function's required parameters from the container.

A composed application takes its modules through
a discovered `Installer` in `app.packages`. A
standalone kernel takes them through `create_kernel`:

```{literalinclude} ../../tests/docs/examples/invoicing.py
:language: python
:pyobject: invoicing_kernel
```

```{literalinclude} ../../tests/docs/examples/invoicing.py
:language: python
:pyobject: issue_as
```

## Adapters that declare agents and MCP servers

An adapter that calls an agent or an MCP server declares it with an `Agent()`
or `Mcp()` default in its constructor. The use case still depends only on the
port.

```{literalinclude} ../../tests/docs/examples/crm.py
:language: python
:pyobject: CrmDirectory
```

```{literalinclude} ../../tests/docs/examples/crm.py
:language: python
:pyobject: TriageTicket
```

```{literalinclude} ../../tests/docs/examples/crm.py
:language: python
:pyobject: crm_module
```

- Each declaration resolves to one handle. The handle holds no identity: every
  call acts as the execution identity of that moment, so an adapter bound as a
  singleton serves each caller as itself.
- A composed application that hosts agents binds the resolvers of its agent
  runtime on the executor. Start-up walks every `Agent()` and `Mcp()`
  declaration its use cases, jobs and callbacks reach, through constructors,
  factory functions and `execute()` parameters, and checks each against the
  `ai:` section: an unknown agent or MCP server stops start-up with
  `AgentCompilationError`, and each `include` is checked against the server's
  tool list. An
  entry point that does not host agents refuses any such declaration, unless
  its recipe is lenient: then those declarations are not checked and fail
  when first used.
- A standalone kernel takes the resolvers as
  `create_kernel(agent_resolver=..., mcp_resolver=...)`, or later through
  `UseCaseExecutor.bind_agent_resolver` and `bind_mcp_resolver`. A resolver is
  a `loom.core.execution.ExecutionResolver`: `resolve(declaration, annotation)`
  returns the handle, and the handle reads the identity at call time.

## Autowiring and start-up refusals

A composed application analyses its dependency graph at the end of
registration, from its use cases, jobs and job callbacks. It refuses, with the
path from the root, as a `BootstrapError` wrapping
`loom.core.di.graph.DependencyGraphError`:

- a key with no binding;
- a cycle of constructor parameters;
- an `APPLICATION` binding holding a `REQUEST` one, or a `Caller()`;
- an input marker (`Input()`, `Query()`, `LoadById()`, `Load()`, `Exists()`)
  as a constructor default;
- a required constructor parameter that names nothing the container serves
  (a primitive, `typing.Any`, or a class nothing is bound under);
- a job callback asking for a `RepoFor[Model]` whose model has no repository
  mapping. A use case, job or adapter asking for one fails when it is compiled.

The graph reads constructor signatures of classes with ordinary `__new__` and
metaclass `__call__`, and signatures of plain functions. A lambda is a plain
function, but dependencies hidden in its body or closure contribute no graph
edges; a bound method or another callable whose signature is not read also
contributes none. Those dependencies are not checked at start-up and fail at
first build. Bind a class or a plain function with explicit parameters to keep
its dependencies inside this guarantee.

When the container is sealed, its singletons are built with no execution. A
singleton the graph reaches that declares an `Agent()`, `Mcp()` or other kind
with no resolver bound is refused at start-up with its path. A singleton
nothing reaches is not built: it warns with
`loom.core.di.errors.DependencyWarning` and fails with the reason when first
resolved.

A standalone `create_kernel` validates its `APPLICATION` bindings and runs
`kernel.factory.verify()` over its use cases at start-up. Missing bindings and
cycles therefore stop kernel creation:

```{literalinclude} ../../tests/docs/examples/refusals.py
:language: python
:pyobject: ScheduleReminder
```

`create_kernel` raises `BootstrapError` caused by
`DependencyGraphError: ScheduleReminder -> clock: ReminderClock: no binding is registered for it`.

## Overrides and reserved keys

The framework owns some keys. A module that binds one fails right after it
runs:

- `create_kernel` reserves `UseCaseFactory`, `UseCaseExecutor`,
  `UseCaseRegistry` and `ApplicationInvoker`
  (`loom.core.bootstrap.kernel.KERNEL_KEYS`) and raises `BootstrapError`
  naming the module and the key.
- A composed application also reserves `SqlQueryService`, `CallerBoundSql`,
  `Observability`, `StoreCatalog`, `ElevationSink` and each store's
  `SessionManager` or `MongoSessions`, and raises
  `loom.composition.ReservedBindingError`.

```{literalinclude} ../../tests/docs/examples/refusals.py
:language: python
:pyobject: factory_module
```

Overrides are DI modules that run right after the caller's modules, under the
same reserved-key check (`Recipe.overrides`). The testing harness sets them; no
production entry point does. An override binds a test double of a repository
under the key its store would bind it, so the double is what the use case
gets. The caller's modules and the overrides run before the framework's
defaults, which leave what they bound alone.

A composed application's container is sealed once it has started:
`register`, `register_default`, `register_instance` and `register_repo` on it
raise `loom.core.di.ContainerSealed`, and so do declaring an execution kind on
`container.kinds` and binding a resolver (`bind_agent_resolver`,
`bind_mcp_resolver`). Bind every key in an installer, and every resolver before the
seal. The container `create_kernel` returns stays unsealed.

## The composition pipeline

`Project.compose(runtime)` runs these phases in
order; `create_app` loads the project (`Project.load`) and calls
`Project.serve`. Nothing registers after `register`.

1. `plan`: the runtime returns its `Recipe`; failing deploy checks stop
   start-up in a deployed environment.
2. `build_stores`: every store the discovered models use is built.
3. `register`: the container is filled in this order: the seed keys
   (`AppConfig`, `Environment`, `Observability`, `StoreCatalog`), the
   caller's modules, the overrides, each store's repositories, the cache
   module, the kernel (which compiles the use cases), the job service, the SQL
   collaborators.
4. `analysed`: the dependency graph is analysed from the use cases, jobs and
   callbacks (the refusals above).
5. `wire_ai`: the agent host is built when the entry point hosts agents,
   every `Agent()` and `Mcp()` declaration is checked, and the resolvers are
   bound.
6. `seal`: the container is sealed and its singletons built; each use case and
   job is checked against the stores it writes to; the factory verifies the
   use cases.

The application logs one `loom.composition.composed` event with the
milliseconds of each phase (`phase_ms`).

## Call-time identity

### The acting identity

The identity a request runs as governs everything that request causes: nested
executions, the tasks it starts, the jobs and callbacks it dispatches,
row-level security and audit. Code reads it where it runs, not where it was
built:

```{literalinclude} ../../tests/docs/examples/identity.py
:language: python
:pyobject: AuditStamp
```

```{literalinclude} ../../tests/docs/examples/identity.py
:language: python
:pyobject: stamp_as
```

- `execution_identity()` returns the identity the execution in progress acts
  as. `live_execution_identity()` also refuses it past the delegation
  deadline. `current_identity()` returns `ANONYMOUS` when no caller acts, and
  `current_caller()` returns `None`.
- An execution given no identity acts as the identity acting: the enclosing
  execution's, the transport's, or the one a task inherited.
- To act as someone else, switch explicitly:
  `executor.run(UseCase, identity=X)`, `invoker.for_identity(X)` or
  `ExecutionScope(X)`. Each switch is logged at `WARNING` as
  `identity_switched`. `set_identity` inside an execution, or in a task it
  started, raises `IdentityFixed`.
- A custom transport installs the verified caller with
  `set_identity(identity)` and restores it with `reset_identity(token)`. Code
  that opens an execution by hand uses `ExecutionContext.entered(identity,
  what=...)` and closes it with `close()` in a `finally` block, or uses
  `ExecutionScope`.

### Delegation window

Work a request causes acts as its caller for at most
`identity.max_delegation_seconds` (default 3600) from the request's start. The
composition sets it for the process with `configure_delegation(seconds)`; a
value that is not a finite number above zero is a `ConfigError`. A kernel built
with `create_kernel` outside the composition uses the default.

Expiry is checked where work starts (an execution, a job, a callback,
`ExecutionScope`), at the start of every SQLAlchemy transaction, and by
`execution_identity()` read from a task whose execution ended. It raises
`DelegationExpired`, an `Unauthenticated`. `current_identity()` never checks
it, so work started before the deadline may finish. Run long background work
as a system identity, switching explicitly with
`executor.run(UseCase, identity=...)`.

A dispatched job carries the authenticated caller and its deadline,
`captured_delegation()`, in every backend (inline, Celery, SQS). An anonymous
caller is carried as no caller:

```{literalinclude} ../../tests/docs/examples/identity.py
:language: python
:pyobject: captured_by_dispatch
```

### Fail closed

- `execution_identity()` outside any execution raises `ExecutionRequired`; in
  an execution with no caller it raises `Unauthenticated`.
- A use case declaring `Caller()`, `Agent()` or `Mcp()` run with no identity
  raises `MissingCaller`, an `Unauthenticated` naming the use case, the
  parameter and its marker.
- An `Agent()` or `Mcp()` handle called outside any execution raises
  `ExecutionRequired`; called past the delegation deadline it raises
  `Unauthenticated` before reaching the model, the MCP server or the database.
- A singleton is built with no execution: a constructor reading the execution
  identity raises `ExecutionRequired`.

### Job callbacks act as their dispatcher

Job callbacks (`on_success`, `on_failure`; inline and Celery, sync and async)
are built and called inside an execution acting as the identity captured when
the job was dispatched. A constructor `Caller()`, a handle, row-level security
and a job the callback dispatches see the dispatcher. A job dispatched with no
caller gives its callbacks none, and a callback declaring `Caller()` then
raises `Unauthenticated`. `REQUEST` bindings of a callback are built once per
callback run.

## Nested executions get a fresh context

A `REQUEST` binding is one instance per execution. A nested execution, the
same identity or another, gets instances of its own. Outside any execution,
each `resolve` builds a new one.

```{literalinclude} ../../tests/docs/examples/identity.py
:language: python
:pyobject: baskets
```

`first is again` and `nested is not first`. Tasks an execution creates while it
runs share its instances. A task that outlives its execution keeps acting as
its caller, but a `REQUEST` binding read there raises `ExecutionClosed`.

## Testing with doubles

Bind the double in a module that runs after the application's modules. A
later binding replaces an earlier one; `register_instance` binds the object
you hold, so the test can inspect it.

```{literalinclude} ../../tests/docs/examples/doubles.py
:language: python
:pyobject: RecordingNotifier
```

```{literalinclude} ../../tests/docs/examples/doubles.py
:language: python
:pyobject: with_double
```

`invoicing_kernel(with_double(notifier))` builds the kernel with the double;
after `issue_as(kernel, ada, "INV-7")`, `notifier.issued` is
`[("INV-7", "ada")]`. An `ExecutionResolver` double serves `Agent()` and
`Mcp()` declarations the same way, passed to `create_kernel(agent_resolver=...,
mcp_resolver=...)`. For HTTP tests, `HttpTestHarness.inject_repo` binds a fake
repository as an override (see [Testing](../rest/testing.md)).

## How hexagonal loom is

loom is a framework: it provides the adapters, so three levels are judged
separately.

- **The application is strictly hexagonal.** Use cases declare only ports in
  their constructor. Adapters declare their markers (`Agent()`, `Mcp()`,
  `Caller()`) in their own constructor, and loom resolves them per execution.
  Application code depends on no infrastructure.
- **loom's own layers point inwards.** `tests/unit/test_layers.py` enforces
  it: `loom.core` imports none of `loom.composition`, `loom.rest`,
  `loom.celery` and `loom.ai`, and `loom.composition` imports no transport.
- **loom keeps the coupling a framework accepts.** msgspec is its model
  language (models, commands, configuration). The standard `logging` module and
  OpenTelemetry sit behind `loom.core.logger` and `loom.core.observability`. contextvars and
  asyncio are the Python runtime.

Known debt:

- Island clients under `loom.connections` (`aws`, `mongo`, `clickhouse`)
  still import their drivers. The store adapters live in `loom.stores`.
- The logger does not fit AWS Lambda. Its redesign is planned next.

## Migrating ports, DI and identity

| Change | Before | After |
|---|---|---|
| REST adapter builds the use case | `RestUseCaseDispatcher(executor)`; `await adapter.handle(CreateUser(repo=users), ExecutionRequest(params=p, dependencies={User: users}))` | `RestUseCaseDispatcher(kernel.executor, factory=kernel.factory)`; `await adapter.handle(CreateUser, ExecutionRequest(params=p))`, with `users` bound in the container. `ExecutionRequest.dependencies` is removed. |
| Sealed container | `application.kernel.container.register(...)` after start-up | Bind the key in a module: a discovered `Installer` in `app.packages`. A sealed container raises `ContainerSealed`. |
| Kernel keys | A module binding `UseCaseFactory`, `UseCaseExecutor`, `UseCaseRegistry` or `ApplicationInvoker` was silently replaced | Drop the binding; `create_kernel` raises `BootstrapError`, a composed application `ReservedBindingError`. |
| Acting as someone else | `set_identity(X)` then a call, inside a use case | `executor.run(UseCase, identity=X)` or `invoker.for_identity(X).invoke(...)`. `set_identity` inside work raises `IdentityFixed`. |
| Long background work | Ran as the request's caller indefinitely | Runs as the caller for at most `identity.max_delegation_seconds`; switch to a system identity with `executor.run(UseCase, identity=...)`. |
| Job wire | `encode_identity(identity)` only | `Delegation(caller, until).encoded()` and `Delegation.decoded(payload)` carry the deadline; `encode_job_message(..., identity=)` also takes a `Delegation`. Messages without `until` get a fresh window at the worker. |
| Agent and MCP resolvers | `(name, identity)` and `(server, include, identity)` callables | Bind an `ExecutionResolver` with `resolve(declaration, annotation)`; positional callables raise `TypeError`. |
| AI dependency bundle | Tools acted as the `identity` of the dependency bundle | The bundle needs only `container`; an `identity` or `invoker` on a custom bundle is not read. Tools act as `execution_identity()`. |
| Execution plan | `ExecutionPlan(agent_bindings=..., mcp_bindings=...)` | `ExecutionPlan(execution_bindings=...)` with `ExecutionBinding` entries; the two properties still read. |
| `Scope.REQUEST` | A new instance per `resolve` | One instance per execution, shared by its tasks; a nested execution gets its own. |
| Re-registering a key | Built instances kept | The key's built instance is dropped and every binding is recompiled at its next resolve. |
| Kernel builder | `bootstrap_app(...)` returning `BootstrapResult`; `KernelRuntime`; `kernel.app` | `create_kernel(...)` returning `Kernel`, the only builder; the invoker is `kernel.invoker`. `loom.composition.create_app(...)` composes a whole application. |
| Bare factory | `UseCaseFactory(LoomContainer())` | `loom.composition.create_app(...)` or `create_kernel(...)`; the bare form emits `DeprecationWarning`. |
| Patch targets | `loom.composition.build.configure_job_service`, `.reject_manifest_agents_without_ai_section`, `.load_application_config`, `.add_code_path` | `loom.composition.phases.configure_job_service`, `loom.composition.project.load_application_config`, `loom.composition.project.add_code_path`. The manifest-agents check has no patch target: it runs inside the AI wiring and raises `ConfigError`. |
| Runtime events | `ExecutionEvent(status="success")` | `ExecutionEvent(status=Status.SUCCESS)`, with `Phase` for `error_kind` (`loom.core.engine.events`). |
| Compute and rule steps | Telling `ComputeStep` from `RuleStep` by type | Both are `CommandStep`; read the plan field holding them (`compute_steps`, `rule_steps`). |
| Execution logs | `[EXEC] <use case>` start line; `[STEP]` lines; `[RULE]` warnings | One terminal `[DONE]` or `[FAIL]` line per execution; `steps` and `broken_rules` fields on it. |
