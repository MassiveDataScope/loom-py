# Composition and lifecycle

Every loom entry point — the REST API, the Celery worker, a script, a test —
composes its application the same way. `loom.composition` loads the
configuration, discovers the code, builds persistence, the kernel,
observability, the job and SQL services and, when the entry point hosts them,
the agents, and returns an `Application` that owns every resource it built. A
transport adapter then mounts itself on that application. Nothing in
`loom.composition` imports FastAPI, Starlette, the Celery worker or
`loom.rest`.

```python
from loom.composition import create_app
from loom.rest.fastapi.adapter import FastAPIAdapter

app = create_app("config/api.yaml", adapter=FastAPIAdapter())
```

When `app.entry` is set, `create_app("config/api.yaml")` selects the adapter
from the loaded configuration. The built-in entries are `fastapi` and
`lambda`; any other entry names a function of `lambda.functions`, served by
`LambdaAdapter`. An explicit `adapter=` takes precedence. Without either
`app.entry` or `adapter=`, startup raises `ConfigError`.

`entry: lambda`, or `LambdaAdapter()` without a declared function, serves
every interface over HTTP. Several Lambda functions can share one YAML file:
name each one under `lambda.functions`, declare what invokes it under its
`trigger`, then set `app.entry` to the function to serve:

```yaml
app:
  name: catalog
  entry: public_api
lambda:
  functions:
    public_api:
      trigger:
        source: http
        interfaces: [CatalogInterface, SearchInterface]
    admin_api:
      trigger:
        source: http
        interfaces: [AdminInterface]
        auth:
          jwt:
            secret_path: /secrets/admin.key
            algorithms: [HS256]
            audience: admin
            issuer: https://idp.example.com
    worker:
      trigger:
        source: jobs
        queue: {name: jobs, batch_size: 10}
```

A `trigger` takes the keys of its `source` only:

| `source` | Invoked by | Keys |
|---|---|---|
| `http` | API Gateway | `interfaces`, `auth` |
| `jobs` | An SQS queue of loom `JobEnvelope` messages | `queue`: a block of `name`, `batch_size`, `batching_window_s`, `max_concurrency`, `report_batch_item_failures` |
| `eventbridge`, `s3`, `sns`, `schedule` | The service, directly or through a queue | `job`, `identity`; directly `maximum_retry_attempts` (2) and `maximum_event_age_s` (21600); through a queue the `queue` block of `jobs` |

An HTTP function serves only the interfaces it lists, a Python `RestInterface`
by its class name and a YAML one by its `app.rest.interfaces` key; other routes
return 404. Its `auth` uses the `app.rest.auth` schema and replaces that
section for the function; an authenticator passed in code needs `custom: true`
there, as it does in `app.rest.auth`. In staging and production,
`LOOM__APP__ENTRY=admin_api` can select a function without changing the YAML.
An interface can belong to only one function. Startup refuses unknown or
ambiguous names and warns about discovered interfaces assigned to none.

## Event triggers

An `eventbridge`, `s3`, `sns` or `schedule` function runs the job it declares
for each event:

```yaml
lambda:
  functions:
    invoices_in:
      trigger:
        source: eventbridge
        job: ImportInvoice
        identity: {subject: svc:invoices, roles: [invoice_importer], attributes: {tenant: acme}}
    invoices_buffered:
      trigger:
        source: eventbridge
        queue: {name: invoice_events}
        job: ImportInvoice
        identity: {subject: svc:invoices, roles: [invoice_importer]}
```

| `source` | The job's `Input()` | Span id |
|---|---|---|
| `eventbridge` | The event's `detail` | The event's `id` |
| `schedule` | The schedule's input, as configured in EventBridge Scheduler | None |
| `sns` | The notification's `Message`, a JSON object | Its `MessageId` |
| `s3` | One `S3Object` (`loom.serverless.aws.lambda_`) per object: `bucket`, `key` (URL-decoded; untrusted, validate it before using it as a path or an identifier), `size`, `etag`, `version_id`, `sequencer`, `event_name`, `event_time` | None |

The declared `source` picks how the event is read; loom never guesses it from
the event. Startup and `loom check --deploy` refuse a job that is not
discovered, that declares no `Input()` command or that requires another
parameter, a `job` or `identity` given by a placeholder, two functions
consuming the same queue, and a `config.overridable` entry reaching
`lambda.functions.*.trigger`, `lambda.handler` or `app.discovery`.

The job runs as the declared `identity`, with
`mechanism: lambda:<function>:<source>`, and never as anything the event
says. `identity` is required on `eventbridge`, `s3`, `sns` and `schedule`
triggers: a trigger without one fails to load, so no automatic change runs
without an actor, and the audit log records this identity as the actor of
every change the job makes. The identity grants roles only: it implies no elevation
and no `BYPASSRLS`. Whoever may invoke the function, or write to its queue,
runs the declared job as the declared identity: restrict
`lambda:InvokeFunction` and the queue policy, and pin such a function in code
with `LambdaAdapter(function=...)`.

When AWS itself records who caused the event, the identity acts on their
behalf: the audit log's `on_behalf_of` holds the `userIdentity.principalId` of
an S3 record, or the `detail.userIdentity.arn` of an EventBridge event whose
`source` starts with `aws.` (`PutEvents` refuses that prefix, so no publisher
can forge one). A custom EventBridge event, SNS and `schedule` name nobody,
whatever their payload says. The `loom:` attribute prefix is reserved: an
`identity.attributes` key under it fails to load.

Invoked directly, the function fails the invocation whenever a job fails,
retryable or not, a `Command` validation error included. Lambda retries the
event `maximum_retry_attempts` times within `maximum_event_age_s`, then sends
it to the function's on-failure destination, which the deployment must
configure; an event is never dropped silently. An S3 or SNS event holding
several records runs one job per record, in order, and stops at the first
failure, so a retry runs the earlier records again: jobs must be idempotent.
The S3 `s3:TestEvent` runs no job. An SNS message that is not a
`Notification`, a subscription confirmation included, is refused; loom never
fetches its `SubscribeURL`. The failure log names the function, job and
error, never the payload.

A `queue:` block reads each message body as the source delivers it to the
queue it names and reports each failed message, a failure that is not
retryable included, so the queue retries it and then sends it to its
dead-letter queue. The block takes the keys of a `jobs` trigger's `queue`; the
retry keys apply to a direct trigger only. A `jobs` function consumes a
message whose job fails for good, as before. An SNS subscription to that queue
must not use raw message delivery.

The rule or pattern, bucket and prefix, topic or schedule expression that
send the events are not loom configuration: the deployment creates them.

The design and its sources are recorded in ADR 0007
(`docs/architecture/adr/0007-composition-root.md`).

## Entry points

| Function | Returns | Use it for |
|---|---|---|
| `create_app(*config_paths, adapter=..., ...)` | what the adapter's `bind` returns: a `FastAPI` app, a `Celery` app, a handler | starting a process; `adapter=` may be omitted when `app.entry` is set |
| `build_application(*config_paths, adapter=..., ...)` | the composed, closed `Application`; no transport mounted | tests, scripts, a host that drives the application itself |
| `compose_application(source, adapter, modules=...)` | the composed, closed `Application` | a host that already holds a loaded `CompositionSource` |

`create_app` and `build_application` take the same arguments: the YAML
`config_paths` or `config_dir`, `environment`, `code_path`, `resolvers` and
`modules`, exactly as the REST `create_app` documented in
[Environments](environments.md) and [Bootstrap with YAML](rest.md#bootstrap-with-yaml).
`build_application` still needs `adapter=`: the adapter's recipe decides what
is composed (pool profile, discovery, backend), even though no transport is
mounted.

The transport-specific factories stay, with the same signatures, as
shortcuts over the composition:

| Shortcut | Equivalent to |
|---|---|
| `loom.rest.fastapi.create_app(..., metrics_registry=, authenticator=)` | `create_app(..., adapter=FastAPIAdapter(metrics_registry=, authenticator=))` |
| `loom.celery.auto.create_app(..., jobs=, callbacks=, metrics=)` | `create_app(..., adapter=CeleryAdapter(jobs=, callbacks=, metrics=))` |
| `loom.celery.bootstrap.bootstrap_worker(...)` | the same composition; returns `WorkerBootstrapResult` |

`FastAPIAdapter` lives in `loom.rest.fastapi.adapter` and `CeleryAdapter` in
`loom.celery.adapter`.

## What `create_app` runs

The order is fixed; no adapter reorders it:

1. Resolve the environment, select and load the configuration (resolvers,
   `LOOM__*` overrides).
2. Configure observability from `observability`.
3. Put the code path on `sys.path`.
4. Ask the adapter for its recipe: `adapter.recipe(source)`. Discovery runs
   here, inside the adapter.
5. Run the deploy checks, skipping the areas the recipe names
   (`skip_checks`).
6. Build the backend with the recipe's profile, then `prepare_models` (compile
   the tables, bind the row-level-security settings).
7. Build the kernel: your `modules` first, then the repositories, then the
   cache module.
8. Register `JobService` (Celery when a `celery` section is present, inline
   otherwise), `SqlQueryService`, `CallerBoundSql` and `ObservabilityRuntime`.
   `loom.celery` is imported lazily: a `celery` section that only dispatches
   jobs does not load the worker.
9. When the recipe hosts agents and an `ai` section exists, build the agent
   runtime and verify the `Agent()` and `Mcp()` markers.
10. Verify every use case's dependencies (`UseCaseFactory.verify()`).
11. Hand the application to the adapter: `adapter.bind(application)`.

No step talks to a database. Building a backend creates its engine or client,
not a connection; only `Application.check()` and `Application.running()`
reach the outside.

## Recipes and adapters

An adapter is any object with two methods, the `Adapter` protocol:

- `recipe(source) -> Recipe` declares what to compose. It is synchronous and
  opens nothing.
- `bind(application) -> T` mounts the transport and returns what the entry
  point serves. It opens nothing either: the application's lifecycle owns
  every resource.

A `Recipe` is a frozen dataclass:

| Field | Meaning |
|---|---|
| `profile` | Pool defaults of the entry point: `SERVER_PROFILE`, `WORKER_PROFILE`, `STREAMING_PROFILE` or your own `RuntimeProfile`. A key in `stores.default.pool` still wins. |
| `discovered` | The `DiscoveryResult` to compile: models, use cases, interfaces, agent specs, after whatever the adapter added or pruned. |
| `default_backend` | Backend of the default store when `stores.default` is not declared. Default `none`. |
| `skip_checks` | Deploy-check areas the entry point does not serve, such as `REST_CHECKS` for a worker. |
| `agents` | Whether the entry point hosts the agent runtime; without it an `ai` section is ignored. Default `False`. |
| `agent_endpoints` | Whether the adapter mounts agent endpoints. `False` mounts none, so only the agents an `Agent()` marker names are compiled, as on Lambda. Matters only with `agents=True`. Default `True`. |
| `metrics` | Use-case metrics, or `None`. |
| `pool_metrics` | Connection-pool metrics, for an entry point that serves a scrape. |
| `connection_only` | Ask the backend for its connection only, no repository (see [Workers without models](#workers-without-models)). |

A complete adapter, for a script that runs use cases with no transport:

```python
# app/script.py
from loom.composition import Application, CompositionSource, Recipe, create_app
from loom.core.checks import REST_CHECKS
from loom.core.identity import Identity
from loom.core.persistence import WORKER_PROFILE


class ScriptAdapter:
    def recipe(self, source: CompositionSource) -> Recipe:
        return Recipe(
            profile=WORKER_PROFILE,
            discovered=source.discover(),               # app.discovery
            default_backend="sqlalchemy",               # without stores.default
            skip_checks=frozenset({REST_CHECKS}),
        )

    def bind(self, application: Application) -> Application:
        return application


async def main(operator: Identity) -> None:   # Product: a model of your app
    application = create_app("config/script.yaml", adapter=ScriptAdapter())
    async with application.running():
        invoker = application.kernel.app.for_identity(operator)
        await invoker.entity(Product).get(params={"id": 1})
```

`application.kernel` carries the container, the use-case factory, the
executor and the invoker; `application.config`, `environment`,
`observability`, `sql` and `ai` expose the rest of what was composed.

## Lifecycle

An `Application` owns its resources: the cache gateways, the ClickHouse SQL
registry, the agent runtime and the persistence store. They open in that
order and close in reverse. Each resource's startup checks — for the store,
schema creation or verification, the connecting role, the row-level-security
guard and time-zone columns; for the ClickHouse SQL registry, its own
connection checks — run right before it opens, and all of them run in
`check()`.

| Call | Effect | Errors |
|---|---|---|
| `await application.check()` | every resource's startup checks; no connection is left open when it returns | the `ConfigError` of the resource that fails |
| `async with application.running():` | checks and opens each resource in order, yields the application | `RuntimeError` if it is already open; a resource's failure to open, after what had opened is closed |
| `async with application.running(check=False):` | opens without checking | the same |
| leaving the `async with` | closes in reverse order | the failure itself if one resource fails to close; an `ExceptionGroup` if several do |

- A failure to close never hides the error that ended the block: it is
  raised from it. Failures to close what had opened, after a failure to
  open, are attached to the original error as notes.
- No resource can suppress the block's exception.
- `application.is_open` reports whether `running()` is between opening and
  closing. A closed application opens again; each resource reopens only if
  its own manager supports it.
- Open and close on the same event loop: a connection belongs to the loop
  that created it.

A transport that opens and closes in separate calls — the Celery signals,
a Lambda's Init and shutdown — uses `ApplicationHost(application, check=True)`
instead, since each call runs in a task of its own and an agent runtime or an
MCP client must be closed by the task that opened it:

- `await host.start()` spawns one owner task that enters `running()` and
  returns once the application is open, or raises the failure to open as
  itself. A host starts once; a second `start()` raises `RuntimeError`.
- `await host.stop()` closes in that owner task and waits for it; concurrent
  calls all wait, and only the first raises a failure to close. Without a
  start, or once closed, it does nothing.
- A `stop()` during the opening makes `start()` raise `RuntimeError` once what
  opened is closed; a cancelled `start()` closes what opened, and a failure to
  close that no `stop()` raises is logged.

`check()` leaving no connection open is what makes a forking server safe:
the parent checks, the children open, and nothing the parent touched crosses
the fork.

## Wiring per adapter

| Adapter | Checks | Opens | Closes |
|---|---|---|---|
| FastAPI | lifespan | lifespan | lifespan |
| Celery, prefork pool | `worker_init`, in the parent | `worker_process_init`, in each child, with `check=False` | `worker_process_shutdown` and `worker_shutdown`, best effort |
| Celery, any other pool (`solo`, `threads`, ...) | `worker_init` | `worker_init` | `worker_shutdown` |

The FastAPI lifespan only enters `application.running()`: a failure to check
or open stops the server before it serves. The Celery details — what happens
when a child fails to open, and why closing is best effort — are in
[Celery worker](../rest/celery.md#application-lifecycle).

## Workers without models

A Celery worker whose discovery finds no model (raw-SQL or pure jobs) sets
`connection_only` in its recipe. A backend that honours it (`sqlalchemy`):

- registers its `SessionManager` and unit of work, and no repository;
- builds no cursor codec (the `loom.cursors.E001` deploy check still applies,
  since it reads only the configuration);
- leaves the compiled model registry as it is;
- probes at startup with one connection running `SELECT 1`, then releases the
  pool: no table is created and no external schema or column is inspected;
- on Postgres, still refuses a superuser or `BYPASSRLS` connecting role, and
  refuses `create_all` over scoped tables, so the probe cannot open a path
  around row-level security;
- keeps the row-level-security settings installed but unbound, so on Postgres
  with `schema.mode: external` every session fails closed instead of running
  unscoped.

## Startup errors

Composition refuses to build an application that would fail on its first
request or task:

- `UnservedModelError` (`loom.core.use_case.errors`) — a use case or job reads
  a model no repository serves, through `self.main_repo` or through
  `LoadById`, `Load` or `Exists`. It carries the typed `issue` (code
  `UNSERVED_MODEL`, the component, the parameter) and the `model`, and is a
  `ResolutionError`, so a handler catching missing bindings catches it too.
  Every entry point raises it from `verify()`, before serving.
- `ResolutionError` — any other constructor dependency of a use case that is
  not bound.
- `ConfigError` — an incomplete or invalid configuration, an unknown
  persistence backend, or a backend whose extra is not installed (the message
  names `loom-kernel[<backend>]`).
- `ReservedBindingError` (`loom.composition`, code
  `CompositionErrorCode.RESERVED_BINDING`): one of your `modules` registered a
  framework-owned key. `.key` is the key and `.issue` names the module (see
  [Caller bindings and framework defaults](#caller-bindings-and-framework-defaults)).
  It is an entry of the `IssueCode` catalogue and carries a `CompileIssue`
  naming the binding.
- `ElevableScopeInSeveralStores` (`loom.core.persistence.elevation`, code
  `ElevationErrorCode.ELEVABLE_SCOPE_IN_SEVERAL_STORES`): a scope that is
  elevable, or named in `AuthzProduct.elevations`, is declared by two or more
  stores, so an elevation could not reach exactly one of them. It is a
  `ConfigError` and carries a `CompileIssue` naming the scope.
- `UncomposedWriteStore` (`loom.core.use_case.errors`, code
  `UseCaseErrorCode.UNCOMPOSED_WRITE_STORE`): a use case or job writes, as
  its primary or a `Joined` store, to a store with no unit of work, because
  no discovered model belongs to it. It is a `ConfigError` and carries a
  `CompileIssue` naming the component and the store.
- `DeployCheckError` — a deploy check of level error in production or
  staging.

## Caller bindings and framework defaults

Your `modules` run before loom registers its own services. What happens to a
key you bind depends on who owns it:

| Key | A binding in your `modules` |
|---|---|
| `JobService` | kept: the framework's default (Celery or inline) is built and registered only when nothing is bound |
| A repository, or a capability key such as `Listable[Order]` | kept; the usual way to replace one is `repository_for` or a custom `DefaultRepositoryBuilder` (keyed `(DefaultRepositoryBuilder, store)` for a store other than `default`) |
| `CachedCalls`, `RepositoryDecorator` | kept |
| `SessionManager` and `MongoSessions`, also keyed `(…, store)` for any declared store, `SqlQueryService`, `CallerBoundSql`, `ObservabilityRuntime`, `ElevationSink`, `RuntimeExecutor`, `UseCaseRegistry`, `ApplicationInvoker` | reserved: startup fails with a typed reserved-binding error naming the key |

The reserved keys are the framework's own: `ElevationSink` sets and clears the
row-level-security elevation inside the transaction, and the SQL and
observability services are wired into other consumers, so a replacement would
either be ignored or weaken a guarantee. Failing at startup says so instead of
silently using the framework's instance.

`register_default` is public, for a module of your own that provides a default
someone else may override:

```python
container.register(JobService, MyJobService, scope=Scope.APPLICATION)
container.register_default(JobService, InlineJobService)  # kept: MyJobService
```

The cache instances (`CacheConfig`, `CacheGateway`, `CacheGateways`,
`GenerationalDependencyResolver`) are built by the cache module from the
`cache` section; configure them there rather than binding them.

## Later deliveries

The composition is the base of two later deliveries of the same
specification, which add no promise to this page yet: several named stores
in one application, each with its own unit of work, and AWS Lambda adapters
for HTTP and SQS. Until they land, an application serves one persistence
backend, its `stores.default` store, selected by `stores.default.backend`.
