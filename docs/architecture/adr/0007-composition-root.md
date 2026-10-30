# ADR 0007: A transport-neutral composition root

- Status: Accepted
- Branch: `feat/composition-recipe`
- Applies to: the release after loom-kernel 9.2.0
- Specification: `.specify/specs/004-hexagonal-composition/` (composition
  delivery; the named stores and the Lambda adapters are later deliveries)

## Context

ADR 0006 (`docs/architecture/adr/0006-hexagonal-layers.md`) placed the
composition root in its own layer and left `create_app` and the worker
bootstrap there "until they move into it". Two problems kept them apart:

- **`create_app` composed and mounted HTTP in one function.** Configuration,
  discovery, persistence, kernel, observability, deploy checks, agents, cache
  and jobs were built inside the FastAPI factory, and every resource (the
  persistence store, the ClickHouse SQL registry, the agent runtime, the
  cache gateways) was opened and closed by the FastAPI lifespan. Nothing
  could compose an application without loading FastAPI, and a host that runs
  the ASGI lifespan once per request (Mangum's default) would open and close
  pools, schema checks and clients on every invocation.
- **The worker had a duplicate composition, without row-level security.**
  `bootstrap_worker` wired SQLAlchemy by hand from `database`, outside the
  `loom.persistence.backends` contract. It applied no row-level-security
  session settings, no pool reset and no `schema.mode: external` checks,
  served SQLAlchemy only, and never closed its cache gateways. Two
  compositions also meant two places to change for every new service.

The Lambda adapters ahead need an application they can open once per
execution environment and invoke per event, with no HTTP framework in the
process.

## Decision

### One composition root, in `loom.composition`

`loom.composition.create_app(*config_paths, adapter=..., ...)` is the only
place that composes an application. `build_application` takes the same
arguments and returns the composed, closed `Application` without mounting a
transport; `compose_application(source, adapter, modules=...)` does the same
from an already loaded `CompositionSource`. The steps run in a fixed order:
configuration and environment, observability, code path, the adapter's
recipe (discovery runs here), deploy checks, backend `build` and
`prepare_models`, kernel (caller modules, then repositories, then cache),
`JobService`, SQL services and `ObservabilityRuntime`, agents when the recipe
hosts them, `UseCaseFactory.verify()`, and last `adapter.bind(application)`.
No step talks to a database.

`loom.composition` is a top-level package, outside `loom.core`, and imports
no transport; `tests/unit/test_layers.py` and
`tests/unit/composition/test_boundary.py` enforce both, as ADR 0006 set out.

The transport factories stay as shortcuts with their signatures:
`loom.rest.fastapi.create_app` over `FastAPIAdapter`,
`loom.celery.auto.create_app` and `bootstrap_worker` over `CeleryAdapter`.

### Adapters declare a recipe

An adapter implements the `Adapter` protocol:

- `recipe(source) -> Recipe` is synchronous, declarative and opens nothing.
  The `Recipe` names the pool profile, the final `DiscoveryResult`, the
  backend, the deploy-check areas to skip, whether agents are hosted, the
  use-case and pool metrics, and `connection_only` for an entry point with no
  model.
- `bind(application) -> T` mounts the transport and returns what the entry
  point serves; it opens nothing either.

One hook to declare and one to mount, rather than several contribution hooks
with an implicit order. The repository type moved to
`PersistenceBackend.repository_type` (a class variable) so the REST recipe
can prune CRUD routes before anything is built.

### `Application` owns the lifecycle

`Application` holds the compiled kernel and a tuple of typed `Resource`s,
ordered by `ResourceKind`: cache gateways, SQL registry, agent runtime,
store. `running()` opens them in that order on an `AsyncExitStack` and closes
them in reverse; `check()` runs every resource's startup checks and leaves no
connection open.

- Opening an open application raises `RuntimeError`.
- A failure to open closes what had opened and propagates, with the failures
  to close attached as notes.
- On the way out every resource is closed. One failure is raised as itself,
  several as an `ExceptionGroup`, and neither replaces the error that ended
  the block. The standard library only chains close errors through
  `__context__`, so the stack's exits are wrapped to collect them.
- A closed application reopens.

Checking is separate from opening. `PersistenceWiring.startup_checks` holds
the schema, role, guard and time-zone checks; `lifespan_init` only opens and
releases. `prepare_models` binds the row-level-security settings and
validates elevations in every process, and resets the compiled registry
before compiling, so closing no longer resets it.

### Wiring per adapter

| Adapter | Checks | Opens | Closes |
|---|---|---|---|
| FastAPI | lifespan | lifespan | lifespan |
| Celery prefork | `worker_init` (parent) | `worker_process_init` (child, `check=False`) | `worker_process_shutdown` / `worker_shutdown`, best effort |
| Celery solo, threads and any other pool | `worker_init` | `worker_init` | `worker_shutdown` |

Because `check()` leaves no connection open, a prefork worker is fork-safe
by construction: the parent checks once, then each child opens on the async
bridge that runs its tasks. A failed check raises `WorkerTerminate`, so no
child is created. A child that fails to open is not killed, since the pool
would respawn it into the same failure; its tasks fail with
`WorkerNotOpenError` (`loom.celery.errors`), chained to the cause. Neither
shutdown signal is guaranteed, so closing is idempotent, logs its failures
and raises nothing into Celery. Each handler carries a `dispatch_uid`, so
composing again replaces the previous handlers.

### No dependency-injection library

`LoomContainer` stays. The libraries reviewed (svcs, dishka, wireup,
dependency-injector) document neither a guaranteed close order nor
re-initialisation after `fork`, and the lifecycle belongs to `Application`,
not to the container. The container gains `register_default`, which binds a
key only when nothing is bound yet; the composition and the repository
registration use it for what a caller may replace (`JobService`, the
repositories and their capability keys).

### Framework-owned bindings are reserved

The keys the framework wires into other consumers are reserved:
`SessionManager`, `SqlQueryService`, `CallerBoundSql`, `ObservabilityRuntime`,
`ElevationSink`, `RuntimeExecutor`, `UseCaseRegistry` and
`ApplicationInvoker`. A caller module that registers one fails composition at
startup with `loom.composition.ReservedBindingError`
(`CompositionErrorCode.RESERVED_BINDING`), which carries a `CompileIssue`
naming the module and the key (ADR 0003 decision B, as `UnservedModelError`
does). Letting the framework's instance win silently would leave the
caller believing its binding was in use; letting the caller's win would let a
replacement `ElevationSink` leave elevated scopes set, or a replacement
`SqlQueryService` miss `CallerBoundSql`, which is built from the framework's
service. `JobService` stays overridable, and repositories are replaced through
`repository_for` or a custom `DefaultRepositoryBuilder`.

## Consequences

Incompatible changes, listed in the PR description without `!` or
`BREAKING CHANGE` (spec FR-015):

| Before | After |
|---|---|
| `PersistenceWiring.default_repository_type` | `PersistenceBackend.repository_type` (class variable) |
| `PersistenceWiring.lifespan_init` checked and opened | `startup_checks` checks; `lifespan_init` only opens and releases |
| Closing SQLAlchemy reset the model registry | it does not; `prepare_models` resets it before compiling |
| Worker: SQLAlchemy only, from `database`, without row-level security | backend from `persistence.backend` (else `sqlalchemy` when `database` holds a key of its own contract, such as `url`, else `none`), with row-level security |
| A caller module could bind any key; the framework's registration replaced it silently | `JobService` and repositories stay overridable; the framework-owned keys are reserved and binding one fails startup |
| Worker: cache gateways never closed | closed with the application |
| Deploy checks before discovery | after discovery, before any resource is built |

Also observable:

- A worker that serves models runs the startup checks: when `schema.mode` is
  not `external`, `check()` runs `create_all`, once, in the parent of a
  prefork worker.
- The ClickHouse SQL registry's startup checks run in `Application.check()`,
  with the store's, rather than when the registry opens.
- `loom.celery` is imported lazily: an API whose configuration has a `celery`
  section, to dispatch jobs, does not load the worker.
- The worker resolves its backend through `resolve_backend`, so a missing
  `sqlalchemy` extra is a `ConfigError` naming `loom-kernel[sqlalchemy]`
  instead of an `ImportError`.
- A `LoadById`, `Load` or `Exists` over a model no repository serves fails at
  startup with `UnservedModelError`, a typed `ResolutionError`, in the API as
  well as in the worker.
- Every Celery task is registered with `shared=False`: it belongs to its own
  Celery app, and a second app finalized in the same process no longer
  receives tasks bound to the first one's runtime.
- A worker with no model composes `connection_only`: no repository, no cursor
  codec, and its startup check is one `SELECT 1`. On Postgres that probe still
  refuses a superuser or `BYPASSRLS` connecting role and `create_all` over
  scoped tables, and its row-level-security settings stay unbound and fail
  closed. The `loom.cursors.E001` deploy check still applies, since it reads
  only the configuration.
- The worker gains `JobService` and `SqlQueryService` in its container; agent
  and MCP markers are still refused there.

The named-stores catalogue, the per-store units of work and the Lambda HTTP
and SQS adapters build on this root in later deliveries of the same
specification; none of their contracts is promised here.

## Sources

Consulted on 2026-10-21 (official documentation and source code):

- Mark Seemann, *Composition Root*:
  <https://blog.ploeh.dk/2011/07/28/CompositionRoot/>
- Alistair Cockburn, *Hexagonal architecture*:
  <https://alistair.cockburn.us/hexagonal-architecture/>
- Litestar, `on_app_init` versus the lifespan:
  <https://docs.litestar.dev/latest/usage/applications.html>
- Python `contextlib.AsyncExitStack`:
  <https://docs.python.org/3/library/contextlib.html>, and CPython
  `Lib/contextlib.py` (close errors are only chained through `__context__`)
- svcs: <https://svcs.hynek.me/en/stable/core-concepts.html>; dishka's Celery
  integration: <https://dishka.readthedocs.io/en/stable/integrations/celery.html>;
  dependency-injector resources:
  <https://python-dependency-injector.ets-labs.org/providers/resource.html>
- SQLAlchemy, connection pools with multiprocessing or `os.fork()`:
  <https://docs.sqlalchemy.org/en/20/core/pooling.html#using-connection-pools-with-multiprocessing-or-os-fork>
- Django, `AppConfig.ready()` may run more than once, so it must not query
  the database
- Celery signals: <https://docs.celeryq.dev/en/stable/userguide/signals.html>,
  and `celery/worker/worker.py`, `celery/concurrency/prefork.py`
- FastAPI lifespan events: <https://fastapi.tiangolo.com/advanced/events/>,
  and `starlette/routing.py`
- For the later Lambda delivery, verified now so as not to close doors:
  Mangum's adapter (<https://github.com/Kludex/mangum/blob/main/mangum/adapter.py>),
  the Lambda execution environment
  (<https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtime-environment.html>),
  SQS error handling
  (<https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-errorhandling.html>),
  Powertools batch processing
  (<https://docs.aws.amazon.com/powertools/python/latest/utilities/batch/>)
  and SAM builds
  (<https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/using-sam-cli-build.html>)

## Follow-ups

- #424 tracks the gaps found after `register_default`: reserving the
  framework-owned bindings, building `CallerBoundSql`, the job service and
  `Application.observability` from the registered binding, and keeping the
  privileged-role refusal in the connection-only probe. This ADR records the
  outcome decided for each; the issue closes with their implementation.
- The cache instances (`CacheConfig`, `CacheGateway`, `CacheGateways`,
  `GenerationalDependencyResolver`) are still registered by the cache module
  over a caller's binding and are not in the reserved list.
