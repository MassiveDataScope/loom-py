# Celery worker

`loom-kernel` bootstraps Celery workers from the same config, discovery and
composition used by the REST API: the worker gets the same persistence backends,
row-level security and startup checks. Jobs are first-class citizens — they
support `Input()`, `LoadById()`, `Rule`, and full DI injection just like use cases.

---

## Install

```bash
pip install "loom-kernel[celery]"
```

---

## Core concepts

| Concept | Purpose |
|---------|---------|
| `Job` | Background unit of work — runs asynchronously on a worker |
| `JobService.dispatch()` | Enqueue a job after the current UoW commits (fire-and-forget) |
| `JobService.run()` | Execute a job immediately in the calling process |
| `JobCallback` | Hook invoked by the broker on task success or failure |
| `bootstrap_worker()` | Single entry point to start a Celery worker |
| `CeleryAdapter` | The worker's adapter over the shared composition (`loom.celery.adapter`) |
| `WorkerNotOpenError` | Raised by every task of a worker process whose application failed to open |
| `CeleryConfig` | YAML-driven broker and worker settings |
| `JobConfig` | Per-job routing and retry overrides from YAML |

---

## Define a job

A `Job` declares its queue via `__queue__` and implements `execute()`.
`LoadById` loads an entity from the repository before `execute()` runs — same
pattern as use cases:

```python
# app/product/jobs.py
from loom.core.command import Command
from loom.core.job.job import Job
from loom.core.use_case import Input, LoadById

from app.product.model import Product


class SendRestockEmailJobCommand(Command, frozen=True):
    product_id: int
    recipient_email: str


class SendRestockEmailJob(Job[bool]):
    """Send a restock notification email. Returns True when email was sent."""

    __queue__ = "notifications"
    __retries__ = 2
    __timeout__ = 30  # soft time limit in seconds

    async def execute(
        self,
        product_id: int,
        cmd: SendRestockEmailJobCommand = Input(),
        product: Product = LoadById(Product, by="product_id"),
    ) -> bool:
        if product.stock > 0:
            return False
        # send email to cmd.recipient_email …
        return True
```

### Job ClassVars reference

| ClassVar | Type | Default | Description |
|----------|------|---------|-------------|
| `__queue__` | `str` | `"default"` | Target Celery queue |
| `__retries__` | `int` | `3` | Max automatic retries on failure |
| `__countdown__` | `int` | `0` | Delay in seconds before first execution |
| `__timeout__` | `int \| None` | `None` | Soft time limit in seconds |
| `__priority__` | `int` | `0` | Task priority (broker-dependent) |

All ClassVars can be overridden per-environment from YAML without touching code —
see {ref}`rest-per-job-overrides-from-yaml`.

---

## Dispatch a job

Use `JobService.dispatch()` inside a use case to enqueue a job after the current
Unit of Work commits. The job is **not sent** if the UoW rolls back — dispatch is
transactionally safe:

```python
# app/product/use_cases.py
from loom.core.job.service import JobService
from loom.core.use_case.use_case import UseCase
from loom.core.use_case import Input, LoadById

from app.product.model import Product
from app.product.jobs import SendRestockEmailJob
from app.product.callbacks import RestockEmailSuccessCallback, RestockEmailFailureCallback


class DispatchRestockEmailUseCase(UseCase[Product, DispatchRestockEmailResponse]):
    def __init__(self, job_service: JobService) -> None:
        self._jobs = job_service

    async def execute(
        self,
        product_id: str,
        cmd: DispatchRestockEmailCommand = Input(),
        product: Product = LoadById(Product, by="product_id"),
    ) -> DispatchRestockEmailResponse:
        handle = self._jobs.dispatch(
            SendRestockEmailJob,
            params={"product_id": int(product_id)},
            payload={
                "product_id": int(product_id),
                "recipient_email": cmd.recipient_email,
            },
            on_success=RestockEmailSuccessCallback,
            on_failure=RestockEmailFailureCallback,
        )
        # handle.job_id is a stable UUID you can return to the caller
        return DispatchRestockEmailResponse(job_id=handle.job_id, queue=handle.queue)
```

`dispatch()` returns a `JobHandle` immediately — the broker call happens after the
UoW commits. The HTTP response does not wait for the job to complete.

### dispatch() parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `job_type` | `type[Job]` | Concrete Job subclass to enqueue |
| `params` | `dict \| None` | Path/query params for `execute()` signature |
| `payload` | `dict \| None` | JSON body passed as `Input()` command |
| `on_success` | `type[Callback] \| None` | Callback invoked by the broker on success |
| `on_failure` | `type[Callback] \| None` | Callback invoked by the broker on failure |
| `queue` | `str \| None` | Override the job's `__queue__` for this dispatch only |
| `countdown` | `int \| None` | Delay in seconds before execution |
| `priority` | `int \| None` | Override the job's `__priority__` |
| `eta` | `datetime \| None` | Absolute datetime for first execution |

---

## Run a job inline

`JobService.run()` executes a job **immediately** in the calling process and returns
the result. Use it when the use case needs the output before responding — for example
in a synchronous workflow step:

```python
class BuildProductSummaryUseCase(UseCase[Product, ProductSummaryResponse]):
    def __init__(self, job_service: JobService) -> None:
        self._jobs = job_service

    async def execute(self, product_id: str) -> ProductSummaryResponse:
        # runs immediately, no broker involved
        summary = await self._jobs.run(
            BuildProductSummaryJob,
            params={"product_id": int(product_id)},
        )
        return ProductSummaryResponse(product_id=int(product_id), summary=summary)
```

`run()` works identically in both `InlineJobService` (development / tests) and
`CeleryJobService` (production). When Celery is configured, it bypasses the broker
entirely — the job runs in the API process via the same DI container.

Use `dispatch()` for fire-and-forget work; use `run()` when you need the result now.

---

## Callbacks

Callbacks are injected by the DI container and can interact with the application
via `ApplicationInvoker`. Implement `on_success` and/or `on_failure` — both can be
sync or async:

```python
# app/product/callbacks.py
from typing import Any
from loom.core.use_case.invoker import ApplicationInvoker
from app.product.model import Product


class RestockEmailSuccessCallback:
    """Tags the product category when a restock email is sent successfully."""

    def __init__(self, app: ApplicationInvoker) -> None:
        self._app = app

    async def on_success(self, job_id: str, result: Any, **context: Any) -> None:
        if not bool(result):
            return  # job ran but nothing was sent
        product_id = context.get("product_id")
        entity = self._app.entity(Product)
        product = await entity.get(params={"id": product_id})
        if product:
            await entity.update(
                params={"id": product_id},
                payload={"category": f"{product.category}-restock-notified"},
            )

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: Any) -> None:
        pass  # log or send alert


class RestockEmailFailureCallback:
    """Tags the product category when restock email dispatch fails."""

    def __init__(self, app: ApplicationInvoker) -> None:
        self._app = app

    def on_success(self, job_id: str, result: Any, **context: Any) -> None:
        pass

    async def on_failure(
        self, job_id: str, exc_type: str, exc_msg: str, **context: Any
    ) -> None:
        product_id = context.get("product_id")
        entity = self._app.entity(Product)
        product = await entity.get(params={"id": product_id})
        if product:
            await entity.update(
                params={"id": product_id},
                payload={"category": f"{product.category}-restock-failed"},
            )
```

### Callback method signatures

```python
# called on task success — result is the value returned by Job.execute()
async def on_success(self, job_id: str, result: Any, **context: Any) -> None: ...

# called on task failure — exc_type / exc_msg describe the exception
async def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: Any) -> None: ...
```

`**context` contains the raw `payload` dict passed to `dispatch()` — use it to
recover entity IDs and other parameters the callback needs.

> **Fire-and-forget**: when a job dispatched via `dispatch()` fails, the failure
> callback is invoked and the exception is **silenced**. The HTTP layer is not affected.
> Use `run()` when you need the exception to propagate synchronously.

---

## Post-commit failures

A job's own unit of work — opened by the executor around its `execute()` —
can queue post-commit actions of its own: dispatches to further jobs, cache
generation bumps, `on_transaction_committed` hooks. When one of those fails,
`execute()` raises `PostCommitError(failures=(...))` instead of the
exception the failed action raised, carrying whether anything committed.

- **`committed=True`** — the job's transaction committed. The worker task
  treats it as a terminal failure: it is **not retried**, and
  `JOB_EXHAUSTED` is emitted instead of `JOB_RETRYING`. Retrying would run
  the job's `execute()` a second time even though its write already
  committed; the fix belongs to whatever the post-commit action talks to
  (the broker, the cache), not to re-running the job.
- **`committed=False`** — the job owned no unit of work (declared
  `read_only`, or the worker runs without a persistence backend). Nothing
  was written, so the task is retried like any other failure.

See [Execution lifecycle](use-case-dsl.md#execution-lifecycle) for how the
executor drives commit and drains post-commit actions.

---

## YAML configuration reference

### `celery` section

```yaml
# config/celery.yaml
celery:
  broker_url: ${oc.env:CELERY_BROKER_URL,redis://redis:6379/1}
  result_backend: ${oc.env:CELERY_RESULT_BACKEND,redis://redis:6379/2}

  # Worker pool
  worker_concurrency: 4               # concurrent worker processes (default: 4)
  worker_prefetch_multiplier: 1       # 1 = fair dispatch, avoids starvation (default: 1)
  worker_max_tasks_per_child: 1000    # recycle worker after N tasks, prevents leaks (default: 1000)

  # Serialization
  task_serializer: json               # default: json
  result_serializer: json             # default: json
  accept_content: [json]              # default: [json]

  # Time
  timezone: UTC
  enable_utc: true

  # Queues
  # Declare explicit queues so the worker knows which to consume.
  # If omitted, Celery creates queues on demand.
  queues: [default, notifications, analytics, erp]

  # task_default_queue: queue used for tasks/callbacks without an explicit queue=.
  # MUST be one of 'queues' — the framework raises ValueError at startup otherwise.
  # Callbacks (on_success / on_failure) land here when no queue is set.
  task_default_queue: default

  # For integration tests: run tasks synchronously in the calling process.
  task_always_eager: false

  # Async bridge settings used by async jobs/callbacks.
  runtime:
    backend: asyncio          # asyncio or trio
    use_uvloop: true          # only applies to asyncio on supported platforms
    shutdown_timeout_ms: 10000
```

(rest-per-job-overrides-from-yaml)=
### `jobs` section — per-job overrides

Override any Job ClassVar from YAML without touching Python code. Only the fields
you declare are applied; unset fields keep their ClassVar defaults:

```yaml
# config/worker.yaml
jobs:
  SendRestockEmailJob:
    queue: notifications   # overrides __queue__ = "notifications"
    retries: 2             # overrides __retries__ = 2
    timeout: 30            # overrides __timeout__ = 30 (soft time limit)

  BuildProductSummaryJob:
    queue: analytics
    retries: 1

  SyncProductToErpJob:
    queue: erp
    retries: 3
    countdown: 5           # delay 5 seconds before first attempt
    priority: 5
```

| Field | Job ClassVar | Description |
|-------|-------------|-------------|
| `queue` | `__queue__` | Target Celery queue |
| `retries` | `__retries__` | Max automatic retries |
| `countdown` | `__countdown__` | Delay in seconds before execution |
| `timeout` | `__timeout__` | Soft time limit in seconds |
| `priority` | `__priority__` | Task priority |

---

(rest-worker-bootstrap)=
## Worker bootstrap

### Option 1: Discovery from YAML (recommended)

Declare `app.discovery` in `worker.yaml` and let the framework discover jobs,
use cases, and interfaces automatically. This is the most production-friendly option:

```yaml
# config/worker.yaml
includes:
  - celery.yaml   # broker settings

app:
  discovery:
    mode: modules
    modules:
      include:
        - app.product.model
        - app.product.jobs
        - app.product.use_cases
        - app.product.interface
        - app.product.callbacks

persistence:
  cursors:
    signing: ephemeral   # development.yaml only; production.yaml configures keys

database:
  url: ${oc.env:DATABASE_URL}
```

```python
# src/app/worker_main.py
from loom.celery.bootstrap import bootstrap_worker
from app.runtime_config import worker_config_paths

result = bootstrap_worker(*worker_config_paths())
celery_app = result.celery_app
```

Discovery automatically includes all use cases — callbacks can call
`ApplicationInvoker.entity(...)` without any extra configuration.

### Option 2: Manifest (explicit, deterministic)

Define a manifest module that lists all jobs and callbacks explicitly.
Ideal for production when you want full control over what is registered:

```python
# app/manifest.py
from app.product.jobs import SendRestockEmailJob, BuildProductSummaryJob, SyncProductToErpJob
from app.product.callbacks import RestockEmailSuccessCallback, RestockEmailFailureCallback
from app.product.use_cases import DispatchRestockEmailUseCase, BuildProductSummaryUseCase
from app.product.model import Product

MODELS = [Product]
JOBS = [SendRestockEmailJob, BuildProductSummaryJob, SyncProductToErpJob]
CALLBACKS = [RestockEmailSuccessCallback, RestockEmailFailureCallback]
USE_CASES = [DispatchRestockEmailUseCase, BuildProductSummaryUseCase]
```

```yaml
# config/worker.yaml
includes:
  - celery.yaml

app:
  discovery:
    mode: manifest
    manifest:
      module: app.manifest

persistence:
  cursors:
    signing: ephemeral   # development.yaml only; production.yaml configures keys

database:
  url: ${oc.env:DATABASE_URL}
```

### Option 3: Explicit jobs in code

For simple workers or micro-services with a single job, pass jobs directly.
If `app.discovery` is also present in the YAML (e.g. inherited from a shared
`base.yaml`), use cases discovered there are automatically added to the registry —
callbacks can use `ApplicationInvoker` without any extra parameter:

```python
# src/app/worker_main.py
from loom.celery.bootstrap import bootstrap_worker
from app.product.jobs import SendRestockEmailJob
from app.product.callbacks import RestockEmailSuccessCallback

result = bootstrap_worker(
    "config/worker.yaml",   # includes base.yaml → app.discovery auto-supplements registry
    jobs=[SendRestockEmailJob],
    callbacks=[RestockEmailSuccessCallback],
)
celery_app = result.celery_app
```

If `app.discovery` is **not** in the YAML and callbacks need `ApplicationInvoker`,
pass the interfaces whose routes the callbacks interact with:

```python
from app.product.interface import ProductInterface

result = bootstrap_worker(
    "config/worker.yaml",
    jobs=[SendRestockEmailJob],
    interfaces=[ProductInterface],     # auto-extracts use-cases from routes (including AutoCRUD)
    callbacks=[RestockEmailSuccessCallback],
)
```

Or pass use-case classes directly for non-AutoCRUD scenarios:

```python
from app.product.use_cases import GetProductUseCase, UpdateProductUseCase

result = bootstrap_worker(
    "config/worker.yaml",
    jobs=[SendRestockEmailJob],
    use_cases=[GetProductUseCase, UpdateProductUseCase],
    callbacks=[RestockEmailSuccessCallback],
)
```

`interfaces=` and `use_cases=` can be combined. Both are additive on top of any
discovered compilables — they never disable discovery.

### Secrets in YAML (`resolvers=`)

`create_app` and `bootstrap_worker` register loom's built-in AWS resolvers,
`secrets` (Secrets Manager) and `ssm` (SSM Parameter Store), by default. A
worker booted through the factory reads a secret with no code beyond the
factory call:

```yaml
# config/worker.yaml
celery:
  broker_url: ${secrets:/prod/worker/broker-url}
  result_backend: ${ssm:/prod/worker/result-backend}
```

```python
# src/app/worker_main.py
from loom.celery.auto import create_app

celery_app = create_app("config/worker.yaml")
```

The built-in resolvers use boto3's default region and credential chain, and
create their client only when a placeholder resolves: a YAML with no
`${secrets:...}` or `${ssm:...}` never touches AWS and boots without boto3.
When a placeholder does resolve and boto3 is missing, the error names the extra
to install, `loom-kernel[config-ssm]`.

`resolvers=` adds your own prefixes or overrides a built-in by name. Any object
with a `name` and a `resolve(key) -> object` works:

```python
from loom.celery.auto import create_app

class VaultResolver:
    name = "vault"

    def resolve(self, key: str) -> str:
        return read_vault_secret(key)

celery_app = create_app("config/worker.yaml", resolvers=[VaultResolver()])
```

A resolver you pass with the same name as a built-in wins; a built-in default
never replaces a resolver already registered earlier in the process, so
calling the factory more than once is safe. `load_config` registers no
defaults, and resolvers passed to it explicitly replace an earlier registration
of the same name.

---

## Callbacks and ApplicationInvoker — how it works

Callbacks are DI-injected. When a callback declares `app: ApplicationInvoker` in
its constructor, the container provides an `AppInvoker` backed by the worker's
`UseCaseFactory` and `UseCaseRegistry`.

`app.entity(Product)` constructs a key like `"product:get"` and resolves it against
the registry. **The use-case must be compiled in the worker process** — if it is not,
a `KeyError` is raised at callback execution time.

The three ways to ensure this are described in {ref}`rest-worker-bootstrap`:
1. Use discovery — all use cases are included automatically
2. Pass `interfaces=` — use cases are extracted from interface route declarations
3. Pass `use_cases=` — declare use cases explicitly

When using the `jobs=` explicit bootstrap **and** `worker.yaml` inherits
`app.discovery` from a base config, the framework supplements the registry
automatically from the discovery config — no code change needed.

---

## Start the worker

```bash
# development
uv run celery -A app.worker_main:celery_app worker --loglevel=INFO

# specific queues only
uv run celery -A app.worker_main:celery_app worker \
    --queues=notifications,analytics \
    --loglevel=INFO

# multiple queues with concurrency
uv run celery -A app.worker_main:celery_app worker \
    --queues=notifications,analytics,erp \
    --concurrency=4 \
    --loglevel=INFO
```

---

## Application lifecycle

The worker composes on the same recipe as the API
([Composition and lifecycle](../getting-started/composition.md)):
`bootstrap_worker`, `loom.celery.auto.create_app` and
`loom.composition.create_app(..., adapter=CeleryAdapter(...))` build the same
application. What stays specific to the worker is its discovery, its pool
profile (`WORKER_PROFILE`), the agent markers it refuses, and which Celery
signal checks, opens and closes the application:

| Pool | Checks | Opens | Closes |
|---|---|---|---|
| prefork (the default) | `worker_init`, once, in the parent | `worker_process_init`, in each child, without checking again | `worker_process_shutdown` and `worker_shutdown`, best effort |
| any other (`solo`, `threads`, ...) | `worker_init` | `worker_init` | `worker_shutdown` |

```python
# src/app/worker_main.py
from loom.celery.adapter import CeleryAdapter
from loom.composition import create_app

celery_app = create_app("config/worker.yaml", adapter=CeleryAdapter())
```

- The startup checks — `create_all`, or with `schema.mode: external` the
  tables, the connecting role and the row-level-security guard — run once per
  worker, in the parent, before any child exists. They leave no connection
  open, so no pool crosses the fork, and a failed check stops the worker
  (`WorkerTerminate`, exit status 1) before it creates a child.
- Each prefork child opens the application on the async bridge that runs its
  tasks, so every connection is created on the loop that uses it. Opening
  creates no connection: the engine connects on first use.
- A child that fails to open is not killed — the pool would respawn it into
  the same failure. The failure is logged once, and every task of that child
  fails fast with `WorkerNotOpenError` (`loom.celery.errors`), chained to the
  failure to open. It is a system error: a retry may land on a healthy child.
- Neither shutdown signal is guaranteed by Celery. Closing is idempotent and
  best effort: a failure to close is logged, never raised into Celery. The
  cache gateways are closed with the rest (they used to be left open).
- The signal handlers carry a fixed `dispatch_uid`: composing a worker again
  in the same process (an integration test, say) replaces the previous
  handlers, and only the latest application is checked, opened and closed.
- Every task is registered with `shared=False`, on its own Celery app only,
  so a second worker app composed in the same process never serves a task
  bound to the first one.

### Persistence

The worker resolves its backend like the API, through the
`loom.persistence.backends` entry points:

1. `persistence.backend`, when set — `sqlalchemy`, `mongo`, `dynamodb`,
   `none` or a backend of your own;
2. else `sqlalchemy`, when `database` is the worker's own section (it holds
   a key of the `database` contract, such as `url`);
3. else `none`: a worker of pure jobs pays for no persistence.

With `sqlalchemy` the worker applies row-level security exactly as the API
does: session settings per transaction, the pool reset, and the
`schema.mode: external` checks. A backend whose extra is not installed stops
startup with a `ConfigError` naming `loom-kernel[<backend>]`. Agent and MCP
markers are still refused at startup: the worker hosts no agent runtime.

A worker that discovers no model runs connection-only: no repository and no
cursor codec, and its startup check is a single `SELECT 1` that, on Postgres,
still refuses a superuser or `BYPASSRLS` role and `create_all` over scoped
tables; see
[Workers without models](../getting-started/composition.md#workers-without-models).
A job or use case that reads a model no repository serves — through
`main_repo`, `LoadById`, `Load` or `Exists` — fails the worker at startup
with `UnservedModelError`, not its first task.

---

## Integration tests

Set `task_always_eager: true` in the test config so tasks run synchronously in the
same process without requiring a broker:

```yaml
# config/test.yaml
celery:
  broker_url: "redis://localhost:6379/15"
  result_backend: "redis://localhost:6379/15"
  task_always_eager: true
  runtime:
    backend: asyncio
    use_uvloop: false
```

Or patch it in a pytest fixture:

```python
import pytest
from app.worker_main import celery_app

@pytest.fixture(autouse=True)
def celery_eager(monkeypatch):
    celery_app.conf.task_always_eager = True
    yield
    celery_app.conf.task_always_eager = False
```

---

## Docker-compose stack

The companion [`dummy-loom`](https://github.com/the-reacher-data/dummy-loom) ships a
full compose stack with postgres, redis, API, worker, and Flower:

```bash
make up      # start all services
make logs    # follow logs for all services
make down    # tear down
```

Services after `make up`:

| Service | Port | Description |
|---------|------|-------------|
| `api` | 8000 | FastAPI application (Swagger at `/docs`) |
| `worker` | — | Celery worker (queues: notifications, analytics, erp) |
| `flower` | 5555 | Celery Flower dashboard |
| `postgres` | 5432 | PostgreSQL 16 |
| `redis` | 6379 | Broker and result backend |

Flower is available at `http://localhost:5555` after `make up`.
