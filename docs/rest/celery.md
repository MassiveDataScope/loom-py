# Celery worker

`loom-kernel` bootstraps Celery workers from the same config, discovery and
composition used by the REST API: the worker gets the same store backends,
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
| `JobService.dispatch_after_commit()` | Enqueue a job after the current UoW commits (fire-and-forget) |
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
    __retry__ = Backoff(first_s=10, factor=3, max_s=600)
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
| `__retry__` | `Backoff \| None` | `None` | Delay between attempts; `None` takes the loom default `Backoff(5, 2, 300)` |
| `__countdown__` | `int` | `0` | Delay in seconds before first execution |
| `__timeout__` | `int \| None` | `None` | Soft time limit in seconds |
| `__priority__` | `int` | `0` | Task priority (broker-dependent) |

All ClassVars can be overridden per-environment from YAML without touching code —
see {ref}`rest-per-job-overrides-from-yaml`.

---

## Dispatch a job

Use `JobService.dispatch_after_commit()` inside a use case to enqueue a job after the current
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
        handle = self._jobs.dispatch_after_commit(
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

`dispatch_after_commit()` returns a `JobHandle` immediately — the broker call happens after the
UoW commits. The HTTP response does not wait for the job to complete.

### dispatch_after_commit() parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `job_type` | `type[Job]` | Concrete Job subclass to enqueue |
| `params` | `dict \| None` | Path/query params for `execute()` signature |
| `payload` | `dict \| None` | JSON body passed as `Input()` command |
| `on_success` | `type[Callback] \| None` | Callback invoked by the broker on success |
| `on_failure` | `type[Callback] \| None` | Callback invoked by the broker on failure |
| `queue` | `str \| None` | Override the job's `__queue__` for this dispatch only |
| `countdown` | `int \| None` | Delay in seconds before execution, 0 through 900 inclusive |
| `priority` | `int \| None` | Override the job's `__priority__` |

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

Use `dispatch_after_commit()` for fire-and-forget work; use `run()` when you need the result now.

### Delays inline

`InlineJobService` honours `countdown`, `JobRetry` and failure backoff: a
delayed job waits in the memory of the process on a `JobTimer` and runs again
with its attempt and deferrals carried. Exhausted attempts reject the job and
call `on_failure`. A job delayed this way is lost when the process stops (the
timer logs them, by job and task id, in one line at shutdown), so the service logs a warning at
start-up: use Celery or SQS in production. A Lambda function composes no
timer: its inline service refuses a countdown above zero with `JobDelayRefused`,
rejects a `JobRetry`, and rejects a failing job at its first attempt with its own error.

---

## Callbacks

A callback is a subclass of `JobCallback` defined in a package listed in
`app.packages`: the scan finds it, and Celery registers a success and a failure
task for it. Both hooks do nothing until overridden, so override `on_success`,
`on_failure` or both, each as `def` or `async def`. The DI container builds the
callback, so it can interact with the application via `ApplicationInvoker`.
A `dispatch_after_commit(..., on_success=X)` naming a class the scan did not find is refused
with a `ValueError`, by every job service the composition builds, before anything is
sent:

```python
# app/product/callbacks.py
from typing import Any
from loom.core.job.callback import JobCallback
from loom.core.use_case.invoker import ApplicationInvoker
from app.product.model import Product


class RestockEmailSuccessCallback(JobCallback):
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


class RestockEmailFailureCallback(JobCallback):
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

`**context` contains the raw `payload` dict passed to `dispatch_after_commit()` — use it to
recover entity IDs and other parameters the callback needs.

> **Fire-and-forget**: when a job dispatched via `dispatch_after_commit()` fails, the failure
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
  `JOB_REJECTED` is emitted instead of `JOB_FAILED`. Retrying would run
  the job's `execute()` a second time even though its write already
  committed; the fix belongs to whatever the post-commit action talks to
  (the broker, the cache), not to re-running the job.
- **`committed=False`** — the job owned no unit of work (declared
  `read_only`, or the worker runs without a store backend). Nothing
  was written, so the task is retried like any other failure.

See [Execution lifecycle](use-case-dsl.md#execution-lifecycle) for how the
executor drives commit and drains post-commit actions.

---

## invoke or dispatch

`ApplicationInvoker.invoke(UseCase, params=..., payload=...)` runs another use
case now: synchronously from the caller's point of view (awaited), in the same
process, and inside the caller's open unit of work, so it shares the caller's
transaction and commits or rolls back with it. It returns the use case's result.

`invoke` takes no `identity=` argument. The identity is bound on the invoker:
`AppInvoker.for_identity(identity)` returns a copy that invokes every use case
as that identity (for example `invoker.for_identity(operator)`). A nested
invocation that joins the caller's unit of work must run as the same identity;
otherwise the executor raises `NestedIdentityMismatch` ("invoke it with the same
identity, or dispatch a job that runs as the other one"). Any nested execution
while an elevation is active raises `NestedElevation`. See
ADR 0008
(`docs/architecture/adr/0008-stores-and-units-of-work.md`).

`JobService.dispatch_after_commit(Job, payload=..., params=...)` is asynchronous: the job is
sent after the caller's commit, runs in its own unit of work, possibly in
another process, and is delivered at least once. It returns a `JobHandle`, not
the result.

Use `invoke` when the caller needs the result or the two writes must be atomic;
use `dispatch_after_commit` for work that can happen later, or that must run as another
identity.

---

## Anti-patterns

- `asyncio.gather` over several `invoke` calls: one invocation scope has one
  owner task; a participant touched from a `gather` child raises
  `ScopeOwnershipError`. Await the calls one after another.
- Callbacks (`on_success` / `on_failure`, or `on_all_success` /
  `on_any_failure` on `dispatch_parallel`) with the SQS backend:
  `dispatch_after_commit` refuses them with `SQSDispatchError` (code
  `sqs_callbacks`).
- `handle.wait()` with the SQS backend: an SQS `JobHandle` carries no result
  to wait for. Use `run()` or `invoke` when you need the result.
- Renaming or moving a `Job` class while messages are in flight: the envelope
  names the job `loom.job.<qualname>` (Celery task name and SQS message
  alike), so old messages no longer route to a job. Drain the queue first, or
  keep the old class until it is empty.
- Queue URLs or boto3 clients inside a use case: the configuration belongs in
  `sqs.queues` and `aws:`; a use case injects `JobService` and names a queue
  at most.
- `dispatch_after_commit` for work the request needs now: it runs after the
  commit, so the response cannot depend on it. Use `invoke`.
- Non-idempotent jobs: delivery is at least once with SQS and with Celery
  (tasks are registered with `acks_late=True`, so a worker lost mid-task
  re-runs it). Make `execute()` safe to repeat, for example keyed by a
  business id.

A transactional outbox, for exactly-once hand-off between the commit and the
broker, is deferred.

---

## Pick the job backend

A use case injects `JobService` and never names a backend. `app.job_service`
names the backend for every queue `celery.queues` does not list: `inline`,
`celery`, `sqs` or a `module:Class` reference. The effective queue of a job is the
`queue=` of the call, otherwise its `__queue__` after the `jobs.<Job>.queue`
override.

```yaml
app:
  name: shop
  job_service: inline

celery:
  broker_url: ${oc.env:CELERY_BROKER_URL}
  result_backend: ${oc.env:CELERY_RESULT_BACKEND}
  queues: [default, notifications, erp]

jobs:
  SyncProductToErpJob:
    queue: erp
```

Here `notifications` and `erp` go to Celery and any other queue runs inline.
`task_default_queue` (`default` unless set) must be listed in `queues`, but it
stays with `app.job_service`: a job left on `__queue__ = "default"` runs inline.

- Only the backends in use are built: with no `celery.queues` and an inline
  default, Celery is not even imported.
- One backend is registered as itself; two sit behind a `JobRouter`.
- `run()` goes to the default backend (in-process for `inline`, `celery` and `sqs`).
- `dispatch_parallel()` over jobs of several backends returns one `JobGroup`
  in input order, and refuses callbacks with `MixedJobBackendsError`.
- Startup fails with `JobBackendError` for an unknown name or when Celery or
  SQS is needed without its `loom[celery]` or `loom[aws]` extra.
- A `module:Class` backend is built as `Class(context)`, where `context` is a
  `loom.composition.JobBackendContext`.
- `LOOM__APP__JOB_SERVICE` can override the key in production and staging,
  but never with a `module:Class` reference.

Without `app.job_service` the old rule still applies, with a
`DeprecationWarning` when Celery is installed or configured: a `celery`
section and the extra select Celery, and anything else runs inline.

### SQS

`app.job_service: sqs` sends each dispatched job to Amazon SQS as one message
whose body is the `JobEnvelope` the Lambda SQS adapter runs. Install
`loom-kernel[aws]`.

```yaml
app:
  name: shop
  job_service: sqs

aws:
  region: eu-west-1

sqs:
  queues:
    default:
      url: https://sqs.eu-west-1.amazonaws.com/123456789012/shop
      dead_letter: redrive
    orders:
      url: https://sqs.eu-west-1.amazonaws.com/123456789012/orders.fifo
      dead_letter: discard
```

```python
class ShipOrderJob(Job[None]):
    __queue__ = "orders"
    __group_by__ = "customer_id"  # payload field -> MessageGroupId
```

- `sqs:` takes the `aws:` client keys, overriding them, plus `queues`.
- Each queue is a mapping: its `url` and its `dead_letter` policy, `redrive`
  (default) or `discard`. A queue written as a bare URL is refused at load,
  naming the mapping.
- Only the jobs SQS serves need a URL: those on `celery.queues` go to Celery.
- Startup fails with `SQSQueueConfigError` when a job's queue has no URL, or
  when a job on a FIFO queue (URL ending in `.fifo`) declares no `__group_by__`.
- On a FIFO queue, the SHA-256 of the group field's value is the
  `MessageGroupId` (it meets SQS's limits and keeps the value out of SQS), and
  the task id is the `MessageDeduplicationId`.
- `dispatch_after_commit` refuses an out-of-bound countdown (above 900 or
  negative) with `ValueError` before anything is sent. It raises
  `SQSDispatchError`, before anything is sent, for: callbacks, a `priority`
  other than the job's, a queue without a URL, any delay on a FIFO queue
  (FIFO queues take a delay per queue only), a FIFO payload whose group field
  is missing or null, a body holding a character SQS refuses, or a body over
  256 KiB.
- The message is sent after the commit, on the shielded publish lane, so a
  cancellation after the commit does not lose it. A failed send is logged and
  raised as `SQSDispatchError` with the job, task id, queue and AWS error code,
  never the payload.
- `run()` executes in-process.

#### Batches

After the commit, the jobs one execution dispatched leave in `SendMessageBatch`
calls, one queue after another, in dispatch order: FIFO order holds within each
queue.

```yaml
sqs:
  batch: true
  batch_size: 10
  max_concurrent_sends: 1
  queues:
    default:
      url: https://sqs.eu-west-1.amazonaws.com/123456789012/shop
```

- `batch` (default `true`): `false` sends each job with its own `SendMessage`.
- `batch_size` (1–10, default 10): entries per call; a call also never carries
  more than 1 MiB of bodies, the
  [SendMessageBatch](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html)
  limit.
- `max_concurrent_sends` (default 1): queues sent at the same time; the calls
  of one queue always run one after another.
- On a standard queue, the entries SQS refuses in a call are sent once more.
  A FIFO queue resends nothing and, after a failure, holds back its later
  calls (code `NotSent`), so no job overtakes the one that failed.
- What still failed, and every entry of a call that raised, is logged one by
  one with job, task id, queue and AWS error code, then raised together as one
  `SQSDispatchError` (`code="sqs_batch_partial_failure"`) carrying the count
  and codes.
- If the shielded send itself dies (the loop closing), each job not yet handed
  to SQS is logged as `PostCommitShieldedActionNeverRan`.
- Nothing waits for jobs from other requests: a batch never outlives the
  execution that committed it.

#### Lambda consumers

A `lambda.functions` entry whose trigger is `source: jobs`, or an event
trigger with a `queue:` block, declares its event source mapping in that
block, under the queue's `name`:

```yaml
lambda:
  functions:
    worker:
      trigger:
        source: jobs
        queue:
          name: jobs
          batch_size: 100
          batching_window_s: 5
          max_concurrency: 50
          report_batch_item_failures: true
```

- `name` is a key of `sqs.queues`.
- At Init the function checks the queue it consumes, and only that one: an
  application that only dispatches calls neither `GetQueueAttributes` nor
  `GetQueueUrl`.
- `batch_size` (1–10000; 10 at most on a FIFO queue; over 10 needs
  `batching_window_s` of at least 1), `batching_window_s` (0–300) and
  `max_concurrency` (2–1000) are the
  [event source mapping](https://docs.aws.amazon.com/lambda/latest/api/API_CreateEventSourceMapping.html)
  `BatchSize`, `MaximumBatchingWindowInSeconds` and
  [`ScalingConfig.MaximumConcurrency`](https://docs.aws.amazon.com/lambda/latest/api/API_ScalingConfig.html).
  loom validates them for the deployment but does not apply them.
- `report_batch_item_failures` (default `true`) must match the mapping's
  `ReportBatchItemFailures`: `false` makes the handler raise
  `SQSBatchFailedError` when a record fails, so Lambda retries the whole batch.
- An http function, and a direct event trigger, refuse the `queue:` block.

#### Dead letters and attempts

`dead_letter` names the queue's dead-letter policy and who owns the attempt
limit:

| `dead_letter` | Dead-letter queue | The attempt limit |
|---|---|---|
| `redrive` (default) | the one in the queue's `RedrivePolicy` | the `maxReceiveCount` of that policy; a job consumed from the queue may not declare `max_attempts` in its own `jobs.<Job>.retry` |
| `discard` | none | `max_attempts` of the job (loom default 3) |

- A function consuming a `redrive` queue reads the queue's `RedrivePolicy`
  once, at Init. Without one, startup fails naming the queue and both options:
  attach a dead-letter queue, or set `dead_letter: discard`.
- `jobs.defaults.retry.max_attempts` does not apply to jobs consumed from a
  `redrive` queue: `maxReceiveCount` wins. Startup logs the effective policy of
  the jobs of the consumed queue.
- A function consuming a queue that `sqs.queues` does not map fails startup
  naming `sqs.queues.<name>`.

**IAM.** The role of a function consuming a queue needs:

| Action | Resource |
|---|---|
| `sqs:ChangeMessageVisibility`, `sqs:SendMessage`, `sqs:GetQueueAttributes` | the source queue |
| `sqs:SendMessage`, `sqs:GetQueueUrl` | the dead-letter queue |

The producer role needs `sqs:SendMessage` on the queues it dispatches to.

**Moving dead letters back.** A message sent to a dead-letter queue directly,
not by SQS through the `RedrivePolicy`, has no source queue recorded. In
LocalStack 4.14, `StartMessageMoveTask` without a `DestinationArn` fails on
it, and with `DestinationArn` set to the source queue it moves it back. In the
console, choose "Redrive to custom destination", not "Redrive to source queue";
with the API or CLI, always pass `DestinationArn`.

### Who can run a job

The identity in a job envelope is not signed. Anyone allowed to
`sqs:SendMessage` on the queue, or to publish to an SNS topic subscribed to it,
can run any job as any subject, row-level security included.

- The queue policy is the authorization boundary: allow `sqs:SendMessage` only
  to the producer's role.
- The same holds for a Celery broker: only the producers may publish to it.
- Signing the envelope is planned.

---

## YAML configuration reference

### `celery` section

In `config/celery.yaml`:

```yaml
celery:
  broker_url: ${oc.env:CELERY_BROKER_URL,redis://redis:6379/1}
  result_backend: ${oc.env:CELERY_RESULT_BACKEND,redis://redis:6379/2}

  worker_concurrency: 4
  worker_prefetch_multiplier: 1
  worker_max_tasks_per_child: 1000

  task_serializer: json
  result_serializer: json
  accept_content: [json]

  timezone: UTC
  enable_utc: true

  queues: [default, notifications, analytics, erp]

  task_default_queue: default

  task_always_eager: false

  runtime:
    backend: asyncio
    use_uvloop: true
    shutdown_timeout_ms: 10000
```

| Key | Meaning |
| --- | --- |
| `worker_concurrency` | concurrent worker processes (default: 4) |
| `worker_prefetch_multiplier` | 1 = fair dispatch, avoids starvation (default: 1) |
| `worker_max_tasks_per_child` | recycle worker after N tasks, prevents leaks (default: 1000) |
| `task_serializer` | default: json |
| `result_serializer` | default: json |
| `accept_content` | default: [json] |
| `queues` | Declare explicit queues so the worker knows which to consume. If omitted, Celery creates queues on demand. |
| `task_default_queue` | queue used for tasks/callbacks without an explicit queue=. MUST be one of 'queues' — the framework raises ValueError at startup otherwise. Callbacks (on_success / on_failure) land here when no queue is set. |
| `task_always_eager` | For integration tests: run tasks synchronously in the calling process. |
| `runtime` | Async bridge settings used by async jobs/callbacks. |
| `runtime.backend` | asyncio or trio |
| `runtime.use_uvloop` | only applies to asyncio on supported platforms |

(rest-per-job-overrides-from-yaml)=
### `jobs` section — per-job overrides

Override any Job ClassVar from YAML without touching Python code. Only the fields
you declare are applied; unset fields keep their ClassVar defaults. Every process
that discovers the job applies them, the API included. Each application keeps its
own values and the ClassVars stay as declared, so two applications composed in one
process never see each other's `jobs` section. An invalid section raises
`ConfigError` at startup:

In `config/worker.yaml`:

```yaml
jobs:
  SendRestockEmailJob:
    queue: notifications
    retry: {max_attempts: 4}
    timeout: 30

  BuildProductSummaryJob:
    queue: analytics
    retry: {first_s: 1, max_attempts: 2}

  SyncProductToErpJob:
    queue: erp
    retry: {factor: 1.5, jitter: false}
    countdown: 5
    priority: 5
```

| Field | Job ClassVar | Description |
|-------|-------------|-------------|
| `queue` | `__queue__` | Target Celery queue |
| `retry` | `__retry__` | Retry policy, see below |
| `countdown` | `__countdown__` | Delay in seconds before execution |
| `timeout` | `__timeout__` | Soft time limit in seconds |
| `priority` | `__priority__` | Task priority |

### Retry policy

A job is attempted at most `max_attempts` times (3 by default). After a failed
attempt it waits `Backoff(first_s, factor, max_s, jitter)`: `first_s * factor ** (attempt - 1)`
seconds, capped at `max_s` (never above 900) and, with `jitter` (the default), drawn
uniformly from zero to that value. A job may raise `JobRetry(reason, delay_s=None)`
to run again later without using an attempt, or `JobRejection(reason)` to give up at
once; an error whose `retryable` is `False` is rejected at its first attempt.

The most specific layer wins, and a section that sets only some fields inherits the
rest from the layer below: `jobs.<Job>.retry`, then `jobs.defaults.retry`, then the
job's `__retry__`, then the loom default. `max_attempts` is only set in YAML.
A job reads the delivery it runs under by declaring `delivery: JobDelivery | None =
Delivery()` on `execute` (its `attempt` and `deferrals`); a job run by another job gets
`None`.

```yaml
jobs:
  defaults:
    retry: {max_attempts: 5}
  SyncProductToErpJob:
    retry: {first_s: 30}
```

Start-up logs one line with each job's effective policy and the layer it came from.

---

(rest-worker-bootstrap)=
## Worker bootstrap

### Package discovery

Declare application packages in `config/worker.yaml`. The worker recursively
scans their modules for jobs, use cases, and models:

```yaml
app:
  name: acme-worker
  packages: [acme]

stores:
  settings:
    cursors:
      signing: ephemeral
  default:
    backend: sqlalchemy
    url: ${oc.env:DATABASE_URL}
```

```python
from loom.celery.bootstrap import bootstrap_worker

result = bootstrap_worker("config/worker.yaml")
celery_app = result.celery_app
```

The scanner sorts classes by defining module and qualname. Job callbacks, the
`JobCallback` subclasses of those packages, are found the same way: no
`callbacks=` argument exists.

### Option 3: Explicit jobs in code

For simple workers or micro-services with a single job, pass jobs directly.
If `app.packages` is also present in the YAML (e.g. inherited from a shared
`base.yaml`), use cases discovered there are automatically added to the registry —
callbacks, found in the same packages, can use `ApplicationInvoker` without any
extra parameter:

```python
# src/app/worker_main.py
from loom.celery.bootstrap import bootstrap_worker
from app.product.jobs import SendRestockEmailJob

result = bootstrap_worker(
    "config/worker.yaml",   # includes base.yaml → app.packages supplements registry
    jobs=[SendRestockEmailJob],
)
celery_app = result.celery_app
```

A callback is found only through `app.packages`. Without it, a worker whose jobs
need `ApplicationInvoker` passes the interfaces whose routes they interact with:

```python
from app.product.interface import ProductInterface

result = bootstrap_worker(
    "config/worker.yaml",
    jobs=[SendRestockEmailJob],
    interfaces=[ProductInterface],     # auto-extracts use-cases from routes (including AutoCRUD)
)
```

Or pass use-case classes directly for non-AutoCRUD scenarios:

```python
from app.product.use_cases import GetProductUseCase, UpdateProductUseCase

result = bootstrap_worker(
    "config/worker.yaml",
    jobs=[SendRestockEmailJob],
    use_cases=[GetProductUseCase, UpdateProductUseCase],
)
```

`interfaces=` and `use_cases=` can be combined. Both are additive on top of any
discovered compilables — they never disable discovery.

### Secrets in YAML (`resolvers=`)

`create_app` and `bootstrap_worker` register loom's built-in AWS resolvers,
`secrets` (Secrets Manager) and `ssm` (SSM Parameter Store), by default. A
worker booted through the factory reads a secret with no code beyond the
factory call:

In `config/worker.yaml`:

```yaml
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
to install, `loom-kernel[aws]`.

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
`app.packages` from a base config, the framework supplements the registry
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
- On asyncio one owner task, `ApplicationHost` (`loom.composition`), opens
  and closes the application, so a resource that must be closed by the task
  that opened it closes cleanly; on trio the application is entered and
  exited in two bridge calls, which is safe because the worker hosts no agents.
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
`loom.stores.backends` entry points:

1. `stores.default.backend`, when the `stores.default` section is declared —
   `sqlalchemy`, `mongo`, `dynamodb`, `none` or a backend of your own;
2. else `none`: a worker of pure jobs pays for no persistence, and stores
   declared only for streaming sinks (`stores.<name>`) do not change that.

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

In `config/test.yaml`:

```yaml
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
