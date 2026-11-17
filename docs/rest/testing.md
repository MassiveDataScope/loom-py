# REST Testing

`loom.testing` provides lightweight, deterministic test utilities for the REST
stack. You can test use cases in isolation, run full HTTP-level assertions
against a FastAPI `TestClient`, or seed real SQLAlchemy repositories for
integration tests.

## Unit-test a use case directly

`UseCaseTest` compiles and executes the real execution plan without HTTP or
framework overhead.

```python
from loom.testing import UseCaseTest

async def test_create_user():
    fake_repo = InMemoryRepository(User)
    result = await (
        UseCaseTest(CreateUserUseCase())
        .with_main_repo(fake_repo)
        .with_input(full_name="Ada Lovelace", email="ada@example.com")
        .run()
    )
    assert result.email == "ada@example.com"
```

Builder methods: `with_params`, `with_input`, `with_command`, `with_loaded`,
`with_deps`, `with_main_repo`. The `.plan` property returns the compiled
`ExecutionPlan`.

## HTTP-level testing

`HttpTestHarness` builds a `TestClient` from `RestInterface` declarations with
fake repositories injected by model type.

```python
from loom.testing import HttpTestHarness, InMemoryRepository

harness = HttpTestHarness()
harness.inject_repo(Product, InMemoryRepository(Product))
client = harness.build_app(interfaces=[ProductRestInterface])

resp = client.get("/products/")
assert resp.status_code == 200
```

Force errors on specific methods:

```python
from loom.core.errors import NotFound
harness.force_error(Product, "get_by_id", NotFound("not found"))
```

### Substituting dependencies

A harness composes the application once per fixture scope and a test swaps
what it needs without composing again:

```python
import pytest
from loom.testing import HttpTestHarness, InMemoryRepository, Override

@pytest.fixture(scope="module")
def harness():
    baseline = InMemoryRepository(Product)
    harness = HttpTestHarness(overrides=[Override.repository(Product, baseline)])
    harness.client = harness.build_app(interfaces=[ProductRestInterface])
    return harness

@pytest.fixture
def products(harness):
    double = InMemoryRepository(Product)
    double.seed(Product(id=1, name="kettle"))
    with harness.overriding(Override.repository(Product, double)):
        yield double

def test_reads_the_double(harness, products):
    assert harness.client.get("/products/1").json()["name"] == "kettle"
```

The model must be served when the harness is built, here by the baseline
override: `overriding` swaps what is bound and binds nothing new.

`Override.repository(Model, double)` replaces the model's one repository, so
injection, `RepoFor[Model]` and every other capability key,
`catalog[Model]` and `catalog.for_table(...)` all see the double.
`Override.instance(Key, value)` replaces any `APPLICATION` binding.
Both are accepted by `overrides=` at build time and by `harness.overriding(...)`
(also `LambdaTestClient(..., overrides=[...])` and `client.overriding(...)`)
at any later time. Swapping and restoring never compose the application
again. Overrides of one key must exit in reverse order. On an exit out of
order the exiting override is removed and a `ResolutionError` is raised; the
overrides still open stay active, and the original comes back only when all
of them have exited.

A singleton the application already built from the real repository, such as
a service that received it in its constructor, keeps the real one: override
that service, or the repository at build time through `overrides=`.

A unit test with no composed application builds a catalog directly:

```python
from loom.core.stores.repository import RepositoryCatalog

catalog = RepositoryCatalog.from_repositories({Product: InMemoryRepository(Product)})
```

Every model needs its own `__tablename__`. It indexes tables and stores like a composed catalog and raises the same
`for_table` errors.

## Golden harness

`GoldenHarness` runs use cases in isolation with injected fake repositories.
`run_with_baseline` asserts execution time against a threshold.

## Integration tests

`RepositoryIntegrationHarness` (via `build_repository_harness`) seeds real
SQLAlchemy repositories with declarative scenarios. It takes the `CursorCodec`
its repositories issue cursors with; `CursorCodec(signing="ephemeral")` suits
most tests, and `CursorCodec(signing="unsigned")` lets a test write a token by
hand.

## Snapshot testing

`serialize_plan` produces a deterministic, JSON-serialisable snapshot of an
`ExecutionPlan` for golden tests.

## Testing Lambda functions

The Lambda helpers in `loom.testing` call the real handler in process, the way
AWS would, without AWS. `LambdaTestClient` needs the `lambda` and `testing`
extras.

### HTTP functions

`LambdaTestClient` is an `httpx.Client`, like FastAPI's `TestClient`: every
call, argument and response attribute of httpx works. Each request becomes an
API Gateway HTTP API event (payload 2.0), or a REST API one with
`event_format="v1"`, so Mangum, the middleware and the authentication run as
deployed. Give it a configuration path and the function of `lambda.functions`
to serve, plus any `create_app` option:

```yaml
lambda:
  functions:
    orders:
      trigger:
        source: http
        interfaces: [OrderInterface]
```

```python
from loom.testing import LambdaTestClient

def test_get_order(token: str) -> None:
    with LambdaTestClient("config.yaml", function="orders", environment="test") as client:
        response = client.get("/orders/7", headers={"authorization": f"Bearer {token}"})

    assert response.status_code == 200
    assert response.json()["id"] == 7
```

It also takes the handler `create_app` returned, which the test then closes
itself.

### Events of every trigger

`LambdaEvent` builds the event AWS delivers to a function, one typed
constructor per trigger source, so a test calls the real handler with it:

| Constructor | Event |
|---|---|
| `LambdaEvent.http(method, path, query=, headers=, json_body=, body=)` | An API Gateway HTTP API request (`event_format="v1"` for REST) |
| `LambdaEvent.jobs(Job, payload, params=, identity=)` | One SQS record of the `JobEnvelope` `SQSJobService` sends |
| `LambdaEvent.sqs_record(body)` | One SQS record whose body is written by hand |
| `LambdaEvent.eventbridge(detail, event_id=)` | An EventBridge event |
| `LambdaEvent.s3(objects, bucket=)` | An S3 notification, one record per key or `key`/`size`/`version_id` mapping |
| `LambdaEvent.s3_test_event(bucket=)` | The `s3:TestEvent` S3 sends when a notification is configured |
| `LambdaEvent.sns(message, notification_id=, type=)` | The event of a Lambda subscription |
| `LambdaEvent.schedule(payload)` | The schedule's input, passed as is |

An EventBridge, S3, SNS or schedule event is a `QueueableEvent`: its
`.via_sqs(message_id=, group_id=, queue_arn=)` is the one SQS record the queue
in between delivers, `group_id` makes that queue FIFO, and an SNS message is
queued as the SNS envelope the subscription stores. An HTTP request is never
queued and a `jobs` event already is a record, so neither has `via_sqs`. Every SQS record also takes `message_id`,
`group_id` and `queue_arn`. A handler fed SQS records returns the ones SQS
should retry:

```python
from loom.composition import create_app
from loom.testing import LambdaEvent

def test_a_message_naming_an_unknown_job_is_retried() -> None:
    handler = create_app("config.yaml", environment="test")  # app.entry: worker
    try:
        records = [
            *LambdaEvent.jobs(SendEmailJob, {"to": "ada@example.com"})["Records"],
            *LambdaEvent.sqs_record('{"job": "Missing"}', message_id="2")["Records"],
        ]
        result = handler({"Records": records}, None)
    finally:
        handler.close()

    assert result == {"batchItemFailures": [{"itemIdentifier": "2"}]}
```

A `jobs` function declares the queue it consumes (`trigger.queue`), and it
settles each record by what its job came to, so the test configuration maps
that queue and the record names it. `loom.testing.TEST_QUEUE` is the
`sqs.queues` entry that the default ARN of a `LambdaEvent` record names
(`loom-jobs`; a record built with `group_id` names `loom-jobs.fifo`, which it
does not cover: pass `queue_arn=` for it):

```yaml
sqs:
  queues:
    jobs: {url: https://sqs.us-east-1.amazonaws.com/000000000000/loom-jobs, dead_letter: discard}
lambda:
  functions:
    worker: {trigger: {source: jobs, queue: {name: jobs}}}
```

Binding the function builds its SQS client and, for a `redrive` queue, reads
its policy: **patch the AWS client** in the test (for instance
`unittest.mock.patch("boto3.Session")`), or it makes real AWS calls. A
malformed message is rejected and is not in `batchItemFailures`.

A direct trigger returns nothing and raises when a job fails:

```python
def test_an_invoice_event_imports_it(handler) -> None:
    handler(LambdaEvent.eventbridge({"number": "F-7"}), None)
```

### Dispatched jobs

`RecordingJobService` sends nothing: it records each dispatch once the
execution that made it commits, with its job, payload, params, effective queue
and caller, and records nothing when it rolls back. Select it in the test
configuration:

```yaml
app:
  job_service: loom.testing:RecordingJobService
```

```python
from loom.core.job.service import JobService
from loom.testing import LambdaTestClient, RecordingJobService

def test_placing_an_order_ships_it(token: str) -> None:
    with LambdaTestClient("config.yaml", function="orders", environment="test") as client:
        client.post("/orders/", json={"sku": "lamp"}, headers={"authorization": f"Bearer {token}"})
        assert client.application is not None
        jobs = client.application.kernel.container.resolve(JobService)

    assert isinstance(jobs, RecordingJobService)
    assert [(job.job_type, job.queue) for job in jobs.dispatched] == [
        (ShipOrderJob, "shipping")
    ]
```

`dispatched` lists every committed dispatch as a frozen `DispatchedJob`
(`job_type`, `payload`, `params`, `queue`, `identity`), payload and params
copied at dispatch. `run()` executes a job in process. It also works without
configuration, as `RecordingJobService()`; it then records dispatches made
outside an execution on `flush_pending_dispatches()` and cannot `run()`.

### Beyond the process

`sam local invoke` runs the built function in the Lambda container, for what
only the image shows: packaging, the handler path, the runtime. For the AWS
APIs themselves, loom's own integration tests run against LocalStack:
`docker compose -f docker-compose.local.yaml up -d localstack`, then set
`LOOM_AWS_IT_URL=http://localhost:4566` (see
[store backends](stores.md)).
