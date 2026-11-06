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

`lambda_event(source, payload, via=None, **meta)` builds the event AWS
delivers to a function, so a test calls the real handler with it:

| `source` | Event | `payload` | `meta` |
|---|---|---|---|
| `http` | API Gateway HTTP API (`event_format="v1"`: REST API) | JSON body | `method`, `path`, `query`, `headers`, `body`, `event_format` |
| `jobs` | One SQS record of the `JobEnvelope` `SQSJobService` sends | The job's payload | `job`, `params`, `identity`, `trace_id` |
| `eventbridge` | An EventBridge event | `detail` | `event_id`, `detail_type`, `event_source` |
| `s3` | An S3 notification | None | `objects` (keys, or `key`/`size`/`etag`/`version_id`/`sequencer` mappings), `bucket`, `event_name`, `test_event` |
| `sns` | The event of a Lambda subscription | `Message` | `notification_id`, `topic_arn`, `subject`, `type` |
| `schedule` | The schedule's input | The input | None |

`via="sqs"` wraps the event as one SQS record of the queue in between. An
event carried by SQS (`jobs` or `via="sqs"`) also takes `message_id`,
`group_id` (a FIFO queue), `queue_arn` and `body`, a message body written by
hand instead of the one built. A `jobs` or `via="sqs"` handler returns the
records SQS should retry:

```python
from loom.composition import create_app
from loom.testing import lambda_event

def test_a_poison_message_is_retried() -> None:
    handler = create_app("config.yaml", environment="test")  # app.entry: worker
    try:
        records = [
            *lambda_event("jobs", {"to": "ada@example.com"}, job=SendEmailJob)["Records"],
            *lambda_event("jobs", body="{", message_id="2")["Records"],
        ]
        result = handler({"Records": records}, None)
    finally:
        handler.close()

    assert result == {"batchItemFailures": [{"itemIdentifier": "2"}]}
```

A direct trigger returns nothing and raises when a job fails:

```python
def test_an_invoice_event_imports_it(handler) -> None:
    handler(lambda_event("eventbridge", {"number": "F-7"}), None)
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
[persistence backends](persistence-backends.md)).
