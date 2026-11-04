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
      http: [OrderInterface]
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

### SQS functions

`sqs_record(Job, payload, params=, identity=, message_id=, group_id=, queue_arn=)`
builds a record whose body is the `JobEnvelope` `SQSJobService` sends;
`group_id` makes the queue a FIFO one. `sqs_raw_record(body)` carries any other
body. The handler returns the records SQS should retry:

```python
from loom.composition import create_app
from loom.serverless.aws.lambda_ import LambdaSQSAdapter
from loom.testing import sqs_raw_record, sqs_record

def test_a_poison_message_is_retried() -> None:
    handler = create_app("config.yaml", adapter=LambdaSQSAdapter(), environment="test")
    try:
        records = [
            sqs_record(SendEmailJob, {"to": "ada@example.com"}, message_id="1"),
            sqs_raw_record("{", message_id="2"),
        ]
        result = handler({"Records": records}, None)
    finally:
        handler.close()

    assert result == {"batchItemFailures": [{"itemIdentifier": "2"}]}
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
