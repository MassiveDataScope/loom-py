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

## Test data

`make(Model, **fields)` builds a valid, real instance of any dataclass or
msgspec Struct, with no declaration. It needs the `testing` extra
(polyfactory). Values you pass are used as given; the rest is generated
deterministically, so the same call builds the same instance.

```python
from loom.testing import make

user = make(User, email="ada@example.com")
```

A rule on one field lives in the type, where `make` and `__post_init__` read
the same constant:

```python
limit: Annotated[int, msgspec.Meta(ge=MIN_PAGE_LIMIT, le=MAX_PAGE_LIMIT)] = 50
```

A rule that spans fields (such as `first <= last`) or references another type
gets a `LoomFactory` subclass next to the type. Registered with
`__set_as_default_factory_for_type__`, it also builds that type wherever it is
nested:

```python
from polyfactory.fields import Use
from loom.testing import LoomFactory

class BackoffFactory(LoomFactory[Backoff]):
    __set_as_default_factory_for_type__ = True

    first_s = Use(lambda: 1.0)
    max_s = Use(lambda: 30.0)
```

A test passes explicitly every value it asserts on: generated values are valid,
not stable, and must never decide the outcome.

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
queued and a `jobs` event already is a record, so neither has `via_sqs`. Every
SQS record also takes `message_id`, `group_id` and `queue_arn`. A handler fed SQS
records returns the ones SQS should retry. The sample below tests a `jobs`
function end to end. The package `acme_mail` has an empty
`src/acme_mail/__init__.py` and `src/acme_mail/jobs.py`:

```python
from loom.core.job.job import Job
from loom.core.job.policy import Backoff


class Smtp:
    down = False


smtp = Smtp()


class SendEmailJob(Job[None]):
    __queue__ = "jobs"
    __retry__ = Backoff(first_s=0, factor=1, max_s=0, jitter=False)

    def execute(self, to: str) -> None:
        if smtp.down:
            raise ConnectionError("smtp is down")
```

For plain `pytest` to find the package, add to `pyproject.toml`:

```toml
[tool.pytest.ini_options]
pythonpath = ["src"]
```

`config.yaml`:

```yaml
app: {name: acme-mail, job_service: sqs, packages: [acme_mail]}
sqs:
  queues:
    jobs: {url: "https://sqs.eu-west-1.amazonaws.com/111122223333/jobs", dead_letter: discard}
lambda:
  functions:
    worker: {trigger: {source: jobs, queue: {name: jobs}}}
```

`conftest.py`:

```python
import pytest
from moto import mock_aws

from acme_mail.jobs import smtp
from loom.testing import LambdaWorker


@pytest.fixture
def aws(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")
    monkeypatch.setenv("MOTO_ACCOUNT_ID", "111122223333")
    with mock_aws():
        yield


@pytest.fixture
def worker(aws):
    with LambdaWorker("config.yaml", function="worker", environment="test") as w:
        yield w


@pytest.fixture
def mail():
    smtp.down = False
    return smtp
```

`test_mail.py`:

```python
from acme_mail.jobs import SendEmailJob


def test_a_failed_email_is_reported_and_comes_back(worker, mail):
    mail.down = True
    first = worker.deliver(SendEmailJob, params={"to": "ada@example.com"})
    assert len(first["batchItemFailures"]) == 1
    mail.down = False
    assert worker.redeliver() == {"batchItemFailures": []}  # the retry ran: Backoff(first_s=0)
```

A `jobs` function declares the queue it consumes (`trigger.queue`), and it
settles each record by what its job came to. `LambdaWorker` needs the
`testing-aws` extra (moto). It enters moto's `mock_aws()` itself, creates that
queue and, for `dead_letter: redrive`, its dead-letter queue with a real
`RedrivePolicy`, through the application's own `sqs` client, and binds the
function over it. It sets no environment: botocore resolves the region as it
does for the application (`aws.region`, a profile of `~/.aws/config`,
`AWS_DEFAULT_REGION`), and moto's account is `MOTO_ACCOUNT_ID`, or
`123456789012`. The queue moto makes must be where the configuration says: when its region or
account differs from the configured URL's, construction raises a `ConfigError`
naming the URL it made, what differs and the way out. A URL that is not an AWS
SQS one is refused the same way. Nothing the `sqs` client sends ever leaves the
process: the application's `sqs` client carries a last `before-send` handler that refuses any
request moto did not answer, so an endpoint override (`aws.url`, `sqs.url`,
`AWS_ENDPOINT_URL`, `AWS_ENDPOINT_URL_SQS`, a profile `endpoint_url`) or a
dualstack endpoint fails with a `ConfigError` naming the endpoint; a FIPS
endpoint gets moto's own 404. Clients of other services follow plain moto
semantics: only the `sqs` client is guarded. A named profile (`aws.profile`)
skips moto's fake environment credentials, so the profile must carry its own.
With no region at all, botocore's own `NoRegionError` stands. With no fixture at all, `loom.testing.TEST_QUEUE` (the
entry the default ARN of a `LambdaEvent` record names: `loom-jobs`; a record
built with `group_id` names `loom-jobs.fifo`, so pass `queue_arn=` for it) plus
`aws.region: us-east-1` in the configuration works as it is. A user's own
`mock_aws()` outside is joined, and closing the worker leaves it running.

`worker.deliver(job_type, payload, params=, identity=)` sends the job as
`SQSJobService` does (its envelope, and the queue's own FIFO placement), receives
it and invokes the function with a record that carries the real receipt handle
and `ApproximateReceiveCount` of that delivery. It returns the handler's result
and, as Lambda does, deletes the messages the result does not report.
`worker.redeliver()` does the same with what the queue shows, up to the
trigger's `batch_size` (10 at most), and invokes nothing when nothing is
visible: a failed message comes back once its visibility timeout, set from the
job's backoff, ends, counted as a new attempt. `worker(event, None)` passes a
raw event, whose receipt handle is not in the queue: moto refuses the call that
settles it, and loom logs that and reports the message. `worker.sqs` is a boto3
`sqs` client of the mock, and `worker.queue_url` and `worker.dead_letter_url`
name the queues, so a test reads what the function left in them. Workers on the
same queue share it, so nested workers see each other's messages. A message that
cannot be read is abandoned and is not in `batchItemFailures`.

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

### Delayed inline jobs

The inline job service waits on a `JobTimer`. A test replaces it with a
`ManualTimer`, which never sleeps and runs a step only when the test moves its
clock:

```python
from loom.core.job.timer import JobTimer
from loom.testing import ManualTimer, Override

timer = ManualTimer()
harness = HttpTestHarness(overrides=[Override.instance(JobTimer, timer)])

# a dispatch with countdown=60, a JobPostponement or a failure backoff now waits
await timer.advance(59)  # nothing runs
await timer.advance(1)  # the job runs, then any step it scheduled within the move
```

Bind the timer at composition, as above: a later `overriding` does not reach a
service that is already built.

`advance(seconds)` runs, in order of time, every step whose time has come,
including the steps those steps schedule inside the move; `pending` counts
the steps still waiting.

### Beyond the process

`sam local invoke` runs the built function in the Lambda container, for what
only the image shows: packaging, the handler path, the runtime. For the AWS
APIs themselves, loom's own integration tests run against LocalStack:
`docker compose -f docker-compose.local.yaml up -d localstack`, then set
`LOOM_AWS_IT_URL=http://localhost:4566` (see
[store backends](stores.md)).
