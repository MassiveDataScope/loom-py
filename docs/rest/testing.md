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
