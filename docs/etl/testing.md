# ETL Testing

`loom.etl.testing` provides in-memory harnesses for testing `ETLStep`
subclasses without spinning up a real catalog or writing to Delta Lake.

## Test a single step with Polars

`PolarsStepRunner` seeds source tables as plain Python tuples, runs one step,
and returns a `StepResult` for assertions.

```python
from loom.etl.testing import PolarsStepRunner

def test_clean_orders():
    runner = PolarsStepRunner()
    runner.seed("raw.orders", [(1, 10.0), (2, -5.0)], ["id", "amount"])
    result = runner.run(CleanOrdersStep, NoParams())

    result.assert_count(1)
    result.assert_schema({"id": "Int64", "amount": "Float64"})
```

`StepResult` methods: `assert_schema`, `assert_count`, `assert_not_empty`,
`show`, `to_polars`.

### Steps that declare `FromConfig` values

`with_config` gives the runner the config a step's `FromConfig` values resolve
from, so a test passes a fake token instead of setting an environment variable.
The keys are checked when the step compiles, as `ETLRunner` does.
`SparkStepRunner` has the same method.

```python
def test_fetch_uses_the_token():
    runner = PolarsStepRunner().with_config({"respondio": {"api_token": "fake"}})
    result = runner.run(FetchMessages, DailyParams(run_date=date(2026, 9, 25)))
    result.assert_not_empty()
```

A step whose target the harness cannot capture (an `IntoTemp`, say) can be
called directly: `FetchMessages().execute(params, api_token="fake", ...)`.

### Sources that declare `parse_json`

The harness passes seeded frames to the step as they are; it does not decode
the columns a source declares with `parse_json`. To give the step what the
Polars reader would, decode the seed with `decode_json_columns`, which runs
the reader's own decoding (including the normalisation of fields whose JSON
type changes between rows) on an eager or lazy frame:

```python
from loom.etl.backends.polars import decode_json_columns

events = decode_json_columns(raw_events, EnrichEvents.events.json_columns)
result = EnrichEvents().execute(params, events=events.lazy())
```

`json_columns` is the tuple of declarations on a `FromTable` or `FromFile`.

## Reusable seed datasets with `ETLScenario`

`ETLScenario` stores seed data as plain tuples so the same dataset can be used
with any backend runner.

```python
from loom.etl.testing import ETLScenario

ORDERS = (
    ETLScenario()
    .with_table("raw.orders", [(1, 10.0), (2, 20.0)], ["id", "amount"])
)

def test_double_amount(loom_polars_runner):
    ORDERS.apply(loom_polars_runner)
    result = loom_polars_runner.run(DoubleAmountStep, NoParams())
    result.assert_count(2)
```

Pytest fixtures: `loom_polars_runner`. Import `loom.etl.testing.spark` or
register it explicitly in Spark-enabled suites to use `loom_spark_runner`.

## Spark integration tests

Use `SparkTestSession` for a real PySpark + Delta session:

```python
from loom.etl.testing.spark import SparkTestSession

with SparkTestSession.start() as spark:
    runner = SparkStepRunner(spark)
    runner.seed("raw.orders", [(1, 10.0)], ["id", "amount"])
    result = runner.run(CleanOrdersStep, NoParams())
    result.assert_count(1)
```

## Compile-time stubs

- `StubCatalog` — in-memory catalog.
- `StubSourceReader` / `StubTargetWriter` — in-memory I/O.
- `StubRunObserver` — records pipeline lifecycle events.
