# ETL

`loom.etl` is a declarative ETL subsystem with compile-time validation,
backend-agnostic declarations, and a single runtime entrypoint (`ETLRunner`).

## Install

Choose one backend:

```bash
pip install "loom-kernel[etl-polars]"
# or
pip install "loom-kernel[etl-spark]"
```

The `etl-spark` and `pyspark` extras install PySpark and delta-spark only on
Python < 3.13, because PySpark 3.5 supports Python up to 3.12. The Spark runtime
that runs the job (EMR, Glue, Databricks) fixes the PySpark version, and with it
the Python version; on Python 3.13 or later these extras resolve without Spark.

## Minimal pipeline

```python
from datetime import date

import polars as pl
from loom.etl import (
    ETLParams,
    ETLStep,
    ETLProcess,
    ETLPipeline,
    ETLRunner,
    FromTable,
    IntoTable,
)


class DailyParams(ETLParams):
    run_date: date


class CleanOrders(ETLStep[DailyParams]):
    orders = FromTable("raw.orders").columns("id", "amount", "run_date")
    target = IntoTable("staging.orders").replace()

    def execute(self, params: DailyParams, *, orders: pl.LazyFrame) -> pl.LazyFrame:
        return orders.filter(pl.col("amount") > 0)


class DailyProcess(ETLProcess[DailyParams]):
    steps = [CleanOrders]


class DailyPipeline(ETLPipeline[DailyParams]):
    processes = [DailyProcess]


runner = ETLRunner.from_dict(
    storage={
        "engine": "polars",
        "defaults": {"table_path": {"uri": "/var/lib/loom/lake"}},
    }
)
runner.run(DailyPipeline, DailyParams(run_date=date(2026, 3, 30)))
```

## File source with JSON payload + final CSV report

```python
import polars as pl
from loom.etl import ETLStep, ETLProcess, ETLPipeline, FromFile, FromTable, IntoFile, IntoTable, Format

class Payload:
    store: str
    amount: float
    items: int

class LoadEvents(ETLStep[DailyParams]):
    events = FromFile("/data/raw/events.csv", format=Format.CSV).parse_json("payload", Payload)
    target = IntoTable("staging.events").replace()

    def execute(self, params: DailyParams, *, events: pl.LazyFrame) -> pl.LazyFrame:
        return events.select(
            pl.col("payload").struct.field("store").alias("store"),
            pl.col("payload").struct.field("amount").alias("amount"),
            pl.col("payload").struct.field("items").alias("items"),
        )

class BuildReport(ETLStep[DailyParams]):
    events = FromTable("staging.events")
    target = IntoFile("/data/exports/daily_report.csv", format=Format.CSV)

    def execute(self, params: DailyParams, *, events: pl.LazyFrame) -> pl.LazyFrame:
        return events.group_by("store").agg(
            pl.col("amount").sum().alias("gross_amount"),
            pl.col("items").sum().alias("item_count"),
        )

class ReportProcess(ETLProcess[DailyParams]):
    steps = [LoadEvents, BuildReport]

class ReportPipeline(ETLPipeline[DailyParams]):
    processes = [ReportProcess]
```

## File aliases (FileLocator)

Hard-coding file paths in pipelines couples the logic to the infrastructure.
Use `FromFile.alias()` / `IntoFile.alias()` to declare a **logical name** and
resolve it at runtime through the storage config.

Declare aliases in the `storage.files` block:

```yaml
storage:
  engine: polars
  files:
    - name: events_raw
      path:
        uri: s3://raw-bucket/events/
        storage_options:
          AWS_REGION: eu-west-1
    - name: exports_daily
      path:
        uri: s3://exports-bucket/daily/
```

Reference them in pipelines using the alias API:

```python
from loom.etl import ETLStep, FromFile, IntoFile, Format

class LoadEvents(ETLStep[DailyParams]):
    events = FromFile.alias("events_raw", format=Format.CSV)
    target = IntoFile.alias("exports_daily", format=Format.PARQUET)

    def execute(self, params: DailyParams, *, events: pl.LazyFrame) -> pl.LazyFrame:
        return events
```

The runner resolves aliases to physical URIs at job startup —
pipelines never hard-code storage paths or credentials.

You can also implement `FileLocator` directly for custom routing strategies:

```python
from loom.etl.storage import FileLocator, FileLocation

class MyFileLocator:
    def locate(self, name: str) -> FileLocation:
        return FileLocation(uri_template=f"s3://my-bucket/{name}/")
```

---

## YAML config (Polars path)

```yaml
storage:
  engine: polars

  defaults:
    table_path:
      uri: s3://my-lake
      storage_options:
        AWS_REGION: ${oc.env:AWS_REGION}
        AWS_ACCESS_KEY_ID: ${oc.env:AWS_ACCESS_KEY_ID}
        AWS_SECRET_ACCESS_KEY: ${oc.env:AWS_SECRET_ACCESS_KEY}

  tmp_root: /var/lib/loom/lake/_tmp

  temp:
    root: s3://my-lake/_checkpoints
    storage_options:
      aws_server_side_encryption: aws:kms
      aws_sse_kms_key_id: alias/loom-temp

observability:
  log:
    enabled: true
  otel:
    enabled: false
  prometheus:
    enabled: true
    pushgateway_url: ${oc.env:PUSHGATEWAY_URL,http://127.0.0.1:9091}
  lineage:
    enabled: true
    # Choose exactly one destination:
    root: /var/lib/loom/lake/_runs
    # database: ops
```

Server-side encryption declared in `storage.temp.storage_options` is honoured by
the Polars engine only; with `engine: spark` the checkpoint write goes through
Hadoop's S3A, which takes it from `fs.s3a.server-side-encryption-*` on the
session instead.

```python
from loom.etl import ETLRunner

runner = ETLRunner.from_yaml("config/etl.yaml")
```

## Config values and secrets (`FromConfig`)

A step that needs a value from the config (an API token, a page size, a whole
settings block) declares it with `FromConfig`, next to its sources. The
executor passes the value to `execute()` as a keyword argument named like the
attribute. The step never reads an environment variable, and the value never
travels as a pipeline param, so an orchestrator such as Prefect neither shows
nor stores it.

```python
import msgspec
import polars as pl

from loom.etl import ETLParams, ETLStep, FromConfig, IntoTemp


class RespondioSettings(msgspec.Struct, kw_only=True):
    api_token: str
    page_size: int = 100


class FetchMessages(ETLStep[DailyParams]):
    api_token = FromConfig("respondio.api_token")          # str by default
    respondio = FromConfig("respondio", RespondioSettings)  # a typed section
    target = IntoTemp("messages")

    def execute(
        self,
        params: DailyParams,
        *,
        api_token: str,
        respondio: RespondioSettings,
    ) -> pl.LazyFrame: ...
```

The key is a dot-separated path into the same YAML `ETLRunner.from_yaml` loads,
outside `storage:`. Interpolations and resolvers apply, so the file decides
where the value comes from:

```yaml
storage:
  engine: polars
  # ...

respondio:
  api_token: ${oc.env:RESPONDIO_API_TOKEN}     # injected by the task definition
  # api_token: ${secrets:/prod/respondio/token} # or AWS Secrets Manager
  page_size: 200
```

- **When it resolves.** `ETLRunner.run` compiles the pipeline and checks every
  `FromConfig` key before any step runs: a missing key, an interpolation that
  cannot resolve or a value that does not validate as the declared type fails
  with `ETLCompilationError` (`UNRESOLVED_CONFIG_VALUE`). Each step then
  resolves its values again when it executes, never at import.
  `${oc.env:...}` is read on every resolution; a resolver's result
  (`${secrets:...}`, `${ssm:...}`) is cached by the config for the lifetime of
  the runner, so a rotated secret takes effect with the next runner.
- **What is checked without a config.** `ETLCompiler()` alone checks the shape:
  every `FromConfig` attribute needs a keyword-only parameter of the same name
  in `execute()`, and may not share its name with a source alias (or with
  `client` on a `ClientStep`). A `StepSQL` cannot declare one.
- **Containment.** A plan carries the key and the type, never the value, and
  loom writes the value to no log, lifecycle event or checkpoint. A resolution
  failure names the key and the expected type only: `ConfigValueError` (run
  time) and `ETLCompilationError` (compile time) carry no chained exception,
  since a `msgspec` message may quote the value. What the step does with the
  value it receives is up to the step.
- **Types.** What `msgspec.convert` accepts: `str`, `int`, `Literal[...]`, a
  `msgspec.Struct` or a dataclass. A null or absent key is reported as not
  set, so a `FromConfig` value is always required.
- **Without `from_yaml`.** Pass `config_context=ConfigContext(...)` to
  `ETLRunner`, `ETLRunner.from_config` or `ETLExecutor`.

## Agents (`WithAgent`)

A step that needs a model to answer something about each row declares the
agent with `WithAgent("name", output=T)` and receives an `AgentMapper` in
`execute()`. See [Agents in ETL steps](agents.md).

## Write modes

Every `IntoTable` target declares exactly one write mode by chaining a method.

### append

Adds rows to the table. Creates the table on first write.

```python
target = IntoTable("staging.orders").append()
```

### replace

Full overwrite. Replaces all data in the table.

```python
target = IntoTable("staging.orders").replace()
# Overwrite schema too:
target = IntoTable("staging.orders").replace(schema=SchemaMode.OVERWRITE)
```

### replace\_partitions

Replaces only the partitions **present in the batch**. The writer collects
the distinct partition values from the frame at write time — no params required.

```python
target = IntoTable("staging.orders").replace_partitions("year", "month")
```

Use this for incremental loads where the batch carries its own partition identity
(e.g. daily runs writing the day's data).

### replace\_partition

Replaces a **specific partition** whose values come from run params. Resolves
the equality predicate at runtime without collecting from the frame.

```python
from loom.etl import params

target = IntoTable("staging.orders").replace_partition(
    year=params.run_date.year,
    month=params.run_date.month,
)
```

Use this when the partition to replace is known at pipeline design time (e.g.
reprocessing a single day).

**Difference from `replace_partitions`:**

| | `replace_partitions` | `replace_partition` |
|---|---|---|
| Partition values | Collected from frame | Resolved from params |
| Collect step | Yes (distinct) | No |
| Use case | Batch carries its partition | Reprocessing a known partition |

### replace\_where

Replaces rows matching an arbitrary predicate. Accepts the full predicate DSL.

```python
target = IntoTable("staging.orders").replace_where(
    col("date").between(params.start_date, params.end_date)
)
```

### upsert

MERGE on key columns. Inserts new rows and updates existing ones.

```python
target = IntoTable("events.orders").upsert(
    keys=("order_id",),
    partition_cols=("year", "month"),  # strongly recommended for large tables
    exclude=("created_at",),           # columns excluded from UPDATE SET
)
```

`partition_cols` is optional but strongly recommended — without it every MERGE
forces a full table scan.

### update

Matched-only MERGE. Rows whose keys match are updated; source rows without a
match are ignored — nothing is ever inserted. Use it when an insert would be
a bug, e.g. repairing columns of an existing table. The target table must
already exist: a missing table is a write error, not a create.

```python
target = IntoTable("events.orders").update(
    keys=("order_id",),
    partition_cols=("year", "month"),  # strongly recommended for large tables
    include=("status",),               # only these columns are updated
)
```

---

## Running only selected stages

Use `include` with process or step class names:

```python
runner.run(
    DailyPipeline,
    DailyParams(run_date=date(2026, 3, 30)),
    include=["DailyProcess", "CleanOrders"],
)
```

If no name matches, `InvalidStageError` is raised.

## Spark runtime

For Databricks/Unity Catalog runtime:

```python
from loom.etl import ETLRunner

runner = ETLRunner.from_spark(spark)
```

This wires Spark reader/writer/catalog automatically.

## Testing ETL steps

Use the built-in test harnesses:

- `loom.etl.testing.PolarsStepRunner`
- `loom.etl.testing.spark.SparkStepRunner`
- `loom.etl.testing.ETLScenario`

These let you seed source tables, run one step in isolation, and assert output
without wiring full storage infrastructure.

## File aliases (FileLocator)

Hard-coding file paths in pipelines couples the logic to the infrastructure.
Use `FromFile.alias()` / `IntoFile.alias()` to declare a **logical name** and
resolve it at runtime through the storage config.

Declare aliases in the `storage.files` block:

```yaml
storage:
  engine: polars
  files:
    - name: events_raw
      path:
        uri: s3://raw-bucket/events/
        storage_options:
          AWS_REGION: eu-west-1
    - name: exports_daily
      path:
        uri: s3://exports-bucket/daily/
```

Reference them in pipelines:

```python
from loom.etl import ETLStep, FromFile, IntoFile, Format

class LoadEvents(ETLStep[DailyParams]):
    events = FromFile.alias("events_raw", format=Format.CSV)
    target = IntoFile.alias("exports_daily", format=Format.PARQUET)

    def execute(self, params: DailyParams, *, events: pl.LazyFrame) -> pl.LazyFrame:
        return events
```

The runner resolves aliases to physical URIs at job startup — pipelines never
hard-code storage paths or credentials.

You can also implement the `FileLocator` protocol directly for custom routing:

```python
from loom.etl.storage import FileLocator, FileLocation

class MyFileLocator:
    def locate(self, name: str) -> FileLocation:
        return FileLocation(uri_template=f"s3://my-bucket/{name}/")
```

---

## Loading config from cloud storage

`ETLRunner.from_yaml()` accepts cloud URIs (`s3://`, `gs://`, `abfss://`, `r2://`, …)
in addition to local paths. Files are fetched via `fsspec` at startup:

```python
runner = ETLRunner.from_yaml("s3://my-bucket/config/etl.yaml")
```

`fsspec` is an extra: it is imported only when a path carries a cloud scheme, so a
configuration that lives entirely on local disk loads without it.

### Composing files with `includes`

A file may declare a top-level `includes` list. The entries are loaded first, in
order, and the declaring file's own keys are merged on top. This works on any
scheme, so a layout uploaded to object storage loads exactly as it does from disk:

```yaml
# s3://my-bucket/config/etl.yaml
includes:
  - common.yaml            # relative to s3://my-bucket/config/
  - tables/*.yaml          # every YAML under s3://my-bucket/config/tables/
  - /etc/loom/site.yaml    # absolute local path; schemes may be mixed

storage:
  engine: polars
```

Rules:

- **Relative entries** resolve against the including file, whatever its scheme.
  An entry with its own scheme or an absolute path is used as is, so a local file
  may include an `s3://` URI and a cloud file may include a local path.
- **Globs** (`*`, `?`, `[`) are expanded through the standard library for local
  paths and through `fsspec` for cloud URIs. Only regular `.yaml` / `.yml` files
  match; matches are merged in **lexicographic order of their resolved path**,
  independent of the backend's listing order.
- **Zero matches is an error.** A plain entry that does not exist, or a glob that
  matches nothing, raises `ConfigError` naming the entry and the file that declared
  it. Nothing is skipped silently.
- **Cycles are detected**: a file that includes a file that includes it back raises
  `ConfigError`, on local and cloud paths alike.
- Includes are resolved recursively; an interpolation inside an entry
  (`pool.${oc.env:VARIANT,local}.yaml`) is resolved before the path is.

### Explicit layering

`load_config` and `ConfigContext.from_yaml` also accept several files, merged
left-to-right so a later file overrides an earlier one:

```python
from loom.core.config import load_config

cfg = load_config("s3://my-bucket/config/base.yaml", "s3://my-bucket/config/prod.yaml")
```

`ETLRunner.from_yaml()` takes a single file; use `includes` inside that file when
the storage config is split.

### Keyed collections

`load_config(..., keyed=)` and `ConfigContext.from_yaml(..., keyed=)` name the
dotted paths of collections whose mapping form is merged **by key** within an
`includes` composition. For those paths:

- a key declared by two files of the same composition raises `ConfigError` naming
  the key and both declaring files, however deep the include nesting;
- a list form in one file and a mapping form in another raises `ConfigError`;
- explicit layers passed to `load_config` still override by key, as layering
  always has; only the list-versus-mapping mismatch is reported across them.

```python
from loom.core.config import load_config
from loom.etl.storage import STORAGE_KEYED_COLLECTIONS

cfg = load_config("config/etl.yaml", keyed=STORAGE_KEYED_COLLECTIONS)
```

`STORAGE_KEYED_COLLECTIONS` is `("storage.tables", "storage.files", "storage.profiles")`.
`ETLRunner.from_yaml()` passes it for you. Any other entry point that loads the same
file without `keyed=` (a REST or Celery app built with `create_app`, or a hand-written
`load_config` call) merges those mappings with OmegaConf's usual deep-merge and
performs **no duplicate check**. See the ETL guide for the mapping form of
`storage.tables` / `storage.files`, `storage.profiles`, and a split-layout example.

---

## Pluggable config resolvers

Extend the YAML loader with custom `${prefix:key}` placeholders to fetch secrets
from external stores (AWS SSM, Azure Key Vault, …) at parse time:

```python
from loom.core.config import load_config

class SsmResolver:
    name = "ssm"

    def __init__(self, region: str) -> None:
        self._region = region

    def resolve(self, key: str) -> str:
        import boto3
        client = boto3.client("ssm", region_name=self._region)
        return client.get_parameter(Name=key, WithDecryption=True)["Parameter"]["Value"]

cfg = load_config("config/etl.yaml", resolvers=[SsmResolver("eu-west-1")])
```

In YAML, reference secrets via `${ssm:/path/to/secret}`:

```yaml
storage:
  catalogs:
    main:
      token: ${ssm:/prod/databricks/token}
```

Resolvers run at job startup — secret rotation takes effect on the next run
without redeployment.

---

## End-to-end example

A full working example with Polars and Spark pipelines, Delta Lake, and observability is
available in the companion repository:
[`dummy-loom-etl`](https://github.com/the-reacher-data/dummy-loom-etl).

---

## API reference

The ETL API reference is generated from public docstrings:

- [ETL API reference](../reference/api/etl)
