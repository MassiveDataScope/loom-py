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

### historify

For SCD Type 2 history tracking use `IntoHistory` instead of `IntoTable`.
See the dedicated [Historify](#historify) section below for full examples,
SNAPSHOT vs LOG mode, and configuration of boundary columns.

| Target | Use case | Details |
|---|---|---|
| `IntoTable(...).append()` | Add rows | Above |
| `IntoTable(...).replace()` | Full overwrite | Above |
| `IntoTable(...).replace_partitions(...)` | Replace batch partitions | Above |
| `IntoTable(...).replace_partition(...)` | Replace known partition | Above |
| `IntoTable(...).replace_where(...)` | Predicate-based overwrite | Above |
| `IntoTable(...).upsert(...)` | Merge by key | Above |
| `IntoTable(...).update(...)` | Update matches only, never insert | Above |
| `IntoHistory(...)` | SCD Type 2 historification | [Below](#historify) |

---

## Historify

SCD Type 2 historification tracks how entity states change over time by creating
a new row for every meaningful change and closing the previous open row.

```python
from loom.etl import IntoHistory, params

target = IntoHistory(
    "wh.dim_players",
    keys=("player_id",),
    track=("team_id",),
    effective_date=params.run_date,
)
```

### Configuration

| Parameter | Default | Description |
|---|---|---|
| `ref` | — | Logical Delta table reference |
| `keys` | — | Entity identity columns (e.g. `("player_id",)`) |
| `track` | `None` | Columns whose change triggers a new history row. `None` means all non-key columns |
| `effective_date` | — | `ParamExpr` for SNAPSHOT mode, or column name for LOG mode |
| `mode` | `"snapshot"` | `"snapshot"` (full state) or `"log"` (event stream) |
| `valid_from` | `"valid_from"` | Period-start boundary column |
| `valid_to` | `"valid_to"` | Period-end boundary column (`NULL` = open) |
| `deleted_at` | `"deleted_at"` | Soft-delete audit column (only written when `delete_policy="soft_delete"`) |
| `delete_policy` | `"close"` | `"ignore"` / `"close"` / `"soft_delete"` |
| `partition_scope` | `None` | Partition columns for read/write pruning |
| `allow_temporal_rerun` | `False` | Allow backfills with past `effective_date` |

### SNAPSHOT mode example

In SNAPSHOT mode the incoming frame represents the full current state of the
dimension. The engine compares it against the existing open vectors and inserts
new rows only for changed states.

**Day 1 incoming snapshot**

| player_id | team_id |
|---|---|
| 1 | RM |

**Result after historify**

| player_id | team_id | valid_from | valid_to |
|---|---|---|---|
| 1 | RM | 2024-01-01 | `NULL` |

**Day 2 incoming snapshot (player changed team)**

| player_id | team_id |
|---|---|
| 1 | BCA |

**Result after historify**

| player_id | team_id | valid_from | valid_to |
|---|---|---|---|
| 1 | RM | 2024-01-01 | 2024-01-01 |
| 1 | BCA | 2024-01-02 | `NULL` |

The old row was closed (`valid_to = effective_date - 1`) and the new row was
opened.

### LOG mode example

In LOG mode the incoming frame is an event stream. Each event carries its own
`effective_date` column.

```python
target = IntoHistory(
    "wh.dim_subscriptions",
    keys=("subscription_id",),
    track=("plan",),
    effective_date="event_date",
    mode="log",
)
```

**Incoming event log**

| subscription_id | plan | event_date |
|---|---|---|
| 1 | basic | 2024-01-01 |
| 1 | pro | 2024-06-01 |

**Result after historify**

| subscription_id | plan | valid_from | valid_to |
|---|---|---|---|
| 1 | basic | 2024-01-01 | 2024-05-31 |
| 1 | pro | 2024-06-01 | `NULL` |

The first event is automatically closed by the second one. The last event
remains open.

### Custom boundary column names

If your data warehouse uses different naming conventions, the three generated
columns are fully configurable:

```python
target = IntoHistory(
    "wh.dim_players",
    keys=("player_id",),
    track=("team_id",),
    effective_date=params.run_date,
    valid_from="vf",
    valid_to="vt",
    deleted_at="removed_at",
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

`fsspec` is an extra and is only imported for cloud paths; a configuration kept on
local disk, includes and globs included, loads without it.

### Composing files with `includes`

A file may declare a top-level `includes` list. Entries load first, in order, and
the declaring file's own keys merge on top. Includes work on any scheme:

```yaml
# s3://my-bucket/config/etl.yaml
includes:
  - common.yaml            # relative to the including file, on any scheme
  - tables/*.yaml          # glob, matches merged in lexicographic order
  - /etc/loom/site.yaml    # absolute local path; schemes may be mixed
```

- Relative entries resolve against the including file; entries with a scheme or an
  absolute path are used as is, so local and cloud files may include each other.
- Globs expand through the standard library locally and through `fsspec` on cloud
  URIs; only regular `.yaml` / `.yml` files match, sorted by resolved path.
- An entry (plain or glob) that matches no file raises `ConfigError` naming the
  entry and the declaring file. Circular includes raise `ConfigError` too.

`ETLRunner.from_yaml()` takes one file; explicit layering of several files
(`load_config("base.yaml", "prod.yaml")`, later overriding earlier) is available on
`load_config` and `ConfigContext.from_yaml`.

---

## Splitting the storage config across files

`storage.tables` and `storage.files` are lists in the examples above. A list cannot
be split across files: OmegaConf replaces a list wholesale when it merges, so only
the last included file's tables would survive. Three YAML additions make a split
layout safe.

### Tables and files as a mapping keyed by name

Both collections also accept a **mapping keyed by logical name**. The key becomes
the route's `name`; the value holds the remaining fields (`ref` / `catalog` or
`path`). Mapping form and list form bind to the same `TableRoute` / `FileRoute`
values:

```yaml
storage:
  tables:
    orders.header:
      path:
        uri: s3://lake/orders/header
    orders.line:
      ref: sales.orders_line
      catalog: main
  files:
    orders.export:
      path:
        uri: s3://exports/orders/
        storage_options:
          AWS_REGION: eu-west-1
```

Within an `includes` composition mappings merge **by key**, so `tables/orders.yaml`
and `tables/billing.yaml` contribute to one collection. The loader guards the merge:

- the same name declared in two included files is a `ConfigError` naming the key and
  both files (`storage.tables['orders.header'] is declared in '.../billing.yaml' and
  in '.../orders.yaml'`); a duplicate is never resolved by "last wins";
- list form in one file and mapping form in another is a `ConfigError`; use one form
  within a composition;
- files passed explicitly as layers (`load_config("base.yaml", "prod.yaml")`) still
  override by key, as explicit layering always has;
- the list form is unchanged, including its duplicate-name validation.

### Storage profiles

YAML anchors do not cross file boundaries. `storage.profiles` declares named,
partial `path:` settings once, in any file of the composition, and each route
refers to one with `profile:` inside its `path:` block:

```yaml
storage:
  profiles:
    standard:
      writer:
        compression: ZSTD
      target_file_size: 134217728
      commit:
        custom_metadata:
          team: billing
    large:
      writer:
        compression: ZSTD
      target_file_size: 536870912
      delta_config:
        delta.logRetentionDuration: interval 30 days
      storage_options:
        AWS_REGION: eu-west-1

  defaults:
    table_path:
      uri: s3://lake
      profile: standard

  tables:
    billing.invoice:
      path:
        uri: s3://lake/billing/invoice
        profile: large
    billing.ledger:
      path:
        uri: s3://lake/billing/ledger
        profile: large
        target_file_size: 268435456   # own field wins over the profile's

  files:
    billing.statement:
      path:
        uri: s3://exports/billing/statements/
        profile: large                # file routes take storage_options only
```

How a profile is applied:

- A profile may set `storage_options`, `writer`, `target_file_size`, `delta_config`
  and `commit`. It never carries `uri`; a profile with `uri` or any other field is
  rejected naming the field.
- `profile:` is accepted in the `path:` block of a table route, a file route and
  `storage.defaults.table_path`. It is consumed while the YAML is normalised and
  never reaches `TablePathConfig` / `FilePathConfig`.
- Resolution is **per field, replace not merge**: a field the route sets itself
  replaces the profile's field entirely (there is no key-level merge inside
  `writer`, `commit`, `delta_config` or `storage_options`); every other field
  comes from the profile.
- A file route takes only `storage_options` from its profile; the other fields are
  ignored, never rejected.
- An unknown profile name is a `ValueError` naming the route
  (`storage.tables['billing.invoice'].path.profile='huge' is not defined in
  storage.profiles`).

Profiles resolve when the `StorageConfig` is built through `ETLRunner.from_yaml` or
`ETLRunner.from_dict`; building the structs directly in Python does not apply them.

### Split layout example

```text
config/
├── storage.yaml          # entry point
├── common.yaml           # engine, defaults, profiles, catalogs
└── tables/
    ├── billing.yaml
    └── orders.yaml
```

```yaml
# config/storage.yaml
includes:
  - common.yaml
  - tables/*.yaml

observability:
  log:
    enabled: true
```

```yaml
# config/common.yaml
storage:
  engine: polars
  profiles:
    standard:
      writer:
        compression: ZSTD
      target_file_size: 134217728
  defaults:
    table_path:
      uri: s3://lake
      profile: standard
      storage_options:
        AWS_REGION: ${oc.env:AWS_REGION}
```

```yaml
# config/tables/orders.yaml
storage:
  tables:
    orders.header:
      path:
        uri: s3://lake/orders/header
        profile: standard
    orders.line:
      path:
        uri: s3://lake/orders/line
        profile: standard
```

```yaml
# config/tables/billing.yaml
storage:
  tables:
    billing.invoice:
      path:
        uri: s3://lake/billing/invoice
        profile: standard
  files:
    billing.statement:
      path:
        uri: s3://exports/billing/statements/
```

```python
from loom.etl import ETLRunner

runner = ETLRunner.from_yaml("config/storage.yaml")
# or, once uploaded: ETLRunner.from_yaml("s3://my-bucket/config/storage.yaml")
```

Removing `orders.line` from `orders.yaml` removes exactly that table; declaring it
again in `billing.yaml` fails at load with both file names in the message.

### Which entry points check duplicates

The by-key merge and its duplicate check are driven by the `keyed=` parameter of
`load_config` and `ConfigContext.from_yaml`. `ETLRunner.from_yaml()` passes
`loom.etl.storage.STORAGE_KEYED_COLLECTIONS`
(`"storage.tables"`, `"storage.files"`, `"storage.profiles"`). Other consumers that
want the same guarantees must pass it themselves:

```python
from loom.core.config import ConfigContext
from loom.etl.storage import STORAGE_KEYED_COLLECTIONS

ctx = ConfigContext.from_yaml("config/storage.yaml", keyed=STORAGE_KEYED_COLLECTIONS)
```

An entry point that loads the same file **without** `keyed=` (today, a REST or
Celery app built with `create_app`) merges `storage.tables` and friends with
OmegaConf's default deep merge: the last included file wins silently and no
duplicate is reported. Keep this in mind when one YAML tree is shared between an
ETL and an application.

---

## Pluggable config resolvers

`${prefix:key}` placeholders in YAML are filled in at parse time by *resolvers*.
loom ships two, backed by AWS: `secrets` (Secrets Manager) and `ssm` (SSM
Parameter Store). `ETLRunner.from_yaml` registers both by default, so a project
booted through the factory reads a secret with no code beyond the factory call:

```yaml
# config/etl.yaml
storage:
  catalogs:
    main:
      token: ${secrets:/prod/lakehouse/token}
      workspace: ${ssm:/prod/lakehouse/workspace}
```

```python
from loom.etl import ETLRunner

runner = ETLRunner.from_yaml("config/etl.yaml")
```

The built-in resolvers use boto3's default region and credential chain (the
environment, an instance or task role, `~/.aws/config`). Nothing talks to AWS
until a placeholder actually resolves: a YAML without `${secrets:...}` or
`${ssm:...}` never creates a client and boots without boto3 installed. When a
placeholder does resolve and boto3 is missing, the error names the extra to
install, `loom-kernel[config-ssm]`.

Resolvers run at job startup — secret rotation takes effect on the next run
without redeployment. Values are fetched once per loaded config object
(OmegaConf caches resolver results), so a rotated secret is seen on the next
boot, not mid-run.

### Your own resolvers

Pass `resolvers=` to add custom prefixes (Azure Key Vault, HashiCorp Vault, …).
A resolver is any object with a `name` and a `resolve(key) -> object`:

```python
from loom.etl import ETLRunner

class VaultResolver:
    name = "vault"

    def __init__(self, url: str) -> None:
        self._url = url

    def resolve(self, key: str) -> str:
        return read_vault_secret(self._url, key)

runner = ETLRunner.from_yaml(
    "config/etl.yaml",
    resolvers=[VaultResolver("https://vault.internal")],
)
```

```yaml
storage:
  catalogs:
    main:
      token: ${vault:kv/prod/lakehouse/token}
```

Precedence is by name:

- A resolver you pass with the same name as a built-in (`secrets`, `ssm`) is
  the one used. Pin a region that way instead of relying on the default chain:

  ```python
  from loom.core.config.ssm import SsmResolver

  runner = ETLRunner.from_yaml("config/etl.yaml", resolvers=[SsmResolver(region="eu-west-1")])
  ```

- A built-in default never replaces a resolver already registered in the
  process, whether by an earlier factory call or by `load_config`. Calling the
  factory repeatedly in one process is safe.
- Resolvers passed explicitly, to a factory or to `load_config`, replace an
  earlier registration of the same name.

`load_config` itself registers no defaults. When you load YAML by hand, pass
the resolvers you need; `default_resolvers()` returns loom's built-ins:

```python
from loom.core.config import default_resolvers, load_config

cfg = load_config("config/etl.yaml", resolvers=default_resolvers())
```

The same `resolvers=` parameter, with the same defaults and precedence, is on
the REST and Celery `create_app` and on `StreamingRunner.from_yaml`.

---

## Deploying ETLs to Prefect

`discover_and_deploy_etls` registers one Prefect deployment per ETL. The ETLs
come from one of two sources, and exactly one must be given (both or neither is
a `ValueError`):

- `flows_package=`: a dotted Python package with one module per ETL, each
  calling `etl_flow(...)` at import time. This is the existing path and its
  behaviour is unchanged.
- `config=`: YAML declarations, with no Python module per ETL. `config` is a
  local path, a cloud URI (`s3://`, `gs://`, …) or a glob on either:

```python
from loom.prefect.deploy import discover_and_deploy_etls

deployment_ids = discover_and_deploy_etls(
    config="config/etls/*.yaml",
    work_pool="loom-fargate",   # default; "loom-docker" for local dev
    env="prod",                 # picks environments.<env> from each declaration
)
```

Globs follow the rules of [`includes`](#composing-files-with-includes): only
regular `.yaml` / `.yml` files match, sorted by resolved path, and a pattern
that matches nothing is a `ConfigError`.

### Deploying one flow

`deploy_flow` registers the deployment of a single flow returned by
`etl_flow`, `backfill_flow` or `maintenance_flow`, exactly as
`flows_package=` would register it. Use it when flows are built on demand
(for instance by a module `__getattr__`) rather than kept as one module
attribute per ETL, or to deploy one ETL by name:

```python
from flows import daily_orders  # an etl_flow(...), built eagerly or by __getattr__

from loom.prefect import deploy_flow

deployment_id = deploy_flow(
    daily_orders,
    work_pool="loom-fargate",   # used when the YAML sets no work_pool for env
    env="prod",
)
```

The entrypoint is `<source_file>:<flow function>`, relative to the
`working_dir` job variable (`/app/src` by default), so the worker rebuilds the
flow by importing that attribute. Any other object is a `TypeError`.

### Declaring ETLs in YAML

A declaration file has one of two shapes. The first is a **per-ETL document**:
the YAML `etl_flow` already reads (`schedule`, `params`, `tags`, `environments`,
`retry`, `notifications`, `correlation_field`, `trigger`) plus the keys that replace the
Python module:

```yaml
# config/etls/daily-orders.yaml
etl: daily-orders                              # optional; defaults to the file stem
pipeline: orders.pipelines.DailyOrdersPipeline  # dotted path to an ETLPipeline subclass
params_type: orders.pipelines.DailyOrdersParams # optional, see below
storage_config_path: /app/config.yaml          # optional; this is the default
correlation_field: run_date
schedule:
  cron: "0 6 * * *"
  timezone: Europe/Madrid
params:
  run_date: ${today-1d}
tags: [orders, daily]
environments:
  prod:
    work_pool: loom-fargate
    job_variables:
      cpu: 1024
      image: registry.example.com/orders:latest
retry:
  flow_retries: 2
  flow_retry_delay_seconds: 300
```

- `pipeline` (required): dotted path `package.module.Class` to an `ETLPipeline`
  subclass.
- `params_type` (optional): dotted path to the `msgspec.Struct` whose fields
  become the flow's typed parameters. When omitted it is inferred from the
  pipeline's generic binding, `class DailyOrdersPipeline(ETLPipeline[DailyOrdersParams])`.
  An explicit `params_type` always wins; a pipeline that leaves the parameter
  unbound and omits the key is a `ConfigError` naming the ETL.
- `storage_config_path` (optional): the loom storage YAML the runner reads at
  flow-run time inside the container. Defaults to `/app/config.yaml`, as with
  `etl_flow`; `LOOM_STORAGE_CONFIG_PATH` still overrides it at run time.
- The ETL name is `etl:` when present, otherwise the file stem
  (`daily-orders.yaml` → `daily-orders`). It is the Prefect flow name and the
  deployment name, verbatim.

The second shape is one file with an **`etls:` mapping** of name to the same
body:

```yaml
# config/etls/billing.yaml
etls:
  monthly-close:
    pipeline: billing.pipelines.MonthlyClosePipeline
    schedule:
      cron: "0 3 1 * *"
    params:
      run_date: ${today-1d}
    tags: [billing]
  invoice-sync:
    pipeline: billing.pipelines.InvoiceSyncPipeline
    params:
      run_date: ${today}
```

Both shapes may appear across the files of one glob. A matched file that
declares neither `etls:` nor `pipeline` is a `ConfigError` naming the file, so
keep ETL declarations in their own directory (`config/etls/`) or in one `etls:`
file rather than next to the storage YAML.

### Placeholders count from the run's scheduled start

A parameter may hold a placeholder: `${today}`, `${today±Nd}`, `${yesterday}`,
`${now}` or `${now±N}` with `d`, `h` or `m`. Inside a flow run it counts from
the run's scheduled start, read in UTC, not from the moment the run executes:

- a scheduled run resolves against its slot, even when it starts late or
  after midnight;
- a retry, by the engine or through "Retry" in the UI, resolves against the
  same slot;
- a manual run resolves against the time it was created.

A late run's `${now}` therefore covers up to its scheduled time. The run name
follows the same rule.

Once resolved, the flow writes the values back to its own run: the
parameters the API stored for it, with each placeholder replaced by its value
(dates and datetimes in ISO 8601, a backfill's `start_from` included). Nothing
else is added or changed. The run then shows the window it processed, and a
run resubmitted from the UI ("Retry") or copied reproduces it exactly: its
parameters are concrete, so it makes no call. A retry inside the same process
(`retries`) still holds the placeholders it started with, resolves them to the
same values and repeats the call, which is harmless.

The update is a single attempt with a 10-second timeout. If it fails, the flow
logs it at `ERROR`, in the run's logs, and runs anyway; the run then keeps
showing the placeholders.

Because every attempt of a run resolves the same values, the correlation id,
and therefore the manifest a retry resumes from, is stable across attempts.

### Running a deployment after another one

A per-flow YAML may name the deployment whose completed runs start this one,
and the parameters it copies from them:

```yaml
trigger:
  after: orders_ingest_daily
  inherit_params: [updated_at_from, updated_at_to]
```

`after` is the upstream deployment as `<flow>/<deployment>`, or a single name
when the flow and the deployment share it, as every loom ETL's do. The
deployer registers a Prefect `DeploymentEventTrigger` that fires on
`prefect.flow-run.Completed` of a run related to both that flow and that
deployment; a deployment name alone is only unique within its flow.

Each name in `inherit_params` is copied from the completed run's stored
parameters. Since the run records its placeholders resolved, the downstream
run receives the same concrete window. The name must therefore be stored on the
upstream run, which means declared under the upstream YAML's `params` (or
submitted with the run): a value the upstream flow only took from its
signature default is not stored, so it renders as empty text, which a `str`
field accepts as `""` and a `date` or `datetime` field fails to decode. The trigger passes each value as text,
so only a field declared exactly `str`, `date` or `datetime` can be inherited;
an optional field is refused because a null would arrive as the text `"None"`.

- Parameters not inherited take this deployment's defaults; a placeholder
  among them resolves against the downstream run's own creation time.
- If the upstream run could not record its parameters, the downstream run
  inherits the placeholder text and resolves it against its own creation
  time, which may not be the upstream slot.
- `trigger` and `schedule` may both be set, and the ETL then runs on both: on
  its schedule and after each completed upstream run.
- The upstream name is not checked against the deployments that exist: a
  trigger naming no deployment never fires.

The block is decoded strictly: `after` and `inherit_params` are its only keys,
`after` must name a deployment, without surrounding spaces in either part, and
each inherited name must be a field of the flow's params type.

The block decodes into `FlowTrigger` (`from loom.prefect import FlowTrigger`,
also in `loom.prefect.flow`). Building the flow runs
`loom.prefect.flow.check_inherited_params` on it; a CI check can run the same
validation on a YAML file without building the flow:

```python
from datetime import date

import msgspec
import yaml

from loom.prefect.flow import check_inherited_params, flow_settings_from_mapping


class MyParams(msgspec.Struct):
    run_date: date
    updated_at_from: str
    updated_at_to: str


settings = flow_settings_from_mapping(
    yaml.safe_load(
        """
        trigger:
          after: orders_ingest_daily
          inherit_params: [updated_at_from, updated_at_to]
        """
    )
)
check_inherited_params(settings.trigger, MyParams, name="orders_report")
```

In CI, load the flow's YAML file instead of the inline document and pass the
flow's own params type.

It raises `ValueError` for a name that is not a field of the params type, or
whose field is not declared exactly `str`, `date` or `datetime`, and does
nothing when the trigger is `None`.

### Validation before any deployment

Every declaration is read and checked before the first deployment is
registered, so a broken file fails the whole run instead of leaving a partial
set of deployments behind. Each error is a `ConfigError` naming the ETL and the
offending value:

- `pipeline` and `params_type` must import and have the right kind: an
  `ETLPipeline` subclass and a `msgspec.Struct` subclass respectively.
- The same ETL name declared in two files, or twice across `etls:` entries, is
  rejected; the message names both files.
- Names stay free-form, but each must map to a Python identifier once hyphens
  are replaced by underscores (the same rule `etl_flow` applies to its flow
  body): `daily-orders` → `daily_orders` is fine; `sales.daily` or
  `2024-close` are rejected. Two names that collapse to one identifier
  (`daily-orders` and `daily_orders`) are rejected as well.

Deployment metadata (name, tags, parameters, schedule, work-pool job variables,
retry policy, correlation field, notifications) is derived exactly as
`etl_flow` derives it, through the same loader.

### What the worker runs

A YAML-declared ETL has no module of its own, so the deployment's entrypoint is
a module loom owns, `loom.prefect.deploy.entrypoint.<attribute>`, where
`<attribute>` is the identifier derived above (`daily-orders` →
`loom.prefect.deploy.entrypoint.daily_orders`). That module rebuilds the flow
from the declaration file named by the `LOOM_ETL_CONFIG` environment variable.

The deployer fills that variable in two places: it exports it in its own
process for the duration of each deployment call (Prefect imports the
entrypoint on the deploy host too) and records it in the deployment's
`job_variables.env` as `LOOM_ETL_CONFIG=<uri of the declaring file>` so the
worker sees the same value. Keys you set under
`environments.<env>.job_variables.env` are merged with it; a declaration
that sets `LOOM_ETL_CONFIG` itself is a `ConfigError` naming the ETL, so the
recorded value is always the validated one.

Two consequences for the worker image:

- loom must be installed, since the entrypoint module lives in it.
- The declaration file must be reachable at the recorded URI from inside the
  container: bake `config/etls/` into the image at the same path, or point
  `config=` at a cloud URI the task role can read. The recorded value is the
  resolved path of the file (never the glob), so each deployment names exactly
  one file.

`LOOM_ETL_CONFIG` unset, or naming a file with no ETL for the requested
attribute, is a `ConfigError` at import time listing the known attributes.
The worker imports and validates only the requested ETL; sibling declarations
in the same file are checked at deploy time.

YAML-declared ETLs have no manifest store and take their retries from
`retry:` only; use `flows_package=` when an ETL needs a `ManifestStore` or a
dedicated `flow_config_path`.

### Trust boundary

A declaration file is executed configuration: its dotted paths are imported on
the deploy host and inside the worker, and the worker reads the file at every
run. Write access to it, and to anything it `includes`, therefore equals
deploy rights. Keep it in the image, or in a bucket whose write ACL matches the
image registry. Never embed credentials in the URI: it is stored in the
deployment's job variables in clear text. Anyone who can override job variables
in Prefect can override `LOOM_ETL_CONFIG`, and a declaration may not set
`LOOM_ETL_CONFIG` itself.

### Duplicates inside one file versus across files

A duplicate ETL name is always rejected as a `ConfigError`, and nothing is
deployed. Inside a single YAML document a repeated key (two `etls:` entries
with the same name) is refused while the file is parsed, and the error names
the file. Across files (two matched files, or a file and one it `includes`)
the error names both files.

---

## End-to-end example

A full working example with Polars and Spark pipelines, Delta Lake, and observability is
available in the companion repository:
[`dummy-loom-etl`](https://github.com/the-reacher-data/dummy-loom-etl).

---

## API reference

The ETL API reference is generated from public docstrings:

- [ETL API reference](../reference/api/etl)
