# ETL Quickstart

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

## Basic write modes

Every `IntoTable` target declares exactly one write mode by chaining a method.

| Mode | What it does | Example |
|------|--------------|---------|
| `append` | Add rows to the table | `IntoTable("staging.orders").append()` |
| `replace` | Full overwrite | `IntoTable("staging.orders").replace()` |
| `replace_partitions` | Overwrite only partitions present in the batch | `IntoTable("staging.orders").replace_partitions("year", "month")` |
| `replace_partition` | Overwrite a single known partition | `IntoTable("staging.orders").replace_partition(year=params.run_date.year)` |
| `replace_where` | Overwrite rows matching a predicate | `IntoTable("staging.orders").replace_where(col("date") == params.run_date)` |
| `upsert` | Merge on key columns | `IntoTable("staging.orders").upsert(keys=("order_id",))` |
| `update` | Update matching rows only — never inserts | `IntoTable("staging.orders").update(keys=("order_id",))` |

See the [ETL pipelines guide](../etl/pipelines.md) for the full write-mode reference.

## YAML config

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
    root: /var/lib/loom/lake/_runs
```

```python
from loom.etl import ETLRunner

runner = ETLRunner.from_yaml("config/etl.yaml")
```

## File aliases

Hard-coding file paths couples logic to infrastructure. Use aliases instead:

```yaml
storage:
  engine: polars
  files:
    - name: events_raw
      path:
        uri: s3://raw-bucket/events/
        storage_options:
          AWS_REGION: eu-west-1
```

```python
from loom.etl import ETLStep, FromFile, IntoFile, Format


class LoadEvents(ETLStep[DailyParams]):
    events = FromFile.alias("events_raw", format=Format.CSV)
    target = IntoFile.alias("exports_daily", format=Format.PARQUET)

    def execute(self, params: DailyParams, *, events: pl.LazyFrame) -> pl.LazyFrame:
        return events
```

## Next steps

- [ETL pipelines guide](../etl/pipelines.md) — full write modes, Spark runtime, cloud config, and pluggable resolvers
- [ETL testing guide](../etl/testing.md) — in-memory runners, scenarios, and stubs
- [ETL examples](../etl/examples.md) — companion repository with runnable Polars and Spark pipelines
- [API reference](../reference/api/etl)
