# Loom models as ETL tables

A loom model gives the ETL a table's name, columns and types. On read, on write
(a cast to the model, nothing more) and in tests, nothing else re-declares them.
Strings keep working unchanged: every builder accepts a model wherever it
accepts a table name.

```python
from datetime import datetime
from decimal import Decimal
from typing import Annotated

from loom.core.model import BaseModel, Field
from loom.core.model.types import DateTime, Numeric


class Order(BaseModel):
    __tablename__ = "order"
    order_id: Annotated[str, Field(primary_key=True)]
    customer_id: Annotated[str, Field(index=True)]
    amount: Annotated[Decimal, Numeric(precision=12, scale=2)]
    placed_at: Annotated[datetime, DateTime(tz=True)]


class CustomerTotal(BaseModel):
    __tablename__ = "customer_total"
    customer_id: Annotated[str, Field(primary_key=True)]
    spent: Annotated[Decimal, Numeric(precision=14, scale=2)]
    orders: int
```

```python
class TotalCustomers(ETLStep[Since]):
    orders = FromTable(Order).where(col(Order.c.placed_at) >= params.since)
    target = IntoTable(CustomerTotal).upsert()

    def execute(self, params: Since, *, orders: pl.LazyFrame) -> pl.LazyFrame:
        return orders.group_by(Order.c.customer_id).agg(
            pl.col(Order.c.amount).sum().alias(CustomerTotal.c.spent),
            pl.len().alias(CustomerTotal.c.orders),
        )
```

## The model is the table

`__tablename__` is the logical reference. The lake routes it exactly as the same
string: a `storage.tables` entry wins, else a two or three part ref is qualified
by the default catalog, else `defaults.table_path` plus the name. The model says
what the table is; YAML says where it lives.

A dotted ref is allowed for a model that lives only in the lake. The SQL store
refuses a dotted `__tablename__` when the model is registered: a model that
lives in both places uses a simple name plus a lake route.

Migrating from a hand-written schema is copying the ref into the model:

```python
# before
orders = FromTable("sales.orders").with_schema(OrderSchema)
target = IntoTable("sales.customer_total").upsert(keys=("customer_id",))

# after
class CustomerTotal(BaseModel):
    __tablename__ = "sales.customer_total"
    ...

orders = FromTable(Order)
target = IntoTable(CustomerTotal).upsert()
```

No YAML change is needed: the ref routes as before.

## Column names: `Model.c`

`Model.c.<field>` is the column name as a `str`. A misspelt field raises
`AttributeError` naming the model and the field, at import for a class body. A
field named `c` is refused at class creation. `TableRef.c` is gone: `.c` always
means names.

## Builders

| Declaration | With a model |
|---|---|
| `FromTable(Order)` | `FromTable("order").with_schema(Order)`: both carry the model, model columns are cast, extra table columns pass through |
| `IntoTable(CustomerTotal)` | table name and write schema from the model |
| `IntoHistory(CustomerTierHistory, keys=(CustomerTierHistory.c.customer_id,))` | `date_type` comes from the model's `valid_from` (`Date` is `"date"`, `DateTime` is `"timestamp"`); modes and the other arguments stay on `IntoHistory` |
| `FromTable("raw.orders").with_schema(Order)` | any table source can take a model as its schema, and keeps its own table name |

`with_schema(Model)` on a table named by string switches that source to model mode:
the model is translated and its columns are checked at compile time, as for
`FromTable(Model)`. Passing anything other than a model (a tuple of `ColumnSchema`, a
struct, a dataclass) keeps the plain cast-only behaviour. A model used this way still
needs `__tablename__` and resolvable annotations; otherwise compilation fails with
`MODEL_COLUMN_TYPE_REFUSED`.

`IntoHistory` needs the model to declare the named `valid_from` / `valid_to`
(and `deleted_at` under `delete_policy="soft_delete"`). An explicit `date_type`
that contradicts the model fails at compile time. Only `schema=STRICT` is
available for a model.

A declaration only names the model; the compiler translates it. A column named in a
declaration that the model lacks fails at compile time and the error names the
model: the keys, `include`, `exclude`, `track` and partition columns of a target, its
`replace_where` predicate, and a model source's `columns(...)`, `where(...)` predicate and
`parse_json(...)` columns.

## Upsert and update keys

Without `keys`, `upsert()` and `update()` merge on `conflict_key_of(Model)`: the
business key, else the primary key.

```python
IntoTable(CustomerTotal).upsert()                                    # customer_id
IntoTable(CustomerTotal).upsert(keys=(CustomerTotal.c.orders,))      # another key
IntoTable(CustomerTotal).update(include=(CustomerTotal.c.spent,))
```

Explicit `keys` stay allowed. Every key and every `include` / `exclude` column
must be a model column. Without `keys`, a model whose key cannot come from the
frame (autoincrement, derived or server-filled primary key and no business key)
is refused at compile time.

## Lake types

One row per kind. The lake unit for time is microseconds.

| Model type | Delta | Polars | Spark |
|---|---|---|---|
| `TinyInteger` / `SmallInteger` / `Integer` | byte / short / integer | Int8 / Int16 / Int32 | Byte / Short / Integer |
| `BigInteger`, and a bare `int` | long | Int64 | LongType |
| `Float` | double | Float64 | DoubleType |
| `Boolean` | boolean | Boolean | BooleanType |
| `String(n)`, `Text`, `JsonStr` | string | String | StringType |
| `Uuid` (canonical text) | string | String | StringType |
| `JSON` opaque (JSON text) | string | String | StringType |
| `JSON` struct or list | struct / array | Struct / List | StructType / ArrayType |
| `Bytes` | binary | Binary | BinaryType |
| `Date` | date | Date | DateType |
| `DateTime(tz=True)` | timestamp | Datetime(us, UTC) | TimestampType |
| `DateTime(tz=False)` | timestamp_ntz | Datetime(us) | TimestampNTZType |
| `Numeric(p, s)`, p up to 38 | decimal(p,s) | Decimal(p,s) | DecimalType(p,s) |
| dialect types | the row of their portable counterpart | | |

Refused at compile time with `ETLCompilationError` code `MODEL_COLUMN_TYPE_REFUSED` (and
one `loom.etl.model_refused` log), naming model, table, field and fix:

- `Time`: Delta has no time type.
- `Numeric()` unbounded, and `Numeric` with precision above 38; also a nested
  `Decimal` with no precision.
- `TSVECTOR` and any undeclared type (a bare `Decimal`, a plain `Enum`, a value
  class). Store a `str` or a bounded `Numeric` and convert in the domain.

Unsigned integer widths are not part of the vocabulary: Delta stores them as the
same-width signed type and overflows. `int` is `BigInteger` everywhere, so ids
and epoch milliseconds never overflow; declare `Integer` for a 32-bit column.

### JSON columns

| Field annotation | Lake column |
|---|---|
| a struct sub-entity | struct |
| `list[X]` | list of X |
| `dict`, `Any`, a union of structs, a recursive sub-entity (from the repeat down), `tuple`, `set`, `frozenset` | JSON text |
| `JsonStr` | text |

Nested struct field names are the `encode_name`s, so a sub-entity declared with
`rename="camel"` is stored as camel case. An absent field and a null field are
both null. Renaming a sub-entity field is schema evolution.

### Reading JSON

A model JSON column that arrives as JSON text is decoded on write with the
model's struct. Every value is fitted to its declared type by one rule per leaf
type, and a value that does not fit follows the column's policy.

| Declared type | A value fits when it is |
|---|---|
| `Int8` ... `Int64`, `UInt8` ... `UInt64` | a JSON integer, not a boolean, within the type's range |
| `Float32`, `Float64` | a finite JSON number within the type's range |
| `Boolean` | `true` or `false` |
| `Utf8` | any value: a string as it is, anything else as its JSON text |
| `Date` | `YYYY-MM-DD`, or a valid ISO datetime of which the date part is kept |
| `Datetime` | a valid ISO datetime, or a date at midnight; no offset is UTC and an offset is converted to the instant it denotes |
| `Decimal(p, s)` | a JSON number or numeric text that fits `p` digits after rounding half up to `s` |
| `Enum` | one of its members |
| `Array(n)` | a JSON array of exactly `n` fitting items |
| `Struct`, `List` | an object or an array whose values fit; an absent field is null |

`JsonMismatch.FAIL` (the default, and the only policy of a model write) raises an
`InvalidValue` naming the column, the path of the value and the rule it broke.
`JsonMismatch.NULL` nulls the value and keeps its siblings; a document that is not
JSON, or whose top level does not fit, is null.

Leaf types JSON text cannot hold (`Binary`, `Time`, `Duration`, `Null`,
`Categorical`) are refused at compile time with an `ETLCompilationError` naming the
field and the type.

Documents whose shape varies opt in per column:

```python
from loom.etl import JsonMismatch

snapshots = FromTable("raw.snapshots").parse_json(
    "payload", Snapshot, on_mismatch=JsonMismatch.NULL
)
```

Fixtures rarely cover every shape of a document store's documents, so a variable
Mongo collection should read its JSON columns with `JsonMismatch.NULL` from the start.

Polars fits every document in Python, in chunks, on that column only; Spark reads
the column with `from_json`, in `FAILFAST` or `PERMISSIVE` mode. The engines
differ in these values, which the Polars rule table refuses or nulls:

| Value | Polars | Spark |
|---|---|---|
| `NaN`, `Infinity`, or a number beyond `Float32` | refused or null | kept |
| a negative number under an unsigned type | refused or null | kept |
| a date with a suffix that is not a time, or an invalid time | refused or null | the date part is kept |
| a number under a timestamp | refused or null | read as epoch seconds |
| an offset under a naive timestamp | UTC wall time | refused or null |
| numeric text with thousands separators under a decimal | refused or null | read |
| an element of a list that does not fit | that element is null | the list is null |
| malformed JSON, or a non-object under a struct | null | a struct of nulls under `NULL` |
| an `Enum` member, an array width | checked | no such type |

## What a write does

A write casts the frame to the model's lake schema, and that is all:
`replace`, `append`, `upsert`, `update`, `replace_partitions` and historify share
it. Input-only columns (`delete_flag`, `delete_marker`), the boundary columns
historify computes and the audit columns a Polars write stamps (`audit.enabled`) are
kept, with the types the stamp gives them. If the existing table's physical schema disagrees
with the model the writer fails as it did before. Drift detection, exact-column
rules, creation rules and schema evolution are not supported yet.

## Testing from the model

Seed and assert with the model and `make`, with no column tuples:

```python
from loom.testing import make

day = datetime(2026, 1, 2, tzinfo=UTC)
runner.seed_rows(
    make(Order, order_id="o1", customer_id="c1", amount=Decimal("10"), placed_at=day),
    make(Order, order_id="o2", customer_id="c1", amount=Decimal("12"), placed_at=day),
)
result = runner.run(TotalCustomers, Since(since=day))

result.assert_rows(make(CustomerTotal, customer_id="c1", spent=Decimal("22"), orders=2))
assert result.rows(CustomerTotal)[0].orders == 2
```

- `seed_rows(*instances)` writes with the model's lake schema, never inferred
  dtypes. It refuses two instances that share a `conflict_key_of` value, because
  `make` repeats what it does not generate: pass each row its key.
- `result.rows(Model)` returns instances, cast to the model's lake types.
- `result.assert_rows(*instances)` ignores order and compares the exact row set
  over every model column.
- `make` ignores `Numeric` and `DateTime` markers: pass `Decimal` and aware
  `datetime` values yourself.
- Spark timestamps are read as UTC instants on any machine.

The same methods exist on `PolarsStepRunner`, `SparkStepRunner` and
`ETLScenario`.

## Behaviour changes

What changes for an existing pipeline when it moves to this release:

- `int` is `BigInteger`: the SQL DDL of an undeclared `int` is now BIGINT, and
  autogenerate shows it.
- A naive `datetime` is UTC.
- An unbounded `Decimal` is refused on the lake; declare `Numeric(precision, scale)`.
- Spark writes a tz-less datetime as `timestamp_ntz`, as Polars already did.
- Spark decimals keep their declared precision and scale; a decimal with no
  scale was read as scale 0.
- JSON decoding is strict: a value that does not fit its declared type raises. A pipeline
  that relied on tolerant decoding sets `on_mismatch=JsonMismatch.NULL` on the column.
- The SQL store refuses a field whose type is undeclared, where it used to store
  it as JSON.
- `JsonStr` is `Text`.
- `LakeTranslator.annotation_type` and `with_schema` refuse with `InvalidValue`, not `TypeError`. A
  string annotation, an unresolved forward reference such as a raw `__annotations__` value under
  `from __future__ import annotations`, is refused: pass resolved hints from
  `typing.get_type_hints(cls)`.
- `TableRef.c` and `ColumnRef` are removed: use `Model.c.<field>`.
- ETL error classes follow core's categories: `SchemaError`,
  `EmptyReplaceError`, the historify conflicts and other validation errors are
  `InvalidValue`; `UnsupportedUpsertDeleteFlagError` and `SchemaNotFoundError`
  are `InvalidUse`; `MaintenanceError` and `CheckpointCleanupError` are core
  `SystemError`. `except` clauses on the old bases, `code` and retryability keep
  working as before; `ETLCompilationError` is unchanged.
