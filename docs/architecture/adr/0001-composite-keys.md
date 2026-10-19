# ADR 0001: Composite primary keys in relations and REST routes

- Status: Proposed
- Issue: #338
- Applies to: loom-kernel 2.20.0 and later

## Context

Since 2.20.0 the SQLAlchemy repository works with composite primary keys:
`get_by_id`, `exists`, `update` and `delete` take the full key as a tuple, the
cursor tie-breaks on every key column and the cache keys entries by the full key.

Two features still refuse a composite key:

- Relation steps and backend projections on a composite-key owner raise
  `ValueError` at compile time (`_refuse_composite_key_loads`), because the
  loaders group parent rows by the first key column.
- Auto-CRUD mounts no `/{id}` routes for a composite-key model, because one path
  segment cannot carry the key.

Three existing defects were found while designing this:

1. A single-key owner whose MANY_TO_ONE target is keyed `(account_id, id)` joins
   on `id` only (`_fetch_many_to_one`). Without RLS, rows of two accounts that
   share an `id` collide and an owner can receive another account's row.
2. Placeholders in `RestInterface.prefix` are not passed to the use case: path
   parameters are read from `route.path` only, while the route is registered on
   prefix + path. Nothing refuses a placeholder in the prefix.
3. Auto-CRUD types the item parameter from a field named `id`; a model keyed by
   another name gets an untyped `str` parameter.

## Decision

### DSL

No new argument for relations. The same declarations compile and load
correctly; loom reads the join columns from the `ForeignKeyConstraint` it
already builds for scoped models.

```python
class Ticket(BaseModel, RowScoped):                 # key (account_id, id)
    __tablename__ = "ticket"
    account_id: int = ScopedField(primary_key=True, scope="account")
    id: int = ColumnField(primary_key=True)
    customer_id: int = ColumnField(foreign_key="customer.id")
    customer: CustomerOut | None = RelationField(
        foreign_key="customer.id", cardinality=Cardinality.MANY_TO_ONE, profiles=("detail",))
```

`LoadById` gains a tuple form, because loom cannot guess which execute
parameters make up the key:

```python
ticket: Ticket = LoadById(Ticket, by=("account_id", "id"))
```

A composite foreign key between unscoped models stays out of scope: loom has
no way to declare one today and nothing needs it.

### Routes

The item path holds the key columns that are neither placeholders in the
prefix nor the request's scope boundary column. Every prefix placeholder must
name a model field, or the interface fails to compile.

| Model | Declaration | Routes |
|---|---|---|
| RowScoped `(account_id, id)` | `prefix = "/tickets"`, `auto = True` | `GET /tickets/`, `POST /tickets/`, `GET\|PATCH\|DELETE /tickets/{id}`; `account_id` comes from the request scope binding |
| Unscoped `(order_id, line_no)` | `prefix = "/orders/{order_id}/lines"`, `auto = True` | `GET /orders/{order_id}/lines/`, `GET\|PATCH\|DELETE /orders/{order_id}/lines/{line_no}` |

A child that does not belong to its parent answers the same 404 as a missing
row. Generated nesting stops at one parent level; deeper paths need an explicit
route. The parent filter is applied outside the caller's filter group so an
`OR` group cannot swallow it.

Rejected: one path segment holding an encoded key (untyped parts, custom codec,
`/` must be percent-encoded) and a path parameter for the scope boundary column
(the URL could name another tenant).

### Internal design

- `primary_key_of(obj, names)` in `core/model/introspection.py`, next to
  `get_id_attributes`: a scalar for one column, a tuple in declaration order for
  several. Consumers: repository mixins, projection runtime, auto-CRUD.
- `core/backend/core_model.py`: relation steps carry tuples of column names and
  group by tuple; row-value `tuple_(...).in_()` for several columns, `col.in_()`
  for one, so single-key SQL does not change.
- `core/backend/sqlalchemy.py`: read join columns from the foreign-key
  constraint; `_inspect_secondary` walks `foreign_key_constraints`.
- `rest/autocrud.py`: one typed path parameter per remaining key column; path
  parameters extracted from the full path.
- Deleted: `_refuse_composite_key_loads`, `_fields_loaded_by_key`,
  `_find_owner_fk_col`, the `/{id}` subtraction in auto-CRUD.
- Mongo, DynamoDB and `InMemoryRepository` keep refusing composite keys.

Open point: how auto-CRUD reaches the request's scope binding without `rest`
depending on the SQLAlchemy RLS package. Proposal: move the binding resolver,
which depends only on `core.identity`, into `loom.core`.

### Performance

Composite keys must keep the loading cost loom has today:

- No lazy loading. ORM relationships stay `lazy="noload"`
  (`core/backend/sqlalchemy.py`, `_relationship_kwargs`); reading a relation
  attribute never issues a query.
- One batched query per relation step for a whole page, whatever the number of
  rows: `WHERE (fk_1, fk_2) IN ((…), (…))` replaces `WHERE fk IN (…)`. A page of
  N rows with K relation steps costs 1 + K statements, never 1 + N × K.
- Relations load only for the profile that declares them (`profiles=("detail",)`);
  the default list profile loads none.
- The bound parameters of one IN list are page size × key width. With the
  maximum page size of 1000 and a two-column key that is 2000 parameters,
  below Postgres's limit of 65535 per statement. A wider key or a larger
  maximum page size needs chunking.
- The foreign-key columns of a composite relation need a composite index in
  the same column order, or the row-value IN scans the child table. To be
  verified: whether loom creates that index for the composite foreign keys it
  generates.
- To be verified: the relation batches of one read run with `asyncio.gather` on
  the same `AsyncSession` (`core_model.py`, the call to `_fetch_relation_batch`).
  SQLAlchemy does not support concurrent use of one `AsyncSession`; this applies
  to single keys today and must be checked before adding composite loads.

Every relation test counts the statements it issues (1 + K) and runs on
Postgres with `EXPLAIN` showing an index scan for the row-value IN.

## Standards

No ISO standard governs REST route design. Each rule cites the standard that
applies and whether it is normative or a convention.

| Rule | Standard | Kind |
|---|---|---|
| Path segments separated by `/` | RFC 3986 §3.3 | Normative syntax; parent/child reading is a convention |
| Reserved characters and percent-encoding in a segment | RFC 3986 §2.1, §2.2 | Normative |
| Paths are case-sensitive | RFC 3986 §6.2.2.1 | Normative |
| GET is safe; GET, PUT and DELETE are idempotent | RFC 9110 §9.2 | Normative |
| PATCH is not idempotent | RFC 5789 | Normative |
| 404 instead of 403 to hide existence | RFC 9110 §15.5.4 | Allowed by the standard; choosing it is policy |
| Every template expression has a required path parameter | OpenAPI 3.1, path templating | Normative within OpenAPI |
| Parameter names equal field names, `*_id` representation term | ISO/IEC 11179-5 | Standard for data elements; applying it to URLs is a convention |
| Plural collection nouns | Google AIP-122 | Convention |
| kebab-case literal segments, snake_case parameters | none | Convention |
| Times in filters and cursors | ISO 8601, RFC 3339 profile | Normative where used |

## Tests that fail on 2.20.0

1. MANY_TO_ONE on a composite-key owner loads each owner's own target.
2. ONE_TO_MANY: parents `(1, 5)` and `(2, 5)` each get only their own children.
3. MANY_TO_MANY through a scoped secondary picks the right join columns.
4. A backend projection counts per full key and a custom `load_many` receives tuples.
5. Defect 1: customers `(1, 7)` and `(2, 7)`; a ticket in account 2 gets account 2's customer.
6. Auto-CRUD on a composite model publishes both path parameters; a mismatched parent answers 404.
7. A prefix placeholder reaches the use case and appears in OpenAPI.
8. RowScoped `/tickets/{id}` bound to tenant A for a row of tenant B answers 404 without RLS.

## Consequences and timing

No consumer needs relations, projections, auto-CRUD or nested routes on a
composite-key model today. Build those when the first one appears.

Do now, independent of any consumer: refuse `{` in `prefix` at compile time,
and record defect 1 in #338.

Estimated effort: relations and projections about 2 days; multi-parameter
auto-CRUD and the prefix fix 1 to 1.5 days; scope from the request 1 to 2 days.
