# Persistence backends

`persistence.backend` names a plugin. loom resolves it from the
`loom.persistence.backends` entry point group at bootstrap, asks it once for
everything the host needs, and never imports a backend it was not asked for.
Four ship with loom — `sqlalchemy`, `dynamodb`, `mongo` and `none` — and a
package of your own registers a fifth the same way.

```yaml
persistence:
  backend: ledger          # resolved by name; unknown → startup error listing the registered ones
```

## Writing a backend

A backend is a class with a no-argument constructor, a `name` and one method:
`build(ctx, models, runtime) -> PersistenceWiring`. It reads its own
configuration section from the context it is given; the host knows nothing
about that section. `runtime` is a `BackendRuntime`: the
`RuntimeProfile` of the entry point building it (`server`, `worker`, ...),
whose defaults size any connection pool the backend opens, and an optional
pool-metrics sink. A backend with no pool ignores it.

```python
# billing_ledger/backend.py
from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any, ClassVar

import msgspec

from loom.core.config import ConfigContext, ConfigError
from loom.core.model import BaseModel
from loom.core.model.introspection import get_id_attribute
from loom.core.persistence import BackendRuntime, PersistenceWiring
from loom.core.repository import RepositoryBuildContext, build_repository_registration_module

from billing_ledger.client import LedgerClient
from billing_ledger.repository import RepositoryLedger


class _LedgerConfig(msgspec.Struct, kw_only=True):
    url: str
    namespace: str = "orders"


class LedgerBackend:
    name: ClassVar[str] = "ledger"

    def build(
        self, ctx: ConfigContext, models: Sequence[type[BaseModel]], runtime: BackendRuntime
    ) -> PersistenceWiring:
        cfg = ctx.section_optional("persistence.ledger", _LedgerConfig)
        if cfg is None:
            raise ConfigError("persistence.backend is 'ledger' but 'persistence.ledger' is missing.")
        client = LedgerClient(cfg.url, cfg.namespace)
        return PersistenceWiring(
            uow_factory=None,
            repo_registration_module=build_repository_registration_module(
                models=models,
                build_registered_repository=lambda context, registration: _build(client, context),
                default_repository_type=RepositoryLedger,
            ),
            lifespan_init=lambda: _lifespan(client),
            default_repository_type=RepositoryLedger,
            prepare_models=_validate_identifiers,
            readiness=client.ping,
        )


def _build(client: LedgerClient, context: RepositoryBuildContext) -> Any:
    return RepositoryLedger(client, context.model)


def _validate_identifiers(models: Sequence[type[BaseModel]]) -> None:
    for model in models:
        get_id_attribute(model)  # a model without a primary key fails here, at startup


@asynccontextmanager
async def _lifespan(client: LedgerClient) -> AsyncIterator[None]:
    await client.connect()
    try:
        yield
    finally:
        await client.close()
```

```toml
# pyproject.toml of the billing_ledger package
[project.entry-points."loom.persistence.backends"]
ledger = "billing_ledger.backend:LedgerBackend"
```

Two packages registering the same name is a `DuplicateEntryPointError` at
startup: the host fails closed instead of picking one.

### The wiring

`PersistenceWiring` is a frozen dataclass; every field is consumed once, by
the host, in this order:

| Field | What the host does with it |
|---|---|
| `uow_factory` | Bound to the kernel executor. `None` means "no unit of work": use cases run without one, and deferred job dispatch never fires. |
| `prepare_models` | Called right after `build` with the discovered models — the slot where SQLAlchemy compiles its tables and where a document store validates identifiers. Raise `ConfigError` for a model the backend cannot serve. Defaults to `no_model_preparation`. |
| `default_repository_type` | The class whose declared capabilities decide which auto-CRUD operations a model gets (see below). `None` means the backend serves no repositories, and an interface with `auto_crud_model` refuses to boot. |
| `repo_registration_module` | A DI module run against the container; registers one repository per model. |
| `lifespan_init` | An async context manager entered at application startup and exited at shutdown, after the SQL registry and the AI runtime so their clients close even when it fails. Defaults to `no_lifespan`. |
| `readiness` | Optional `async () -> bool`. Aggregated by `GET /health`; absent, the route reports no backends. |

A readiness probe never raises: log the failure and answer `False`, so the
route answers `503 degraded` instead of `500`.

See [Execution lifecycle](use-case-dsl.md#execution-lifecycle) for how the
executor drives the unit of work `uow_factory` produces — commit, rollback,
and the post-commit actions that run once it has closed.

## Capabilities and the gate

Repositories declare what they can do by inheriting capability protocols:
`Readable`, `Creatable`, `BulkCreatable`, `Updatable`, `Deletable`, `Listable`
and `Countable` (`loom.core.repository.abc`). `capabilities_of(repository_type)`
is the single detector — the registration module binds a DI key per declared
protocol, and the auto-CRUD gate mounts one operation per protocol:

| Auto-CRUD operation | Requires |
|---|---|
| `create` | `Creatable` |
| `get` | `Readable` |
| `list` | `Listable` |
| `update` | `Updatable` |
| `delete` | `Deletable` |

The gate looks at the class that will serve the model — the explicit
`repository_for` registration when there is one, else the backend's
`default_repository_type` — and:

- narrows an interface with an empty `include` to the supported operations
  (an `OrderInterface` over `dynamodb` mounts get, create, update and delete);
- refuses, at startup, an explicit `include` naming an operation the class
  lacks, naming model and backend;
- refuses an interface whose backend supports none of the five.

```python
from loom.core.repository.registration import capabilities_of

capabilities_of(RepositoryLedger)   # (Readable, Creatable, BulkCreatable)
```

Swapping the `DefaultRepositoryBuilder` does not move the gate: it keeps
reading `default_repository_type`, so a custom builder must declare the same
capabilities or register its repositories explicitly.

### `BulkCreatable`

`create_many(data) -> tuple[Model, ...]` persists a sequence and returns the
entities in input order. SQLAlchemy writes the batch with one multi-row
`INSERT`; a backend whose store has no multi-row write declares the protocol
only if it can honour the order and document its atomicity.

### `UnsupportedQuery`

A capability the class does not declare is absent from DI and from the
generated routes. What the protocol cannot express — a lookup the backend can
only serve by scanning, a cursor from another backend — is refused at run time
with `UnsupportedQuery(backend, model, reason)`, mapped by the REST layer to
`400` with code `unsupported_query`:

```python
from loom.core.repository.abc.errors import UnsupportedQuery

raise UnsupportedQuery("dynamodb", "orders.Order", "get_by on 'customer_id' needs an index")
```

## Dependency verification at boot

Once every service is registered — persistence, job service, SQL, observability,
AI — `create_app` calls `UseCaseFactory.verify()`. Every registered use case's
constructor is checked against the container, and a missing binding raises
`ResolutionError` naming the use case, the parameter and the key. A backend
that registers `Readable[Order]` but not `Listable[Order]` therefore fails the
bootstrap of a use case injecting `Listable[Order]`, not its first request.
Use cases of routes the gate pruned are dropped before that check.

## Cursor tokens

Keyset pagination has one token contract for every backend
(`loom.core.repository.abc.cursor`, decided in ADR 0004,
`docs/architecture/adr/0004-signed-cursors.md`). A token carries the
issuing backend's name, the sort key values of the last row on the page and the
primary key as tie-breaker, and it is signed with HMAC-SHA256:

```text
base64url(payload) "." base64url(version || len(key_id) || key_id || mac)
```

The signature also covers what the token is bound to, which never travels in
it: the model, the normalised filters (datetimes compared as UTC instants, the
order of the conditions ignored), the sort and the caller's scope. A token
replayed with other filters, another sort, for another model or in another
tenant does not verify. The SQLAlchemy repositories bind the scope to the
row-level-security scope values of the caller (see
[Postgres row-level security](postgres-rls.md)); without them, and on Mongo,
the scope is `""`, which binds the token to no tenant on purpose.

Every backend issues and verifies tokens through one `CursorCodec`, built at
startup from the `persistence.cursors` section and registered in DI. The code
default is `signed` in every environment; each opt-out is written in the file
of the environment that needs it:

```yaml
# production.yaml
persistence:
  cursors:
    current_key_id: "2026-10"          # signs new tokens
    keys:                              # every key that still verifies
      "2026-10": {env: CURSOR_KEY_2026_10}
      "2026-07": {env: CURSOR_KEY_2026_07}
```

```yaml
# development.yaml
persistence:
  cursors:
    signing: ephemeral
```

```yaml
# test.yaml
persistence:
  cursors:
    signing: unsigned
```

| Key | Default | Meaning |
|---|---|---|
| `signing` | `signed` | `signed`, `ephemeral` or `unsigned` |
| `current_key_id` | — | Key id that signs new tokens; must be one of `keys` |
| `keys` | `{}` | Key id → exactly one source, `path` (a file, such as a mounted secret) or `env` (an environment variable). The source holds the key in standard padded base64, surrounding whitespace ignored, decoding to at least 32 random bytes; `openssl rand -base64 32` produces one. A passphrase or any other text is refused. Key ids match `[A-Za-z0-9._-]{1,64}` |

Key material never appears in the configuration, so a dump of it cannot leak
a key, and a configuration error names the key id, never the key or its path.
To rotate, add the new key, make it `current_key_id`, and drop the old id once
the tokens it signed no longer matter: tokens are short-lived page pointers,
so one deployment cycle is usually enough.

Without a key, startup fails in every environment with a `ConfigError` naming
the `development.yaml` opt-out. The two opt-outs log a WARNING at startup:

- `signing: ephemeral` signs with a random key generated per process. Tokens
  do not survive a restart and another instance refuses them. For
  `development.yaml`.
- `signing: unsigned` signs nothing, so a token can be read and written by
  hand. For `test.yaml` and debugging only.

Production uses `signed`. In production and staging the deploy check
`loom.cursors.E001` fails startup, and `loom check --deploy` fails, when a
`database` or `persistence.mongo` section is set and `signing` is anything but
`signed` or `current_key_id` names no entry of `keys` (see
[Environments](../getting-started/environments.md#deploy-checks)).
`persistence.cursors` cannot be set from a `LOOM__*` variable in those
environments: it is not on the override allowlist, and no `config.overridable`
entry may open a key under it. A key's `env` source names
the variable holding the key, which must not start with `LOOM_`; on ECS it is
a `secrets` entry of the task definition.

Decoding is strict in every mode: a token that is not
unpadded base64url, a payload that is not the token record, a datetime key
without a time zone, a key of an unknown type, a token of another backend, or
one whose key count differs from the sort or the primary key, or whose value
has another type than its column, is refused: a string is not an integer and
is never parsed into a datetime, date, time, UUID or decimal. Every
refusal is `UnsupportedQuery` — `400 unsupported_query` on the wire — and its
message never echoes the token. An empty token is refused too, on every
backend. A cursor page orders only by non-null columns of a type a token
carries (`bool`, `int`, `float`, `str`, `datetime`, `date`, `time`, `UUID`,
`Decimal`, an enum or a `Literal`): a dotted path, a JSON or `bytes` column or
an optional column answers `400 unsupported_query` before the query runs. A
page whose last row holds a non-finite number answers the same 400 when its
next token is issued, after the query has run. A naive datetime key, such as a
`DateTime(tz=False)` column, is issued as UTC with an explicit offset and
compared as naive UTC again against that column.

A service paginating by hand resolves the same codec and binds its own scope:

```python
from loom.core.model.introspection import scalar_column_annotations
from loom.core.repository.abc import Cursor, CursorCodec, PaginationMode, QuerySpec, SortSpec

codec = container.resolve(CursorCodec)
query = QuerySpec(sort=(SortSpec("updated_at", "DESC"),), pagination=PaginationMode.CURSOR)
token = codec.encode(
    Cursor(backend="grants", keys=(updated_at,), tie_breaker=grant_id),
    model="access.Grant",
    query=query,
    scope=tenant_id,
)
position = codec.decode(
    token,
    backend="grants",
    model="access.Grant",
    query=query,
    scope=tenant_id,
    columns=scalar_column_annotations(Grant),   # or {"updated_at": datetime, "id": UUID}
    primary_key=("id",),
)
position.keys, position.tie_breaker   # (updated_at, grant_id); datetimes always aware
```

A composite primary key is carried whole: the tie-breaker is the tuple of its
values in key order, and `decode(..., primary_key=("tenant", "id"))` refuses a
token carrying another number of key values. Tokens issued before signed cursors share the
fate of any invalid token: clients must treat tokens as opaque and restart from
the first page.

## `sqlalchemy` connection pool

The API, the Celery worker and the streaming SQLAlchemy sinks read the same
`database` section — at `database`, at `database.<name>` for a shared
streaming database, or inline in a sink with `url` — and build their engine
through one constructor. Pool keys live under `database.pool`:

```yaml
database:
  url: postgresql+asyncpg://app@db/app
  connect_args:              # passed to the driver's connect() as written
    timeout: 5               # asyncpg: seconds to establish a connection
    command_timeout: 10      # asyncpg: seconds per statement
  pool:
    size: 5                  # connections kept open, > 0
    max_overflow: 5          # extra connections under load, >= 0
    timeout: 10              # seconds a caller waits for a free connection, > 0
    recycle: 1800            # seconds before a connection is replaced, > 0
    pre_ping: true           # check a connection before handing it out
```

Each key left out takes the default of the entry point, sized for how many
operations one of its processes runs at once:

| Entry point | `size` | `max_overflow` | `timeout` | `recycle` | `pre_ping` |
|---|---|---|---|---|---|
| API (`create_app`) | 5 | 5 | 10 s | 1800 s | on |
| Celery worker | 2 | 0 | 30 s | 1800 s | on |
| Streaming sink | 2 | 0 | 30 s | 1800 s | on |

- One process opens at most `size + max_overflow` connections; the instances
  times that bound must fit the database's `max_connections`. A caller that
  finds the pool exhausted waits `timeout` seconds, then fails with a
  checkout timeout rather than hanging.
- A Celery worker running `worker_pool: threads` or `gevent` with a
  concurrency above one per process needs `database.pool.size` at least that
  concurrency.
- Keep `recycle` below the idle timeout of the server or of any proxy in
  front of it, so a connection the other side dropped is never reused.
- In-memory SQLite (`sqlite+aiosqlite:///`) holds a single shared connection:
  `size`, `max_overflow` and `timeout` do not apply to it and are left out.
- Unknown keys under `database.pool` stop startup naming the key. The flat
  keys this section once took (`pool_size`, `max_overflow`, `pool_timeout`,
  `pool_recycle`, `pool_pre_ping`) stop startup naming their `pool.`
  replacement. Keys outside `pool` and `schema` are left to the other readers
  of the section (migrations, row-level security, streaming).
- The size, overflow, timeout and recycle keys can be overridden per deploy
  (`LOOM__DATABASE__POOL__SIZE=1`); see [Environments](../getting-started/environments.md).

### Pool metrics

With `observability.prometheus.enabled`, the API publishes, with
labels `application`, `entrypoint` and `database`:

| Metric | Type | What |
|---|---|---|
| `loom_db_pool_size` | gauge | Connections the pool keeps open. |
| `loom_db_pool_checked_out` | gauge | Connections in use. |
| `loom_db_pool_overflow` | gauge | Overflow connections in use beyond the size. |
| `loom_db_pool_checkout_seconds` | histogram | Checkout duration, pre-ping included. |
| `loom_db_pool_checkout_timeouts_total` | counter | Checkouts that ran out of time. |

Gauges are read when the registry is scraped; the timing exists only when
metrics are enabled, so a disabled exporter costs nothing per checkout. The
API publishes on its metrics registry. The Celery worker does not publish pool
metrics yet: a prefork worker records them in each child process, which needs
Prometheus multiprocess mode to be scraped.

## `dynamodb` client

Every bound of the boto3 client is finite by default; botocore's own read
timeout (60 s) is longer than a typical serverless invocation.

```yaml
persistence:
  backend: dynamodb
  dynamodb:
    region: eu-west-1
    table: app
    pool:
      size: 32                 # HTTP connections and worker threads
    connect_timeout: 2         # seconds
    read_timeout: 5            # seconds
    max_attempts: 3            # total, the first attempt included
    retry_mode: standard       # standard | adaptive
```

Repository calls run on a thread pool as wide as `pool.size`, so
that many calls proceed in parallel in one process; the trace id and other
context variables stay visible inside the threads.

The section is `AwsClientConfig` plus `table`: the same keys and defaults
apply wherever loom calls AWS (the ETL's `storage.dynamodb`, the `secrets`
and `ssm` resolvers, the SNS notifier), and `profile` and `url` (an
endpoint override; absent, the AWS regional endpoint) are accepted too. Credentials never come from configuration; boto3's default
chain applies. See ADR 0006 (`docs/architecture/adr/0006-hexagonal-layers.md`).

## Health

`GET /health` aggregates the readiness of the configured backend; its shape, the
default authentication exclusion and the reserved-path rule are described in
[Bootstrap with YAML](../getting-started/rest.md#get-health). The route caches
the probe result for a short TTL and shares one in-flight probe between
concurrent requests, so a burst of anonymous calls costs the backend at most
one probe per TTL; a probe that exceeds its timeout reports the backend as not
ready. Keep `/health` off the public ingress: it is anonymous by design and
meant for the orchestrator.

### `dynamodb` permissions

The `dynamodb` readiness probe calls `DescribeTable`, so the application's
role needs `dynamodb:DescribeTable` on the table ARN in addition to the item
permissions (`GetItem`, `PutItem`, `UpdateItem`, `DeleteItem`) the
repositories use. Without it the probe logs the refusal and `/health` answers
`503 degraded` while the repositories keep working.

## `mongo`

`persistence.backend: mongo` binds every discovered model to a collection of
one database through pymongo's async client. Needs the `mongo` extra
(`loom-kernel[mongo]`, pymongo ≥ 4.9 for the async API); selecting the
backend without it fails at startup naming the extra.

```yaml
persistence:
  backend: mongo
  mongo:
    url: mongodb://localhost:27017/?replicaSet=rs0
    database: shop
    transactions: false          # true needs a replica set
    id: uuid4                    # or objectid
    collections:                 # optional, keyed by model class name
      Order: orders_v2
    pool:
      size: 50                   # optional; driver default when absent
      timeout: 2                 # seconds waiting for a free connection; optional
    server_selection_timeout: 5  # seconds; optional, driver default when absent
```

| Key | Default | Meaning |
|---|---|---|
| `url` | required | Connection string handed to `AsyncMongoClient`. |
| `database` | required | Database every collection is taken from. |
| `transactions` | `false` | `true` runs each unit of work as one client session and transaction; `false` is a no-op unit of work, every write autocommits. |
| `id` | `uuid4` | How keys are minted when the input carries none (see below). |
| `collections` | `{}` | Collection name per model **class name**; absent models use `__tablename__`. |
| `username`, `password` | unset | Credentials; win over those of the `url`. |
| `connect_timeout`, `read_timeout`, `server_selection_timeout` | unset | Seconds; forwarded to the driver only when set. |
| `pool.size`, `pool.timeout` | unset | Connections kept, and seconds a caller waits for one; forwarded only when set. |

The section rejects unknown keys, so a misspelt option is a `ConfigError`
at startup rather than a silently ignored setting, and startup logs
`mongo unit of work: transactions=<bool>` once.

The connection keys (`url`, `database`, `username`, `password`,
`connect_timeout`, `read_timeout`, `server_selection_timeout`, `pool`,
`server_api_version`) are `MongoConnection`'s,
the same in the ETL's `storage.mongo` and in a change-stream source
(`mongo.source`); `transactions`, `id` and `collections` belong to the
repository alone.

**`_id` mapping.** The model's primary key, whatever its name, is stored as
`_id` and mapped back on read: a model keyed by `slug` never sees `_id`, and
the output struct carries `slug`. A client-supplied key always wins; the id
policy is consulted only when the input carries none (field unset or `None`).

**Id policies.** `uuid4` (default) mints a UUID v4 and stores it as a string;
the model declares its key as `str` or `UUID`, and the output annotation
restores the type. `objectid` is an opt-in for collections already keyed by
BSON `ObjectId`: the model declares `str`, the store holds `ObjectId`, and a
lookup with a string that is not a valid `ObjectId` matches nothing. A model
declaring `autoincrement=True` is refused at startup naming the model:
MongoDB has no sequences.

**Values.** The client is built `tz_aware`, and datetimes are stored as
BSON keeps them: UTC at millisecond precision (a naive value is taken as
UTC). A `DateTime(tz=False)` column reads back naive UTC; the default
`DateTime(tz=True)` reads back aware UTC. `date`, `time`, `Decimal` and
non-key `UUID` values are stored as strings and restored by the model's
annotation on read, and filter values receive the same conversion, so the
output of `create` equals a later read and a filter compares against what
the document holds. Two limits follow from string storage: range operators
(`GT`, `GTE`, `LT`, `LTE`) and sorts on a `Decimal` column are
`UnsupportedQuery` (string order is not numeric order), while `date`
ranges compare ISO strings, which order correctly. `LIKE` / `ILIKE`
patterns collapse runs of `%` and accept at most eight wildcards; a longer
pattern is `UnsupportedQuery`.

**Duplicates.** A duplicate key on `create` or `create_many` is `Conflict`
(`409`). `create_many` is one ordered `insert_many`; on a duplicate the
documents written before it are deleted again so nothing persists, unless a
transaction is active, in which case its abort discards them. If that
compensating delete fails, the driver error surfaces instead of `Conflict`
(carrying a note that names it) and the store may hold the partial batch.

**Transactions.** `transactions: true` requires a replica set (a single-node
one is enough; `docker-compose.local.yaml` ships one as `mongo`). Every
repository call inside a use case then runs in the session of the enclosing
unit of work, and a failing second write leaves nothing behind. With
`transactions: false` the unit of work is a no-op like `dynamodb`'s.

**Readiness.** `GET /health` runs `admin.command("ping")`; any failure is
logged and reported as not ready.

**Indexes.** Keyset pagination sorts by the requested fields and appends
`_id` ascending as the tie-breaker, so a list sorted by `created_at` walks a
compound index `(created_at, _id)`; without one MongoDB sorts in memory and
refuses once the sort exceeds its memory limit. Create the index yourself: loom
does not manage collection indexes. A sort that already names the primary key
gets no tie-breaker: it is walked in the requested direction (`id DESC` stays
descending).

## Portable models

The same `ColumnField` model serves `sqlalchemy`, `mongo` and `dynamodb`
today, and the ClickHouse backend that arrives with the next backends of this
feature, when its key is minted by loom and its timestamps by the backend:

```python
from datetime import datetime

from loom.core.model import BaseModel, ColumnField, ServerDefault


class Order(BaseModel):
    __tablename__ = "orders"

    id: str = ColumnField(primary_key=True, server_default=ServerDefault.UUID4)
    customer: str = ColumnField(length=64)
    amount: int = ColumnField()
    created_at: datetime = ColumnField(server_default=ServerDefault.NOW)
```

Switching the store is the backend name plus its section; the model, the use
cases and the routes do not change:

```yaml
persistence:
  backend: mongo            # was: sqlalchemy
  mongo:
    url: mongodb://localhost:27017/?replicaSet=rs0
    database: shop
```

Four limits decide what does not travel:

- **Autoincrement is SQL-only.** `autoincrement=True` is refused at startup
  on `mongo` (and on the ClickHouse backend when it arrives); `dynamodb`
  still accepts it today, with a deprecation planned. Use
  `ServerDefault.UUID4`.
- **Relations are SQL-only.** `Relation` / `Projection` fields and the
  `EXISTS` / `NOT_EXISTS` filter operators compile on `sqlalchemy` only;
  elsewhere they are `UnsupportedQuery`. A dotted filter path
  (`category.name`) is `UnsupportedQuery` outside `sqlalchemy`, and on
  `sqlalchemy` it compiles without a join, so each root row comes back once
  per matching related row, related to it or not (pinned by
  `test_a_filter_on_a_related_column_selects_each_matching_row_once` until
  relation filtering lands). REST routes cannot declare one.
- **Not every backend has every capability.** `dynamodb` today mounts no
  `list` / `count` (listing by a declared index arrives with the next
  backends of this feature); the ClickHouse backend, when it arrives, offers
  no `update` / `delete`. Routes for a capability the backend lacks are not
  mounted, and an interface whose `include` names one explicitly fails at
  boot naming model, operation and backend.
- **A `time` with a UTC offset does not reach a SQL `Time` column.** On
  `sqlalchemy` a `date` field is inferred as a `Date` column and a `time`
  field as a `Time` column without time zone, which holds no offset; the
  repository refuses a `time` with an offset written to it
  (`boundary_validation`, REST `422`) or compared with it
  (`query_validation`, REST `400`). `mongo` stores `time` as ISO 8601 text
  and keeps the offset. Only two SQLAlchemy columns keep the written offset:
  - a `Time` column with time zone,
    `ColumnField(ColumnType("Time", kwargs={"timezone": True}))`, on Postgres
    only (`timetz`): on any other dialect startup refuses the column with a
    `ConfigError` naming `table.column`. It takes only a `time` with an
    offset; a naive one is refused like an aware one for a plain `Time`
    column;
  - a JSON column, `ColumnField(JSON)`, which stores the ISO text.

  On Postgres (`timestamptz`) and `mongo`, a `datetime` field
  (`DateTime(tz=True)`) keeps the instant, read back in UTC, not the written
  offset.
- **A `UUID` field is a UUID column on `sqlalchemy`.** A `uuid.UUID` field
  without a declared type is inferred as a `Uuid` column: `uuid` on
  Postgres, `CHAR(32)` holding the hex form on a dialect without a native
  UUID type, such as SQLite. `Postgres.UUID` stays the Postgres-only choice.
  `mongo` stores a UUID as its text form. On every backend the field is a
  scalar column, so REST `filterable`, `sortable` and `ALL` take it.

A `Decimal` column declares its column type. `sqlalchemy` refuses a model
with an undeclared one when it compiles it, naming the field: Postgres rounds
a value to a `NUMERIC` column's scale and refuses one past its precision, so
loom does not guess them. Declare the precision and scale, or `Numeric()` for
an unbounded `NUMERIC`:

```python
from decimal import Decimal
from typing import Annotated

from loom.core.model import BaseModel, ColumnField, Numeric


class Measurement(BaseModel):
    __tablename__ = "measurements"

    value: Annotated[Decimal, Numeric(precision=12, scale=2)]
    exact: Annotated[Decimal, Numeric()]
    id: int = ColumnField(primary_key=True)
```

## Upgrade notes

- Every engine's connection follows one standard
  (ADR 0006 (`docs/architecture/adr/0006-hexagonal-layers.md`)):
  `url`, `username`/`password`, `connect_timeout` and `read_timeout` in
  seconds, `max_attempts`, and the pool under `pool` (`pool.size`,
  `pool.timeout`). The old keys stop startup naming the new one:

  | Section | Old key | New key |
  |---|---|---|
  | `persistence.dynamodb`, `storage.dynamodb` | `endpoint_url` | `url` |
  | `persistence.dynamodb`, `storage.dynamodb` | `max_pool_connections` | `pool.size` |
  | `persistence.mongo`, `storage.mongo`, `mongo.source(s)` | `uri` | `url` |
  | `persistence.mongo`, `storage.mongo`, `mongo.source(s)` | `max_pool_size` | `pool.size` (above zero; the old `0` for "no limit" is gone) |
  | `persistence.mongo`, `storage.mongo`, `mongo.source(s)` | `server_selection_timeout_ms` | `server_selection_timeout`, in seconds |
  | `sql.connections.<name>`, `storage.clickhouse`, `database.<name>` of a sink | `send_receive_timeout` | `read_timeout` |
  | `sql.connections.<name>`, `storage.clickhouse`, `database.<name>` of a sink | `pool_size` | `pool.size` |

  The `LOOM__` overrides follow: `LOOM__PERSISTENCE__DYNAMODB__POOL__SIZE`,
  `LOOM__STORAGE__DYNAMODB__POOL__SIZE`, `LOOM__SQL__CONNECTIONS__<NAME>__POOL__SIZE`
  and `LOOM__SQL__CONNECTIONS__<NAME>__READ_TIMEOUT`. The Mongo connection also
  takes `username`, `password`, `connect_timeout`, `read_timeout` and
  `pool.timeout`.
- `database` takes `connect_timeout` and `read_timeout` (seconds) for a
  `postgresql+asyncpg` URL, passed to asyncpg as `timeout` and
  `command_timeout`. With another driver, set the driver's own option in
  `connect_args`; setting a timeout in both places stops startup. The
  `DatabaseConfig` repr now hides the credentials of `url`.
- The client islands moved: `loom.core.connections.clients.<engine>` is now
  `loom.core.connections.<engine>.client`, and the loaders `aws_island`,
  `mongo_island` and `clickhouse_island` are imported from
  `loom.core.connections.<engine>`. A missing extra now raises loom's
  `MissingExtraError` (an `ImportError`) naming the extra, from the ETL
  sources and from the Mongo change-stream source alike.
- Each engine's connection has one type, shared by every context
  (ADR 0006 (`docs/architecture/adr/0006-hexagonal-layers.md`)):
  `AwsClientConfig`, `MongoConnection`, `ClickHouseConnection` and
  `DatabaseConfig`, in `loom.core.connections`. `loom.etl.storage.ClickHouseConfig`
  and `MongoConfig` and the ETL's `DynamoDbConfig` are gone; so are
  `loom.core.locator.DatabaseConfig` and `SchemaConfig`.
- `storage.dynamodb.enabled` is removed: declaring `storage.dynamodb` enables
  DynamoDB sources. `storage.mongo` and `storage.clickhouse` are disabled by
  omitting them; an empty `uri`/`url` fails. The ETL DynamoDB client now fails
  within 2 s to connect, 5 s to read and three attempts, as the repository's
  does (`storage.dynamodb.read_timeout` and the other bounds are overridable
  per environment).
- A ClickHouse sink's `database.<name>` is a ClickHouse connection:
  `http_url` is now `url`, and `username`, `password` and timeouts are
  accepted. The ETL ClickHouse sources, targets and client steps accept
  `connection=ClickHouseConnection(...)`; `url=` stays as a shortcut.
- `database.schema` refuses an unknown key.
- The `secrets`/`ssm` resolvers and the SNS notifier build their boto3
  client with the same bounds (2 s connect, 5 s read, three attempts).
- A `sql.connections.<name>.url` whose query sets an option the SQL service
  decides (`autogenerate_session_id`, `session_id`, `query_retries`, `role`,
  `readonly`, `username`, `user`, `password`) fails at startup: the driver
  would let it replace the service's choice.
- `database` pool keys moved under `database.pool` (`pool_size` →
  `pool.size`, `pool_pre_ping` → `pool.pre_ping`, ...), the same in the API,
  the worker and streaming sinks; the old keys stop startup naming the new
  one. `SessionManager.from_config` and `loom.streaming.SqlAlchemyDatabaseConfig`
  are replaced by `build_session_manager(DatabaseConfig, profile)` and
  `loom.core.persistence.DatabaseConfig`.
- Pool defaults now come from the entry point. The worker opens at most two
  connections per process (it allowed thirty); the API keeps five plus five
  and recycles connections every 1800 s. Declare `database.pool` to keep
  other values.
- `PersistenceBackend.build` takes a third argument, the `BackendRuntime`.
- A worker with a malformed `database` section now fails at startup instead
  of running without its database.

- Repository outputs are now built through `to_struct`. A struct value held
  in a loosely annotated field (`Any`, `list[dict[str, Any]]`) arrives as a
  dict with encoded (camelCase) keys; a related value is re-typed to the
  annotated struct.
- `UseCaseFactory` no longer injects constructor parameters that carry a
  default value, nor plain generic containers (`tuple[str, ...]`,
  `dict[str, Any]`); the default applies. Capability keys (`Listable[M]`)
  and Protocol-typed parameters are still injected and verified at boot.
- Cursor tokens issued before this version are rejected with
  `400 unsupported_query`: clients restart from the first page.
- An explicit `exclude_paths` list must include `/health`, or the probe
  answers `401`.
