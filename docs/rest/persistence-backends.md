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
`build(ctx, models) -> PersistenceWiring`. It reads its own configuration
section from the context it is given; the host knows nothing about that
section.

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
from loom.core.persistence import PersistenceWiring
from loom.core.repository import RepositoryBuildContext, build_repository_registration_module

from billing_ledger.client import LedgerClient
from billing_ledger.repository import RepositoryLedger


class _LedgerConfig(msgspec.Struct, kw_only=True):
    url: str
    namespace: str = "orders"


class LedgerBackend:
    name: ClassVar[str] = "ledger"

    def build(self, ctx: ConfigContext, models: Sequence[type[BaseModel]]) -> PersistenceWiring:
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
(`loom.core.repository.abc.cursor`): the base64url form of a msgspec record
carrying the issuing backend's name, the sort key values of the last row on the
page and the primary key as tie-breaker. Backends issue and consume it through
`encode_cursor(backend, keys, tie_breaker)` and
`decode_cursor(token, backend, model, key_count=len(sort))`, which rejects a
token whose key count differs from the sort; each repository class names
itself with a `backend_name` class variable. A composite primary key is carried
whole: the tie-breaker is the tuple of its values in key order, and
`decode_cursor(..., id_count=2)` rejects a token carrying another number of
key values.

```python
from loom.core.repository.abc.cursor import decode_cursor, encode_cursor

token = encode_cursor("sqlalchemy", keys=[created_at, 1042], tie_breaker=1042)
cursor = decode_cursor(token, "sqlalchemy", "billing.Invoice", key_count=2)
cursor.keys, cursor.tie_breaker   # (created_at, 1042), 1042
```

A token that does not decode, or that another backend issued, is
`UnsupportedQuery` — `400 unsupported_query` on the wire. Tokens issued by the
SQLAlchemy backend before this contract share that fate: clients must treat
them as opaque and restart from the first page.

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
    uri: mongodb://localhost:27017/?replicaSet=rs0
    database: shop
    transactions: false          # true needs a replica set
    id: uuid4                    # or objectid
    collections:                 # optional, keyed by model class name
      Order: orders_v2
    max_pool_size: 50            # optional; driver default when absent
    server_selection_timeout_ms: 5000   # optional; driver default when absent
```

| Key | Default | Meaning |
|---|---|---|
| `uri` | required | Connection string handed to `AsyncMongoClient`. |
| `database` | required | Database every collection is taken from. |
| `transactions` | `false` | `true` runs each unit of work as one client session and transaction; `false` is a no-op unit of work, every write autocommits. |
| `id` | `uuid4` | How keys are minted when the input carries none (see below). |
| `collections` | `{}` | Collection name per model **class name**; absent models use `__tablename__`. |
| `max_pool_size`, `server_selection_timeout_ms` | unset | Forwarded to the driver only when set. |

The section rejects unknown keys, so a misspelt option is a `ConfigError`
at startup rather than a silently ignored setting, and startup logs
`mongo unit of work: transactions=<bool>` once.

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
    uri: mongodb://localhost:27017/?replicaSet=rs0
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
  field as a `Time` column, which holds no offset; the repository refuses a
  `time` with an offset written to it (`boundary_validation`, REST `422`) or
  compared with it (`query_validation`, REST `400`). `mongo` stores `time` as
  ISO 8601 text and keeps the offset. A `datetime` field keeps the offset on
  both.

## Upgrade notes

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
