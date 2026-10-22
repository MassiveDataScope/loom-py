# ADR 0006: Hexagonal layers and one connection type per engine

- Status: Accepted
- Branch: `refactor/engine-connections`
- Applies to: the release after loom-kernel 9.0.0

## Context

loom is driven by several transports (the REST API, the Celery worker,
streaming, ETL, Prefect flows) and drives several engines (PostgreSQL through
SQLAlchemy, DynamoDB, MongoDB, ClickHouse, AWS services). Two problems grew
from leaving the layers implicit:

- **The same engine was configured differently in each context.** Eleven
  types described four engines' connections. The ETL DynamoDB source had no
  timeouts (botocore waits 60 s for a read), ClickHouse credentials and
  timeouts existed only for the API's SQL service, `database.schema` had two
  definitions with overlapping keys, and the streaming sinks read their
  ClickHouse database as an unvalidated mapping.
- **Each context built its own clients.** ClickHouse clients were built in
  three places, Mongo clients in three, boto3 clients in four, so a bound, a
  credential or a pool size had to be changed in every one of them, and some
  imported the SDK inside a function, against the island rule of
  [Clean architecture rules](../clean-architecture.md).

The AWS Lambda work ahead (one artifact, several functions, each with its own
transport and its own engine) needs both fixed first: every Lambda must build
the same client, with the same bounds, from the same keys.

## Decision

### Layers

Dependencies point inwards: a layer imports only the layers listed above it.

| Layer | Packages | Rule |
|---|---|---|
| Domain and application | `loom.core.model`, `use_case`, `engine`, `command`, `projection`, `identity`, `authz` | no infrastructure, no SDK |
| Ports | `loom.core.persistence.abc`, `uow.abc`, `repository.abc`, `sql.abc`, `job.service`, `cache` | protocols and value types; no SDK |
| Configuration contracts | `loom.core.config`, `loom.core.connections.<engine>`, `loom.core.persistence.database`, `loom.core.persistence.profile` | validated data; no SDK |
| Outbound adapters (islands) | `loom.core.repository.{sqlalchemy,dynamodb,mongo}`, `loom.core.sql.clickhouse`, `loom.core.connections.<engine>.client`, `loom.prometheus` | implement the ports; the only modules importing an SDK |
| Composition root | `create_app` and the worker bootstrap | compose the application; the transport-neutral root is the next step |
| Inbound adapters | `loom.rest.fastapi`, `loom.celery`, the ETL and streaming runners, Prefect flows | translate a transport; build no resource of their own |

This table classifies what exists. Packages move only when work touches them.

### One connection type per engine

`loom.core.connections` declares one type per engine, and imports no driver.
Each engine has a package of its own, whose `__init__` only re-exports:

```text
loom/core/connections/
  _base.py            ConnectionSettings, ConnectionPool, merge_extra, redact_uri, reject_retired_keys
  <engine>/config.py  the connection type(s), the pool type, client_options()
  <engine>/loader.py  the <Engine>Island Protocol and its typed loader
  <engine>/client.py  the island: imports the SDK at its top, builds the client
  relational.py       names DatabaseConfig (defined in loom.core.persistence.database)
```

The relational type stays with the persistence layer and is not re-exported by
the package: a reader of a connection type (the config resolvers among them)
would otherwise load the persistence layer.

| Engine | Type | Contexts |
|---|---|---|
| Relational | `DatabaseConfig` (defined in `loom.core.persistence.database`, named by `loom.core.connections.relational`) | `database` for the API, the worker, streaming, migrations and the row-level-security tooling |
| AWS | `AwsClientConfig` | `persistence.dynamodb` (+ `table`), `storage.dynamodb`, the `secrets`/`ssm` resolvers, the SNS notifier |
| MongoDB | `MongoConnection` | `persistence.mongo` (+ repository keys), `storage.mongo`, `mongo.source(s)` (+ change-stream keys) |
| ClickHouse | `ClickHouseConnection` | `sql.connections.<name>` (+ roles and limits), `storage.clickhouse`, `database.<name>` of a streaming sink |

A context declares the engine's type, or a subclass adding only its own keys;
it never declares a connection key again. A subclass repeats
`frozen=True, kw_only=True` (msgspec applies `kw_only` per class, and type
checkers refuse a non-frozen subclass of a frozen class); it changes a default
by redeclaring the field. Secrets never show in a repr: `password` is masked
and the credentials of a URL are replaced by `***`.

### Connection standard

Wherever an engine has the concept, its type names it the same way and in the
same unit:

| Field | Meaning | Unit |
|---|---|---|
| `url` | address or DSN; for AWS the endpoint override (`None` is the AWS regional endpoint) | — |
| `username`, `password` | explicit credentials; win over those of the URL | — |
| `connect_timeout` | longest time to establish a connection | seconds |
| `read_timeout` | longest wait for one operation's response | seconds |
| `max_attempts` | total attempts, the first one included | — |
| `pool.size` | connections the client keeps | — |
| `pool.timeout` | longest wait for a free pooled connection | seconds |

No field ends in `_ms`: every duration is in seconds. A field only one engine
has keeps the engine's name at the top level (`region`, `profile`,
`server_selection_timeout`, `executor_threads`, `server_api_version`). The pool
is a `ConnectionPool` (`size`), which an engine extends (`AwsPool` defaults
`size` to 32, `MongoPool` adds `timeout`, the relational `PoolConfig` adds
overflow, recycle and pre-ping). `password` is masked and the credentials of
`url` are replaced by `***` in every repr, the relational one included.

Each engine type has one method, `client_options()`, that translates its
standard and engine fields into the driver's keyword names and units, leaving
out what is unset: `Session.client` and `botocore.Config` keywords for AWS,
`MongoClient` keywords in milliseconds for MongoDB, `get_client` keywords for
ClickHouse (`read_timeout` becomes `send_receive_timeout`), the asyncpg
`connect()` arguments for `DatabaseConfig` (`timeout` and `command_timeout`,
for a `postgresql+asyncpg` URL only; another driver keeps its own options in
`connect_args`). It returns plain data and imports no SDK; the SDK objects
(`botocore.Config`, `ServerApi`, the urllib3 pool manager) are built only in
the island, from what `client_options()` returned.

A key the standard retired (`endpoint_url`, `max_pool_connections`, `uri`,
`max_pool_size`, `server_selection_timeout_ms`, `send_receive_timeout`,
`pool_size`) stops startup with a `ConfigError` naming its replacement, in
every section of the engine.

### One client builder per engine

Each engine's client is built in one island, `loom.core.connections.<engine>.client`
(`aws`, `mongo`, `clickhouse`); the relational one is `build_session_manager`,
in the SQLAlchemy island. Core paths load an island with the typed loader of
the engine's contract package (`aws_island`, `mongo_island`,
`clickhouse_island` from `loom.core.connections.<engine>`, wrapping
`import_optional`); an island may import another island directly (the
DynamoDB repository and the SQL service's ClickHouse transport do). A context
calls the builder passing only options the connection does not decide
(`tz_aware` for the Mongo repository, `query_retries` for the SQL service); an
option naming a connection field, or one `client_options()` and the island
already produced, is refused. boto3 clients get a session of their own, as
boto3 advises for concurrent use.

Each loader declares the island's surface as a Protocol, with the island's own
return types (a `MongoClient`, an `AsyncMongoClient`; `Any` for boto3 and
clickhouse-connect, which ship no stubs). Each island assigns itself to that
Protocol under `TYPE_CHECKING`, so pyright and mypy both refuse an island that
drifts from its contract.

### Enforcement

- `tests/unit/test_connections_containment.py`: the connection types and the
  loaders load no SDK, reading configuration loads no persistence layer, and
  each SDK is imported at run time only by the modules listed for it (an
  import under `if TYPE_CHECKING:` loads nothing and is not counted).
- `tests/unit/core/connections/test_single_source.py`: each engine's identifying
  keys are declared by one type, and no module outside the
  `loom.core.connections.<engine>.client` islands builds a driver client.
- `tests/unit/core/connections/test_standard.py`: no connection or pool type
  has a retired field name or a `_ms` field, and every engine type defines
  `client_options()`.
- `tests/unit/core/connections/test_client_options.py` pins each translation,
  `test_islands.py` checks the real drivers accept it, and
  `test_retired_keys.py` checks every section names the replacement of a
  retired key.

## Consequences

- A bound, a credential or a proxy setting is changed once and reaches every
  context; the ETL DynamoDB sources now fail within 2 s to connect and 5 s to
  read, three attempts, as the repository does.
- `storage.dynamodb.enabled` is gone (declaring the section enables DynamoDB
  sources), an empty `storage.mongo.url` or `storage.clickhouse.url` no longer
  disables them (omit the section), a sink's `database.<name>.http_url` is now
  `url`, and `database.schema` refuses an unknown key.
- The connection standard renamed keys: `endpoint_url` → `url` and
  `max_pool_connections` → `pool.size` (AWS); `uri` → `url`, `max_pool_size` →
  `pool.size` and `server_selection_timeout_ms` → `server_selection_timeout`
  in seconds (MongoDB); `send_receive_timeout` → `read_timeout` and
  `pool_size` → `pool.size` (ClickHouse). The `LOOM__` allowlist follows.
- A caller that loads an island lets `MissingExtraError` propagate; only the
  configuration resolvers turn it into a `ConfigError`, since they run before
  any other error handling exists.
- The transport-neutral composition root, the Lambda adapters and a catalogue
  of named stores build on these types without adding new ones.
