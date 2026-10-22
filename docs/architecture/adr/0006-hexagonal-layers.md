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
| Outbound adapters (islands) | `loom.core.repository.{sqlalchemy,dynamodb,mongo}`, `loom.core.sql.clickhouse`, `loom.core.connections.clients.*`, `loom.prometheus` | implement the ports; the only modules importing an SDK |
| Composition root | `create_app` and the worker bootstrap | compose the application; the transport-neutral root is the next step |
| Inbound adapters | `loom.rest.fastapi`, `loom.celery`, the ETL and streaming runners, Prefect flows | translate a transport; build no resource of their own |

This table classifies what exists. Packages move only when work touches them.

### One connection type per engine

`loom.core.connections` declares one type per engine, and imports no driver.
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

### One client builder per engine

Each engine's client is built in one island of `loom.core.connections.clients`
(`aws`, `mongo`, `clickhouse`); the relational one is
`build_session_manager`, in the SQLAlchemy island. A context loads the island
with the typed loader of `loom.core.connections.clients` (`aws_island`,
`mongo_island`, `clickhouse_island`, wrapping `import_optional`), or imports it
directly when it is itself an island of the same SDK, and calls its builder, passing only options the
connection does not decide (`tz_aware` for the Mongo repository,
`query_retries` for the SQL service); an option naming a connection field is
refused. boto3 clients get a session of their own, as boto3 advises for
concurrent use.

### Enforcement

- `tests/unit/test_connections_containment.py`: the connection types load no
  SDK, reading configuration loads no persistence layer, and each SDK is
  imported only by the modules listed for it.
- `tests/unit/core/connections/test_single_source.py`: each engine's identifying
  keys are declared by one type, and no module outside
  `loom.core.connections.clients` builds a driver client.

## Consequences

- A bound, a credential or a proxy setting is changed once and reaches every
  context; the ETL DynamoDB sources now fail within 2 s to connect and 5 s to
  read, three attempts, as the repository does.
- `storage.dynamodb.enabled` is gone (declaring the section enables DynamoDB
  sources), an empty `storage.mongo.uri` or `storage.clickhouse.url` no longer
  disables them (omit the section), a sink's `database.<name>.http_url` is now
  `url`, and `database.schema` refuses an unknown key.
- The transport-neutral composition root, the Lambda adapters and a catalogue
  of named stores build on these types without adding new ones.
