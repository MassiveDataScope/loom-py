# ADR 0008: Named stores, one unit of work per store, ordered commits

- Status: Accepted
- Branch: `feat/stores`
- Applies to: the release after the composition root (ADR 0007)
- Specification: `.specify/specs/004-hexagonal-composition/` (US6 and US7)

## Context

An application can now declare several persistence backends. Today only one
unit of work exists per process, and repositories reach their session through
one context variable per backend. That causes two real faults:

- **A repository of another store writes into the wrong session.** Inside a
  SQLAlchemy unit of work, every SQLAlchemy repository reuses
  `get_active_session()` (`src/loom/core/repository/sqlalchemy/transactional.py`),
  whatever engine it belongs to. An `audit` repository inside a `default`
  unit of work writes into `default`'s session.
- **A write to another store commits on its own, outside the unit of work.**
  Outside an active unit of work a repository opens its own session and
  commits per call (`src/loom/core/repository/sqlalchemy/repository.py`).
  `RuntimeExecutor._factory_to_own` joins any active unit of work regardless
  of its store. A use case that fails after such a write leaves it committed,
  and nothing reports it.

We weighed this against how other frameworks handle several stores in one
operation. Most handle one transaction per store and leave the order to the
developer: Django (`transaction.atomic(using=)`), Prisma, TypeORM, MikroORM,
Micronaut Data and EF Core across connections. Only SQLAlchemy offers real
two-phase commit (`twophase=True`). Spring Data deprecated
`ChainedTransactionManager` (ordered best-effort commits) because applications
used it without being able to recover from the partial state it leaves.
Quarkus refuses to enlist two non-XA datasources in one transaction. .NET made
implicit escalation to distributed transactions opt-in and Windows-only. Axon
documents that its unit-of-work commit is not atomic.

We also chose not to build the most complete design. Every concept below is
there because it fixes a fault we have today or keeps a later delivery
possible without changing the use-case DSL.

## Decision

### Stores

Each store answers what it supports through `StoreCapabilities`: whether it
serves models, whether its unit of work can roll back (a future `Joined`
store), and whether its repositories page with cursors. Its backend
describes it on first read, so loading the catalog still resolves no secret.
Callers ask the capability, never the backend's name.

- `stores.<name>` declares each store with its backend (`sqlalchemy`,
  `mongo`, `dynamodb`) and connection.
- A model declares `__store__`; without it, the model belongs to `default`.
- The retired keys `database`, `database.<name>`, `persistence.backend`,
  `persistence.mongo`, `persistence.dynamodb` and `__database__` fail with a
  migration message. `persistence.cursors` stays.
- Each store has its own engine or client, metadata, repositories, startup
  checks, readiness and pool metrics. A foreign key or relation across stores
  fails at startup.

### Connection-only stores

- `stores.<name>` with `backend: clickhouse` declares a connection-only
  store: the ClickHouse connection (`url`, `username`, `password`, ...) and
  nothing else. It is not a persistence backend.
- It serves no model and no repository: a model whose `__store__` names it
  fails at startup. It is never transactional, so it is never a primary store
  and can never be `Joined`; `stores.default` cannot be one, and the catalog
  refuses it when it loads.
- Streaming and ETL sinks (`store: <name>`) and, later, post-commit and
  outbox writers take their connection from it. A ClickHouse sink requires
  `backend: clickhouse` and a SQLAlchemy sink `backend: sqlalchemy`; a
  mismatch fails when the flow compiles.

### One unit of work per store, owned by the invocation

- An invocation scope owns a map from store key to unit of work, for every
  HTTP request, job or message. It replaces the process-wide unit-of-work
  context variable and the per-backend session context variables.
- A store's unit of work opens on its first use in the invocation. Nested use
  cases join the invocation scope, not a single unit of work.
- Using the scope from a task other than the one that owns it is an error.
- **The primary store** of a use case is the `__store__` of its main model. A
  plain repository reads from any store and writes only to the primary store.
- **`Joined[...]`** on an injected repository is the only new public concept.
  It lets that repository write to its store, which joins the invocation and
  commits with it. Writing through a plain repository to a store other than
  the primary raises `UndeclaredStoreWrite` before the driver is called.
- A store that cannot roll back (DynamoDB, Mongo with `transactions: false`)
  cannot be `Joined`: composition fails at startup. Its writes go through the
  existing post-commit channel or stay in a use case whose primary store it is.

### Reading a store's session

- Repositories, `@transactional`, the elevation sink and the Mongo
  `session_provider` find their store's session through one checkpoint of
  the invocation scope. It refuses a task other than the one that owns the
  invocation, and a session another session manager or client opened. A
  store that is not enlisted reads with a session per call.
- `SessionManager.active_session` is the only public way to read a store's
  session. Repositories reach the same session through
  `SessionManager.joined_session(write=...)`, which adds their write intent
  for the checkpoint; it is not meant for raw SQL. Mongo has the same property on `MongoSessions`, the object the
  backend hands its repositories as `session_provider`. Both are resolved by
  dependency injection, so two stores of the same backend never return each
  other's session, and both keep their exact session type.
- The property returns `None` outside an invocation or when the store is not
  enlisted. Its value depends on the context it is read from, like
  `request.user`, and it raises `ScopeOwnershipError` from a `gather` child,
  a thread or a task that outlives its use case.
- No facade reads the session of every backend: it would return `object` and
  lose the typing. `in_atomic_transaction()` stays, because it answers
  another question for third parties and the ETL.
- Raw SQL that must run in the invocation's transaction injects the store's
  `SessionManager` and uses its `active_session`. `session_manager.session()`
  opens a session outside the invocation, which commits on its own.

### One coordinator

- The executor does not commit. It hands the scope to a single coordinator,
  which is internal and replaceable:
  1. flush every participant;
  2. commit in first-write order;
  3. stop at the first failure and roll back the participants not yet
     committed.
- Participants close in reverse enlistment order. A unit of work that does
  not declare the explicit `phased` marker is driven through
  `__aenter__`/`__aexit__`, whatever other methods it has: it commits and
  closes in one call, so it commits at that closing position, after the
  stores loom drives phase by phase, never in first-write order.
- It is named for what it is, ordered commits. Nothing in loom calls it a
  transaction across stores, and no API promises atomicity across stores.
- The result is a typed outcome per store: committed, rolled back, uncertain
  (with its cause) or non-transactional. An invocation with any store that is
  not committed fails, even when the primary store committed. The outcome is
  logged with the store keys, for reconciliation.
- Post-commit actions (cache invalidation, job dispatch, writes to ClickHouse
  or to an outbox) belong to the store that owns them and run only if that
  store committed.

### Row-level security

Every session the scope opens carries the invocation's identity and the
scoped settings of its own store. That includes a read-only session on a
store other than the primary, and a store that joins later through `Joined`
or a nested use case.

- Elevation flags are set and cleared on the session of the store they apply
  to.
- The pool reset, the privileged-role refusal and the guard checks run per
  store.
- A scoped table reachable through a store without settings fails closed, as
  it does today.
- The executor binds the identity it is given for the whole execution, its
  post-commit drain included, so `Caller()` and every session read the same
  identity.
- A nested execution that joins the invocation scope as another identity is
  refused (`NestedIdentityMismatch`), and so is any nested execution while an
  elevation is active (`NestedElevation`). Elevations are never inherited.
- An inline job dispatched without a transport runs as `ANONYMOUS`.
- The identity carried by a job envelope is not signed: whoever can publish to
  the broker or queue can name any subject. Signing it is deferred.

### Identifiers

loom does not require client-generated identifiers. The guide recommends them
(UUIDv7 or ULID) for a model that another store references. That way the
identifier exists before any I/O and a retry does not create a duplicate.
Serial identifiers keep working. A deploy check may warn about a
cross-store reference to a serial key; it never fails, and it can be silenced
as described in ADR 0005.

## Deferred, until a use case asks for it

- `Autonomous[...]`: a repository that commits on its own, such as an audit
  row that must persist even if the primary write fails.
- `Ref[...]` dependency edges between models, for a declared commit order
  instead of first-write order.
- A buffered, transactional DynamoDB unit of work through `TransactWriteItems`
  (100 items, 4 MB, two write units per item).
- Retry policies per backend beyond what each driver already does: Mongo
  `UnknownTransactionCommitResult` and `TransientTransactionError`, DynamoDB
  `TransactionCanceledException`.
- The cross-store orchestrator (merging stores on the same physical database,
  two-phase commit): a separate specification. The coordinator's two seams,
  enlisting a store and finishing the invocation, the separable
  flush/commit/rollback phases and the per-store physical identity are what
  it plugs into.

Rejected:

- Selecting a transaction mode per annotation.
- Implicit escalation to distributed transactions.
- Configurable coordinator modes.
- Mandatory identifier types.

## Testing requirements

A delivery is not done until all of these pass:

- **Contract suites:** the repository and unit-of-work contract suites run per
  store and per backend, including Mongo on the replica set from
  `docker-compose.local.yaml`.
- **Two Postgres stores with row-level security:**
  - no tenant sees another tenant's rows through either store;
  - concurrent invocations with different identities never share settings or
    sessions;
  - read-only sessions on the non-primary store apply the same settings;
  - a nested use case on another store keeps the caller's identity;
  - elevation flags never leak to the other store.
- **Fault injection in the coordinator:** a failure or a cancellation at every
  position (before the flush, during the flush, before and during each
  commit), each asserting the per-store outcome and what was persisted.
- **No leaks:** after a run of invocations, every pool is empty and every
  context variable is reset, including after errors and cancellations.
- **Branch coverage:** 100 % on the scope and the coordinator.
- **Characterisation tests:** the existing single-store behaviour stays
  unchanged, and the unit-of-work matrix from T044 still passes.

## Consequences

- An application with one store behaves as before, apart from the renamed
  configuration keys.
- Cross-store writes are visible in the use case's constructor, and a
  forgotten `Joined` fails in the first test that writes.
- The fault where a repository of another store wrote into the wrong session
  is gone.
- Partial failures across stores remain possible during the commit phase. They
  are reported per store, and loom never claims to have rolled them back.
- A nested `execute(identity=Y)` inside an execution of `X` used to give `Y`
  to `Caller()` and `X` to row-level security. Both now see `Y`, and joining
  `X`'s unit of work as `Y` is refused.

## Sources

- Django multiple databases and transactions:
  https://docs.djangoproject.com/en/5.2/topics/db/multi-db/,
  https://docs.djangoproject.com/en/5.2/topics/db/transactions/
- SQLAlchemy session transactions and two-phase commit:
  https://docs.sqlalchemy.org/en/20/orm/session_transaction.html
- Spring Data `ChainedTransactionManager`:
  https://docs.spring.io/spring-data/commons/docs/current/api/org/springframework/data/transaction/ChainedTransactionManager.html
- Quarkus datasources and XA: https://quarkus.io/guides/datasource
- .NET implicit distributed transactions:
  https://learn.microsoft.com/en-us/dotnet/api/system.transactions.transactionmanager.implicitdistributedtransactions
- Axon unit of work:
  https://docs.axoniq.io/axon-framework-reference/4.13/messaging-concepts/unit-of-work/
- Transactional outbox and saga:
  https://microservices.io/patterns/data/transactional-outbox.html,
  https://microservices.io/patterns/data/saga.html
- DynamoDB transactions:
  https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html
- MongoDB transactions: https://www.mongodb.com/docs/manual/core/transactions/
- ClickHouse transactions: https://clickhouse.com/docs/guides/developer/transactional
