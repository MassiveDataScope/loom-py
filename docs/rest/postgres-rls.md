# Postgres row-level security

Authorization in code answers *may they do this here?*. Row-level security in Postgres
makes the database enforce the same boundary on every row, so a query that forgets a
`WHERE` cannot cross it. loom offers two levels:

| Level | You write | loom does |
|---|---|---|
| [The provider](#the-provider) | the policies, the roles and the grants, by hand | sets transaction-local settings from a provider you inject |
| [Row-scoped tables](#row-scoped-tables) | a marker on the model and a configuration block | creates, protects, migrates and verifies the tables; you write no SQL and no `GRANT` |

Both are generic. A product that isolates tenants is one use; a product that isolates
regions, accounts or owners is another. loom names none of them.

```{contents}
:local:
:depth: 2
```

## The provider

A {class}`~loom.core.repository.sqlalchemy.session_manager.SessionManager` can begin
every transaction by setting transaction-local Postgres settings taken from a provider
you inject. A provider is a callable that returns the settings for the transaction about
to start, or `None` when there is no request context. This one comes from a product that
isolates tenants:

```python
from collections.abc import Mapping
from contextvars import ContextVar

from loom.core.repository.sqlalchemy import SessionManager

current_tenant: ContextVar[str | None] = ContextVar("current_tenant", default=None)
current_subject: ContextVar[str | None] = ContextVar("current_subject", default=None)


def request_settings() -> Mapping[str, str] | None:
    tenant = current_tenant.get()
    if tenant is None:
        return None
    settings = {"app.tenant_id": tenant}
    subject = current_subject.get()
    if subject is not None:
        settings["app.subject"] = subject
    return settings


sessions = SessionManager(
    "postgresql+asyncpg://app:secret@db/app",
    session_settings=request_settings,
)
```

Each outer transaction then starts with one round trip, shown here with literal values
although the executed statement carries only bound parameters:

```sql
SELECT set_config('app.tenant_id', 'acme', true), set_config('app.subject', 'u-1', true)
```

The `true` makes both settings local to the transaction. They vanish at `COMMIT` or
`ROLLBACK`, so a connection returns to the pool clean, and the same holds behind an
external pooler in transaction mode such as PgBouncer.

What the manager guarantees:

- Every key and value is bound as a parameter. The compiled SQL never contains a value.
- A key must look like `prefix.name`, with one or more dotted parts, which is what
  Postgres requires of a custom setting. Anything else raises `ValueError`; a value that
  is not a `str` raises `TypeError`.
- A provider that returns `None` or an empty mapping sets nothing. It never substitutes
  a default value.
- A provider that raises, or returns something invalid, fails the transaction before its
  first product statement reaches the database. The connection is invalidated, so the
  session refuses further statements until you roll it back; the next transaction calls
  the provider again.
- A savepoint (`begin_nested()`) does not call the provider again.
- Without a provider, the manager behaves exactly as it did before.
- A provider on a database other than Postgres is rejected at construction.
- {attr}`~loom.core.repository.sqlalchemy.session_manager.SessionManager.has_session_settings`
  tells whether a manager has a provider.

### The policy recipe

Write the policy to tolerate a missing setting and to treat it as no boundary.
`current_setting(name, true)` returns `NULL` when the setting was never set and `''`
after a transaction reset it, so `NULLIF` collapses both into `NULL`, and `NULL` matches
no row:

```sql
ALTER TABLE grants ENABLE ROW LEVEL SECURITY;
ALTER TABLE grants FORCE ROW LEVEL SECURITY;

CREATE POLICY grants_tenant ON grants
    USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''));
```

- `ENABLE` turns the policy on. `FORCE` applies it to the table owner too. Neither
  restrains a superuser or a role with `BYPASSRLS`, which is why the roles below matter.
- `USING` filters reads; `WITH CHECK` refuses writes that would land in another
  boundary's rows.
- A transaction without context (startup, a background job, a health probe) sees zero
  rows and gets no error. That is the fail-closed behaviour you want. A policy that
  treats a missing setting as a wildcard, for example with
  `OR current_setting('app.tenant_id', true) IS NULL`, would instead open every row to
  every transaction without context.

The same recipe works for any boundary you can express as a session value, for example
`subject = NULLIF(current_setting('app.subject', true), '')` on a table of personal data.
Never store `''` in a column a policy compares against; a `CHECK (subject <> '')` keeps
the empty string from matching an absent setting.

### The roles

Three database roles, two managers:

```sql
CREATE ROLE app_owner NOLOGIN NOBYPASSRLS;
CREATE ROLE app_migrator LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD '...' IN ROLE app_owner;
CREATE ROLE app LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD '...';
CREATE ROLE app_platform LOGIN NOSUPERUSER BYPASSRLS PASSWORD '...';

ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA public
    GRANT SELECT ON TABLES TO app_platform;
```

The application role gets **no default privileges**. Grant it table by table, in the same
migration step that enables and forces the policy, so a table created without that step
is one the application cannot read at all (`permission denied`) rather than one it reads
across every boundary. A policy-less table with an automatic `SELECT` is the one
combination that leaks.

```python
app_sessions = SessionManager(APP_URL, session_settings=request_settings)
platform_sessions = SessionManager(PLATFORM_URL)
```

- **Owner.** `app_owner` owns every table, view and function and has neither
  `BYPASSRLS` nor a login. Migrations connect as `app_migrator`, a member of
  `app_owner`, and start with `SET ROLE app_owner` so that everything they create
  belongs to the owner. Neither `app` nor `app_platform` may be a member of `app_owner`,
  and no relation may ever be owned by `app_platform`. Ownership matters beyond `FORCE`:
  Postgres evaluates a view's policies as the view's owner, so a view owned by a
  `BYPASSRLS` role would hand every boundary's rows to whoever can select from it.
  Create views over protected tables `WITH (security_invoker = true)` (Postgres 15 or
  later) and avoid `SECURITY DEFINER` functions that read them.
- **Application.** `app` owns nothing and cannot bypass policies. It can only ever see
  the rows its transaction's settings select, and only on tables it was granted
  explicitly together with their policy.
- **Platform.** Work across every boundary (platform tasks, support tooling) goes
  through a second manager with `app_platform` and **no provider**. loom does not
  distinguish the two; the separation is configuration you make explicit, and you should
  audit every use of the platform manager.
- Never reuse the application manager with a special boundary value to mean "everyone".
- Never set a session-scoped value (`SET`, or `set_config(..., false)`) on the
  application pool: it would survive the transaction and leak to the next client of the
  pooled connection.

### Limits of the provider

- The settings apply to transactions opened through the manager's `Session`. A raw
  `engine.connect()` does not run the provider, and neither does a `create_all` that runs
  at startup. With a non-owner application role, the schema must already exist, created
  by migrations running as `app_owner`; never let the platform role create it, or it
  becomes the owner.
- Postgres only. The mechanism relies on `set_config` and custom settings.
- The manager rejects `isolation_level="AUTOCOMMIT"` at construction, because autocommit
  would discard the settings right after they are set. A per-statement
  `execution_options(isolation_level="AUTOCOMMIT")` is outside what it can check; do
  not use one on a protected table.
- The values travel as statement parameters, so `echo=True` and the text of a
  `DBAPIError` include them. Pass `hide_parameters=True` to the engine when the boundary
  or subject identifiers are sensitive.
- On this level loom sets values and nothing else; the policies, roles and grants are
  yours. Row-scoped tables, below, are the level where loom creates and checks them.

## Row-scoped tables

The model is the single source of truth. A marker on the model says which tables are
scoped, by which scopes and how far each scope reaches. From that marker loom derives the
local schema, the generated migrations, the database guard and the verification. You
write no policy, no `GRANT` and no role DDL.

An application without marked models changes nothing: `create_all` at startup, SQLite or
Postgres, no bootstrap.

### Marking a model

Mix {class}`~loom.core.model.RowScoped` into the model and name a scope on each scoping
column with `ColumnField(scope=..., on=..., elevable=...)`:

```python
import datetime as dt

from loom.core.model import BaseModel, ColumnField, OnDelete, Privilege, RowScoped
from loom.core.model.types import BigInteger, DateTime, Integer, Text


class Account(BaseModel):
    __tablename__ = "accounts"
    __privileges__ = {"readers": frozenset({Privilege.SELECT})}
    id: int = ColumnField(BigInteger, primary_key=True)
    label: str = ColumnField(Text)


class Entry(BaseModel, RowScoped):
    __tablename__ = "entries"
    __unique__ = (("account_id", "reference"),)
    __indexes__ = (("account_id", "booked_on"),)
    account_id: int = ColumnField(
        BigInteger,
        primary_key=True,
        scope="account",
        foreign_key="accounts.id",
        on_delete=OnDelete.RESTRICT,
    )
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    clerk: str = ColumnField(Text, scope="clerk", on="write", elevable=True)
    reference: str = ColumnField(Text)
    booked_on: dt.datetime = ColumnField(DateTime())


class EntryLine(BaseModel, RowScoped):
    __tablename__ = "entry_lines"
    account_id: int = ColumnField(BigInteger, primary_key=True, scope="account")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    entry_id: int = ColumnField(Integer, foreign_key="entries.id", on_delete=OnDelete.CASCADE)
    text: str = ColumnField(Text)
```

`account_id` is the **boundary scope** of both scoped tables: it reaches reads and
writes, it is not elevable and it is never nullable. `clerk` is a write-only, elevable
scope: a clerk reads every entry of the account and modifies only their own, unless an
[elevation](authorization.md#elevating-a-write-scope) opens it. `Account` is a global
table, unscoped.

The three scope attributes:

| Attribute | Values | Default | Meaning |
|---|---|---|---|
| `scope` | an identifier, `^[a-z_][a-z0-9_]*$` | none | the scope name; loom derives the session keys `loom.scope.<scope>` and, when elevable, `loom.scope.<scope>.any` |
| `on` | `"read"`, `"write"`, `"both"` | `"both"` | which commands the scope filters |
| `elevable` | `True`, `False` | `False` | whether an elevation may open the scope; only with `on="write"` |

A table may declare several scopes, each on its own column, of any name and type. A
scoped table admits every row privilege by default; a read-only one narrows
`__scope_privileges__`:

```python
class Reading(BaseModel, RowScoped):
    __tablename__ = "readings"
    __scope_privileges__ = frozenset({Privilege.SELECT})
    account_id: int = ColumnField(BigInteger, primary_key=True, scope="account")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
```

A surrogate integer inside a composite primary key needs `autoincrement=True`: without
it SQLAlchemy creates no sequence for a composite key. A product that prefers its own
vocabulary aliases the marker in its own package, for example `TenantScoped = RowScoped`;
loom never sees the alias.

### Compile rules

{func}`~loom.core.model.scope_columns` and the SQLAlchemy compiler check every marked
model. Each rule fails with `ValueError` naming the rule, the model and the column, at
`compile_all`, before any database is touched.

| Rule | What it requires |
|---|---|
| C1 | A marked table has exactly one boundary scope (`on="both"`, not elevable). A composite boundary is out of scope |
| C2 | `elevable=True` only with `on="write"`, so no table is scoped only by an elevable scope |
| C3 | Scope names are identifiers, unique within the table |
| C4 | No `scope`, `on` or `elevable` on an unmarked model; no `on` or `elevable` without `scope` |
| C5 | Every primary key, `__unique__` entry and `unique=True` column of a scoped table contains the boundary column, without exception |
| C6 | A single-column FK between two scoped tables compiles to `(boundary, col) -> (boundary, ref)`. The referenced table declares the same boundary scope and a key that is exactly `(boundary, ref)`, its primary key or a `__unique__` entry, with no other column; `on_delete` is `RESTRICT`, `NO ACTION` or `CASCADE`, never `SET NULL` or `SET DEFAULT` |
| C7 | No FK from an unscoped table to a scoped table |
| C8 | The boundary column is not nullable. Other scope columns are the model's choice; a row with a `NULL` read scope is invisible to every non-bypass user |
| C9 | An FK from a scoped table to a global table, the boundary column included, uses `RESTRICT` or `NO ACTION`, and the global table's `__privileges__` give no group `UPDATE` or `DELETE` (`INSERT` is allowed) |

Each rule closes a leak that the policy alone cannot. Referential integrity ignores
row-level security, so an FK without the boundary would reference or cascade into another
boundary, and a `23503` would reveal that a row exists there. A unique key without the
boundary turns `23505` and `ON CONFLICT` into the same kind of oracle.

The rules need both ends of every FK. An FK of a scoped model whose target table is not
compiled with it raises `ValueError` at compile time; compile both models together so
C6, C7 and C9 apply.

### Keys, indexes and global privileges

Three class attributes declare what would otherwise be hand SQL in a revision:

| Attribute | Applies to | Effect |
|---|---|---|
| `__unique__` | any model | composite `UNIQUE` constraints; on a scoped table each contains the boundary (C5) |
| `__indexes__` | any model | non-unique indexes; boundary column first is the usual choice on a scoped table |
| `__privileges__` | unscoped models only | `{"readers" \| "writers": frozenset[Privilege]}`; plain `GRANT` statements to the schema groups, plus `USAGE` on serial sequences for `INSERT` |

{class}`~loom.core.model.Privilege` is a closed set: `SELECT`, `INSERT`, `UPDATE`,
`DELETE`. `TRUNCATE`, `REFERENCES` and `TRIGGER` cannot be expressed: `TRUNCATE` ignores
row-level security, `REFERENCES` lets a user build an FK oracle, and `TRIGGER` runs code
as the owner. `__privileges__` on a scoped model, a group other than `readers` or
`writers`, or a value outside `Privilege` raises `ValueError`.

### The policies

On Postgres every scoped table is born with row-level security enabled and forced, and
with exactly one PERMISSIVE policy `TO PUBLIC` per command it admits:

| Command | Policy | Created when |
|---|---|---|
| `SELECT` | `loom_select USING (R)` | always |
| `INSERT` | `loom_insert WITH CHECK (W)` | `INSERT` admitted |
| `UPDATE` | `loom_update USING (W) WITH CHECK (W)` | `UPDATE` admitted |
| `DELETE` | `loom_delete USING (W)` | `DELETE` admitted |

`R` is the AND of the terms of the scopes that reach reads, `W` the AND of those that
reach writes. Each term is an equality:

```sql
account_id = NULLIF(current_setting('loom.scope.account', true), '')::bigint
```

The only OR anywhere is the elevation of an elevable scope:

```sql
(clerk = NULLIF(current_setting('loom.scope.clerk', true), '')::text
 OR current_setting('loom.scope.clerk.any', true) = 'on')
```

Separate policies per scope would combine with OR and let a write land in another
boundary; one policy per command with AND cannot. No policy names a role: the table
privilege is the barrier, and only the schema groups and the bypass users hold one. A
non-empty session value that does not convert to the column type fails the query, closed.

The cast uses the column's base type without its modifier: a `varchar(8)` column is
compared with `::varchar`, so a longer session value never matches a truncated one.
Equal values must be identical, so `protect_scoped_table` rejects with `22023` a scope
column of type `char(n)`, which pads, or one with a nondeterministic collation.

### Session values

Every declared scope needs an explicit source under `database.schema.scopes`:

| Source | Value |
|---|---|
| `identity.subject` | the caller's subject |
| `identity.<attr>` | the caller's verified attribute of that name |
| `request.<name>` | a callable the product registered with `register_scope_source(name, callable)` before startup |

A scope without an entry, a malformed source or an unregistered `request.<name>` raises
`ConfigError` at startup; registering a name twice raises `ValueError`. Register the
product's sources before startup:

```python
from loom.core.repository.sqlalchemy.rls import register_scope_source

register_scope_source("desk", current_desk)
```

The rest is wired by loom. With `database.schema.mode: external` and a Postgres URL, the
standard SQLAlchemy backend:

- installs the scoped session-settings provider on the application's session manager, and
  {func}`~loom.core.repository.sqlalchemy.rls.install_pool_reset`, which runs `RESET ALL`
  when a connection returns to the pool;
- binds the provider at startup to the compiled scoped tables and validates the bindings
  of `database.schema.scopes` against the scopes they declare;
- validates the registered product's `elevations` with
  {func}`~loom.core.repository.sqlalchemy.rls.validate_elevations`;
- registers {class}`~loom.core.repository.sqlalchemy.rls.SQLAlchemyElevationSink` in the
  container as the `ElevationSink`, which the kernel hands to the executor.

A kernel built by hand registers an `ElevationSink` through one of its `modules`. In
`create_all` mode, or on another dialect, the backend installs none of this.

The provider never returns `None`. In every outer transaction it emits every declared
key: the value or `''` for each scope, `'on'` or `''` for each elevation flag. A residue on
a pooled connection is therefore always overwritten, and the pool reset clears it when
the connection returns.

{func}`~loom.core.repository.sqlalchemy.rls.rls_session_settings` builds the same
provider for programmatic use, for a session manager a script builds itself:

```python
from loom.core.locator import load_application
from loom.core.repository.sqlalchemy import SessionManager
from loom.core.repository.sqlalchemy.rls import install_pool_reset, rls_session_settings

application = load_application()
sessions = SessionManager(APP_URL, session_settings=rls_session_settings(application))
install_pool_reset(sessions.engine)
```

`rls_session_settings(application, product=None)` validates the bindings when it is
called. A product provider passed as `product=` may add its own keys; a key under
`loom.scope.`, in any letter case, raises `ValueError`. The automatic wiring merges no
product provider.

### Configuration

```yaml
database:
  url: ${LOOM_DATABASE_URL}
  schema:
    mode: external
    name: ledger
    roles: {owner: ledger_owner, migrator: ledger_migrator}
    database_users:
      ledger_rw: {login: true, access: write}
      ledger_ro: {login: true, access: read}
      ledger_ops: {login: true, access: bypass}
    scopes:
      account: identity.account
      clerk: identity.subject
```

| Key | Default | Meaning |
|---|---|---|
| `mode` | `create_all` | `external` never creates tables; at startup it wires the session values, refuses scoped models on another dialect, a superuser or bypass connection, and a missing table or a scoped table without forced row-level security |
| `allow_unprotected_dialect` | `false` | see [Other dialects](#other-dialects) |
| `name` | none | the application schema; required when any model is `RowScoped` |
| `roles` | none | `owner` and `migrator` |
| `database_users` | none | the login users and their `access` |
| `scopes` | none | one source per declared scope, validated at startup in `external` mode |

{func}`~loom.core.locator.load_application` reads this file (the path given, or
`LOOM_CONFIG`), adds `app.code_path` (default `src`, relative to the configuration file) to `sys.path` as the server does,
discovers and compiles the models into a `MetaData` owned by the returned
{class}`~loom.core.locator.Application` and builds the
{class}`~loom.core.repository.sqlalchemy.rls.BootstrapConfig`. A missing key raises
`ConfigError` naming it; no name has a default. Two applications in one process share no
table, listener or registry.

With scoped models on Postgres, `mode: create_all` fails at startup: the application,
which carries the scope provider, never creates schema. Create it with the migrator.

In `mode: external`, startup fails with `ConfigError` when scoped models meet a dialect
other than Postgres without `allow_unprotected_dialect`, and when the application
connects as a superuser or a role with `BYPASSRLS`, which row-level security does not
restrain. Use the URL of a `read` or `write` user.

### Names

Every name Postgres sees passes one validator,
`loom.core.backend.scoped_ddl.sql_identifier`, and `schema_identifier` for the schema:

- lowercase letters, digits and `_`, starting with a letter or `_`;
- at most 63 characters, so Postgres never truncates it, and at most 47 for the schema,
  so every guard object name derived from it fits;
- not a reserved word, not a special role name (`public`, `current_user`,
  `session_user`, `current_role`, `none`), and no `pg_` prefix.

A name Postgres would fold, truncate or resolve to something else never reaches the
server. `render_bootstrap` raises `ValueError`, `load_application` raises `ConfigError`
naming `database.schema`, and the migration runners raise `ConfigError`.

### Database users and groups

These are database users, unrelated to RBAC roles. The bootstrap creates them from
{class}`~loom.core.repository.sqlalchemy.rls.DatabaseRoles` and
{class}`~loom.core.repository.sqlalchemy.rls.DatabaseUser`:

| User | Attributes | What it holds |
|---|---|---|
| owner | `NOLOGIN NOBYPASSRLS` | owns every relation; under `FORCE` it is subject to the policies like everyone; DML on a scoped table by the owner, or by any non-superuser role that holds its privileges through membership, fails with `LG001` |
| migrator | `LOGIN NOBYPASSRLS NOINHERIT`, no-inherit member of the owner, `SET role = owner` | nothing directly; acts as the owner; the only role besides superusers allowed to be a member of the owner |
| `<schema>_readers` | `NOLOGIN NOBYPASSRLS` | `SELECT` on every scoped table that admits it; its `__privileges__` on global tables |
| `<schema>_writers` | `NOLOGIN NOBYPASSRLS` | the admitted `INSERT`, `UPDATE`, `DELETE`; `USAGE` on serial sequences when `INSERT` is admitted; its `__privileges__` |
| `access="read"` | `NOBYPASSRLS INHERIT` | member of `<schema>_readers` |
| `access="write"` | `NOBYPASSRLS INHERIT` | member of `<schema>_readers` and `<schema>_writers` |
| `access="bypass"` | `BYPASSRLS NOINHERIT`, no membership | `SELECT`, `INSERT`, `UPDATE`, `DELETE` on every table and `USAGE` on every sequence of the schema, through default privileges and idempotent `GRANT ... ON ALL`; exactly `SELECT` on `alembic_version` |

No grant on a table of the schema is written by hand. Scoped tables grant through
protection, global tables through `__privileges__`, users through group membership,
bypass users through the bootstrap. An identity column needs no sequence grant.

### The bootstrap

One idempotent SQL script per application schema creates the users, the groups, the
schema and its guard, `loom_guard_<schema>`:

```python
import asyncio
import os
from pathlib import Path

from loom.core.locator import load_application
from loom.core.repository.sqlalchemy.rls import apply_bootstrap, render_bootstrap


async def main() -> None:
    application = load_application()
    bootstrap = application.bootstrap
    if bootstrap is None:
        raise SystemExit("no RowScoped model in this application")
    Path("db/bootstrap.sql").write_text(render_bootstrap(bootstrap))
    passwords = {
        "ledger_migrator": os.environ["LEDGER_MIGRATOR_PASSWORD"],
        "ledger_rw": os.environ["LEDGER_RW_PASSWORD"],
        "ledger_ro": os.environ["LEDGER_RO_PASSWORD"],
        "ledger_ops": os.environ["LEDGER_OPS_PASSWORD"],
    }
    await apply_bootstrap(os.environ["SUPERUSER_URL"], bootstrap, passwords)


asyncio.run(main())
```

- {func}`~loom.core.repository.sqlalchemy.rls.render_bootstrap` validates every name
  (see [Names](#names)) and returns the script. It contains no password, so you can
  version it in the repository.
- {func}`~loom.core.repository.sqlalchemy.rls.apply_bootstrap` runs the script and then
  sets each password as a SCRAM-SHA-256 verifier computed on the client, in one
  transaction. No cleartext password reaches the server or its log.
- It checks `server_version_num` first and fails below
  {data}`~loom.core.repository.sqlalchemy.rls.MIN_SERVER_VERSION_NUM` (`140000`).
- It needs superuser or `rds_superuser`, because it creates event triggers; otherwise
  `apply_bootstrap` raises `ConfigError` naming the event trigger. There is no degraded
  mode.
- It compares an existing role attribute by attribute and fails without touching
  anything when one differs, so a pre-existing `BYPASSRLS` role is never accepted in
  silence.
- `revoke_public=True`, the default, revokes `ALL` on schema `public` from `PUBLIC`.
- When `alembic_version` already exists, each bypass user is left with exactly `SELECT`
  on it.

Applying it twice changes nothing. Re-applying it is also how you add users and upgrade
loom, below.

#### Compose reference

The official Postgres image applies the rendered script on first start; no custom image
is needed:

```yaml
services:
  postgres:
    image: postgres:17-alpine
    environment:
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    ports:
      - "127.0.0.1:5432:5432"
    volumes:
      - ./db/bootstrap.sql:/docker-entrypoint-initdb.d/10-bootstrap.sql:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 3s
      retries: 20
```

The init runs as the `postgres` superuser against the default database. Run
`apply_bootstrap` once the container is healthy to set the passwords; the script itself
runs again and changes nothing.

#### Supported Postgres versions

| Version | Support |
|---|---|
| below 14 | refused by the bootstrap (`server_version_num` check) |
| 14 and 15 | supported; `verify` reads the migrator's no-inherit membership from `pg_roles.rolinherit` |
| 16 and later | supported; `verify` reads `pg_auth_members.inherit_option` |

Managed services work when the bootstrap runs as `rds_superuser` or an equivalent role
that may create event triggers.

### Local flow

```python
import asyncio
import os

from loom.core.locator import load_application
from loom.core.repository.sqlalchemy.rls import create_schema

asyncio.run(create_schema(os.environ["MIGRATOR_URL"], load_application()))
```

{func}`~loom.core.repository.sqlalchemy.rls.create_schema` always takes the migrator
URL and uses a session manager without session settings. It checks the dialect, checks
that the guard of the schema exists and that its two event triggers, `ddl_command_end`
and `sql_drop`, are present and enabled, runs `create_all` on the application's metadata and
closes with the guard's assertion. Each scoped table is protected in the transaction
that creates it. Then start the application with `mode: external` and the URL of a
`read` or `write` user.

### CI and production with Alembic

The structural tree lives in `alembic/versions`, the data tree in `alembic/data`. Both
use the environment loom ships, copied as `env.py`:

```text
alembic/
  env.py
  script.py.mako
  versions/
  data/
    env.py
    script.py.mako
    versions/
```

```python
import shutil

from loom.core.repository.sqlalchemy.migrations import ENV_TEMPLATE_PATH

shutil.copy(ENV_TEMPLATE_PATH, "alembic/env.py")
shutil.copy(ENV_TEMPLATE_PATH, "alembic/data/env.py")
```

The environment loads the application from `config.attributes["application"]` or, when
that is absent, from `LOOM_CONFIG`. It refuses offline mode, because the guard needs a
live connection. Generate and apply structural revisions with the migrator URL:

```python
from alembic import command

from loom.core.locator import load_application
from loom.core.repository.sqlalchemy.migrations import alembic_config

config = alembic_config("alembic", MIGRATOR_URL)
config.attributes["application"] = load_application()
command.revision(config, message="add entries", autogenerate=True)
command.upgrade(config, "head")
```

{func}`~loom.core.repository.sqlalchemy.migrations.alembic_config` accepts a
`postgresql+asyncpg` URL; a script location whose last element is `data` is the data
tree. The environment calls
{func}`~loom.core.repository.sqlalchemy.migrations.run_migrations`, which:

- validates every name (see [Names](#names));
- switches to the owner and fails unless `current_user` is the owner and
  `current_schema()` the application schema;
- takes `pg_advisory_lock(hashtextextended('loom.schema:' || schema, 0))`, so two
  deployments on one schema serialize and two schemas never block each other;
- creates both version tables, `alembic_version` and `alembic_version_data`, and leaves
  each bypass user with exactly `SELECT` on `alembic_version`, before the first
  revision;
- runs each revision in its own transaction with `lock_timeout` (`5s`) and
  `statement_timeout` (`60s`), then runs the guard's assertion before it commits;
- rejects a revision that sets `data_migration = True`.

The revision hook,
{func}`~loom.core.repository.sqlalchemy.migrations.scope_protection_hook`, is installed
automatically. It reads the compiled metadata and rewrites each autogenerated revision,
one sequence per table, under the guard's hatch and in the revision's transaction:

| Model change | Generated sequence |
|---|---|
| new scoped table | hatch, create, protect |
| scope column added, altered or dropped | hatch, unprotect, change, protect; the inverse in `downgrade` |
| global table with `__privileges__` | create, then the `GRANT` statements |

The hook also emits the composite FKs, `__unique__` and `__indexes__`, and refuses to
write a revision that would leave a scoped table unprotected. A structural revision that
writes rows of a scoped table fails with `LG001`: move the change to the data tree.

Autogenerate never proposes dropping a table the models do not declare: it may be one
that discovery did not find. A drop is written by hand; for a scoped table, in the same
transaction, open the hatch and call `unprotect_scoped_table` before the `DROP`, as in
[Incremental DDL under the hatch](#incremental-ddl-under-the-hatch). Marking an existing
table or unmarking a scoped one is out of scope: no revision is generated for it, and
`check`, `verify` and startup in `external` mode report the mismatch.

{func}`~loom.core.repository.sqlalchemy.migrations.check` is read-only: it takes no
lock, creates no version table and writes nothing.

### Data migrations

Data revisions live only in `alembic/data`, record their state in
`<schema>.alembic_version_data` and declare themselves:

```python
from alembic import op

revision = "d1a2"
down_revision = None
data_migration = True


def upgrade() -> None:
    op.execute("UPDATE entries SET reference = upper(reference)")


def downgrade() -> None:
    pass
```

Apply them with the URL of a user declared with `access="bypass"`:

```python
from alembic import command

from loom.core.repository.sqlalchemy.migrations import alembic_config

command.upgrade(alembic_config("alembic/data", LEDGER_OPS_URL), "head")
```

{func}`~loom.core.repository.sqlalchemy.migrations.run_data_migrations` never switches
role. It requires `rolbypassrls` for `current_user` and that user declared with
`access="bypass"`, takes the same advisory lock, requires the structural tree at head
while it holds the lock, and runs one transaction per revision. It rejects a revision without
`data_migration = True`. The bypass user has no `CREATE` on the schema, so a data
revision cannot change the schema.

### Adding users and upgrading loom

Declare the new user under `database_users` and re-apply the bootstrap with the
superuser URL. A `read` or `write` user joins its groups and works at once; a `bypass`
user receives its privileges on the existing tables. No revision and no table DDL are
needed.

Upgrading loom is the same step: re-apply the bootstrap of each schema. It replaces only
that schema's guard functions and never touches the guard of another schema, so two
products on different loom versions can share a database.

### Incremental DDL under the hatch

Generated revisions already follow this rule; a hand revision or a hand session must too.
Any DDL on a scoped table outside protection runs with the hatch
`loom_guard_<schema>.protecting` on, and is followed by its protection in the same
transaction. Without the hatch, the guard fails it closed. Run these as the migrator.

A new child table with an inline FK to a scoped table:

```sql
BEGIN;
SELECT set_config('loom_guard_ledger.protecting', 'on', true);
CREATE TABLE ledger.entry_notes (
    account_id bigint NOT NULL,
    id serial,
    entry_id integer NOT NULL,
    note text NOT NULL,
    PRIMARY KEY (account_id, id),
    FOREIGN KEY (account_id, entry_id) REFERENCES ledger.entries (account_id, id) ON DELETE CASCADE
);
SELECT loom_guard_ledger.protect_scoped_table('ledger.entry_notes', '[{"col": "account_id", "scope": "account", "on": "both", "elevable": false}]', ARRAY['SELECT','INSERT','UPDATE','DELETE']);
COMMIT;
```

A new partition of a scoped partitioned table:

```sql
BEGIN;
SELECT set_config('loom_guard_ledger.protecting', 'on', true);
CREATE TABLE ledger.events_2027 PARTITION OF ledger.events
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');
SELECT loom_guard_ledger.protect_scoped_table('ledger.events_2027', '[{"col": "account_id", "scope": "account", "on": "both", "elevable": false}]', ARRAY['SELECT']);
COMMIT;
```

An existing table attached as a partition, protected before it is attached:

```sql
BEGIN;
SELECT set_config('loom_guard_ledger.protecting', 'on', true);
CREATE TABLE ledger.events_2028 (LIKE ledger.events INCLUDING ALL);
SELECT loom_guard_ledger.protect_scoped_table('ledger.events_2028', '[{"col": "account_id", "scope": "account", "on": "both", "elevable": false}]', ARRAY['SELECT']);
ALTER TABLE ledger.events ATTACH PARTITION ledger.events_2028
    FOR VALUES FROM ('2028-01-01') TO ('2029-01-01');
COMMIT;
```

The JSON argument is exactly what loom renders from the model: one object per scope with
`col`, `scope`, `on` and `elevable`. `protect_scoped_table` closes the hatch when it
finishes, so the `ATTACH PARTITION` that follows is asserted, and passes because the new
partition is already registered.

Dropping a scoped table by hand opens the hatch and unprotects it in the same
transaction:

```sql
BEGIN;
SELECT set_config('loom_guard_ledger.protecting', 'on', true);
SELECT loom_guard_ledger.unprotect_scoped_table('ledger.entry_notes');
DROP TABLE ledger.entry_notes;
COMMIT;
```

`unprotect_scoped_table` without the hatch fails with `LG002`, and so does the drop of a
registered table, rejected by the `sql_drop` trigger.

### What the guard enforces

`loom_guard_<schema>` belongs to the superuser. Only the owner holds `USAGE` on it and
`EXECUTE` on `protect_scoped_table` and `unprotect_scoped_table`, the only two functions
that write its registry. Two event triggers run its checks: `ddl_command_end` runs the
assertion on every DDL of the database unless the hatch is on, and `sql_drop` rejects
the drop of a registered table. The assertion fails with `LG002` when any of these stops
holding:

- every registered table has row-level security enabled and forced;
- its policies are exactly the registered ones, permissive, with the registered text;
- its grants are only the owner's, `SELECT` to readers, the admitted writes to writers
  and the four row privileges to declared bypass users; never `PUBLIC`, never a column
  grant;
- it has no rule, and its `loom_deny_owner_dml` trigger is present, enabled `ALWAYS`,
  statement-level, without `WHEN`, on the guard's function;
- every unique or exclusion index has the boundary column among its key columns,
  including one created by hand; columns under `INCLUDE` do not count;
- FKs between scoped tables map boundary to boundary and never `SET NULL` or
  `SET DEFAULT`; no FK reaches a scoped table from an unregistered one;
- every partition of a registered table is registered;
- every relation of the schema is owned by the owner, and there is no materialized view;
- no non-bypass member of a schema group is a member of a bypass role;
- no role other than the migrator, superusers aside, is a member of the owner.

### `check` versus `verify`

| | {func}`~loom.core.repository.sqlalchemy.migrations.check` | {func}`~loom.core.repository.sqlalchemy.rls.verify` |
|---|---|---|
| Question | do the models, the migrations and the registry agree? | does the database hold what the declaration says? |
| Compares | tables and columns (Alembic autogenerate), registered tables against the model | the assertion, the guard's two event triggers present and enabled (`guard.event_triggers`), then what the assertion cannot require: presence of group and bypass privileges, exactly `SELECT` for bypass users on `alembic_version`, sequence `USAGE`, `__privileges__` on global tables, C9 actions and group privileges, memberships against `access`, the migrator's no-inherit membership, owner and migrator outside bypass roles |
| Credential | migrator | migrator |
| Writes | none; no lock, no version table | none |
| Result | `ConfigError` naming the first drift | {class}`~loom.core.repository.sqlalchemy.rls.Report` with `ok` and `findings` |
| Where | the pull request gate | after each deployment, and on demand |

The assertion forbids excess at every DDL; `verify` also requires presence. Policy and
`__privileges__` drift belong to `verify`. `check` runs its own event loop, so call it
from a thread without a running loop.

```python
report = await verify(MIGRATOR_URL, application)
for finding in report.findings:
    print(finding.table, finding.check, finding.expected, finding.actual)
```

### Errors

Every `LG001` and `LG002` message starts with `loom_guard[<schema>]:`.

| Signal | Where | Meaning |
|---|---|---|
| `LG001` | `loom_deny_owner_dml` | the owner ran `INSERT`, `UPDATE`, `DELETE` or `TRUNCATE` on a scoped table, usually a structural revision that should be a data revision |
| `LG002` | the assertion, `sql_drop`, C5 in `protect_scoped_table`, `unprotect_scoped_table` | a guard invariant is broken, the message naming the object to repair; or an unprotect without the hatch |
| `22023` | `protect_scoped_table`, the bootstrap | an invalid declaration, a `char(n)` scope column or one with a nondeterministic collation, or a server below Postgres 14 |
| `42501` | `protect_scoped_table`, `unprotect_scoped_table`, the bootstrap | wrong schema, wrong owner, a caller that is not a member of the owner, prior policies, a table not registered, or an existing role that differs from the declaration |
| `ConfigError` | startup, `load_application`, `create_schema`, the runners, `check` | a missing key, an invalid name, a missing or malformed scope binding, a missing guard or guard event trigger, a wrong credential, a superuser or bypass application connection, a dialect that cannot protect, or drift |
| `ValueError` | `compile_all`, the revision hook, `render_bootstrap` | a compile rule, an FK whose target is not compiled with it, a revision that would leave a table unprotected, or an invalid name |

### Other dialects

Scoped models on SQLite or any non-Postgres dialect raise `ConfigError` at startup, in
either mode, and in `create_schema`. Tests that want them anyway declare
`database.schema.allow_unprotected_dialect: true`: the marker then emits nothing, the
tables are created as plain tables, and startup logs a warning naming them. Never set it
outside tests.

### Jobs that cross boundaries

A job that must read or write every boundary uses a second
{class}`~loom.core.repository.sqlalchemy.session_manager.SessionManager` on the URL of
the declared bypass user, without a provider:

```python
operations = SessionManager(LEDGER_OPS_URL)
```

On the application URL without an identity, the same job sees zero rows, by design. This
is a usage pattern, not a loom mechanism. Audit every use of the bypass manager.

## Threat model

- **Trusted in intent.** The owner, migrator and superuser credentials. Their deliberate
  acts are out of scope: turning the hatch on and changing the guard,
  `CREATE TABLE AS` followed by `GRANT`, publications, settings.
- **What the guard does.** It detects accidental drift by those credentials (hand DDL,
  an incomplete revision) in the same DDL that causes it, and closes every path of the
  application user.
- **Session values.** The `loom.scope.*` keys and the hatch are settings any session can
  set. Row-level security defends against missing or wrong filters in application code,
  not against arbitrary code or SQL injection inside the application process (obligation
  P5).

### Residuals

- **Owner DML from product triggers.** `loom_deny_owner_dml` returns without acting when
  `pg_trigger_depth() > 1`, so that referential `CASCADE`, which Postgres runs as the
  owner, completes. The trade-off: owner DML issued from a product trigger is not
  trapped either. Review product triggers on scoped tables.
- **Owner reads in revisions.** A read as the owner inside a structural revision is
  subject to the policies like any other (`TO PUBLIC`): without session values it
  returns zero rows and no error. Writes fail with `LG001`.
- **Forgeable values.** Session values and `Decision` objects can be forged by code
  inside the process.
- **Savepoints.** A savepoint that rolls back reverts an elevation flag set inside it.
  loom never opens one for elevation.
- **Concurrency.** An `AsyncSession` is not safe for concurrent tasks.
- **Adopting existing data.** Composite FKs change the physical key of referencing
  tables. A product that adopts row-scoped tables on existing data needs its own
  expand/contract migration; an FK without the boundary fails the assertion.

### A broken guard blocks all DDL

Each guard asserts its own schema on every DDL of the whole database, with no tag list
and no schema filter. A tag left out would be a hole, and Postgres reports no schema for
`GRANT`, `REVOKE` or `ALTER POLICY`, so no filter could tell whether a DDL touched the
schema. The accepted consequence: while one guard is broken, every DDL in the database
fails with `LG002` and the schema to repair in the message, until it is repaired. It is
never a permission error, because the triggers and the assertion are
`SECURITY DEFINER`.

### Repairing policy text drift

After a major Postgres upgrade or a dump and restore, the text Postgres returns for a
policy's `qual` or `with_check` may differ from the registered one. The assertion then
fails and, as above, blocks all DDL. Repair each named table in one transaction, as the
migrator:

```sql
BEGIN;
SELECT set_config('loom_guard_ledger.protecting', 'on', true);
SELECT loom_guard_ledger.unprotect_scoped_table('ledger.entries');
SELECT loom_guard_ledger.protect_scoped_table('ledger.entries', '[{"col": "account_id", "scope": "account", "on": "both", "elevable": false}, {"col": "clerk", "scope": "clerk", "on": "write", "elevable": true}]', ARRAY['SELECT','INSERT','UPDATE','DELETE']);
COMMIT;
```

Without the hatch, `unprotect_scoped_table` fails with `LG002` and nothing changes. Rehearse it on a copy before the first
deployment and after each major upgrade. Between `unprotect_scoped_table` and
`protect_scoped_table` the table keeps forced row-level security without policies, so a
non-bypass user sees zero rows.

### Elevation and the boundary

`elevate` checks that the decision's grant covers `at`; it does not check that `at` is
the boundary of the current session. The flag opens every row of the elevable scope
within the boundary, so call `elevate` only with a decision evaluated at the boundary
scope (obligation P2). See [Elevating a write scope](authorization.md#elevating-a-write-scope).

## Out of scope

Each case fails explicitly or is documented here; none is resolved in silence.

| Case | Failure or treatment |
|---|---|
| Composite boundary | `ValueError` at compile time; by hand, `protect_scoped_table` fails with `22023` |
| Hierarchical or multi-valued boundary | Not expressible: every term is an equality |
| `NULL` boundary | `ValueError` at compile time; by hand, `protect_scoped_table` fails with `22023` |
| Marking an existing table, or unmarking a scoped one | No revision is generated; `check`, `verify` and startup in `external` mode report the mismatch |
| Dropping a table the models no longer declare | Never autogenerated; written by hand, with the hatch and `unprotect_scoped_table` first for a scoped table |
| Expand and contract in one release | Two deployments; a revision that drops a column still read by the previous release fails there with the Postgres error |
| Adopting existing data with FKs | An FK without the boundary fails the assertion |
| Deliberate acts with owner, migrator or superuser credentials | See [Threat model](#threat-model) |
| Settings forged from the process | Obligation P5 |
| Owner DML from product triggers at depth > 1 | See [Residuals](#residuals) |
| Binding `at` to the boundary inside loom | Obligation P2 |
| Materialized views | By hand in the schema, the assertion fails |
| Postgres without superuser or `rds_superuser` | The bootstrap raises `ConfigError` naming the event trigger |
| Postgres before 14 | The bootstrap fails on the `server_version_num` check |
| Other dialects | `ConfigError` at startup, unless `allow_unprotected_dialect: true` in tests |
| A degraded mode, or an event trigger filtered by schema | No degraded mode (`ConfigError`); no filter (a broken guard blocks all DDL) |
| Cross-boundary jobs on the application credential | They see zero rows; use the bypass manager |
| Side channels | Not addressed by row-level security |
| Logical replication | Not guarded; publications are owner acts |

## Product obligations

loom cannot enforce these; the product must.

| | Obligation |
|---|---|
| P1 | Bind every scope in `database.schema.scopes` and register every `request.<name>` source before startup |
| P2 | Call `elevate` only with a decision evaluated at the scope of the boundary |
| P3 | Before applying an edit to a custom role that is already granted, run `can_grant` for every holder's grant and refuse on any failure |
| P4 | Put data revisions in `alembic/data` with `data_migration = True`, apply them with the bypass user's URL, and run cross-boundary jobs on a second `SessionManager` with that URL |
| P5 | Forbid `set_config(` and `SET loom.` outside loom with a product lint |

## Testing it

loom's own integration tests run against a real Postgres with forced policies and
application users without `BYPASSRLS`:

```bash
docker compose -f docker-compose.local.yaml up -d postgres
LOOM_PG_IT_URI=postgresql+asyncpg://postgres:loom@localhost:55432/postgres \
  uv run pytest tests/integration/core/repository/sqlalchemy/postgres tests/integration/rls -m integration
```

They cover the provider (two boundaries on a shared pool under concurrent transactions,
a transaction with no context, a savepoint, a broken provider, a bypass manager next to
an application one) and three synthetic products of row-scoped tables in separate
schemas of one database, each created, migrated and verified without changing loom,
together with adversarial cases for the guard invariants and the out-of-scope failures.
