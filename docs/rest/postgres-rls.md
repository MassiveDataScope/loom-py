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

Each outer transaction then starts with one round trip. The statement is a constant; the
settings travel as one bound JSON document, here
`{"app.tenant_id": "acme", "app.subject": "u-1"}`:

```sql
SELECT set_config(s.key, s.value, true)
FROM jsonb_each_text(CAST(:settings AS jsonb)) AS s
```

The `true` makes every setting local to the transaction. They vanish at `COMMIT` or
`ROLLBACK`, so a connection returns to the pool clean, and the same holds behind an
external pooler in transaction mode such as PgBouncer.

What the manager guarantees:

- Keys and values are bound as one parameter. The compiled SQL never contains a key or
  a value.
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
ALTER ROLE app_migrator SET role = app_owner;
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
  `app_owner` whose sessions start as `app_owner` (`ALTER ROLE ... SET role`), so that
  everything they create belongs to the owner without a `SET ROLE` in any migration.
  Neither `app` nor `app_platform` may be a member of `app_owner`,
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
column with `ScopedField(..., scope=..., on=..., elevable=...)`; other columns keep
`ColumnField`:

```python
import datetime as dt

from loom.core.model import BaseModel, ColumnField, OnDelete, Privilege, RowScoped, ScopedField
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
    account_id: int = ScopedField(
        BigInteger,
        primary_key=True,
        scope="account",
        foreign_key="accounts.id",
        on_delete=OnDelete.RESTRICT,
    )
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    clerk: str = ScopedField(Text, scope="clerk", on="write", elevable=True)
    reference: str = ColumnField(Text)
    booked_on: dt.datetime = ColumnField(DateTime())


class EntryLine(BaseModel, RowScoped):
    __tablename__ = "entry_lines"
    account_id: int = ScopedField(BigInteger, primary_key=True, scope="account")
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
    account_id: int = ScopedField(BigInteger, primary_key=True, scope="account")
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
| C5 | Every primary key, `__unique__` entry, `__partial_unique__` entry and `unique=True` column of a scoped table contains the boundary column, without exception |
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

These class attributes declare what would otherwise be hand SQL in a revision:

| Attribute | Applies to | Effect |
|---|---|---|
| `__unique__` | any model | composite `UNIQUE` constraints; on a scoped table each contains the boundary (C5) |
| `__indexes__` | any model | non-unique indexes named `ix_<table>_<columns>`; boundary column first is the usual choice on a scoped table |
| `__checks__` | any model | `{rule: sql_expression}`; one `CHECK` constraint per rule, named by the rule |
| `__partial_unique__` | any model | `{rule: (columns, where)}`; one unique index `uq_<table>_<rule>` over `columns`, restricted to the rows matching the SQL predicate `where`; on a scoped table `columns` contains the boundary (C5) |
| `__privileges__` | unscoped models only | `{"readers" \| "writers": frozenset[Privilege]}`; plain `GRANT` statements to the schema groups, plus `USAGE` on serial sequences for `INSERT` |

```python
class Seat(BaseModel, RowScoped):
    __tablename__ = "seats"
    __checks__ = {"status_code": "status_code IN ('active', 'removed')"}
    __partial_unique__ = {"owner": (("tenant_id", "roster_id"), "is_owner")}
    tenant_id: str = ScopedField(String(36), primary_key=True, scope="tenant")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    roster_id: int = ColumnField(Integer, foreign_key="rosters.id")
    status_code: str = ColumnField(Text)
    is_owner: bool = ColumnField(Boolean)
```

A rule is a SQL identifier: lowercase letters, digits and `_`, not a reserved word. The
expression and the predicate are SQL that loom passes through as written; they must be
static literals in the model, never built from a request or a setting (see
[Why the guard is static SQL](#why-the-guard-is-static-sql)). An attribute that is not a
mapping raises `ValueError` naming the model; an unknown column, an empty expression or
predicate, or a rule that is not a string or not an identifier raises `ValueError`
naming the model and the rule. The guard holds a partial unique index to the same rule
as any other: one without the boundary column is refused with `LG002`.

Names are final where loom builds them (`ix_...`, `uq_<table>_<rule>`) and follow
`database.schema.naming_convention` otherwise; see [Configuration](#configuration). A
check, `__indexes__` or `__partial_unique__` name longer than the 63 bytes Postgres keeps
raises `ValueError` at compile time instead of being truncated, and so do two
constraints or indexes of one table that resolve to the same name.

Alembic's autogenerate does not compare `CHECK` constraints, and compares the columns of
an index but not its `WHERE` predicate. A check or partial unique index declared with a
new table is emitted inside its `create_table`, and a new partial unique index on an
existing table as a `create_index`. Adding, changing or removing a check on an existing
table, and changing a predicate, are written by hand in a revision; `check` does not
report them as drift.

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
    guard: loom_guard_ledger
    groups:
      readers: ledger_readers
      writers: ledger_writers
    version_tables:
      structure: alembic_version
      data: alembic_version_data
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
| `mode` | `create_all` | `external` never creates tables; at startup it wires the session values, refuses scoped models on another dialect, a superuser or bypass connection, a missing table, a scoped table that is not protected canonically, and a guard that differs from the released one |
| `allow_unprotected_dialect` | `false` | see [Other dialects](#other-dialects) |
| `name` | none | the application schema; required when any model is `RowScoped` |
| `guard` | none | the guard schema; see [Names](#names) |
| `groups` | none | `readers` and `writers`, the two schema groups |
| `version_tables` | none | `structure` and `data`, the version tables of the two Alembic trees |
| `roles` | none | `owner` and `migrator` |
| `database_users` | none | the login users and their `access` |
| `scopes` | none | one source per declared scope, validated at startup in `external` mode |
| `naming_convention` | SQLAlchemy's | SQLAlchemy `naming_convention` keyed by `pk`, `fk`, `uq`, `ck` and `ix`; applied to the application metadata of the migration path and to the tables the REST runtime and the Celery worker compile, so all of them name every constraint alike. Another key raises `ConfigError` |

A convention that names every constraint loom compiles:

```yaml
database:
  schema:
    naming_convention:
      pk: "pk_%(table_name)s"
      fk: "fk_%(table_name)s_%(column_0_N_name)s"
      uq: "uq_%(table_name)s_%(column_0_N_name)s"
      ck: "ck_%(table_name)s_%(constraint_name)s"
      ix: "ix_%(table_name)s_%(column_0_N_name)s"
```

Use `%(column_0_N_name)s` for `fk`, not `%(column_0_name)s`: every composite FK of a
scoped table starts with the boundary column, so two of them would get the same name;
compilation refuses it with `ValueError`. `ck` takes the rule of `__checks__` as
`%(constraint_name)s`. The names loom builds for `__indexes__` and `__partial_unique__`
are final and no convention renames them.

{func}`~loom.core.locator.load_application` reads this file (the path given, or
`LOOM_CONFIG`), adds `app.code_path` (default `src`, relative to the configuration file) to `sys.path` as the server does,
discovers and compiles the models into a `MetaData` owned by the returned
{class}`~loom.core.locator.Application` and builds the
{class}`~loom.core.repository.sqlalchemy.rls.BootstrapConfig`. A missing key raises
`ConfigError` naming it, and a missing name points at `loom schema init`; no name has a
default. Two applications in one process share no table, listener or registry.

With scoped models on Postgres, `mode: create_all` fails at startup: the application,
which carries the scope provider, never creates schema. Create it with the migrator.

In `mode: external`, startup fails with `ConfigError` when scoped models meet a dialect
other than Postgres without `allow_unprotected_dialect`, and when the application
connects as a superuser or a role with `BYPASSRLS`, which row-level security does not
restrain. Use the URL of a `read` or `write` user. Startup also reads the catalogue and
fails with `ConfigError` when a scoped table is missing or not protected canonically
(forced row-level security, exactly the `loom_*` policies its privileges admit, all
permissive, and its `loom_deny_owner_dml` trigger enabled `ALWAYS` on this guard's
function), or when the guard installed in the database is not one this release of loom
shipped or is below the minimum compatible revision; see
[Why the guard is static SQL](#why-the-guard-is-static-sql) and
[Adding users and upgrading loom](#adding-users-and-upgrading-loom).

### Names

loom names nothing around the schema at run time. Besides the schema, the two roles and
the database users, the product declares the names of
{class}`~loom.core.repository.sqlalchemy.rls.SchemaNames`:

| Key | Names | Derived proposal |
|---|---|---|
| `guard` | the guard schema, which holds the guard's functions, configuration and registry | `loom_guard_<schema>` |
| `groups.readers` | the readers group | `<schema>_readers` |
| `groups.writers` | the writers group | `<schema>_writers` |
| `version_tables.structure` | the version table of the structural tree | `alembic_version` |
| `version_tables.data` | the version table of the data tree | `alembic_version_data` |

`loom schema init`, installed with the `cli` extra, writes the derived proposal once:

```bash
loom schema init ledger --config config/app.yaml
```

Without `--config` it prints the `database.schema` block. With it, it merges the block
into the file and fills only the names that are absent. A name the product already
changed is kept: the command lists it on standard error, exits with status 1 and leaves
the file untouched. The file is rewritten with PyYAML, so comments are not preserved.
A file whose top level, `database` or `database.schema` is not a mapping, or that is not
valid YAML, is refused the same way. From then on the names belong to the product; loom
never derives them again, and a missing one raises `ConfigError` pointing at
`loom schema init`.

`loom schema init` depends only on the standard library and PyYAML: it needs neither the
`sqlalchemy` nor the `rest` extra. Run without the `cli` extra, `loom` prints how to
install it (`pip install "loom-kernel[cli]"`) and exits with status 1. The command tree
lives in `loom.cli.app`.

Every name Postgres sees passes one validator,
`loom.core.schema_names.sql_identifier`, and `schema_identifier` for the schema:

- lowercase letters, digits and `_`, starting with a letter or `_`;
- at most 63 characters, so Postgres never truncates it; at most 47 for the schema, so
  the derived guard name fits; at most 58 for the guard, so the names of its event
  triggers (`<guard>_ddl`, `<guard>_drop`) fit; at most 59 for a version table, so its
  primary key (`<table>_pkc`) fits;
- not a reserved word, not a special role name (`public`, `current_user`,
  `session_user`, `current_role`, `none`), and no `pg_` prefix.

The roles, groups and users must have distinct names, and the two version tables must
differ. A name Postgres would fold, truncate or resolve to something else never reaches
the server: {meth}`~loom.core.repository.sqlalchemy.rls.BootstrapConfig.validated` and
`SchemaNames.derived` raise `ValueError`, while `load_application`, `apply_bootstrap`
and the migration runners raise `ConfigError` naming `database.schema`. A valid name
still reaches Postgres only as a bound parameter, and the guard quotes it itself.

### Database users and groups

These are database users, unrelated to RBAC roles. The bootstrap creates them from
{class}`~loom.core.repository.sqlalchemy.rls.DatabaseRoles` and
{class}`~loom.core.repository.sqlalchemy.rls.DatabaseUser`:

| User | Attributes | What it holds |
|---|---|---|
| owner | `NOLOGIN NOBYPASSRLS` | owns every relation; under `FORCE` it is subject to the policies like everyone; DML on a scoped table by the owner, or by any non-superuser role that holds its privileges through membership, fails with `LG001` |
| migrator | `LOGIN NOBYPASSRLS NOINHERIT`, no-inherit member of the owner, `SET role = owner` | nothing directly; acts as the owner; the only role besides superusers allowed to be a member of the owner |
| readers group (`groups.readers`) | `NOLOGIN NOBYPASSRLS` | `SELECT` on every scoped table that admits it; its `__privileges__` on global tables |
| writers group (`groups.writers`) | `NOLOGIN NOBYPASSRLS` | the admitted `INSERT`, `UPDATE`, `DELETE`; `USAGE` on serial sequences when `INSERT` is admitted; its `__privileges__` |
| `access="read"` | `NOBYPASSRLS INHERIT` | member of the readers group |
| `access="write"` | `NOBYPASSRLS INHERIT` | member of the readers and the writers groups |
| `access="bypass"` | `BYPASSRLS NOINHERIT`, no membership | `SELECT`, `INSERT`, `UPDATE`, `DELETE` on every table and `USAGE` on every sequence of the schema, through default privileges and idempotent `GRANT ... ON ALL`; exactly `SELECT` on the structural version table |

No grant on a table of the schema is written by hand. Scoped tables grant through
protection, global tables through `__privileges__`, users through group membership,
bypass users through the bootstrap. An identity column needs no sequence grant.

The bootstrap sets the `search_path` of the owner and the migrator to the application
schema followed by the guard, and that of every login user to the application schema.
It refuses a role name with a `pg_` or `rds_` prefix.


### The bootstrap

{func}`~loom.core.repository.sqlalchemy.rls.apply_bootstrap` prepares one application
schema in one transaction, with a superuser URL: the guard, the users and groups, the
schema, its grants and the guard's event triggers. The role that runs the first bootstrap
of a schema becomes its installer: it owns the guard, and every later bootstrap of that
schema must run as the same role.

```python
import asyncio
import os

from loom.core.locator import load_application
from loom.core.repository.sqlalchemy.rls import apply_bootstrap


async def main() -> None:
    bootstrap = load_application().bootstrap
    if bootstrap is None:
        raise SystemExit("no RowScoped model in this application")
    passwords = {
        "ledger_migrator": os.environ["LEDGER_MIGRATOR_PASSWORD"],
        "ledger_rw": os.environ["LEDGER_RW_PASSWORD"],
        "ledger_ro": os.environ["LEDGER_RO_PASSWORD"],
        "ledger_ops": os.environ["LEDGER_OPS_PASSWORD"],
    }
    await apply_bootstrap(os.environ["SUPERUSER_URL"], bootstrap, passwords)


asyncio.run(main())
```

Inside that transaction it runs, in order:

1. transaction-local `log_statement = none`, `log_min_duration_statement = -1` and
   `log_parameter_max_length = 0`, so the server logs none of the statements that follow
   nor their parameters;
2. `lock_timeout`, then the schema's advisory lock
   `pg_advisory_xact_lock(hashtextextended('loom.schema:' || schema, 0))`, the same lock
   the migration runners and `create_schema` take, so a bootstrap never runs next to a
   migration of the same schema and waits at most `lock_timeout` (`apply_bootstrap(...,
   lock_timeout="5s")`, the default) before failing;
3. the static preflight, which checks `server_version_num` against
   {data}`~loom.core.repository.sqlalchemy.rls.MIN_SERVER_VERSION_NUM` (`140000`) and
   that the caller is a superuser or a member of `rds_superuser`, then creates the guard
   schema with its `revision` table, or checks that the existing one belongs to the
   calling role and holds loom's revisions;
4. every guard revision the schema has not applied yet, each a static file checked
   against the digest it was released with and recorded with that digest in the guard;
5. the guard's `configure`, which receives the declaration as one bound JSON document
   and creates or checks the roles, the memberships, the application schema, the grants,
   the configuration row and the two event triggers, both `ENABLE ALWAYS`;
6. one password per entry of `passwords`, sent as a SCRAM-SHA-256 verifier computed on
   the client through `set_password_verifier`, which pins `log_statement = none` for
   its own execution. No cleartext password reaches the server or its log, and only the
   migrator and users declared with `login: true` accept one;
7. the catalogue integrity check of
   [Why the guard is static SQL](#why-the-guard-is-static-sql) on the guard it just
   installed, with its owner compared to the calling role; a difference raises
   `ConfigError` and nothing is committed.

What it guarantees:

- No SQL is built from the declaration. The guard is the same static SQL for every
  schema; names travel as bound parameters, after the checks in [Names](#names).
- Below Postgres 14 it fails on the `server_version_num` check with `22023`.
- It needs superuser or `rds_superuser`, because it creates event triggers; otherwise
  `apply_bootstrap` raises `ConfigError` naming the event trigger. There is no degraded
  mode.
- It never adopts what it does not own. A guard schema that exists without loom's
  revisions or with an owner other than the role running the bootstrap, an application
  schema owned by another role than the declared owner, and a guard already configured
  for another schema, owner, migrator, groups, version tables or installer fail with
  `42501`.
- It never adopts a role. On the first bootstrap of a schema, none of the declared roles
  (owner, migrator, groups, users) may exist yet; it creates them all. A later bootstrap
  accepts an existing role only when this guard recorded it, and still compares it
  attribute by attribute: it fails without touching anything when one differs or the role
  is a superuser or holds `CREATEROLE`, `CREATEDB` or `REPLICATION`. Any other existing
  role fails with `42501`, so a pre-existing `BYPASSRLS` role is never accepted in
  silence.
- It audits the memberships of every declared role, as member and as granted role.
  Superusers and the installer aside, the only edges allowed are the migrator in the
  owner and each `read` or `write` user in its groups. Any other edge, including a
  membership in a `pg_*`, `rds_*` or bypass role, and any membership granted
  `WITH ADMIN OPTION`, fails with `42501` naming it.
- A guard that holds a revision this release of loom does not know, or one recorded with
  another digest, raises `ConfigError`: upgrade loom first.
- Revoking `PUBLIC`'s privileges on schema `public` is opt-in, because that schema
  belongs to the whole database: `revoke_public` defaults to `False`, and with `True`
  the bootstrap revokes `ALL` on schema `public` from `PUBLIC`.
- `scram_iterations` is the PBKDF2 iteration count of every verifier. It defaults to
  `4096`, Postgres' own default, and a lower value raises `ConfigError`.
- When the structural version table already exists, each bypass user is left with
  exactly `SELECT` on it.

`load_application` builds the
{class}`~loom.core.repository.sqlalchemy.rls.BootstrapConfig` with both defaults; set them
on the returned value before applying it:

```python
import dataclasses

bootstrap = dataclasses.replace(bootstrap, revoke_public=True, scram_iterations=600_000)
```

Applying it again with the same declaration, as the same installer, converges to the
same state. Re-applying it is also how you add users and upgrade loom, below.

#### Compose reference

The official Postgres image needs no init script and no custom image. The bootstrap needs
a live connection, because it reads the revisions the guard already holds and sends the
declaration as bound data, so it runs once the container is healthy rather than from
`/docker-entrypoint-initdb.d`:

```yaml
services:
  postgres:
    image: postgres:17-alpine
    environment:
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    ports:
      - "127.0.0.1:5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 3s
      retries: 20
```

```bash
docker compose up -d --wait postgres
SUPERUSER_URL="postgresql+asyncpg://postgres:${POSTGRES_PASSWORD}@localhost:5432/postgres" \
  python scripts/bootstrap.py
```

`scripts/bootstrap.py` is the script above. Run it again after every change of
`database_users` and every upgrade of loom.

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
URL and uses a session manager without session settings. It checks the dialect, sets
`lock_timeout` (`create_schema(..., lock_timeout="5s")`, the default) and takes the
schema's advisory lock, the one the bootstrap and the runners take. It then checks that
the migrator lands in the application schema, that `protect_scoped_table` resolves to
the declared guard, that the guard's two event triggers, `ddl_command_end` and
`sql_drop`, are present, enabled `ALWAYS` and owned by the guard schema's owner, and
that the guard is at or above the minimum compatible revision. It runs `create_all` on
the application's metadata and closes with the guard's assertion. Each scoped table is
protected in the transaction that creates it, and each guard call resolves the guard
first, as described in [What the guard enforces](#what-the-guard-enforces). Then start
the application with `mode: external` and the URL of a `read` or `write` user.

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
- never switches role: it fails unless `current_user` is the owner and
  `current_schema()` the application schema, which the bootstrap arranges through the
  migrator's `role` and `search_path` defaults;
- refuses a guard below the minimum compatible revision with `ConfigError`
  (`guard revision pending`), before any guard call;
- sets `lock_timeout` and takes
  `pg_advisory_lock(hashtextextended('loom.schema:' || schema, 0))`, the lock the
  bootstrap and `create_schema` take, so two deployments on one schema serialize, a
  migration never runs next to a bootstrap, and two schemas never block each other;
- creates both declared version tables (`version_tables.structure` and
  `version_tables.data`) through the guard's `prepare_version_tables`, and leaves each
  bypass user with exactly `SELECT` on the structural one, before the first revision;
- runs each revision in its own transaction, which starts by setting `lock_timeout`
  (`5s`) and `statement_timeout` (`60s`) with transaction-local `set_config`, and runs
  the guard's assertion before it commits;
- rejects a revision that sets `data_migration = True`.

The revision hook,
{func}`~loom.core.repository.sqlalchemy.migrations.scope_protection_hook`, is installed
automatically. It reads the compiled metadata and rewrites each autogenerated revision,
one sequence per table, under the guard's hatch and in the revision's transaction. The
revision carries loom's Alembic operations, never SQL text:

| Model change | Generated sequence |
|---|---|
| new scoped table | `op.open_hatch()`, create, `op.protect_scoped_table(...)` |
| scope column added, altered or dropped | `op.open_hatch()`, `op.unprotect_scoped_table(...)`, change, `op.protect_scoped_table(...)` |
| scoped table dropped, as in the `downgrade` of its creation | `op.open_hatch()`, `op.unprotect_scoped_table(...)`, drop |
| global table with `__privileges__` | create, then `op.grant_table(...)` |

A `downgrade` never protects a table with the scopes of the current models, because the
revision it returns to may have had others. Where the `upgrade` would protect, the
`downgrade` unprotects, applies the change and then stops on a generated
`raise NotImplementedError(...)` that names the table: replace that line with
`op.protect_scoped_table(...)` written by hand with the scopes and privileges of the
target revision. The same holds for a scoped table that the `downgrade` recreates.

A new scoped table, as generated:

```python
import loom.core.repository.sqlalchemy.migrations.operations


def upgrade() -> None:
    op.open_hatch()
    op.create_table("entry_notes", ...)
    op.protect_scoped_table(
        "entry_notes",
        [{"col": "account_id", "scope": "account", "on": "both", "elevable": False}],
        ["SELECT", "INSERT", "UPDATE", "DELETE"],
    )
```

The import registers the operations, which live in
{mod}`~loom.core.repository.sqlalchemy.migrations.operations`. Each one executes a constant
statement with bound parameters against the guard named by the runner, resolving the
guard first, and resolves the table in the application schema the runner configured, so
it runs only through `run_migrations`; anywhere else it raises `ConfigError` (`guard
operations run only through loom's run_migrations`).

The composite FKs, `__unique__`, `__indexes__`, `__checks__` and `__partial_unique__` are
not the hook's: Alembic's autogenerate emits them from the compiled metadata, inside
`create_table` or as separate operations such as `create_index`, and the hook leaves
them as they are. What the hook adds is the guard sequence around each create, drop or
scope-column change of a scoped table, and the grants after the creation of an unscoped
table that declares `__privileges__`. A structural revision that writes rows of a scoped table fails with
`LG001`: move the change to the data tree.

Autogenerate never proposes dropping a table the models do not declare: it may be one
that discovery did not find. A drop is written by hand; for a scoped table, in the same
revision, call `op.open_hatch()` and `op.unprotect_scoped_table("table")` before
`op.drop_table("table")`, as in
[Incremental DDL under the hatch](#incremental-ddl-under-the-hatch). Marking an existing
table or unmarking a scoped one is out of scope: no revision is generated for it, and
`check`, `verify` and startup in `external` mode report the mismatch.

{func}`~loom.core.repository.sqlalchemy.migrations.check` is read-only: it takes no
lock, creates no version table and writes nothing.

The migration package, the environment and
{func}`~loom.core.locator.load_application` work without the `rest` extra: they import
nothing from FastAPI.

### Data migrations

Data revisions live only in `alembic/data`, record their state in the data version
table (`version_tables.data`) of the application schema and declare themselves:

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

Declare the new user under `database_users` and re-apply the bootstrap as the same
installer. The new name must not exist yet as a role. A `read` or `write` user joins its
groups and works at once; a `bypass` user receives its privileges on the existing tables.
No revision and no table DDL are needed.

Removing a user from `database_users` and re-applying the bootstrap takes away what it
held: its group memberships, its privileges and default privileges in the schema, its
`USAGE` on the schema and its role settings. The role itself is not dropped, and the
guard forgets it, so declaring the same name again later is refused like any role the
guard did not create. Drop or rename the role by hand, or declare a new name.

Upgrading loom is the same step, in this order: upgrade loom in the application, then
re-apply the bootstrap of each schema with it. The bootstrap applies only the guard
revisions that schema lacks, each checked against its released digest, and never touches
the guard of another schema, so two products on different loom versions can share a
database. A release of loom accepts every released guard revision at or above its
{data}`~loom.core.repository.sqlalchemy.rls.guard_manifest.MIN_COMPATIBLE_GUARD_REVISION`;
below it, the runners and `create_schema` raise `ConfigError` (`guard revision pending`)
and startup refuses to serve until the bootstrap has run. A guard holding a revision the
release does not know is refused everywhere. A released revision is never edited; a
change to the guard ships as a new revision.

### Incremental DDL under the hatch

Generated revisions already follow this rule; a hand revision or a hand session must too.
Any DDL on a scoped table outside protection runs after the guard's hatch is opened, and
is followed by its protection in the same transaction. Without the hatch, the guard fails
it closed. A hand revision uses the operations above (`op.open_hatch()`,
`op.protect_scoped_table(...)`, `op.unprotect_scoped_table(...)`); a hand session calls
the guard's functions, qualified with the guard schema as below. Run these as the
migrator. The examples use the derived guard name, `loom_guard_ledger`.

`open_hatch()` writes a row for the current transaction into the guard, which only a
member of the owner can do; no session setting opens the hatch. The row lives until the
transaction ends or `protect_scoped_table` closes the hatch, and a deferred constraint
trigger on it runs the assertion at `COMMIT`. A transaction that opened the hatch and
leaves a violation behind therefore fails when it commits, with `LG002`, and nothing it
did persists.

A new child table with an inline FK to a scoped table:

```sql
BEGIN;
SELECT loom_guard_ledger.open_hatch();
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

A new partition of a scoped partitioned table, which
[Partitioned tables](#partitioned-tables) does in one call:

```sql
BEGIN;
SELECT loom_guard_ledger.open_hatch();
CREATE TABLE ledger.events_2027 PARTITION OF ledger.events
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');
SELECT loom_guard_ledger.protect_scoped_table('ledger.events_2027', '[{"col": "account_id", "scope": "account", "on": "both", "elevable": false}]', ARRAY['SELECT']);
COMMIT;
```

An existing table attached as a partition, protected before it is attached:

```sql
BEGIN;
SELECT loom_guard_ledger.open_hatch();
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
SELECT loom_guard_ledger.open_hatch();
SELECT loom_guard_ledger.unprotect_scoped_table('ledger.entry_notes');
DROP TABLE ledger.entry_notes;
COMMIT;
```

`unprotect_scoped_table` without the hatch fails with `LG002`, and so does the drop of a
registered table, rejected by the `sql_drop` trigger. Besides dropping the policies and
the owner trigger, `unprotect_scoped_table` disables row-level security and revokes every
privilege the two groups held on the table, so an unprotected table is a plain one that
no non-bypass user can read.

### Partitioned tables

A row-scoped table may be range-partitioned by a timestamp. The model declares it:

```python
class NoteEvent(BaseModel, RowScoped):
    __tablename__ = "note_events"
    __scope_privileges__ = frozenset({Privilege.SELECT})
    __partition_by__ = ("RANGE", "at")
    owner_id: str = ScopedField(String(36), primary_key=True, scope="owner")
    at: dt.datetime = ColumnField(DateTime(), primary_key=True)
    kind: str = ColumnField(Text)
```

The table compiles with `postgresql_partition_by="RANGE (at)"`: `create_schema` and an
autogenerated revision create a partitioned parent, protected like any scoped table. At
compile time the declaration must read `("RANGE", <column>)`; the column must exist, be
a `DateTime` column and belong to the primary key, and every unique key
(`__unique__`, `unique=True`, `__partial_unique__`) must contain it, as Postgres requires.
LIST and HASH partitioning are refused by name, and so is `__partition_by__` on an
unscoped model. Anything else is a `ValueError` naming the model. A scoped foreign key
to a partitioned table can only reference the partition column, since C6 needs a key on
`(boundary, column)` and every key of the target holds the partition column.

`mode: create_all` never creates a partitioned table on Postgres: a partitioned model is
row-scoped, and startup refuses scoped models in that mode. On another dialect, with
`allow_unprotected_dialect`, `postgresql_partition_by` does not apply and `create_all`
creates a plain table that takes inserts without partitions.

The parent holds no rows: an insert needs a partition that covers it, so partitions are
created ahead of time, by the migrator, through the guard. Each partition is named after
the table and the period it holds, `<table>_p<YYYYMM>` for a month (`<table>_p<YYYY>`
for a year, `<table>_p<YYYYMMDD>` for a day); a name longer than 63 bytes raises
`ValueError` before anything runs. Bounds are midnight UTC; a `DateTime(tz=False)` column reads them
as plain midnight. From a revision of the structural tree:

```python
def upgrade() -> None:
    op.ensure_range_partitions("note_events", "2026-01-01", "2027-01-01", interval="month")


def downgrade() -> None:
    op.detach_range_partitions("note_events", "2026-01-01", "2027-01-01", interval="month", drop=True)
```

The downgrade is written by hand: `ensure_range_partitions` leaves existing partitions
alone, so only the revision's author knows which of the range it created, and the
operation has no reverse (`EnsureRangePartitionsOp.reverse()` raises
`NotImplementedError`). The downgrade above drops the whole range with its rows, which is
right only when the upgrade created every partition of it.

From Python, for instance a scheduled job that keeps the next months ready, with the
migrator's URL or an open connection of the migrator:

```python
from loom.core.repository.sqlalchemy.rls import ensure_range_partitions

created = await ensure_range_partitions(
    MIGRATOR_URL, application, NoteEvent, date(2026, 1, 1), date(2026, 7, 1), interval="month"
)
# ("note_events_p202601", ..., "note_events_p202606"); () when they all exist
```

Both call the guard's `create_range_partition(parent, name, lower, upper)` once per
period, with the name and the bounds as bound parameters. It opens the hatch, creates the
partition, and protects it with the parent's registered scopes and privileges, all in
the caller's transaction; a partition that already exists as a protected partition of
the parent is left alone, any other relation with that name is refused. With a URL the
call commits once at the end; with a connection, nothing is committed and the
transaction stays the caller's. A partition takes the parent's policies, owner trigger
and grants; a read or write through the parent is filtered by the parent's policies, and
a read straight from a partition by the partition's own.

Retention detaches, and with `drop=True` drops, the partitions covering a period:

```python
from loom.core.repository.sqlalchemy.rls import detach_range_partitions

await detach_range_partitions(
    MIGRATOR_URL, application, NoteEvent, date(2025, 1, 1), date(2025, 2, 1), drop=True
)
```

The guard's `detach_partition(parent, name, drop)` opens the hatch, unprotects the
partition, runs `ALTER TABLE ... DETACH PARTITION` and drops it when asked. A partition
that no longer exists is skipped. A detached table that is kept is a plain table of the
schema: no row-level security, no group grants, reachable only by the owner and the bypass
users, for archiving; drop it by hand later, since the guard refuses to drop a table that
is no longer a partition. `DETACH PARTITION` takes an `ACCESS EXCLUSIVE` lock on the
parent; `CONCURRENTLY` cannot run in a transaction and is not used.

Only the owner, so the migrator, may create or detach partitions. Both functions refuse
any session that is not a member of the owner, the application users hold no `EXECUTE`
on them and no `CREATE` on the schema, and `ensure_range_partitions` and
`detach_range_partitions` refuse a connection that does not act as the owner inside the
application schema. Both functions need guard revision 2: on an older guard they raise
`ConfigError` (`guard revision pending`) until the bootstrap has run with this release.

Partitions are not in the model. Alembic's autogenerate never proposes to drop them,
because loom's `include_object` skips a table the database has and the models do not;
`check` and `verify` count a registered partition of a partitioned scoped table as part
of its parent, and `verify` reports `partition.registration` when a partition is
registered with other scopes or privileges than its parent. Dropping the partitioned
table in a revision, including the downgrade of the revision that created it, drops its
partitions first with `op.drop_partitions("note_events")`, which the hook emits.

### What the guard enforces

The guard schema, everything in it and its two event triggers belong to the installer,
the superuser or `rds_superuser` member that ran the first bootstrap. Only the owner holds
anything on it: `USAGE`, `SELECT` on its configuration and registry, and `EXECUTE` on
the functions a migration calls, such as `open_hatch`, `protect_scoped_table`,
`unprotect_scoped_table`, `grant_table`, `create_range_partition`, `detach_partition`
and `assert_scoped_schema`. Of those, `protect_scoped_table` and
`unprotect_scoped_table` are the only ones that write its registry; the two partition
functions write it through them. Two event triggers, enabled `ALWAYS`, run its checks: `ddl_command_end`
rejects DDL on the guard schema outside the bootstrap, then, unless the hatch is open,
runs the assertion for every DDL that touches the guard or the application schema and
for every DDL that reports no schema, such as `GRANT` and `REVOKE`; `sql_drop` rejects
DDL that drops guard objects outside the bootstrap and the drop of a registered table,
then, unless the hatch is open, runs the assertion whenever the drop removes an object
of the application schema, so `DROP POLICY` or `DROP TRIGGER` on a scoped table fails
in the same statement while dropping a plain index still succeeds (since guard revision
2; on revision 1 such a drop only failed at the next DDL). The hatch never admits DDL
on the guard itself.

Every call loom makes into the guard (the bootstrap, the runners and their Alembic
operations, `create_schema`, `check` and `verify`) first sets the transaction's
`search_path` to the guard, `pg_catalog` and `pg_temp`, so an unqualified guard function
always resolves to the guard and never to a routine of the application schema; the
guard's own functions pin the same `search_path`. A routine in the application schema
named like a guard function is itself a violation.

The assertion fails with `LG002` when any of these stops holding:

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
- no unregistered table of the schema has row-level security enabled or forced, a
  policy, or a trigger named `loom_*`;
- no routine of the schema has the name of a guard function;
- the schema and every relation in it are owned by the owner, never by a superuser or a
  bypass role, and there is no materialized view;
- no non-bypass member of a schema group is a member of a bypass role;
- no role other than the migrator, superusers aside, is a member of the owner.

### Why the guard is static SQL

The guard is two files shipped inside loom, `rls/guard/preflight.sql` and
`rls/guard/0001.sql`, byte-identical for every schema and every product. Nothing in them
is rendered: the schema, the roles, the groups and the version tables reach Postgres as
bound parameters and live in a configuration table inside the guard. loom's Python code
builds no SQL from runtime values either; its statements are literals with bound
parameters, and a lint in loom's test suite enforces it. The DDL fragments a product
declares on its models, the `__checks__` expressions and the `__partial_unique__`
predicates, are product code with the same trust as a hand-written Alembic revision,
compiled into the table's DDL as written. Each one must be a static literal that
carries no runtime input; loom checks only that it is a non-empty string, so keeping
it static is the product's responsibility. The lint exempts exactly the two calls that
compile them and fails on any other.

- **What runs is what was reviewed and released.** Each file is pinned by its SHA-256 in
  {mod}`~loom.core.repository.sqlalchemy.rls.guard_manifest`. The bootstrap refuses a
  packaged file whose digest differs and records the digest of every revision it
  applies, so the text reviewed in a pull request is the text that runs, with no
  template in between.
- **Tampering with a released revision is detected in CI.** loom's tests compare every
  released revision and the preflight with their pinned digests and reject a revision
  file the manifest does not list. On every pull request, CI also checks that each guard
  file shipped by every release tag is still byte-identical and that every digest it
  pinned is still in the manifest; before publishing, the release workflow runs the same
  check of the candidate wheel against the last wheel published on PyPI. Release tags
  `v*.*.*` are immutable under a repository ruleset, and `CODEOWNERS` requires the
  maintainers' review for `.github/`, `scripts/ci/`, the guard files and the manifest.
- **Tampering with the installed guard is detected.** Each released revision pins six
  catalogue digests besides its file digest: `functions` (name, arguments, result,
  language, security, volatility, strictness, leakproofness, parallel safety, kind and
  source), `relations`, `columns`, `constraints`, `triggers` (rules included) and
  `objects`. The functions identify the revision; the other categories must match that
  revision's digests, and `objects` must be empty, so an operator, type, collation,
  operator class, text search object or statistics object added to the guard is refused.
  {func}`~loom.core.repository.sqlalchemy.rls.integrity.guard_problems` also requires
  every object of the guard to belong to the guard schema's owner, the grants to be
  exactly the owner's (`USAGE` on the schema, `SELECT` on the configuration and registry,
  `EXECUTE` on the functions above) and nothing to anyone else, every function's settings
  to be exactly `search_path=pg_catalog, <guard>, pg_temp` (plus `log_statement=none` on
  `set_password_verifier`), and both event triggers enabled `ALWAYS` on the guard's
  handlers and owned by the guard schema's owner. Startup in `external` mode runs it as
  the application user, on everything the catalogue lets that user read, together with
  the protection of each scoped table, and refuses to serve. `verify` runs it with the
  guard schema's owner compared to the installer recorded in the configuration row, and
  additionally checks that row against the declaration, the registry against the models
  and the memberships of the groups; it reports each difference as a finding. The
  bootstrap runs it before committing.
- **Against a database superuser it is detection, not prevention.** A superuser, or the
  installer, can disable an event trigger, mark a session as installing, or rewrite a
  function. The guard makes such a change visible at the next startup or `verify`; it
  cannot stop it. Use that credential for the bootstrap only.

### `check` versus `verify`

| | {func}`~loom.core.repository.sqlalchemy.migrations.check` | {func}`~loom.core.repository.sqlalchemy.rls.verify` |
|---|---|---|
| Question | do the models, the migrations and the registry agree? | does the database hold what the declaration says? |
| Compares | tables and columns (Alembic autogenerate), registered tables against the model | the installed guard against the released one (`guard.*`: the six catalogue digests, owners against the recorded installer, grants, function settings, the two event triggers, the configuration row including the installer), the registry and each scoped table's protection, the assertion, then what the assertion cannot require: presence of group and bypass privileges, exactly `SELECT` for bypass users on the structural version table, sequence `USAGE`, `__privileges__` on global tables, C9 actions and group privileges, memberships against `access`, undeclared members of the groups (`membership.undeclared`), the migrator's no-inherit membership, owner and migrator outside bypass roles |
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
| `LG002` | the assertion, `ddl_command_end`, `sql_drop`, `COMMIT` of a transaction that opened the hatch, C5 in `protect_scoped_table`, `unprotect_scoped_table` | a guard invariant is broken, the message naming the object to repair; DDL on the guard schema outside the bootstrap; or an unprotect without the hatch |
| `22023` | `protect_scoped_table`, the bootstrap | an invalid declaration, a `char(n)` scope column or one with a nondeterministic collation, or a server below Postgres 14 |
| `42501` | `open_hatch`, `protect_scoped_table`, `unprotect_scoped_table`, the bootstrap | wrong schema, wrong owner, a caller that is not a member of the owner, prior policies, a table not registered, a role that exists and this guard did not create, a recorded role that differs from the declaration, a reserved role prefix, an unexpected or `ADMIN` membership, a guard or application schema the bootstrap does not own, or a bootstrap by another installer |
| `ConfigError` | startup, `load_application`, `apply_bootstrap`, `create_schema`, the runners, the guard operations, `check` | a missing key or name, an invalid name or lock timeout, a missing or malformed scope binding, a missing guard or guard event trigger, a guard that differs from the released one, a guard revision this release does not know or below the minimum compatible one (`guard revision pending`), a guard operation outside `run_migrations`, a bootstrap without superuser, a wrong credential, a superuser or bypass application connection, a dialect that cannot protect, or drift; messages that call for a bootstrap point at `apply_bootstrap` |
| `ValueError` | `compile_all`, the revision hook, `BootstrapConfig.validated`, `SchemaNames.derived` | a compile rule, an FK whose target is not compiled with it, a revision that would leave a table unprotected, or an invalid name |

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
  acts are out of scope: opening the hatch, changing the guard as a superuser (detected,
  not prevented; see [Why the guard is static SQL](#why-the-guard-is-static-sql)),
  `CREATE TABLE AS` followed by `GRANT`, publications, settings.

- **What the guard does.** It detects accidental drift by those credentials (hand DDL,
  an incomplete revision) in the same DDL that causes it, and closes every path of the
  application user.
- **Session values.** The `loom.scope.*` keys are settings any session can set.
  Row-level security defends against missing or wrong filters in application code, not
  against arbitrary code or SQL injection inside the application process (obligation
  P5). The hatch is not a setting: only a member of the owner can open it.

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
- **Foreign keys from other schemas.** The assertion inspects the application schema
  only, so a foreign key declared in another schema towards a scoped table is not
  reported. It needs `REFERENCES`, which loom never grants.
- **Exclusion constraints.** C5 requires the boundary among the key columns of an
  `EXCLUDE` constraint, but not that it is compared with `=`.
- **SCRAM verifiers.** Passwords are hashed on the client without SASLprep, so a
  password that SASLprep would change may not authenticate. Use ASCII passwords, which
  SASLprep leaves unchanged, or leave the user out of `passwords` and set a verifier you
  computed yourself. The iteration count is `scram_iterations` (see
  [The bootstrap](#the-bootstrap)).
- **Removed users.** Removing a user from `database_users` revokes what it held but
  leaves its role in place; see
  [Adding users and upgrading loom](#adding-users-and-upgrading-loom).
- **Process-wide registries.** Scope sources and the authorization product are
  registered per process, not per application.
- **`elevate` inputs.** `elevate` trusts the `catalog=` it receives and does not
  check the decision's subject or expiry; the product evaluates the decision for
  the current identity just before elevating.
- **Test aids.** `register_authz_product` and `clear_authz_product` exist for tests;
  production code publishes the product through the `loom.authz` entry point.
- **`database.schema` as a string.** loom 2.11 ignored a scalar `database.schema`;
  from 2.12 the key is a section, so a string value fails at startup with a
  `ConfigError`. Move a schema name to `database.schema.name`.
- **Discovery imports.** `load_application` reuses the server's discovery, which
  recognises REST interfaces only when the product's modules have already imported
  `loom.rest.model`; it never imports it itself.

### A broken guard blocks DDL on its schema and every schemaless DDL

Each guard asserts its own schema for every DDL that touches its application schema or
its guard schema, with no tag list. Postgres reports no schema for some commands, such
as `GRANT` and `REVOKE`, so no filter could tell whether they touched the schema; the
guard asserts on every one of them too. DDL that touches only other schemas does not run
the assertion. The accepted consequence: while one guard is broken, every DDL on its
schemas and every schemaless DDL in the database fails with `LG002` and the schema to
repair in the message, until it is repaired. It is never a permission error, because the
triggers and the assertion are `SECURITY DEFINER`.

### Repairing policy text drift

After a major Postgres upgrade or a dump and restore, the text Postgres returns for a
policy's `qual` or `with_check` may differ from the registered one. The assertion then
fails and, as above, blocks DDL on the schema and every schemaless DDL. Repair each
named table in one transaction, as the migrator:

```sql
BEGIN;
SELECT loom_guard_ledger.open_hatch();
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
| A degraded mode, or an event trigger filtered by command tag | No degraded mode (`ConfigError`); no tag filter (a broken guard blocks DDL on its schemas and every schemaless DDL) |
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
