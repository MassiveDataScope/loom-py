# Postgres row-level security

Authorization in code answers *may they do this here?*. Row-level security in Postgres
makes the database enforce the same boundary on every row, so a query that forgets a
`WHERE` cannot cross it. loom's part is small and generic: a
{class}`~loom.core.repository.sqlalchemy.session_manager.SessionManager` can begin every
transaction by setting transaction-local Postgres settings taken from a provider you
inject. Your product writes the policies against those settings.

```{contents}
:local:
:depth: 2
```

## The provider

A provider is a callable that returns the settings for the transaction about to start,
or `None` when there is no request context:

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
- A key must look like `prefix.name`, which is what Postgres requires of a custom setting.
  Anything else raises `ValueError`; a value that is not a `str` raises `TypeError`.
- A provider that returns `None` or an empty mapping sets nothing. It never substitutes
  a default value.
- A provider that raises, or returns something invalid, fails the transaction before its
  first product statement reaches the database. The connection is invalidated, so the
  session refuses further statements until you roll it back; the next transaction calls
  the provider again.
- A savepoint (`begin_nested()`) does not call the provider again.
- Without a provider, the manager behaves exactly as it did before.
- A provider on a database other than Postgres is rejected at construction.

## The policy recipe

Write the policy to tolerate a missing setting and to treat it as no tenant.
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
  tenant's rows.
- A transaction without context (startup, a background job, a health probe) sees zero
  rows and gets no error. That is the fail-closed behaviour you want. A policy that
  treats a missing setting as a wildcard, for example with
  `OR current_setting('app.tenant_id', true) IS NULL`, would instead open every row to
  every transaction without context.

The same recipe works for any boundary you can express as a session value, for example
`subject = NULLIF(current_setting('app.subject', true), '')` on a table of personal data.
Never store `''` in a column a policy compares against; a `CHECK (subject <> '')` keeps
the empty string from matching an absent setting.

## The roles

Three database roles, two managers:

```sql
CREATE ROLE app_owner NOLOGIN NOBYPASSRLS;
CREATE ROLE app LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD '...';
CREATE ROLE app_platform LOGIN NOSUPERUSER BYPASSRLS PASSWORD '...';

ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO app;
ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA public
    GRANT SELECT ON TABLES TO app_platform;
```

```python
app_sessions = SessionManager(APP_URL, session_settings=request_settings)
platform_sessions = SessionManager(PLATFORM_URL)
```

- **Owner.** Migrations run as `app_owner`, which owns every table, view and function
  and has neither `BYPASSRLS` nor a login. Ownership matters beyond `FORCE`: Postgres
  evaluates a view's policies as the view's owner, so a view owned by a `BYPASSRLS` role
  would hand every tenant's rows to whoever can select from it. Create views over
  protected tables `WITH (security_invoker = true)` and avoid `SECURITY DEFINER`
  functions that read them.
- **Application.** `app` owns nothing and cannot bypass policies. It can only ever see
  the rows its transaction's settings select. Default privileges keep new tables covered
  without widening grants later.
- **Platform.** Work across every tenant (platform tasks, support tooling) goes through
  a second manager with `app_platform` and **no provider**. loom does not distinguish the
  two; the separation is configuration you make explicit, and you should audit every use
  of the platform manager.
- Never reuse the application manager with a special tenant value to mean "everyone".
- Never set a session-scoped value (`SET`, or `set_config(..., false)`) on the
  application pool: it would survive the transaction and leak to the next client of the
  pooled connection.

## Limits

- The settings apply to transactions opened through the manager's `Session`. A raw
  `engine.connect()` does not run the provider, and neither does a `create_all` that runs
  at startup. With a non-owner application role, the schema must already exist, created
  by the platform role or by migrations.
- Postgres only. The mechanism relies on `set_config` and custom settings.
- The manager rejects `isolation_level="AUTOCOMMIT"` at construction, because autocommit
  would discard the settings right after they are set. A per-statement
  `execution_options(isolation_level="AUTOCOMMIT")` is outside what it can check; do
  not use one on a protected table.
- The values travel as statement parameters, so `echo=True` and the text of a
  `DBAPIError` include them. Pass `hide_parameters=True` to the engine when the tenant
  or subject identifiers are sensitive.
- loom sets values; it never creates or manages policies. Keep them in your migrations,
  next to the tables they protect, and add a test that fails when a table with a tenant
  column has no forced policy, or when any relation in the schema is owned by a
  `BYPASSRLS` role.

## Testing it

loom's own integration tests run against a real Postgres with a forced policy and an
application role without `BYPASSRLS`:

```bash
docker compose -f docker-compose.local.yaml up -d postgres
LOOM_PG_IT_URI=postgresql+asyncpg://postgres:loom@localhost:55432/postgres \
  uv run pytest tests/integration/core/repository/sqlalchemy/postgres -m integration
```

They cover these scenarios: two tenants on a shared pool of four connections
under sixty concurrent transactions, a transaction with no context, a committed
transaction leaving its connection clean, a savepoint, a broken provider and the
rollback that follows, and a `BYPASSRLS` platform manager next to an application one in
the same process.
