# SQL API (ClickHouse)

`loom-kernel` provides an injectable `SqlQueryService` for running SQL through named,
config-driven connections, plus an **optional** generic REST endpoint
(`POST /sql/{connection}`). Every query carries one or more roles derived from the
caller's **verified JWT claims**, intersected with a per-connection allowlist and
applied as per-query settings, so roles never leak across queries sharing the
connection pool.

```{important}
Through the REST endpoint the roles come from the identity, never from the request
body: the body can only **narrow** what the claim already grants. The allowlist is the
ceiling of the connection — the last barrier, not a per-caller permission. Read
{ref}`sql-threat-model` before exposing the endpoint.
```

The first supported backend is ClickHouse. The `sql:` section is backend-agnostic:
each connection declares its `backend` explicitly.

---

## Install

```bash
pip install "loom-kernel[clickhouse]"
# and, if you authenticate the endpoint with the native JWT middleware:
pip install "loom-kernel[jwt]"
```

Minimum versions (enforced fail-closed at startup):

| Component | Minimum | Why |
|-----------|---------|-----|
| ClickHouse server | 24.4 | Per-query `role` HTTP parameter |
| `clickhouse-connect` | 0.9.2 | `role` as a per-query transport setting |

At startup the registry asserts driver support and runs a sentinel-role probe
(`SELECT 1` with a nonexistent role must be rejected with code 511). If the server
silently accepts the sentinel role, startup **aborts** — the server's role enforcement
is never assumed. A connection whose `allowed_roles` holds more than one role also
verifies that the driver can emit repeated `role` HTTP parameters (see
{ref}`sql-multi-role`); if it cannot, startup aborts instead of sending an invalid
single role.

---

## Configuration

Add a top-level `sql:` section. Absent section → zero changes: no connection is
opened and no route is mounted (`SqlQueryService` resolves to a null implementation
that raises an actionable `ConfigError` on first use).

```yaml
sql:
  connections:
    analytics:
      backend: clickhouse
      url: ${oc.env:CLICKHOUSE_ANALYTICS_URL}   # secret via env/SSM — never inline
      # Ceiling for the CONNECTION, never a per-caller permission: the effective
      # roles are these intersected with the caller's verified roles claim.
      allowed_roles: [role_viz_reader, role_viz_sales]
      default_role: role_viz_reader   # required if sql_endpoint.enabled and no allowlist
      readonly: true                  # default: readonly=1 on every query
      default_limit: 1000
      max_limit: 10000
      max_execution_time: 30          # seconds
      max_sql_bytes: 262144
      connect_timeout: 10
      send_receive_timeout: 60
      executor_threads: null          # driver default when null
      pool_size: null                 # driver default when null
      settings: {}                    # extra ClickHouse settings — can never override policy
      sql_endpoint:
        enabled: true                 # opt-in (default false)
        auth: identity                # REQUIRED to mount: 'identity' or 'external'
        path: /sql/analytics          # default: /sql/{connection}
        include_in_schema: false
```

The claim carrying the caller's roles belongs to the authentication mechanism, not
to the connection — it is declared once for the whole app and is **required**
whenever a mounted endpoint has a non-empty `allowed_roles`:

```yaml
app:
  rest:
    auth:
      jwt:
        audience: loom-api
        roles_claim: loom_sql_roles   # verified claim carrying the caller's roles
```

Connections open inside the app lifespan and close on shutdown. A connection that
fails its startup probe aborts the whole app start (explicit fail-fast).

Three config rules make the unsafe shapes **unrepresentable** — they fail at parse or
at startup, never at request time:

| Rule | Why |
|------|-----|
| A mounted endpoint with a non-empty `allowed_roles` requires `auth: identity` | It is the only mode carrying a verified identity to bind the roles to |
| That endpoint also requires a mechanism that binds roles (for JWT, `app.rest.auth.jwt.roles_claim`) | Otherwise any authenticated caller could pick any allowlisted role |
| `auth: identity` requires an authentication mechanism, `app.rest.auth.jwt.audience` when that mechanism is JWT, and the mounted path must not appear in the auth `exclude_paths` | An identity-bound role is worthless if tokens minted for another service are accepted, or if the path skips authentication altogether |

```{note}
The `sql:` section is independent from the ETL `storage.clickhouse` section — they
serve different purposes (interactive queries vs. pipelines). Share the DSN through a
single environment variable if both point at the same cluster.
```

### Query from a use case, as the caller

Two collaborators are always registered in the container (APPLICATION scope) and are
injected by constructor, like any other service:

| Collaborator | Roles come from | Use it for |
|--------------|-----------------|------------|
| `CallerBoundSql` | The verified identity ∩ `allowed_roles` | Anything done **on behalf of a caller** — the default |
| `SqlQueryService` | A `roles` argument written by the calling code | System work with **no caller**: a scheduled job, a migration, a health probe |

`CallerBoundSql` is the default because it is the only one whose privileges are decided
by the caller's own entitlements. It has no `roles` parameter in any form:

```python
from loom.core.identity import Identity
from loom.core.sql import CallerBoundSql
from loom.core.use_case.markers import Caller
from loom.core.use_case.use_case import UseCase


class TopProductsUseCase(UseCase[Product, dict]):
    def __init__(self, sql: CallerBoundSql) -> None:
        self._sql = sql

    async def execute(self, identity: Identity = Caller()) -> dict:
        result = await self._sql.execute(
            "SELECT product, sum(amount) AS total FROM sales "
            "WHERE day >= {start:Date} GROUP BY product ORDER BY total DESC",
            connection="analytics",
            identity=identity,
            parameters={"start": "2026-01-01"},
            limit=100,
        )
        return {"rows": result.rows, "has_more": result.has_more}
```

```{warning}
The identity **must** be declared with `Caller()`. That marker is what makes the
executor inject the identity the transport verified. Written without it —
`async def execute(self, identity: Identity)` — it is an ordinary primitive parameter,
which the compiler binds from the `params` the calling code supplies: the caller then
picks its own identity, and with it the roles. On the agent path those `params` are
tool arguments the **model** writes, and `identity` would appear in the tool schema.
A query bound to a forged identity is not bound at all.
```

A caller holding no allowlisted role for that connection is refused with
`RolesNotBoundError` before the backend is touched — never run as `default_role`. So is
a connection with an empty `allowed_roles`: its only role is the shared one, which is
derived from nobody. Use `SqlQueryService` for that unbound work.

Parameters are always bound **server-side** (`{name:Type}` placeholders) — never
interpolated into the SQL string.

```{note}
`SqlQueryService.roles` takes a sequence: a query may carry several roles and runs with
the **union** of their privileges. Each one is validated against `allowed_roles`; a
single rejected role refuses the whole call instead of silently narrowing it. That
service applies **no identity binding**: the roles are whatever the calling code wrote,
bounded only by the allowlist. Identity binding is what `CallerBoundSql` and the REST
edge add on top.
```

---

## The REST endpoint

A connection mounts `POST /sql/{connection}` only with **double opt-in**:
`sql_endpoint.enabled: true` **and** an explicit `sql_endpoint.auth` value. Without
`auth`, the endpoint does not mount.

| `auth` value | Meaning |
|--------------|---------|
| `identity` | Requires a configured authentication mechanism — the built-in `app.rest.auth.jwt` section (with a validated `audience`) or a `create_app(authenticator=...)` of your own; startup fails with `ConfigError` otherwise. The only mode that can bind roles to an identity |
| `external` | Explicit acknowledgement that the operator provides authentication in front of the app. No identity the framework can read, therefore no allowlist: single-role endpoint only |
| `jwt` | Deprecated alias of `identity`, kept for existing configs. It named a mechanism where the contract only needs "the framework knows the caller" |

`auth` decides **who gets in**; the authentication mechanism decides **what they may
become** once in — for JWT, through `app.rest.auth.jwt.roles_claim`.

Request body — backend settings are rejected by schema:

```json
{
  "sql": "SELECT id, name FROM products ORDER BY id",
  "roles": ["role_viz_reader"],
  "parameters": {"start": "2026-01-01"},
  "limit": 100,
  "offset": 0
}
```

`roles` is optional and can only **narrow**: it must be a subset of the roles the
verified claim already grants. Omitting it runs the query with all of them. Asking for
a role the identity does not hold is a 403, even if the role is allowlisted.

### Response envelope

Every query returns the same tabular envelope (`SqlQueryResult`):

```json
{
  "columns": [
    {"name": "id", "type": "UInt64"},
    {"name": "name", "type": "String"}
  ],
  "rows": [[1, "widget"], [2, "gadget"]],
  "row_count": 2,
  "limit": 100,
  "offset": 0,
  "has_more": false,
  "elapsed_ms": 12.4
}
```

Column types are the native backend names. Serialization covers
datetime/date/UUID/Decimal natively, IPv4/IPv6 as strings, `bytes` as base64, and a
documented `str()` fallback for exotic types — a result never produces a bodyless 500.

### Pagination

- Effective limit: `min(limit or default_limit, max_limit)`; the executor fetches
  `limit + 1` rows to compute `has_more` and trims the extra row.
- `offset` skips rows; both are applied as native query settings — the SQL text is
  never rewritten.
- Backstop: `max_result_rows = max_limit + 1` with `result_overflow_mode='throw'` —
  oversized results fail loudly instead of truncating silently.

```{warning}
Stable pagination requires an explicit `ORDER BY` in your SQL. Each page re-executes
the query; without a deterministic order, rows can repeat or disappear between pages.
```

### Errors

Errors use the standard framework body (`detail.code`, `detail.message`,
`detail.trace_id`):

| Status | When |
|--------|------|
| 400 | Body that is not well-formed JSON (syntax error, bytes that are not UTF-8, empty) — `code: bad_request`, fixed message, the body is never echoed |
| 401 | Missing, expired or otherwise invalid token (emitted by the JWT middleware, before the endpoint) |
| 404 | Unknown connection name |
| 403 | The verified identity grants no allowed role, the body asks for a role the identity does not hold, or the role is outside the allowlist |
| 422 | SQL rejected by the backend (sanitized first line, no host/DSN), well-formed body that does not fit the schema, or SQL larger than `max_sql_bytes` |
| 500 | Backend unreachable — generic message, no URL leaked |

Every 403 caused by identity binding carries the same generic message on purpose (no
oracle telling an attacker which part of their token failed); the precise reason —
connection, subject and cause — is logged server-side at WARNING.

---

## Role model and grants

The policy is fail-closed and resolves in this order for a request reaching a mounted
endpoint whose app declares a `roles_claim`:

1. **Identity.** No verified claims in the request → 403. The claim must be a string
   or a list of non-empty strings; absent, empty or otherwise typed → 403. Nothing is
   coerced.
2. **Intersection.** The authorized set is the claim values ∩ `allowed_roles`. Roles
   the caller claims but the connection does not allow are dropped; if nothing
   survives → 403. `default_role` is **never** a fallback here.
3. **Narrowing.** A `roles` list in the body must be a subset of the authorized set;
   anything else → 403. Absent, it runs with the whole authorized set.
4. **Allowlist, again.** The service re-validates every effective role against
   `allowed_roles` before the executor is touched — the last barrier, independent of
   how the roles were resolved.
5. The effective roles are sent as per-query settings — nothing leaks from one query
   to the next across the shared connection pool.

The invariant, stated once: **identity and body may only ever restrict the set of
roles, never widen it.**

Connections used directly from a use case (no HTTP) skip steps 1-3: there is no
identity to bind, and `allowed_roles` plus `default_role` are the whole policy.

Provision ClickHouse with exactly these statements per data role and connection user:

```sql
-- The data ROLE needs:
GRANT SELECT ON <db>.* TO <role>;
-- (SHOW TABLES/COLUMNS only if callers need introspection without SELECT)

-- The connection USER needs:
GRANT <role> TO <user>;                 -- one per allowlisted role
ALTER USER <user> DEFAULT ROLE NONE;    -- no privileges when no role is applied
```

| Principal | Grant | Purpose |
|-----------|-------|---------|
| Data role (`role_viz_reader`, ...) | `GRANT SELECT ON <db>.*` | Read the data it exposes |
| Connection user | `GRANT <role> TO <user>` per allowlisted role | May activate exactly the allowlisted roles |
| Connection user | `ALTER USER ... DEFAULT ROLE NONE` | Fail-closed: no privileges without an explicit role |
| Startup probe | — | Needs no grant (sentinel role must not exist) |

Note the direct consequence of row two: the connection user must hold **every**
allowlisted role for any of them to be assumable, so ClickHouse alone would happily
grant a caller the most privileged role it is asked for. What stops that is the claim
binding above, not the server. Keep the allowlist as small as the roles your identity
provider actually issues.

---

(sql-multi-role)=

## Several roles in one query

A caller whose claim grants more than one role runs each query with the **union** of
their privileges — ClickHouse activates every role sent with the query:

```json
{"sql": "SELECT * FROM sales JOIN customers USING (id)", "roles": ["role_sales", "role_crm"]}
```

Mechanically, roles travel as **repeated HTTP parameters** (`role=a&role=b`); a
comma-joined value is read as a single role name and rejected with code 511.

```{note}
`clickhouse-connect` 0.15.1 (and upstream `main`) builds the request URL with
`urlencode(params)` without `doseq=True`, so a sequence value would be sent as its
Python repr. The driver boundary
(`loom.core.sql.clickhouse._client`) rebinds that encoder to the `doseq=True` variant:
output is byte-identical for the scalar settings the driver builds today and only
sequences change — and their current encoding is invalid anyway. A startup check
(`ConfigError`) and a unit test pinning the patch point make sure the workaround can
never degrade silently, and it is removed as soon as upstream encodes lists correctly.
```

Single-role connections never depend on that workaround: one role is sent as a plain
scalar, exactly as before.

---

(sql-threat-model)=

## Threat model

```{danger}
**`allowed_roles` is the ceiling of the connection, not a per-caller permission.**
What makes it safe is the identity binding described here. Read this section in full
before mounting the REST endpoint.
```

### What the framework now guarantees

**1. Roles come from a verified identity.** The JWT mechanism verifies signature,
`exp`/`nbf`, `sub` and the configured `aud`/`iss`, then projects the claims onto an
{class}`~loom.core.identity.Identity` — roles from `roles_claim`, string
claims as attributes. The endpoint intersects the roles that identity holds with
`allowed_roles`. The request body cannot select a role: it can only narrow the result.
A caller holding no allowlisted role gets a 403 — never `default_role`. Swapping the
mechanism changes nothing here: the endpoint sees an identity, not a token.

**2. The unsafe shapes are unrepresentable.** A mounted endpoint with a non-empty
allowlist and an auth mode other than `identity` fails at config parse; the same
endpoint without a role-binding mechanism, without `audience` under JWT, or with a
mounted path listed in the auth `exclude_paths`, fails at startup. There is no runtime
path into the old behaviour.

**3. `audience` is mandatory.** Without a validated `aud`, any token signed by the same
key — including one minted for a different service, carrying its own idea of a roles
claim — would be accepted and could name your ClickHouse roles.

**4. Auditability.** Each request emits a span labelled with the effective `roles` and
the caller `subject`; denials are logged at WARNING with connection, subject and cause.
`CallerBoundSql` opens a span of its own for every query it accepts, carrying the same
`roles`, `subject` and `mechanism` labels. It is a different span from the endpoint's,
under a narrower scope and its own name, so a use case acting on behalf of a caller is
auditable on the same terms rather than through the same record.

**Where this comes from.** Measured on ClickHouse 25.3 with a single credential and
only the `role` parameter changed: the default role returned 497 `ACCESS_DENIED`, while
`role=role_viz_reader` returned 1,171,206 rows from a table holding PII. When `role`
travelled in the request body, that was one JSON edit away for any bearer of any valid
token.

### What is still on you

**1. The claim is only as good as its issuer.** The framework verifies the signature
and the audience; it cannot tell whether your identity provider was right to put
`role_viz_sales` in that token. Issue the claim from the same system that owns the
business authorization, and keep token lifetimes short — verification is stateless, so
a role revoked at the IdP stays usable until the token expires.

**2. There is still no per-route authorization.** `JwtAuthMiddleware` is mounted for the
whole application. A valid token reaches every mounted SQL route; what it can *do*
there is now bounded per route by that connection's allowlist ∩ the claim, but the
authentication decision itself remains all-or-nothing.

**3. ClickHouse is the floor, not the ceiling.** The server rejects unknown roles (511
`UNKNOWN_ROLE`) and non-granted roles (512 `SET_NON_GRANTED_ROLE`), which protects
against typos and role injection. It cannot distinguish callers: the connection user
holds every allowlisted role by design. Identity separation exists only because the
framework applies the intersection above.

**4. The unbound path is still reachable.** `SqlQueryService` remains registered, and
its `roles` argument is bound to nobody: a use case that injects it queries with any
allowlisted role, whatever its caller holds. That is the point — system work has no
caller — but nothing stops it being injected where `CallerBoundSql` belongs. Treat the
choice as a review item: inside a request or an agent tool, the bound one is the answer.

**5. `allowed_roles` should list composite roles**, one per exposed profile, not
individual data roles — so the union any caller can reach matches a declared profile.

```yaml
app:
  rest:
    auth:
      jwt:
        audience: loom-analytics
        roles_claim: loom_sql_roles                   # what each caller may use

sql:
  connections:
    analytics:
      backend: clickhouse
      url: ${oc.env:CLICKHOUSE_ANALYTICS_URL}
      allowed_roles: [role_api_viz, role_api_sales]   # ceiling of the connection
      sql_endpoint:
        enabled: true
        auth: identity
```

Without an identity provider that can issue the claim, the single-role shape stays
valid and needs no binding: `allowed_roles: []` plus a fixed `default_role` runs every
caller with exactly one role — no escalation, and no per-caller distinction either.
The binding is decided **per connection**: a single-role connection keeps working
unchanged in an application that also serves a multi-role one.

---

## Hardening

- **Never expose the endpoint without authentication.** The `auth` field is
  mandatory to mount; `external` is an explicit acknowledgement, not a bypass. Every
  mounted endpoint emits a startup warning naming its path, connection, readonly flag,
  auth mode, role count and the mechanism its roles are bound to.
- **Keep the allowlist to the roles your IdP actually issues.** It is the ceiling of
  what any claim can unlock on that connection: a role nobody should reach through
  HTTP does not belong in it (see {ref}`sql-threat-model`).
- **Issue the roles claim from the system that owns the authorization**, and keep
  token lifetimes short: verification is stateless, so a revoked role remains usable
  until the token expires.
- **No SOURCES grants for the connection user.** Table functions such as `url()`,
  `s3()`, `remote()`, `mysql()` and `postgresql()` enable SSRF and data exfiltration
  even under `readonly=1`. Do not grant them.
- **No broad `system.*` access.** Introspection tables leak topology, settings and
  query history.
- **Keep `DEFAULT ROLE NONE`** on the connection user so a query without an applied
  role has no privileges.
- **`readonly: true` is the default.** Disabling it while the endpoint is enabled
  emits a startup warning, and the driver runs with `query_retries=0` to avoid
  re-executing non-idempotent statements.
- **Credentials via `${oc.env:...}` / SSM / Secrets Manager** — never inline. The
  password is redacted from logs, reprs and errors; SQL is logged only at DEBUG
  (truncated), parameters never.
- **Rate limiting is the operator's responsibility** (reverse proxy / API gateway).
- Minimum versions: ClickHouse ≥ 24.4 and `clickhouse-connect` ≥ 0.9.2 — older
  combinations fail the startup probe.

---

## Authentication

`sql_endpoint.auth: identity` requires an authentication mechanism. Any
{class}`~loom.rest.auth.Authenticator` satisfies it — see
{doc}`identity` for a non-JWT one. The batteries-included mechanism is the
framework's native stateless JWT, configured under `app.rest.auth.jwt`:

```yaml
app:
  rest:
    auth:
      jwt:
        secret_path: ${oc.env:LOOM_JWT_SECRET_PATH}   # HS256 — a path, not the value
        # public_keys:                      # RS256/ES256/EdDSA — one PEM per key id
        #   "2026-08": ${oc.env:LOOM_JWT_PUBLIC_KEY_PEM}
        algorithms: [HS256]                 # explicit allowlist; 'none' always rejected
        audience: loom-api                  # REQUIRED by any sql_endpoint with auth: identity
        issuer: null                        # validated only when set
        leeway_seconds: 0
        exclude_paths: [/docs, /redoc, /openapi.json, /metrics, /health]
        roles_claim: loom_sql_roles         # REQUIRED by any mounted sql_endpoint whose
                                            # connection has a non-empty allowed_roles
```

- **Stateless** — signature, `exp` and `sub` (both required) plus `nbf`, and `aud`/`iss`
  when configured, are verified per request. No Redis or server-side session store is
  needed; early token revocation is out of scope.
- **`sub` is mandatory.** A token without a subject carries no identity to authorize
  against, nor to audit afterwards, so it is rejected with 401.
- Exactly one of `secret_path` (HS*) or `public_keys` (RS*/ES*/EdDSA static PEMs, keyed by
  key id) must be set, and the algorithm allowlist must match the key type — validated
  fail-fast at startup.
- **`secret_path` is a path, not the value.** The config is a `msgspec.Struct`, so any
  serializer emits its fields verbatim: holding an HS secret would let a config dump
  publish the key that both verifies *and* signs. Public keys are safe to inline
  because they only verify. The container has to materialize the value to a file:
  Kubernetes mounts a `Secret` as a volume directly, while ECS/Fargate injects
  task-definition secrets as **environment variables only**, so there an entrypoint
  must write the value to a path on a `tmpfs` volume before the app starts.
- With `public_keys`, the key is chosen by the token's `kid` header, never by trying
  each key in turn: exhaustive trial would decouple every algorithm from its key
  family, which is what makes algorithm confusion impossible here. A token with no
  `kid` verifies only when a single key is configured, so a rotation overlap needs the
  issuer to stamp one.
- On success the verified claims are projected onto an
  {class}`~loom.core.identity.Identity` published for the request; on failure
  the response is a 401 with the standard error body, a `WWW-Authenticate` challenge
  and no hint about the cryptographic reason. Read the caller anywhere with
  `current_identity()` — the raw claims are deliberately not exposed.
- **The identity roles are consumed** by the SQL endpoints when `roles_claim` is set:
  that is where an authenticated caller becomes a set of ClickHouse roles
  ({ref}`sql-threat-model`).
- `exclude_paths` bypasses authentication for exact paths (docs, scrape endpoints). A
  mounted SQL path listed there aborts startup — it would serve SQL unauthenticated.
- When the section is present, `create_app` mounts the middleware for the **whole
  app** — authentication is all-or-nothing, there is no per-route authorization. A
  missing `pyjwt` extra fails at startup with an install hint, so the API never starts
  silently unauthenticated.
- `audience` is **required** by any `sql_endpoint` with `auth: identity` (startup fails
  otherwise): without a validated `aud`, any token signed by the same key is accepted
  whatever it was issued for, and could carry the roles claim. Set `issuer` too when
  several issuers share the key material.

### Example token

```json
{
  "sub": "svc-reporting@example.com",
  "aud": "loom-api",
  "exp": 1785600000,
  "loom_sql_roles": ["role_api_viz"]
}
```

The claim may also be a single string (`"loom_sql_roles": "role_api_viz"`). Anything
else — a number, an object, a list holding non-strings — is refused with 403 rather
than coerced.
