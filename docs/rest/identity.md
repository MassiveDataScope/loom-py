# Caller identity and authorization

Loom answers "who is running this?" with one type, {class}`~loom.core.identity.Identity`,
produced by whatever mechanism authenticated the request and consumed by everything
downstream. Business rules never see a token, a header or an ASGI scope, so swapping
JWT for mutual TLS changes the composition root and nothing else.

```{contents}
:local:
:depth: 2
```

---

## The identity

```python
from loom.core.identity import Identity, current_identity

identity = current_identity()

identity.is_authenticated          # False for the anonymous caller
identity.subject                   # stable caller id, "" when anonymous
identity.has_role("role_admin")    # exact match, never a prefix
identity.attribute("email")        # verified string attribute, or None
identity.mechanism                 # "jwt", "api-key", ... — audit only
```

Two accessors fail closed instead of returning `None`:

| Call | Anonymous caller | Authenticated caller missing the value |
|------|------------------|----------------------------------------|
| `require_subject()` | raises `Unauthenticated` (401) | — |
| `require_attribute("email")` | raises `Unauthenticated` (401) | raises `Forbidden` (403) |

The distinction is deliberate: a 401 tells the caller that credentials would help, a
403 tells them they would not.

```{warning}
`repr(identity)` prints attribute **names** but never their values — they are personal
data and must not reach a log through a stray f-string.
```

`current_identity()` never returns `None`: with no authenticated caller it yields
`ANONYMOUS`, so no consumer can mistake "unknown" for "authorized".

---

## Reading the caller from a use case

Declare it in the signature, like every other Loom binding:

```python
from loom.core.identity import Identity
from loom.core.use_case import Caller, UseCase


class SendReceiptUseCase(UseCase[Receipt, None]):
    def __init__(self, mailer: Mailer) -> None:
        self._mailer = mailer

    async def execute(self, receipt_id: int, caller: Identity = Caller()) -> None:
        await self._mailer.send(to=caller.require_attribute("email"), receipt=receipt_id)
```

`Caller()` is a declaration, not an ambient read. The transport hands the identity to
the executor for that one execution, so the same use case behaves identically behind
HTTP and behind a Celery worker, where no context variable would have survived.

```{important}
Binding is **fail-closed**. If a transport executes a use case declaring `Caller()`
without handing an identity over, the execution raises `Unauthenticated` naming the use
case and the parameter. To run without a caller, a transport must pass `ANONYMOUS`
explicitly — the framework never substitutes it.
```

### A policy that narrows a query by owner

The most common use of the caller is not a role check but a data boundary:

```python
from loom.core.repository.abc.query import FilterGroup, FilterOp, FilterSpec, QuerySpec


def owned_by(query: QuerySpec, subject: str) -> QuerySpec:
    """Restrict *query* to the rows the caller owns, whatever else it asked for."""
    owner = FilterSpec(field="owner_id", op=FilterOp.EQ, value=subject)
    existing = query.filters.filters if query.filters else ()
    return replace(query, filters=FilterGroup(filters=(*existing, owner)))


class ListMyOrdersUseCase(UseCase[Order, PageResult[Order]]):
    read_only = True

    async def execute(
        self,
        query: QuerySpec,
        caller: Identity = Caller(),
    ) -> PageResult[Order]:
        return await self.main_repo.find(owned_by(query, caller.require_subject()))
```

The caller controls filters, sorting and pagination; the policy is applied after them,
so a crafted query cannot widen the result set.

---

## Route-level roles

For coarse-grained access, declare the roles on the route instead of writing the check
in every use case:

```python
class ReportsInterface(RestInterface[Report]):
    prefix = "/reports"
    requires_roles = ("report_reader",)          # default for every route
    routes = (
        RestRoute(use_case=ListReportsUseCase, method="GET", path="/"),
        RestRoute(
            use_case=PurgeReportsUseCase,
            method="DELETE",
            path="/",
            requires_roles=("report_admin",),    # route wins over the interface
        ),
    )
```

- Holding **any** declared role is enough; the tuple is a set of alternatives.
- The check runs before the use case is constructed, so a denied caller never causes a
  repository or a session to be resolved.
- A caller without the role — or without an identity at all — gets a `403` with the
  standard error body. The message does not name the required roles: the response must
  not become an oracle for the route's policy.
- Declaring nothing leaves the route open, so this is opt-in and additive.

Use `requires_roles` for "who may reach this endpoint" and `Caller()` for "what this
caller may see". They compose: a route can require a role and the use case can still
narrow the data to the caller's own rows.

---

## Configuring authentication

### The built-in JWT mechanism

```yaml
app:
  rest:
    auth:
      jwt:
        secret_path: ${oc.env:LOOM_JWT_SECRET_PATH}
        algorithms: [HS256]
        audience: loom-api
        roles_claim: loom_roles
```

Claims are projected onto the identity: `sub` becomes the subject, `roles_claim`
becomes the roles, and every other **string** claim becomes an attribute. Registered
claims (`iss`, `aud`, `exp`, `nbf`, `iat`, `jti`) describe the token rather than the
caller and never cross. A malformed roles claim — a number, an object, a list holding
anything but non-empty strings — grants **no** role at all rather than a filtered
subset.

See {doc}`sql` for the full JWT reference and the SQL endpoint startup gates.

### Signing keys from a managed store

`JwtIssuerConfig` accepts the signing key as a filesystem path or as a managed-store
reference — exactly one of the two:

```python
config = JwtIssuerConfig(
    private_key_ref="secrets:/myapp/prod/jwt-signing-key",  # or "ssm:/..."
    algorithm="EdDSA",
    audience="my-api",
    issuer="my-gateway",
    roles_claim="loom_sql_roles",
    kid="2026-08",
)
```

The reference is resolved once, when the issuer loads the key, so the material never
touches disk or config dumps. Rotating the stored value therefore requires a restart.
`key_ref_region` pins the AWS region when it is not in boto3's own chain. Requires the
`loom-kernel[config-ssm]` extra.

### Deriving the verifier from the signing key

The service that both issues and verifies should not configure the public key by hand:
two values that must match are two values that can disagree, and a stale public key
does not fail loudly. Derive it instead:

```python
auth = JwtAuthConfig.from_signing_key(
    "/run/secrets/jwt-signing-key.pem",   # or private_key_ref="secrets:/..."
    kid="2026-09",
    algorithms=("EdDSA",),
    additional_public_keys={"2026-08": previous_public_pem},
)
```

`additional_public_keys` keeps the previous `kid` published for one rotation window —
public material, so unlike the signing key it is safe to carry as a value.

### A mechanism of your own

Implement {class}`~loom.rest.auth.Authenticator` and hand it to `create_app`:

```python
from loom.core.identity import Identity
from loom.rest.auth import Authenticator, RequestCredentials


class ApiKeyAuthenticator:
    name = "api-key"
    provides_roles = True

    def __init__(self, keys: KeyStore) -> None:
        self._keys = keys

    async def authenticate(self, credentials: RequestCredentials) -> Identity | None:
        key = credentials.header("x-api-key")
        owner = await self._keys.owner_of(key) if key else None
        if owner is None:
            return None                      # a refusal, with no reason attached
        return Identity(
            subject=owner.id,
            roles=owner.roles,
            attributes={"email": owner.email},
            mechanism=self.name,
        )


app = create_app("config/app.yaml", authenticator=ApiKeyAuthenticator(store))
```

- `RequestCredentials` exposes headers (case-insensitive), path and peer address —
  and deliberately not the body, which would have to be buffered before deciding
  whether the caller exists at all.
- Returning `None` is a refusal. It carries no reason on purpose: the `401` must not
  say which part of the credentials failed.
- Raising {class}`~loom.rest.auth.AuthenticationUnavailable` means the credentials
  cannot be checked right now — the identity provider's key set, the key store or a
  revocation list is unreachable. The request answers `503` with
  `code: service_unavailable`, a fixed message and the `trace_id`, and no
  `WWW-Authenticate` challenge, so clients keep credentials that are still valid. The
  exception's message and its chained cause go to the server log at `WARNING`, never
  to the caller. Raised outside an authenticator — from a use case, for instance —
  {class}`~loom.rest.errors.HttpErrorMapper` answers `503` with the exception's own
  message, like any `LoomError`. Any other exception from an authenticator keeps
  answering `500`.

  ```python
  try:
      owner = await self._keys.owner_of(key)
  except KeyStoreError as exc:
      raise AuthenticationUnavailable("key store unreachable") from exc
  ```
- `provides_roles` is read by startup gates — a role-based SQL endpoint refuses to
  mount behind a mechanism that issues no role.
- The authenticator argument is mutually exclusive with `app.rest.auth.jwt`: two
  mechanisms would mean two sources of truth for the caller.

Everything else — `requires_roles`, `Caller()`, SQL role resolution — works unchanged,
because none of it knows what a token is.

### Paths served without authentication

Left unset, `exclude_paths` follows the paths the application actually publishes —
`docs_url`, `redoc_url`, `openapi_url`, the metrics endpoint when enabled and
`/health` — instead of a hardcoded list that goes stale the moment an operator moves
Swagger. An explicit list replaces that detection entirely, so it must name `/health`
itself for the readiness probe to stay anonymous.

```{warning}
Exclusions are matched by the **router**, not by string comparison. A route declared
as `/{tenant}` answers `/openapi.json` too, so excluding the schema would serve a
business route with no credentials at all. `create_app` walks the registered routes at
startup and refuses to boot when an exclusion is captured that way.
```

An authenticated application that still publishes its OpenAPI document anonymously
gets a startup warning: the schema lists every route, parameter and field. Set
`app.rest.openapi_url: null` (with `docs_url` and `redoc_url`) in production.

---

## Request limits and CORS

```yaml
app:
  rest:
    max_body_bytes: 1048576        # 1 MiB, applied to every route
    cors:
      allow_origins: ["https://app.example.com"]
      allow_credentials: true
      allow_methods: [GET, POST]
```

- **Body size.** Neither uvicorn nor Starlette caps a request body, so a chunked upload
  with no end takes the worker down. The cap is enforced by a middleware — it covers
  routes the application mounted by hand too — and endpoints with a stricter budget,
  such as the SQL one, apply theirs on top.
- **Pagination.** `?limit=` is clamped to `RestApiDefaults.max_limit` (1000 by
  default) and `?page=` must be a positive integer; anything else answers `400`
  instead of reaching the database as a full scan.
- **CORS** is only mounted when the section exists. `allow_origins: ["*"]` together
  with `allow_credentials: true` fails at config parse: Starlette does not reject that
  pair, it starts reflecting the caller's `Origin` and allowing credentials, which
  turns the wildcard into "any site, with cookies". Preflight `OPTIONS` requests are
  answered before authentication — they carry no credentials by definition — while the
  request that follows is authenticated normally.
- **Trace ids** supplied by the caller are accepted only when they match
  `[A-Za-z0-9._-]{1,128}`; anything else is replaced by a generated one, because the
  value is echoed back and reaches every log line.

---

## Identity in jobs

A context variable does not cross a broker, so the identity travels inside the job
envelope:

```python
class ExportUseCase(UseCase[Export, None]):
    async def execute(self, jobs: JobService = ..., caller: Identity = Caller()) -> None:
        jobs.dispatch(BuildExportJob, payload={"format": "csv"})
```

`dispatch` captures the caller at registration time, the worker republishes it for the
whole task and hands it to the executor, and a job dispatching another job propagates
the same caller onward.

An envelope minted before this contract carries no identity: the job runs without a
caller, and a job declaring `Caller()` fails closed with a message naming what is
missing — rather than running as an unknown caller.

---

## Testing

```python
from loom.core.identity import ANONYMOUS, Identity
from loom.testing.runner import UseCaseTest

caller = Identity(subject="user-1", roles=("report_reader",), mechanism="test")

result = await UseCaseTest(ListMyOrdersUseCase(repo)).with_caller(caller).run()

# Pin the unauthenticated path explicitly:
with pytest.raises(Unauthenticated):
    await UseCaseTest(ListMyOrdersUseCase(repo)).with_caller(ANONYMOUS).run()
```

`GoldenHarness.run(..., identity=...)` takes the same argument. Omitting it on a use
case that declares `Caller()` raises `Unauthenticated`: an authorization test that
forgot to state whose request it is would otherwise be vacuous.

---

## Migrating from `jwt_claims`

`scope["state"]["jwt_claims"]` has been removed. It was a second, transport-shaped
source of truth for a security decision, sitting next to the identity context — and two
sources of truth for who the caller is were the defect this design closes.

| Before | Now |
|--------|-----|
| `request.scope["state"]["jwt_claims"]["sub"]` | `current_identity().subject` |
| `jwt_claims["email"]` | `current_identity().attribute("email")` |
| `jwt_claims.get("loom_roles", [])` | `current_identity().roles` |
| reading claims inside `execute()` | declaring `caller: Identity = Caller()` |

Other renames in the same change:

- `sql_endpoint.auth: jwt` → `auth: identity` (`jwt` still works, with a
  `DeprecationWarning`).
- The `401` error code is now `unauthenticated`, matching
  `ErrorCode`, and the response carries a
  `WWW-Authenticate: Bearer` challenge.
