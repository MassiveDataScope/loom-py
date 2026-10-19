# ADR 0002: Status chosen by the result, 429 and declared query parameters

- Status: Accepted
- Issue: PR #359
- Applies to: loom-kernel 2.20.2 and later; target release 2.21.0
- First consumer: periplo-cloud, control plane, task T9 (grant and role routes)

File and line references in Context and Decision are against loom-kernel
2.20.2.

## Context

periplo-cloud's grant routes need three things loom 2.20.2 cannot express. The
owner chose to add them to loom instead of patching around loom in the
consumer, as the consumer already does for header parameters
(`composition/openapi.py`, `HEADER_PARAMETERS`, which edits the generated
OpenAPI after the fact).

### 1. The success status is fixed per route

- `RestRoute.status_code: int = 200` is one value per route
  (`src/loom/rest/model.py:95`).
- `_execute_route` reads it before running the use case and answers every
  success with it: `status_code = compiled_route.route.status_code`
  (`src/loom/rest/fastapi/router_runtime.py:442`) and
  `MsgspecJSONResponse(content=result, status_code=status_code)` (`:465`).
- OpenAPI documents one success response, `{cr.route.status_code: success_response}`
  (`router_runtime.py:606-608`, `:625`).

An idempotent `POST` that answers 201 when it creates and 200 when the
resource already existed cannot be declared. A use case must not import
FastAPI or know about HTTP, so it cannot return a `Response` either.

### 2. No 429

- `ErrorCode` has no rate-limit member (`src/loom/core/errors/codes.py:20-38`).
- `HttpErrorMapper._STATUS` has no 429 (`src/loom/rest/errors.py:78-91`), and an
  unknown code maps to 500: `self._STATUS.get(error.code, HTTPStatus.INTERNAL_SERVER_ERROR)`
  (`errors.py:105`).
- The only response header the mapper sets is the `401` challenge
  (`errors.py:139-144`).

A use case that refuses a caller for exceeding a budget therefore answers 500.

### 3. Query parameters are QuerySpec or nothing

- A use case receives query parameters only through a parameter typed
  `QuerySpec` (`router_runtime.py:325-338`, `:414-420`).
- Any such route publishes the fixed set `page`, `limit`, `pagination`, `after`,
  `cursor`, `sort`, `direction` (`src/loom/rest/fastapi/openapi.py:44-52`,
  `:170-222`), with `limit` published as `maximum: 1000` whatever the
  configured ceiling (`openapi.py:184`; the ceiling is
  `RestApiDefaults.max_limit`, `model.py:238`, global only: `compiler.py:406`).
- Every other query key becomes an untyped `EQ` filter whose value is guessed
  by `_coerce_scalar` (`router_runtime.py:165-176`, `:235-263`). A key is never
  refused as unknown at the REST layer. Filters are not published in OpenAPI.
- A malformed `page`/`limit` is an `HTTPException(400, str)`
  (`router_runtime.py:266-287`), which `_detail` turns into code
  `bad_request` (`src/loom/rest/fastapi/_errors.py:79-91`). A `limit` above the
  ceiling is clamped, not refused (`router_runtime.py:299-301`).
- The SQLAlchemy query compiler raises `FilterPathError`/`UnsafeFilterError`,
  both `ValueError` (`src/loom/core/repository/sqlalchemy/query_compiler/errors.py:6-30`),
  and `_dispatch_route` answers any non-`LoomError` with 500
  (`router_runtime.py:489-500`). Read in the code, not run end to end.

The consumer's `GET /v1/grants` needs typed equality filters (`membershipId`,
`roleCode`, a `state` enum, `changed_since`), cursor-only pagination and
`limit` 1 to 200, published exactly so in OpenAPI and validated, with an
unknown parameter answered 400 `unsupported_query`
(s2-design § 11.1, § 11.4, § 13, § 16.6; D48, D51).

`ErrorCode.UNSUPPORTED_QUERY` already exists and maps to 400
(`codes.py:35`, `errors.py:87`); today only repositories raise it, through
`UnsupportedQuery(backend, model, reason)` (`src/loom/core/repository/abc/errors.py:9-30`).

### Constraints from the code

- Only `_loom_type.py` may call msgspec or pydantic conversion primitives on a
  boundary value inside `loom.rest`; `tests/unit/test_boundary_calls_containment.py`
  enforces it. `LoomType.from_builtins` converts strictly with msgspec
  (`src/loom/core/model/_loom_type.py:304-309`), so `"50"` does not become `50`.
- The executor binds plain parameters by name and passes an instance of the
  annotated type through untouched (`src/loom/core/engine/executor.py:672-689`,
  `:786-806`). That is how `QuerySpec` reaches a use case today.

## Decision

Three additive changes. No new `RestRoute` field, so the YAML route config
(`src/loom/rest/config.py:99-105`) does not change.

### 1. `Created[T]`: the result says it created

A transport-free wrapper in `loom.core.use_case`:

```python
# loom/core/use_case/outcome.py
@dataclass(frozen=True, slots=True)
class Created(Generic[T]):
    """The use case created ``value``."""

    value: T
    location: str | None = None          # only URI-reference characters, else ValueError
```

- A use case that may create returns `T | Created[T]`. "Created" is a fact
  about the domain operation, not an HTTP status; the use case imports nothing
  from `loom.rest` or FastAPI.
- REST: `_execute_route` answers `Created(v)` with status 201 and body `v`;
  any other result with `route.status_code` as today. One `isinstance` per
  request.
- OpenAPI: when the return annotation contains `Created[X]`, the operation
  documents `201` with the schema of `X` and `route.status_code` with the
  schema of the other members. For `GrantView | Created[GrantView]` both
  responses carry the same `GrantView` schema.
- The REST compiler refuses a return annotation where `Created` wraps a type
  other than one of the remaining union members, or appears twice: the two
  responses would document different bodies for one resource. It also
  refuses `Created` on a route whose `status_code` is already 201. The
  result type and the type `Created` wraps are resolved once and held on
  `CompiledRoute` (`result_type`, `created_type`).
- `LoomRestAdapter` unwraps `Created` the same way (201, the value, and
  `Location` when given). Only the invoker, jobs and Celery receive the
  wrapper as is; a caller that only wants the value reads `.value`.
- The span attribute `status_code` is set before the use case runs
  (`router_runtime.py:446-453`); it keeps recording the declared status.

### 2. `RateLimited`: 429 with `Retry-After`; the limiter stays in the consumer

loom ships the error and its mapping, not a limiter.

```python
# loom/core/errors/codes.py
RATE_LIMITED = "rate_limited"

# loom/core/errors/errors.py
class RateLimited(DomainError):
    """The caller spent its budget; it may retry after ``retry_after`` seconds."""
    def __init__(self, retry_after: float, message: str = "Too many requests.") -> None:
        if not math.isfinite(retry_after):
            raise ValueError(f"RateLimited.retry_after must be finite, got {retry_after!r}")
        super().__init__(message, code=ErrorCode.RATE_LIMITED)
        self.retry_after = max(1, math.ceil(retry_after))   # whole seconds
```

- `HttpErrorMapper._STATUS[ErrorCode.RATE_LIMITED] = HTTPStatus.TOO_MANY_REQUESTS`.
- The mapper adds `Retry-After: <retry_after>` to the `HTTPException` headers,
  next to the existing `401` challenge (`errors.py:139-144`). The domain error
  carries a duration in seconds; only the mapper knows it travels as a header.
- The code is `rate_limited`, named for meaning like `not_found` and
  `conflict`, not for the HTTP phrase.
- The body is the standard one; no new `ErrorBody` field.

The consumer's need (per subject, per process, 30 tokens per 60 s with a burst
of 10, before any lock, 429 without audit, a `WARNING` without the subject) is
met in periplo-cloud by an application port called first in the use case:

```python
class ChangeBudget(Protocol):          # periplo-cloud, access/application/ports.py
    def spend(self, subject_id: str) -> None:
        """Take one token, or raise loom's RateLimited(retry_after=...)."""
```

Why not in loom:

- The policy (who is limited, how much, on which operations) is the
  consumer's, and the same use case is limited whichever transport calls it.
- A per-process bucket is only correct for one instance; a shared limiter
  needs a store. loom would grow a stateful component and a storage port for
  one consumer.
- Calling the port first in `execute` runs before any database statement:
  the unit of work opens an `AsyncSession` (`executor.py:462-483`,
  `src/loom/core/repository/sqlalchemy/uow.py:79-103`), and the session
  manager only creates the session object (`session_manager.py:175-187`);
  SQLAlchemy connects on the first statement.

If the owner wants refusal before the use case is built, see open question 1.

### 3. `Query()`: a declared query struct

A marker in `loom.core.use_case`, bound like `Input()`:

```python
async def execute(self, query: GrantQuery = Query(), caller: Identity = Caller()) -> GrantPage: ...
```

- The annotation must be a strict `msgspec.Struct` (`forbid_unknown_fields=True`)
  or a strict pydantic model, checked with `loom_type()` at compile time, as
  any authored boundary type. The struct is the whole query contract: names
  (through `rename` or a per-field `name`), types, defaults and constraints
  (`Meta(ge=, le=)`, enums, `Meta(tz=True)`).
- loom imposes no pagination semantics on it. Cursor-only pagination is a
  `cursor: str | None` field and no `page`; the maximum limit is
  `Annotated[int, Meta(ge=1, le=200)]`. No `max_limit` or
  `pagination_mode` knob is added.
- Use case compiler: one `Query()` per `execute`; recorded as an ordinary
  `ParamBinding` (so the executor does not change) plus
  `ExecutionPlan.query_binding: QueryBinding | None = None` for transports.
- REST compiler refuses, with `InterfaceCompilationError`: `Query()` together
  with a `QuerySpec` parameter (`QuerySpec` itself or a union holding it); a
  field whose wire name equals a path parameter or `profile`; a field that is
  not a scalar (a string, UUID, date-time and other string formats included,
  an integer, number, boolean, enum or `Literal`, each optionally nullable).
  A `Query()` type that is not a strict model is a `CompilationError` naming
  `UseCase.execute` and the parameter.
- `LoomType` gains `from_query(params: Mapping[str, str]) -> Any`, strict
  (decided in review, replacing lax conversion). The integer, number and
  boolean fields, read once from the type's JSON Schema, take the JSON number
  grammar (RFC 8259 §6): a number matches
  `-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?` and is finite, an
  integer matches `-?(0|[1-9][0-9]*)` and lies in `[-2**63, 2**64)` (the
  range msgspec handles natively; a longer text is never passed to `int`,
  so CPython's digit limit cannot surface), and a boolean is `true` or
  `false`. A date or time field refuses any numeric text, `+5`, `007` and
  `.5` included, because pydantic reads those as timestamps even in strict
  mode. Every other value stays text. The result is then validated
  strictly: msgspec `convert(..., strict=True)`, pydantic
  `validate_json(..., strict=True)`. So `5.0`, `1e2`, `+5`, `007`, `.5`,
  `5.` or an integer outside the range, `1` for a boolean, `null` for a
  non-string field and a Unix timestamp for a date-time are refused. It raises `BoundaryValidationError` like the other
  entry points, and keeps every conversion call inside `_loom_type.py`.

Request handling, in order:

1. A key that is neither a declared wire name nor `profile` (when the route
   exposes it and `execute` takes a `profile` parameter, the same condition
   OpenAPI publishes it under) → 400 `unsupported_query`. Any other `profile`
   is unknown.
2. A key given twice, `profile` included → 400 `unsupported_query`.
3. A `profile` the route does not allow → 400 `query_validation` on field
   `profile`, with a fixed message and without the value.
4. `from_query` → 400 `query_validation` with `violations` on a bad value
   (wrong type, out of range, unknown enum value, missing time zone).
5. Only then `profile` is resolved, and the use case is built; a refused
   query never constructs it.
6. The typed struct is passed to `execute`.

The accepted names and the decoder are resolved once per route at startup and
held in `_RouteRuntime`.

OpenAPI: one `in: query` parameter per field, built once from
`LoomType.schema()`: `name` is the wire name, `required` is true only without
a default, the `null` branch of an optional field is dropped (absence is the
`None`), `description` comes from `Meta(description=...)`, and enum
definitions go to `components.schemas` through the existing
`_rewrite_to_component_refs`. Nothing else is published for the route,
except `profile` where the route exposes it and `execute` takes it.

Routes with a `QuerySpec` parameter, or with neither, behave as today, except
that request parameters are resolved before the use case is built.

A `Job` that declares `Query()` is refused at compile time with a
`CompilationError`: a job is dispatched with primitive parameters, so the
query struct could never reach it.

## The consumer's three routes

```python
class GrantsInterface(RestInterface[Any]):
    prefix = "/v1/grants"
    tags = ("access",)
    routes = (
        RestRoute(use_case=ListGrants, method="GET", path="", summary="Grants"),
        RestRoute(use_case=CreateGrant, method="POST", path="", summary="Grant a role"),
        RestRoute(use_case=RevokeGrant, method="DELETE", path="/{grant_id}",
                  summary="Revoke a grant"),
    )
```

`path=""` keeps the full path `/v1/grants` (`compiler.py:388`); `"/"` would
publish `/v1/grants/`. The path parameter is `{grant_id}`, the `execute`
parameter name.

```python
class GrantQuery(msgspec.Struct, frozen=True, kw_only=True, rename="camel",
                 forbid_unknown_fields=True):
    membership_id: UUID | None = None
    role_code: TenantRoleCode | None = None                 # StrEnum
    state: GrantStateCode | None = None                     # live | expired | revoked
    changed_since: Annotated[datetime, Meta(tz=True)] | None = msgspec.field(
        default=None, name="changed_since")
    cursor: str | None = None
    limit: Annotated[int, Meta(ge=1, le=200)] = 50


class ListGrants(UseCase[Any, GrantPage]):
    async def execute(self, query: GrantQuery = Query(),
                      caller: Identity = Caller()) -> GrantPage:
        if query.state is not None and query.changed_since is not None:
            raise UnsupportedQuery("periplo.v1", "access_grant",
                                   "state cannot be combined with changed_since")
        ...


class CreateGrant(UseCase[Any, GrantView | Created[GrantView]]):
    async def execute(self, command: CreateGrantCommand = Input(),
                      caller: Identity = Caller()) -> GrantView | Created[GrantView]:
        self._budget.spend(caller.require_subject())          # RateLimited → 429
        change = ...                                          # today's steps
        view = GrantView.of(change.grant)
        return Created(view) if change.changed else view


class RevokeGrant(UseCase[Any, GrantView]):
    async def execute(self, grant_id: UUID, caller: Identity = Caller()) -> GrantView:
        self._budget.spend(caller.require_subject())
        ...                                                   # 200 revoked or unchanged
```

The cross-field rule (`state` with `changed_since`) and the cursor bound to
the filters stay in the consumer: they are its contract, not a query shape.

### OpenAPI produced

```yaml
/v1/grants:
  get:
    parameters:
      - {name: membershipId,  in: query, required: false, schema: {type: string, format: uuid}}
      - {name: roleCode,      in: query, required: false, schema: {$ref: '#/components/schemas/TenantRoleCode'}}
      - {name: state,         in: query, required: false, schema: {$ref: '#/components/schemas/GrantStateCode'}}
      - {name: changed_since, in: query, required: false, schema: {type: string, format: date-time}}
      - {name: cursor,        in: query, required: false, schema: {type: string}}
      - {name: limit,         in: query, required: false, schema: {type: integer, minimum: 1, maximum: 200, default: 50}}
    responses:
      '200': {content: {application/json: {schema: {$ref: '#/components/schemas/GrantPage'}}}}
  post:
    requestBody: {required: true, content: {application/json: {schema: {$ref: '#/components/schemas/CreateGrantCommandRequest'}}}}
    responses:
      '200': {content: {application/json: {schema: {$ref: '#/components/schemas/GrantView'}}}}
      '201': {content: {application/json: {schema: {$ref: '#/components/schemas/GrantView'}}},
              headers: {Location: {schema: {type: string, format: uri-reference}}}}
/v1/grants/{grant_id}:
  delete:
    parameters: [{name: grant_id, in: path, required: true, schema: {type: string, format: uuid}}]
    responses:
      '200': {content: {application/json: {schema: {$ref: '#/components/schemas/GrantView'}}}}
```

msgspec 0.21.1 publishes a plain `datetime` as `{type: string}` without
`format`; `Meta(tz=True)` publishes `format: date-time` and refuses a value
without an offset (checked with a probe). It accepts any offset, not only `Z`.

Error responses stay out of the generated OpenAPI, as today; the consumer's
`ROUTE_ERRORS` keeps documenting them (open question 6).

### Error bodies

All use loom's body (`_errors.py:62-83`, `auth/middleware.py:204-227`).

```json
429, Retry-After: 2
{"detail": {"code": "rate_limited", "message": "Too many requests.", "trace_id": "…"}}

400 (unknown or repeated parameter)
{"detail": {"code": "unsupported_query",
            "message": "The request has a query parameter this route does not accept, or one given twice.",
            "trace_id": "…"}}

400 (bad value)
{"detail": {"code": "query_validation", "message": "Expected `int` <= 200 - at `$.limit`",
            "trace_id": "…", "violations": [{"field": "limit", "message": "Expected `int` <= 200"}]}}
```

The `unsupported_query` 400 does not echo the parameter's name: it is caller
input, and loom already drops input from validation errors
(`_errors.py:94-99`).

Finding (since fixed, #350): msgspec 0.21.1 quotes the value in one message,
`Invalid enum value 'x'` (checked with a probe), although `_loom_type.py:7-9`
said msgspec messages are free of the input. It affected JSON bodies, and
would have affected enum query parameters (open question 5).

## Alternatives rejected

| Gap | Alternative | Why not |
|---|---|---|
| 1 | Route-level callable, `RestRoute(status_for=lambda r: …)` | The body would have to carry the flag the callable reads; OpenAPI cannot learn the statuses from a callable, so a second declaration is needed; YAML routes cannot express it |
| 1 | `Outcome[T](value, status: int)` | An HTTP status inside the use case |
| 1 | Register a pydantic result model as one component under its class name, so 200 and 201 share it | Changes the OpenAPI of every pydantic route; two models with the same class name overwrite each other; a generic `Page[Item]` yields an invalid component key. 200 and 201 may each inline the schema instead |
| 1 | A `created: bool` attribute read by convention | Implicit, untyped, leaks into the response body |
| 1 | Returning a FastAPI `Response` | Breaks the rule that a use case knows no HTTP |
| 2 | `ErrorCode.TOO_MANY_REQUESTS` | The contract's code is `rate_limited`; loom names codes for meaning |
| 2 | `retry_after` in the body | The header is the standard carrier (RFC 9110 §10.2.3); one place only |
| 2 | Rate-limit ASGI middleware in loom | Must match route templates outside the router, run after authentication, and hold state; a per-process store is wrong with several instances |
| 2 | `RestRoute(rate_limit=…)` with a pluggable key function | A new stateful extension point, its clock and its store, for one consumer; open question 1 if the owner wants it |
| 3 | `RestRoute(filters=…, max_limit=…, cursor_only=…)` extending `QuerySpec` | Three knobs and a filter declaration outside the type system; the use case still gets an untyped `FilterGroup` |
| 3 | FastAPI `Depends`/`Query` in `execute` | FastAPI in the use case |
| 3 | Ignore unknown parameters | A misspelled `changed_since` would silently turn an incremental load into a full one |
| 3 | 422 for an unknown parameter | It is not a bad value; the consumer's contract is 400 `unsupported_query` |
| 3 | Validate the query struct strictly with `from_builtins` | Every query value is a string; strict conversion refuses `"50"` for `int` |
| 3 | Lax conversion (`strict=False`, pydantic lax) | Accepts `5.0` for an integer, `1` for a boolean, `null` as `None` and a Unix timestamp as a date-time (msgspec); found in review |
| 3 | pydantic `validate_strings(..., strict=True)` | Probed: still accepts `5.0`, `1`, `nan`, ` 5`, `5_0` and a numeric date-time |

## Standards

| Rule | Standard | Kind |
|---|---|---|
| 201 when the request created a resource, 200 otherwise | RFC 9110 §15.3.1, §15.3.2 | Normative |
| 429 Too Many Requests | RFC 6585 §4 | Normative |
| `Retry-After` as delay-seconds, a non-negative integer | RFC 9110 §10.2.3 | Normative |
| POST is neither safe nor idempotent by method; idempotency here is the resource's | RFC 9110 §9.2.2 | Normative; the 200 replay is policy |
| Query component syntax | RFC 3986 §3.4 | Normative |
| One `in: query` parameter per name, `style: form` by default | OpenAPI 3.1, Parameter Object | Normative within OpenAPI |
| `changed_since` as RFC 3339 date-time | RFC 3339 §5.6 | Normative where used |
| camelCase wire names, `changed_since` an exception | none | Consumer convention |

## Semver

All three fit 2.21.0, on one branch `feat/rest-result-status-rate-limit-declared-queries`.

- Additive: `Created` (with `location`), `Query()`, `QueryBinding`,
  `ExecutionPlan.query_binding` (defaulted), `QueryField`,
  `CompiledRoute.query_fields`, `result_type` and `created_type` (defaulted),
  `build_success_responses`, `ErrorCode.RATE_LIMITED`, `QUERY_VALIDATION` and
  `INSUFFICIENT_AUTHENTICATION`, `RateLimited`, `InsufficientAuthentication`,
  `AgentErrorCode.USECASE_DECLARES_QUERY`, `LoomType.from_query`, a
  keyword-only `code` on `BoundaryValidationError`, `names_query_spec`,
  `created_response`, `RETRY_AFTER_HEADER`, `LOCATION_HEADER`, and an optional
  `component_registry` on `build_query_parameters_schema`.
- `Created` and `Query()` are opt-in, and `QuerySpec` routes keep their
  parameters, filters and clamp.
- Five observable changes, all acceptable in a minor:
  - a custom `LoomError` already using code `rate_limited` now answers 429
    instead of 500;
  - one coded `query_validation` now answers 400, and one coded
    `insufficient_authentication` 401;
  - `LoomType`, a public `Protocol`, gains a method; its two implementations
    are private to `_loom_type.py`;
  - every route resolves its request parameters and body before building the
    use case;
  - `build_success_response_schema` reads `CompiledRoute.result_type`.

## Tests

All under `tests/`, mirroring `src/loom`; none in `src`.

Unit:

- `tests/unit/core/engine/test_query_binding.py`: `Query()` compiles to a
  `ParamBinding` plus `query_binding`; two `Query()` fail; a lax struct
  (no `forbid_unknown_fields`) fails with a `CompilationError` naming the
  parameter.
- `tests/unit/core/model/test_loom_type.py`: `from_query` for msgspec and
  pydantic: `"50"` → 50, UUID, enum, `tz=True` date-time with any offset;
  `5.0`, `1e2`, `1` for a boolean, `null` and a Unix timestamp raise
  `BoundaryValidationError` with the field path.
- `tests/unit/core/errors/test_errors.py`: `RateLimited` rounds 0.2 s up to 1
  and 1.5 s to 2; code `rate_limited`.
- `tests/unit/rest/test_rest_adapter.py` (`TestHttpErrorMapper`, where the
  mapper tests already live): `rate_limited` → 429 with `Retry-After`; 401
  still carries only the challenge; an unknown code is still 500.
- `tests/unit/rest/test_rest_compiler.py`: `Query()` with `QuerySpec`, a field
  named like a path parameter or `profile`, a list or object field: each is an
  `InterfaceCompilationError`; a `Created[X] | Y` with a different `Y` fails.
- `tests/unit/test_boundary_calls_containment.py`: unchanged allow-list proves
  no new conversion call in `loom.rest`.

Router, through `TestClient` on a compiled app (`tests/unit/rest/test_router_runtime.py`
or a new `test_router_result_status.py` and `test_router_declared_query.py`):

1. `Created(v)` → 201 with `v`; a plain `v` → the route's status; a route
   without `Created` is unchanged.
2. OpenAPI of a `T | Created[T]` route lists 200 and 201 with the same schema.
3. A use case raising `RateLimited(2)` → 429, `Retry-After: 2`, standard body
   with `trace_id`.
4. Declared query: valid values reach `execute` typed; unknown key → 400
   `unsupported_query` without the key in the message; repeated key → 400;
   `limit=500`, `state=x`, a bad UUID, a date-time without offset → 400
   `query_validation` with the field; defaults apply when absent.
5. OpenAPI of a declared query: exactly the declared parameters, wire names,
   `minimum`/`maximum`, enum `$ref` in `components.schemas`, no `page`,
   `pagination`, `sort` or `direction`.
6. Regression: a `QuerySpec` route still publishes its seven parameters, still
   turns unknown keys into filters and still clamps `limit`.

Integration (`tests/integration/rest/test_grant_routes.py`): one app over an
in-memory store with the three consumer-shaped routes, asserting the OpenAPI
document against a stored golden, the 201-then-200 create with `Location`,
an unknown list parameter (400) and a spent budget (429). Every other status
is pinned once, in the router unit tests.

## Performance

- `Created`: one `isinstance` per request.
- Declared query: per request, a set difference between the query keys and a
  frozen set of accepted names, then one `from_query` on a cached `LoomType`.
  No reflection: schema, names and decoder are resolved at startup. Cheaper
  than today's `QuerySpec` path, which walks every key, splits on `__` and
  guesses scalar types (`router_runtime.py:235-263`).
- `RateLimited`: no cost in loom outside the error path.
- OpenAPI: built once per application, as today.

## Open questions for the owner

1. **Where the limiter runs.** Recommended: a consumer port called first in
   `execute` (above). Alternative: a loom route hook run next to
   `_authorize_route`, before the use case is built
   (`RestRoute(rate_limit=Limiter)`, with a `Limiter` protocol keyed by
   identity). It adds a public extension point; worth it only if a second
   consumer or transport-level limiting appears.
2. **Bad values in a declared query: 422 or 400** (superseded by the
   standards check: 400 `query_validation`). Recommended 422
   `boundary_validation`, as for a JSON body or a path parameter; the
   consumer's § 13 lists 400 only for cursor and filter combinations.
3. **Repeated scalar parameter.** Recommended 400 `unsupported_query`; list
   fields are refused at compile time until a consumer needs them.
4. **Marker name.** `Query()` matches `Input()`/`Caller()`; it shares its name
   with FastAPI's `Query`, which a use case never imports. Alternative:
   `QueryParams()`.
5. **Enum value echoed by msgspec** (since fixed, #350). Recommended: a separate fix in
   `_msgspec_violation` that drops the quoted value, with its own test.
6. **Error responses in OpenAPI.** loom still publishes no error response;
   the consumer keeps `ROUTE_ERRORS`. Publishing them from loom is a separate
   decision.
7. **`limit` maximum in `QuerySpec` OpenAPI.** `openapi.py:184` publishes 1000
   whatever `RestApiDefaults.max_limit` is. Recommended: a separate `fix:`
   that publishes `effective_max_limit`.
8. **ADR location.** This file sits next to ADR 0001 in
   `docs/architecture/adr/`, not in `docs/adr/`.

## Owner decisions

Accepted with the recommendations above and these decisions:

1. `Created[T]` lives in `loom.core.use_case`. A use case that returns it
   answers 201; any other result answers the route's `status_code`. OpenAPI
   documents both statuses with the same schema. Use cases never import HTTP.
2. `ErrorCode.RATE_LIMITED = "rate_limited"` and `RateLimited(retry_after)`, a
   `DomainError` holding whole seconds rounded up. `HttpErrorMapper` answers
   429 with `Retry-After`. loom ships no limiter (open question 1: the
   consumer's port).
3. The `Query()` marker works over a strict `msgspec.Struct` or a strict
   pydantic model. loom publishes exactly the declared parameters and
   validates them. An unknown or repeated parameter answers 400
   `unsupported_query` (open question 3); an invalid value answered 422
   `boundary_validation`, as body and path parameters do (open question 2),
   until the standards check below changed it to 400 `query_validation`.
   List fields are refused at application start. `LoomType` gains
   `from_query`. Routes using `QuerySpec` are unchanged. The marker keeps the
   name `Query()` (open question 4).
4. `changed_since`-style date-times accept any offset (`Meta(tz=True)`).
5. Out of scope, fixed separately: unknown `QuerySpec` keys becoming filters
   and filter errors answering 500, the `QuerySpec` OpenAPI `maximum: 1000`
   (open question 7), and the enum value quoted by msgspec (open question 5).
   Open questions 6 and 8 stay as written.

### Standards check: a bad query value is 400

RFC 9110 §15.5.21 defines 422 Unprocessable Content as the server
understanding the content type of the request *content* and its syntax but
being unable to process the contained instructions. The query string is part
of the target URI (RFC 9110 §7.1, RFC 3986 §3.4), not content, so 422 does
not fit it. A malformed request target is a client error with no more
specific status: 400 Bad Request (RFC 9110 §15.5.1). 422 stays for bodies,
and path parameters keep FastAPI's 422.

- `ErrorCode.QUERY_VALIDATION = "query_validation"` maps to 400 in
  `HttpErrorMapper`, with the same body and `violations` as
  `boundary_validation`.
- Shape: `BoundaryValidationError` takes a keyword-only `code`, default
  `boundary_validation`. `from_query` keeps raising the default; the REST
  router re-raises a failure of `from_query` with `code=query_validation`.
  One error type keeps the mapper's single `violations` branch for both
  codes; core still names only where a value came from, never an HTTP
  status, and the transport decides which code its query failures carry.
  A separate subclass was not needed: it would duplicate the constructor to
  change one field.

### Addition: step-up authentication (RFC 9470)

Added after the owner's decision that step-up MFA follows RFC 9470. It
shares this ADR's shape: a transport-free domain error that the REST mapper
turns into a status and a header.

- `InsufficientAuthentication(Unauthenticated)` in `loom.core.errors`, with
  its own code `ErrorCode.INSUFFICIENT_AUTHENTICATION =
  "insufficient_authentication"`, an optional `max_age: int | None`
  (seconds) and optional `acr_values: tuple[str, ...]`. It validates what a
  challenge must be able to carry: `max_age` is a non-negative `int`, and
  each ACR value is visible ASCII with no whitespace, double quote or
  backslash.
- `HttpErrorMapper` answers `401` with
  `WWW-Authenticate: Bearer error="insufficient_user_authentication",
  error_description="…"`, plus `max_age="N"` and `acr_values="a b"` when
  present (RFC 9470 §3). The description keeps only the characters RFC 6750
  §3 allows. Every other 401 keeps the plain `Bearer` challenge.
- Being an `Unauthenticated`, it is caught wherever that is.

### Second review round: `Location` and the number grammar

- `Created(value, location=None)`: when set, the 201 carries `Location`
  (RFC 9110 §15.3.2), and OpenAPI declares that header on every 201 it
  documents. `LoomRestAdapter` answers a `Created` the same way. A location
  must be non-empty and hold only characters allowed in an RFC 3986 URI
  reference (unreserved, reserved and percent-encoded); the check is per
  character, not structural. Anything else raises `ValueError`, and a
  non-`str` the `TypeError` Python raises for it.
- Declared numbers follow the JSON number grammar (RFC 8259 §6): `+5`,
  `007`, `.5`, `5.` and `+1.5` are refused, and an integer must lie in
  `[-2**63, 2**64)`. Probed before the change: a huge integer gave no 500 on
  either library; 2**64 was accepted, and 5000 digits answered 400 with
  CPython's digit-limit message. The range check runs on the text length
  first, so that message can no longer surface.
- A `profile` the route does not allow answers 400 `query_validation`.
- A caller other than REST must pass an instance of the query type.

### Review round

The owner's review changed these points, all reflected above:

- Declared queries decode strictly, not in lax mode. The integer, number and
  boolean fields are read from the JSON Schema, not from the Python
  annotations, because pydantic's `AwareDatetime` is not a `date` subclass.
  The schema helpers live once in `loom.core.model._json_schema` (internal), shared by
  `_loom_type.py`, the REST compiler and `openapi.py`.
- `profile` on a `Query()` route is accepted only when exposed, and refused
  when repeated. Request parameters are resolved before the use case is
  built.
- An agent granted a use case that takes `Query()` fails to compile with
  `USECASE_DECLARES_QUERY`: a tool call cannot bind a declared query.
- `Created` on a route with `status_code` 201 is refused. OpenAPI builds both
  success entries in one `build_success_responses`.
- `RateLimited` refuses a non-finite `retry_after` with a `ValueError`.
- Importing every `loom.core` module loads no `loom.rest`, FastAPI or
  Starlette (a containment test).

## Consequences

periplo-cloud T9 removes nothing from `composition/openapi.py` for these
routes and adds no query-parameter patch: loom publishes the parameters and
both success responses. The consumer keeps the bucket, the cross-field rule,
the cursor binding and the error documentation.

Estimated effort: `Created` 0.5 day; `RateLimited` 0.5 day; `Query()` with
`from_query`, compiler checks and OpenAPI 2 days; tests included.
