# ADR 0003: Error placement, codes and mapping

- Status: Decision A Proposed. Decision B Accepted, and applied to the AI,
  streaming and ETL pillars by one hotfix, without shims (owner decision).
- Issues: #351, #352, #353, #354, #355, #356, #357; 3.0 items in #358
- Applies to: loom-kernel 2.21.0 and later. ADR 0002 and
  `InsufficientAuthentication` land first, in 2.21.0.
- Scope: Decision A is design only. Decision B ships with the hotfix that
  applies it.

## Context

loom has no written rule for where an error lives, which code it carries or who
turns it into a status. Evidence is from `master` at `c58f015`, plus the 2.21.0
branch where marked.

### Run-time errors

**Hierarchy.** Seven classes subclass `LoomError` directly instead of
`DomainError` (`src/loom/core/errors/errors.py:31`) or `SystemError` (`:39`):

| Class | Location |
|---|---|
| `BoundaryValidationError` | `core/model/_loom_type.py:83` |
| `UnsupportedBoundaryType` | `core/model/_loom_type.py:108` |
| `PostCommitError` | `core/engine/post_commit.py:29` |
| `JobTimeoutError`, `JobFailedError` | `core/job/handle.py:12`, `:33` |
| `AuthenticationUnavailable` | `rest/auth/abc.py:64` |
| `UnsupportedFormatError` | `etl/backends/_format_registry.py:18` |

The 2.21.0 branch also raises a bare `LoomError(..., code=UNSUPPORTED_QUERY)` in
`_DeclaredQuery.decode`, because `UnsupportedQuery` requires a backend and a
model. `SystemError.__init__` hard-codes `SYSTEM_ERROR`
(`core/errors/errors.py:50-51`).

**Codes.** `LoomError.code` is a plain `str` (`errors.py:25`). Four raw codes
sit outside `ErrorCode` (`core/errors/codes.py:20-38`):

- `boundary_type_unsupported` (`_loom_type.py:116`);
- `job_timeout` (`handle.py:28`);
- `job_failed` (`handle.py:52`);
- `payload_too_large` (`rest/_body.py:30`, `rest/fastapi/sql.py:149`).

`_code_for` derives codes from reason phrases (`rest/fastapi/_errors.py:86-91`).
Those phrases differ by interpreter: 413 gives `request_entity_too_large` on
3.11 and `content_too_large` on 3.13; 422 gives `unprocessable_entity` and
`unprocessable_content`.

**Subsystem errors outside their concept.** The `QueryCompiler` protocol says
compilers raise `UnsupportedQuery` (`core/repository/abc/query_compiler.py:37`,
`:53`). The SQLAlchemy compiler raises `FilterPathError` and `UnsafeFilterError`
as `ValueError` (`core/repository/sqlalchemy/query_compiler/errors.py:6`, `:22`),
and the router answers them with 500 (`rest/fastapi/router_runtime.py:486-499`;
#352). One invalid query can therefore produce three codes:

- `bad_request`, from the router's `HTTPException(400, str)` sites
  (`router_runtime.py:182-286`, `:391-402`);
- `unsupported_query`, from the cursor codec and repositories;
- `query_validation`, from a declared query on 2.21.0.

FastAPI's validation of path and query values answers 422
(`rest/fastapi/_errors.py:45-59`). The owner's decision is 400.

**Mapping.** REST builds error bodies in seven places:

- `rest/errors.py:93-139`;
- `rest/fastapi/_errors.py:45-91`;
- `router_runtime.py:101-110` (500);
- `sql.py:143-153` (413) and `sql.py:202-211` (500);
- `rest/_body.py:57-63`, `:164-171` (413);
- `rest/auth/middleware.py:205-227` (401, 503).

The two 500 bodies sit outside `detail` and use the code `internal_error`.
Every status copies `error.message` (`rest/errors.py:106-110`), so 5xx bodies
leak internal text (#353). The leaking sources are `JobFailedError`'s
`str(cause)` (`handle.py:49-51`), the SQLSTATE (`sqlalchemy/integrity.py:124`)
and constraint and column names (`integrity.py:59-71`, `:103-122`; #354).

Celery retries `DomainError`s (`celery/runner.py:220-227`; #355), against
`core/errors/errors.py:42-44`.

AI has its own table (`ai/fastapi/endpoints.py:74-98`). `TransportError`,
however, takes literal statuses and raw codes at each raise site
(`ai/_transport.py:96-99`, `:165`; `endpoints.py:189-204`, `:243-270`). A
provider's 401 or 403 becomes `UNAUTHORIZED` and then 403
(`ai/engines/pydantic_ai/_errors.py:44-51`; #356), and `str(error)` reaches 5xx
bodies (`endpoints.py:145-152`).

### Compile errors

The three pillars that compile artefacts build their compile issues by hand.

| Pillar | File (lines) | Code enum | Factories | Factory lines |
|---|---|---|---|---|
| AI | `ai/errors.py` (1,679) | `AgentErrorCode`, 88 members | 89 functions | 1,023 |
| Streaming | `streaming/compiler/_errors.py` (477) | `StreamingErrorCode`, 33 | 33 functions | 293 |
| ETL | `etl/compiler/_errors.py` (473) | `ETLErrorCode`, 34 | 35 classmethods (34 plus one shared helper) | 317 |

All counts come from an AST scan of functions whose return annotation is the
issue or error type. They are 157 factories and 1,633 lines in all.

Each factory has the same shape: a code, an f-string message, a component and
an optional field. Only 9 factories compute anything:

- eight join a sequence, with `"none"` when it is empty: AI
  `spec_version_unsupported`, `native_tool_unsupported`,
  `mcp_auth_strategy_unknown`, `engine_not_found`, `engine_api_mismatch`,
  `remote_clients_unknown`, `agent_marker_unknown`, `mcp_marker_unknown`;
- one names a type: streaming `explode_without_router`.

The issue types differ as follows:

| | AI `AgentCompilationIssue` (`ai/errors.py:172`) | Streaming `CompilationIssue` (`streaming/compiler/_errors.py:72`) | ETL `ETLCompilationError` (`etl/compiler/_errors.py:56`) |
|---|---|---|---|
| Kind | frozen struct | frozen struct | exception, one raised per failure |
| `code` | `AgentErrorCode` | `StreamingErrorCode` | `ETLErrorCode` |
| `message` | field | field | only `str(exc)`, no attribute |
| `component` | `str = ""` | `str = ""` | `str`, required |
| `field` | `str \| None = None` | `str \| None = None` | `str \| None = None` |
| Extra | — | `prefixed(prefix)`, 3 call sites in `phases/validate.py` | — |

AI and streaming share one shape. ETL has the same four values on an exception
instead of an issue.

## Decision A: minimal centralisation of run-time errors

Status: Proposed.

**R1. One code catalogue.**

- `ErrorCode` is decorated with `@enum.unique` and is the only catalogue of
  client-facing codes.
- `LoomError.code` is typed `ErrorCode`.
- The four raw codes become members with their current string values.
- `SystemError` takes `code: ErrorCode = ErrorCode.SYSTEM_ERROR` as a keyword.
- No code is derived from text. `_code_for` is replaced by a fixed status → code
  table in the REST mapper that uses the 3.11 spellings.
- The pillar compile enums stay separate catalogues (Decision B), and so does
  `AgentRunErrorCode`. That one is the run-time catalogue of the published AI
  wire contract (`specs/001-ai-agent-layer/contracts/http-sse.md`) and has its
  own six-way retry class.

**R2. Two families.**

- A run-time error that a transport maps is either a `DomainError` (4xx, never
  retried) or a `SystemError(code=…)` (5xx, may be retried).
- No class subclasses `LoomError` directly, apart from those two.
- Nothing raises a bare `LoomError` or `DomainError`.
- Compile and startup errors are not `LoomError`s.

The seven classes rebase without changing any status:

- `BoundaryValidationError` → `DomainError`, keeping `ValueError`;
- `UnsupportedFormatError` → `DomainError`, keeping today's 400;
- `UnsupportedBoundaryType`, `PostCommitError` and the two job errors →
  `SystemError`;
- `AuthenticationUnavailable` → `SystemError(code=SERVICE_UNAVAILABLE)`.

`FilterPathError` and `UnsafeFilterError` subclass `UnsupportedQuery` and keep
`ValueError` as a second base.

**R3. Where `LoomError` classes live.**

- A generic or multi-transport class lives in `loom/core/errors/errors.py`.
- A class used by one pillar only lives in an errors module that pillar
  already has.
- No new files.
- Each moved class stays importable from its old path as the same object
  until 3.0.

| Class | From | To | Why |
|---|---|---|---|
| `UnsupportedQuery` | `core/repository/abc/errors.py` | `core/errors` | Raised by the router, the cursor codec and every repository; `backend` and `model` become optional and its message stops naming the backend |
| `BoundaryValidationError` | `core/model/_loom_type.py` | `core/errors` | Mapped by REST and AI |
| `UnsupportedBoundaryType` | `core/model/_loom_type.py` | `core/errors` | Raised at run time by AI grants (`ai/runtime/_grants.py:212`) |
| `PostCommitError` | `core/engine/post_commit.py` | `core/errors` | Read by REST and Celery |
| `JobTimeoutError`, `JobFailedError` | `core/job/handle.py` | `core/errors` | Any transport awaiting a job |
| `AuthenticationUnavailable` | `rest/auth/abc.py` | `rest/errors.py` | REST only |
| `BodyTooLarge` (becomes a `DomainError` owning `PAYLOAD_TOO_LARGE`) | `rest/_body.py` | `rest/errors.py` | REST only |
| `UnsupportedFormatError` | `etl/backends/_format_registry.py` | `etl/runner/errors.py` | ETL only; the existing ETL run-time errors module |

The `core/sql/abc/errors.py` and `core/repository/abc/errors.py` modules stay,
for SQL subsystem errors and for any further repository errors. Compile and
startup errors do not move.

**R4. One mapper per transport.**

| Transport | The one place |
|---|---|
| REST | `HttpErrorMapper`, `rest/errors.py` |
| AI | `_STATUS_BY_CODE`, `ai/fastapi/endpoints.py` |
| Celery | `_is_retryable`, `celery/runner.py` |

Nothing else builds an error status or an error body.

REST:

- `HttpErrorMapper` gets one method returning `(status, headers, detail)`;
  `to_http` wraps it.
- The ASGI middlewares send what that method returns, so they stay pure ASGI.
- The router and the SQL endpoint raise `LoomError`s:
  - `UnsupportedQuery` for an unknown or forbidden parameter;
  - `BoundaryValidationError(code=QUERY_VALIDATION)` for a bad value.
- An unexpected exception is answered as a `SystemError`.
- FastAPI validation answers 400 `query_validation` when every failing location
  is `path` or `query`, and 422 when the body fails.

AI:

- `TransportError` takes a code from an AI enum, and the table supplies the
  status.
- The provider classifier keeps reading upstream statuses. That is input, not
  mapping.
- A provider 401/403 maps to `PROVIDER_UNAVAILABLE` (503).

Celery never retries a `DomainError` or a committed `PostCommitError`.

A 5xx body carries a fixed text per status ("An unexpected error occurred",
"The service is temporarily unavailable", "The service did not answer in
time"). It keeps `code`, `trace_id` and `committed`. The original message goes
to the log under the same `trace_id`.

**R5. Core never imports `http`, `fastapi` or `starlette`.** Core errors say
what happened; the transport decides the status. The status numbers in the
`core/sql/abc/errors.py:3-7` docstring are removed.

### Tests

Each test lands with the fix it guards and has no allowlist.

**T1: `tests/unit/test_error_placement_containment.py`.** It sits next to
`test_domain_containment.py` and works the same way: it reads `src/loom` with
`ast` and imports nothing, so it needs no optional extras. Every check uses
direct names, with no inheritance index.

| Check | Walks | Fails when |
|---|---|---|
| R1 | `code=` keywords of `super().__init__` calls inside classes in `src/loom` | the value is not `ErrorCode.<MEMBER>` and not a parameter named `code` whose default is `ErrorCode.<MEMBER>` |
| R2 | class bases and calls | a class other than `DomainError`/`SystemError` names `LoomError` as a base; any call to `LoomError(` or `DomainError(`; a class whose name ends in `CompilationError`, `ConfigError` or is `BootstrapError` names a `LoomError` family base |
| R3 | classes whose bases name anything in `loom.core.errors.__all__` | the defining module is not `loom/core/errors/errors.py` or an existing module named `errors.py`/`_errors.py` |
| R4-REST | `src/loom/rest` | `HTTPException(` or a response built with a status of 400 or more, outside `rest/errors.py`. Two exceptions are named in the test because they are not error mapping: the readiness 503 (`rest/fastapi/health.py:117`) and the metrics trailing-slash 404 (`rest/fastapi/auto.py:1435`) |
| R5 | `src/loom/core` | an import of `http`, `fastapi` or `starlette` |

R1 is checked by this test because mypy runs only in pre-commit, not in CI,
and `master` already fails it.

**T2: mapper tests.**

| Test | File | Asserts |
|---|---|---|
| Every code has a status | `tests/unit/rest/test_http_error_mapper.py` | `set(ErrorCode) == set(HttpErrorMapper._STATUS)` |
| A 5xx body leaks nothing | same | for every 5xx code and an unknown code, a `SystemError("secret-…", code=…)` body has the fixed text, no `secret`, and a `trace_id` |
| One body shape | same, against a fixture app | the mapper, request validation (path, query, body), an unknown route, an unexpected exception, the body cap and the auth 401/503 all answer `{"detail": {"code", "message", "trace_id", …}}`, with `code` from `ErrorCode` |
| Celery | `tests/unit/celery/test_runner_retry.py` | a `DomainError` is not retried; a `SystemError` is |
| AI | `tests/unit/ai/test_error_catalogue_totality.py` | the table covers the transport codes; no 5xx echoes the message; a provider 401 and 403 classify to `PROVIDER_UNAVAILABLE` → 503 |

### Steps

| # | Step | Issue | Tests landing |
|---|---|---|---|
| 1 | `@unique`, `code: ErrorCode`, four new members, `SystemError(code=)`, the seven rebases, the R3 moves with re-exports, the docstring fix | #357 | T1 R1, R2, R3, R5; T2 "every code has a status" |
| 2 | `FilterPathError`/`UnsafeFilterError(UnsupportedQuery)`; mistyped filter values → `query_validation`; the router and the 2.21.0 bare raise use `UnsupportedQuery`/`BoundaryValidationError`; `bad_request` disappears; path/query validation → 400 | #352 | T2 "one body shape" (query cases) |
| 3 | One REST mapper: `(status, headers, detail)`, middlewares and SQL endpoint use it, `BodyTooLarge` as a `DomainError`, fixed status → code table, fixed 5xx text, a malformed body → `boundary_validation` | #351, #353 | T1 R4-REST; T2 "5xx leaks nothing", "one body shape" |
| 4 | Integrity messages without constraint, column or key values | #354 | T2 "5xx leaks nothing" covers the SQLSTATE case |
| 5 | Celery retries by family | #355 | T2 Celery |
| 6 | AI: transport code enum, status from the table only, provider 401/403 → 503, fixed 5xx text | #356, #353 | T2 AI |

Steps 1 and 2 come first; steps 3-6 are independent. All of them are 2.x.

Visible changes:

- step 2: codes in 400 bodies, and 422 → 400 for path and query values;
- step 3: 5xx messages, and on Python 3.13 the two fallback codes;
- step 6: one AI status, 403 → 503.

## Decision B: the compile-issue catalogue as data

Status: Accepted. Applied to AI, streaming and ETL by the compile-issue
hotfix. The issue factories are removed, with no shim: the owner treats them
as a defect, and callers move to `Code.X.issue(...)`.

A compile issue is data: a code, a message template, a component and a field.
The catalogue states this once per code, and one constructor builds the issue.

### Core base

The base goes in `loom/core/errors/codes.py`, next to `ErrorCode`: both are
code catalogues. `CompileIssue` is a plain `msgspec.Struct`. Using
`LoomFrozenStruct` would mean importing `loom.core.model`, which already imports
`loom.core.errors.codes`, and that is a cycle.

```python
class Template(NamedTuple):
    text: str                       # str.format template; "{component}" allowed
    field: str | None = None        # template too, e.g. "{alias}"
    component: str = "{component}"  # template; a constant such as "fork" also works


class CompileIssue(msgspec.Struct, Generic[CodeT], frozen=True, kw_only=True):
    code: CodeT
    message: str
    component: str = ""
    field: str | None = None


class IssueCode(StrEnum):
    """Base of a pillar's compile-issue catalogue."""

    def __new__(cls, value: str, template: Template) -> Self: ...  # stores template

    def issue(self, **params: object) -> CompileIssue[Self]:
        """Build the issue; TypeError naming the code on a missing or unknown param."""
```

Parameter rendering:

- `str` is used as is;
- a `type` renders as its `__qualname__`;
- a set is sorted, then joined;
- any other iterable is joined with `", "`, and is `"none"` when empty;
- anything else uses `str()`.

`issue()` compares the parameters it receives with the template's field names
and raises `TypeError(f"{code} takes […], got […]")` on any difference. This
covers misspelled names, which `str.format` would otherwise ignore. Pillar
enums subclass `IssueCode` and add `@enum.unique`.

This was checked on Python 3.11 and 3.13 with msgspec: members compare equal
to their string, iterate normally, encode as the plain string, and a missing
parameter raises.

### Before and after

AI, a plain factory and one that computes:

```python
# before (ai/errors.py, 2 of 89 factories, 25 lines)
def spec_version_unsupported(component: str, found: int, supported: Sequence[int]) -> AgentCompilationIssue:
    known = ", ".join(str(version) for version in supported)
    return AgentCompilationIssue(
        code=AgentErrorCode.SPEC_VERSION_UNSUPPORTED,
        message=f"{component}: spec_version {found} is not supported; supported versions: {known}",
        component=component,
        field="spec_version",
    )

def engine_not_found(name: str, available: Sequence[str]) -> AgentCompilationIssue:
    known = ", ".join(available) if available else "none"
    return AgentCompilationIssue(
        code=AgentErrorCode.ENGINE_NOT_FOUND,
        message=f"engine '{name}' is not installed; available engines: {known}",
        component=name,
        field=_FIELD_AI_ENGINE,
    )

# after (in the enum, 6 lines)
SPEC_VERSION_UNSUPPORTED = "SPEC_VERSION_UNSUPPORTED", Template(
    "{component}: spec_version {found} is not supported; supported versions: {supported}",
    field="spec_version")
ENGINE_NOT_FOUND = "ENGINE_NOT_FOUND", Template(
    "engine '{name}' is not installed; available engines: {available}",
    component="{name}", field="ai.engine")

# call site
AgentErrorCode.SPEC_VERSION_UNSUPPORTED.issue(component=path, found=3, supported=(1, 2))
```

Streaming, a constant component and the one factory that computes:

```python
# before
def fork_not_last() -> CompilationIssue:
    return CompilationIssue(code=StreamingErrorCode.FORK_NOT_LAST,
                            message="fork must be the last node in a process", component="fork")

def explode_without_router(next_node: object | None) -> CompilationIssue:
    got = type(next_node).__name__ if next_node is not None else "nothing"
    return CompilationIssue(code=StreamingErrorCode.EXPLODE_WITHOUT_ROUTER,
                            message=f"Explode must be immediately followed by a Router; got {got}",
                            component="Explode")

# after
FORK_NOT_LAST = "FORK_NOT_LAST", Template(
    "fork must be the last node in a process", component="fork")
EXPLODE_WITHOUT_ROUTER = "EXPLODE_WITHOUT_ROUTER", Template(
    "Explode must be immediately followed by a Router; got {got}", component="Explode")

# call site: the one line that computed moves to the caller
got = type(next_node).__name__ if next_node is not None else "nothing"
StreamingErrorCode.EXPLODE_WITHOUT_ROUTER.issue(got=got)
```

ETL, a field from a parameter and a set:

```python
# before
@classmethod
def unknown_source_table(cls, step: type, alias: str, table_ref: str) -> ETLCompilationError:
    return cls(code=ETLErrorCode.UNKNOWN_SOURCE_TABLE, component=step.__qualname__,
               message=f"{step.__qualname__}: source '{alias}' references unknown table '{table_ref}'",
               field=alias)

# after
UNKNOWN_SOURCE_TABLE = "UNKNOWN_SOURCE_TABLE", Template(
    "{step}: source '{alias}' references unknown table '{table_ref}'",
    field="{alias}", component="{step}")
MISSING_SOURCE_PARAMS = "MISSING_SOURCE_PARAMS", Template(
    "{step}.execute: source(s) {missing} declared in sources but missing as "
    "keyword-only parameter(s) after '*'", component="{step}")

# call site
raise ETLCompilationError.from_issue(
    ETLErrorCode.UNKNOWN_SOURCE_TABLE.issue(step=step_type, alias=alias, table_ref=ref))
```

### Shared issue type

- `AgentCompilationIssue` and the streaming `CompilationIssue` become aliases of
  `CompileIssue[AgentErrorCode]` and `CompileIssue[StreamingErrorCode]`. They
  have the same fields and defaults.
- `prefixed` becomes a streaming function `prefixed(issue, prefix)` next to its
  three callers.
- The `from_message` helpers keep wrapping bare strings as `UNSPECIFIED`.
- `ETLCompilationError` keeps its constructor in 2.x and gains one
  `from_issue(issue)` classmethod. In 3.0 the constructor takes the issue
  directly.
- Six ETL raises in `etl/compiler/_validators_step.py:462-543` share
  `INVALID_TARGET_TYPE` with six different messages, eight after #368. Each
  gets its own code (open question 4, decided).

### No shims

The factories are removed in 2.x, with no alias, deprecation warning or
metaclass hook: the owner treats them as a defect. Every caller in the
repository moves to `Code.X.issue(...)` in the same change, and the changelog
names the removed functions.

### Trade-offs

- **Type-checked parameters are lost.** Today mypy checks each factory's
  signature. Afterwards a missing or misspelled parameter fails only when the
  issue is built, with a `TypeError` that names the code.
- **Mitigation.** Every code needs a compile-path test that drives the compiler
  into the failure and asserts the code. That builds the issue on the real
  path, so a wrong parameter fails there. A name search of `tests/` (the code
  as `.NAME`, `"NAME"` or a factory call), excluding the four files that call
  factories directly, finds these gaps:
  - AI: `OUTPUT_CHECK_UNRESOLVABLE` (its factory is never referenced in `src`,
    so it is dead code) and `ENDPOINT_AUTH_MISSING`.
  - ETL, no test at all: `UPDATE_EMPTY_UPDATE_SET`.
  - ETL, only factory-unit tests: `INVALID_PARAMS_TYPE`, `INVALID_PARAMS_NAME`,
    `MISSING_SOURCE_PARAMS`, `MISSING_PARAMS_FIELDS`, `UNKNOWN_SOURCE_TABLE`,
    `UNKNOWN_TARGET_TABLE`, `TEMP_NOT_PRODUCED`, `UPSERT_NO_KEYS`,
    `UPSERT_EXCLUDE_INCLUDE_CONFLICT`, `UPSERT_KEY_IN_EXCLUDE`,
    `MISSING_GENERIC_PARAM`, `INVALID_SOURCES_TYPE`, `INVALID_PROCESS_ITEM`,
    `INVALID_STEP_ITEM`.
  - Streaming, no test asserts the code (some assert the message text):
    `STEP_INSTANTIATION_FAILED`, `BINDING_RESOLUTION_FAILED`,
    `BINDING_NOT_PROCESS_NODE`, `FORK_SELECTOR_MISSING`, `SINK_MISSING_NAME`,
    `SINK_CONFIG_INVALID`, `MONGO_CONFIG_INVALID`,
    `BATCH_SCOPE_DIRECT_CONTEXT`, `TERMINAL_NOT_LAST`, `FORK_NOT_LAST`,
    `BROADCAST_NOT_LAST`, `SCOPED_PROCESS_NOT_LAST`, `EXPLODE_WITHOUT_ROUTER`,
    `ROUTER_BRANCH_UNSAFE_NODE`, `ROUTER_BRANCH_FANOUT_UNSUPPORTED`,
    `ROUTER_BRANCH_SHAPE_DIVERGENCE`, `OUTPUT_WITH_FORK`,
    `OUTPUT_WITH_BROADCAST`, `WINDOW_STRATEGY_UNSUPPORTED`,
    `SCOPED_INTO_TOPIC_NOT_LAST`, `SCOPED_PROCESS_UNSUPPORTED_NODE`,
    `ERROR_ENVELOPE_UNPARAMETERIZED`, `SINK_CANNOT_TRACK_COMMITS`.

  Each pillar's migration adds the missing compile-path tests in the same pull
  request, or deletes the dead code.
- **Messages change slightly.** Sets render as `a, b` instead of `['a', 'b']`,
  types render by `__qualname__`, and an empty list renders as `none`.
  Message text is not contract, but tests that match on it change.
- **Defaults move to callers.** `upsert_no_keys(step, method="upsert")` becomes
  an explicit `method=` at its call sites.
- **Factories passed as callables** (`_validate_remote_url(..., mcp_url_invalid)`,
  `ai/config.py:525`) receive the code instead, and call `code.issue(...)`.

### Line estimates

| File | Today | After (estimate) | After (applied) |
|---|---|---|---|
| `core/errors/codes.py` | 38 | about 95 | 140 |
| `ai/errors.py` | 1,679 | about 820 | 941 |
| `streaming/compiler/_errors.py` | 477 | about 220 | 248 |
| `etl/compiler/_errors.py` | 473 | about 240 | 303 |
| Total | 2,667 | about 1,375 | 1,632 |

The applied files are longer than the estimate because the formatter writes
each `NAME = "NAME", Template(...)` that does not fit on one line as a
parenthesised tuple, about two more lines per member. The "Today" column is
the evidence commit; at the base of the hotfix, `codes.py` had 41 lines and
`ai/errors.py` 1,693.

How the estimates were made:

- The factories go: 1,633 lines.
- Each enum member grows from one line to about three (template and field),
  for +476 lines on 155 members.
- Call sites keep about the same line count.

### Steps for Decision B

These steps are independent of Decision A and of each other, and all are 2.x.

1. Core base (`IssueCode`, `Template`, `CompileIssue`) with its unit tests.
2. Streaming.
3. ETL, with `from_issue`.
4. AI.

Each pillar's pull request adds the missing compile-path tests listed above.

### Applied: AI, streaming and ETL

What the hotfix did where this ADR left a choice, and what it found:

- `AgentCompilationIssue` and the streaming `CompilationIssue` are type
  aliases of `CompileIssue[...]`. `UNSPECIFIED` carries the template
  `"{message}"`, so `from_message` builds through `issue()` too.
- Messages keep their text. Where a factory named a type by `__name__`, the
  call site passes `type(node).__name__`, because `__qualname__` would change
  the text for nested classes. A sequence that is empty renders `none` where
  it rendered an empty string; no call site passes an empty one.
- Where the component was one parameter, that parameter is `component`, as
  in the default template. Parameters that carry the redaction rule keep
  their names (`server`, `agent`).
- The use-case `Mcp()` include check shares `TOOL_FILTER_MATCHES_NOTHING`
  with its own message and field, so it builds its issue directly.
  `SPEC_MALFORMED` and one
  `MCP_TRANSPORT_INVALID` site set the field on the issue, because only the
  call site knows it.
- `OUTPUT_CHECK_UNRESOLVABLE` is not dead: `compile_output_check` has used its
  factory since #233. It now has a compile-path test, as does
  `ENDPOINT_AUTH_MISSING`.
- A contract test per pillar renders every code from its declared
  parameters and checks, over the AST of `src/loom`, that every
  `Code.X.issue(...)` call passes exactly those parameters.
- Streaming: two codes could not be raised because their checks were wrong,
  and the hotfix fixes the checks. `Broadcast` was dispatched as a plain
  terminal, so `BROADCAST_NOT_LAST` never fired and broadcast branches were
  never shape-checked. The `ERROR_ENVELOPE_UNPARAMETERIZED` check matched only
  `ErrorEnvelope[X]`, so a bare `ErrorEnvelope` compiled.
- ETL: `ETLCompilationError` keeps its constructor and gains `from_issue`.
  Where a message showed a sorted list or a repr (`['a', 'b']`,
  `IntoTemp('x')`), the call site passes the formatted text, so messages keep
  their text.
- ETL: the historify checks get their own codes (open question 4). Since
  #368 there are eight: `HISTORIFY_KEYS_EMPTY`,
  `HISTORIFY_KEYS_TRACK_OVERLAP`, `HISTORIFY_EFFECTIVE_DATE_IN_TRACK`,
  `HISTORIFY_BOUNDARY_IN_KEYS`, `HISTORIFY_BOUNDARY_IN_TRACK`,
  `HISTORIFY_LOG_EFFECTIVE_DATE_NOT_COLUMN`,
  `HISTORIFY_DELETE_MARKER_REQUIRES_LOG` and
  `HISTORIFY_DELETE_MARKER_DECLARED_COLUMN`. The run-time historify errors in
  `declarative/target/_history/_errors.py` stay where they are: they report
  the data of a run, not a compile failure.
- ETL: `INVALID_SOURCES_TYPE` is removed as dead code. `ETLStep` classifies
  sources as grouped only when they are a `Sources` or a `SourceSet`, so its
  guard could never fail. Every other code listed above has a compile-path
  test that asserts the code. Most already drove the failure and matched
  only its message.

## Alternatives rejected

| # | Alternative | Why not |
|---|---|---|
| 1 | One `errors.py` per subsystem package for every exception, with a ratcheted allowlist | Creates about 15 files and moves 37 classes, for placement alone; the owner chose option A |
| 2 | One framework-wide `loom/errors.py` | A god module that imports every pillar's vocabulary |
| 3 | Merge the compile enums and `AgentRunErrorCode` into `ErrorCode` | 182 members with a different casing contract; compile codes never reach a transport |
| 4 | Derive the AI status table from `HttpErrorMapper` | Needs a translation from `AgentRunErrorCode` that nothing else reads; different body and SSE frame |
| 5 | Rely on mypy for R1 | mypy is not in CI and `master` fails it today |
| 6 | Compile catalogue: one `NamedTuple` constant per code instead of enum members carrying templates | Two parallel catalogues (code enum and templates) that can drift |
| 7 | Keep one alias function per old factory until 3.0 | About 150 one-line functions: the boilerplate this decision removes |
| 8 | Positional-compatible shims | The old positional order differs from the template order in 24 factories; each would need its own declared order, which amounts to keeping the aliases |

## Standards

| Rule | Standard | Kind |
|---|---|---|
| 400 for a malformed request, 422 only for content | RFC 9110 §15.5.1, §15.5.21 | Normative |
| 401 carries `WWW-Authenticate` | RFC 9110 §11.6.1 | Normative |
| Step-up 401 (`insufficient_user_authentication`) | RFC 9470 §3 | Normative (2.21.0) |
| 429 with `Retry-After` | RFC 6585 §4, RFC 9110 §10.2.3 | Normative |
| 413 for an oversized body | RFC 9110 §15.5.14 | Normative |
| 502/503/504 for upstream and availability failures | RFC 9110 §15.6.3-15.6.5 | Normative |
| Problem Details body | RFC 9457 | Normative once adopted (3.0) |
| Generic 5xx text, detail in logs | OWASP ASVS 5.0 V16 | Guidance |

## Follow-ups

- Put mypy in CI and fix `master` until it passes. R1's typing becomes
  enforceable then, and the T1 R1 check can stay as a cheap guard.
- Streaming exceptions (`streaming/core/_exceptions.py:6-31`): export the
  family from `loom.streaming`; `MissingSinkError` is never raised.

### For 3.0 (#358)

- AI `OUTPUT_SCHEMA_VIOLATION` → 502.
- NOT NULL and CHECK violations → 422.
- Split ClickHouse `DatabaseError` (403, 503).
- `UnsupportedFormatError` → `SystemError`/500.
- Rename `SystemError`, which shadows the builtin, and the two
  `CompilationError`s, keeping aliases in 2.x.
- The 500 body under `detail` with `system_error`.
- RFC 9457 Problem Details: opt-in in 2.x, the default in 3.0.
- Drop the re-exports at old paths and the old `ETLCompilationError`
  constructor.

## Open questions for the owner

1. **`bad_request` → `unsupported_query`/`query_validation`, and path/query
   422 → 400, in 2.x (step 2).** Recommended: 2.x. `bad_request` was never in
   `ErrorCode`, it comes from a reason phrase, and ADR 0002 already ships these
   codes and this status in 2.21.0.
2. **Fixed status → code table in 2.x (step 3) although #358 lists it for
   3.0.** Recommended: 2.x, with the 3.11 spellings. Behaviour already differs
   by interpreter, so no single behaviour can be preserved; this keeps 3.11 and
   3.12 unchanged.
3. **Factory shims.** Decided by the owner: no shims. The factories are
   dropped in a 2.x hotfix.
4. **The ETL historify checks sharing `INVALID_TARGET_TYPE`.** Decided: each
   gets its own code in the 2.x hotfix.
