# ADR 0004: Signed, strictly decoded pagination cursors

- Status: Accepted
- Applies to: loom-kernel 2.21.0 and later
- Classification: hotfix; no external legacy, so no compatibility shims

File and line references in Context are against loom-kernel 2.21.0.

## Context

`loom/core/repository/abc/cursor.py` issued keyset-pagination tokens as the
base64url form of a msgspec JSON record `{"b": backend, "k": keys, "id":
tie_breaker}` (`encode_cursor`, `decode_cursor`). Two defects followed.

1. **No integrity.** Anyone could write a token that decoded. Every consumer
   paginating by hand had to re-validate every field itself, and a forged token reached the keyset
   predicate with values the server never issued.
2. **Lenient decoding.** `_DateTimeKey.v: datetime` accepted a naive ISO
   string. A consumer comparing the decoded key with an aware datetime raised
   `TypeError`, so the client got a 500 instead of a 400. Lenient base64
   decoding also skipped characters outside the alphabet.

The token is an opaque, client-held pointer into a result set that is
already visible to the client. What needs protecting is its integrity and
its binding to the query and the tenant it was issued for, not its
confidentiality.

## Decision

### Two layers

- **Strict codec, in every mode.** A malformed token is
  `UnsupportedQuery`, which REST maps to `400 unsupported_query`, never 500.
  Malformed means: not unpadded base64url (RFC 4648 §5), not the token
  record (bad JSON, unknown field, unknown tag), a datetime key without a
  time zone (`Annotated[datetime, Meta(tz=True)]`), a key of a type the
  format does not carry, a key count that differs from the sort, a
  tie-breaker that differs from the primary key, a value whose type is not
  its column's, or another backend. The message is fixed text: it never
  echoes the token, and the decoder's own error, which may quote the
  payload, is not chained.
- **Typed values.** `decode` takes the annotation of each column
  (`columns`, by name) and the primary-key column names (`primary_key`).
  Every sort key and every primary-key value must have its column's type,
  checked with `msgspec.convert` in strict mode: an `int` is not a `str`, a
  `bool` is not an `int`, `None` matches only an optional column, and a
  string is never parsed into a datetime, date, time, UUID or decimal (those
  must arrive as their tagged key; a `time` key keeps the offset it was
  issued with, or none). A sort field missing from `columns` is
  refused, so a token can only position a query on a column whose type is
  known. The repositories pass `scalar_column_annotations(model)`, the
  columns a REST route may declare sortable. Without this check a token
  carrying a string for an integer key, which an `unsigned` codec accepts
  and a holder of the key can sign, reached the database and answered 500.
  `LoomType.from_builtins` is not used because it parses strings into those
  five types.
- **Signed tokens.** HMAC-SHA256 over the payload and its binding, verified
  before the payload is parsed.

### Format and domain separation

```text
token = base64url(payload) "." base64url(version || len(key_id) || key_id || mac)
mac   = HMAC-SHA256(key, "loom.cursor\0" || version || len(key_id) || key_id
                         || len32(binding) || binding || payload)
```

- `version` is one byte (`1`), so the format can change without ambiguity.
- The fixed context string `loom.cursor\0` separates this MAC from any other
  use of the same key.
- Every variable part is length-prefixed, so two different inputs never
  concatenate to the same bytes.
- The key id travels in the tag segment so the verifier selects the key
  without parsing the payload.
- `mac` is compared with `hmac.compare_digest` before the payload is
  decoded; the payload parser only ever sees authenticated bytes.

### Binding

The binding is recomputed from the request and never travels in the token:

- the backend, carried in the payload and therefore signed;
- the model's qualified name;
- the normalised filters: each condition encoded deterministically, the list
  sorted so the order of the conditions does not matter, every datetime
  converted to an aware UTC instant (a naive one is taken as UTC);
- the sort, field and direction, in order;
- the caller's scope.

Page size and page number are not bound. `CursorCodec.encode` and `decode`
take `model`, `query` and `scope` as required keyword-only arguments, so a
caller cannot forget the binding; `scope=""` is the explicit, documented way
to bind a token to no tenant.

The SQLAlchemy repositories bind `scope` to the caller's row-level-security
values: every `loom.scope.*` key the session-settings provider returns,
serialised in key order. A cursor issued in one tenant, or under an
elevation, does not verify in another. Without a provider, and on Mongo,
which has no such scope, the scope is `""`.

### Keys and rotation

- `persistence.cursors` holds `current_key_id` and `keys`, a mapping of key
  id to exactly one source: `path` (a file, the same contract as
  `JwtAuthConfig.secret_path`) or `env` (an environment variable, which is
  how ECS and Fargate inject secrets). Key material never appears in the
  configuration struct, so a configuration dump cannot leak it, and a
  configuration error names the key id, never the key or its path.
- A key is the UTF-8 text of its source with surrounding whitespace removed
  and must be at least 32 bytes, the SHA-256 output length (NIST SP 800-107);
  a shorter one fails startup. Key ids match `[A-Za-z0-9._-]{1,64}`.
- Rotation: add the new key, make it `current_key_id`, keep the old id in
  `keys` while the tokens it signed matter, then remove it. An unknown key id
  is refused like any other invalid token.

### Modes

`persistence.cursors.signing` selects the mode, following Django's pattern:
the code default is the secure one and every opt-out is written explicitly in
the configuration file of the environment that needs it.

| Mode | Signs with | Where | Use |
|---|---|---|---|
| `signed` | the configured keys | default; `production.yaml` configures the keys | production; the only mode for it |
| `ephemeral` | a random 32-byte key per process | `development.yaml` | development; tokens do not survive a restart and another instance refuses them |
| `unsigned` | nothing; the token is `base64url(payload)` | `test.yaml` | tests and debugging, where a token is read or written by hand |

Both opt-outs log a WARNING at startup. Strict decoding applies in all three
modes: it is correctness, not security.

There is no environment logic in the codec. With `signing` absent the mode is
`signed` in every environment, so a backend that serves repositories
(`sqlalchemy`, `mongo`) and a Celery worker that registers repositories fail
at startup without a key, with a `ConfigError` that names the
`persistence.cursors.signing: ephemeral` opt-out for `development.yaml`.

A deploy check, `loom.cursors.E001`, registered with `@deploy_check` from
the deploy-check registry of ADR 0005, refuses a deployment that serves
repositories without signing its cursors with a configured key. It lives in
`loom.core.repository.abc.checks`, which `loom.core.repository.abc` imports,
and `loom check --deploy` imports that package. It applies when the
configuration has a `database` section (SQLAlchemy, and the Celery worker)
or a `persistence.mongo` section, and finds:

- `persistence.cursors.signing` set to anything but the literal `signed`
  (absent means `signed`): `ephemeral` and `unsigned` are for development
  and tests;
- a `current_key_id` that is not a literal entry of `keys`. Startup fails on
  it anyway when the codec is built; the check reports it in CI too.

Like the other checks it reads the keys as written and resolves no
placeholder, so it runs without the key material, and it runs only in
production and staging (ADR 0005). `persistence.cursors.*` is not on the
`LOOM__*` override allowlist, so in production and staging
`LOOM__PERSISTENCE__CURSORS__SIGNING=unsigned` fails at startup before any
check runs. A key's `env` source names the variable holding it; loom reads
it through `loom.core.environment.configured_variable`, which refuses a
`LOOM_*` name, since those are loom's own.

### Wiring and public surface

- One `CursorCodec`, built once per process by the backend's `build()` (and
  by the Celery worker) with `CursorCodec.from_context(ctx)`, passed as a
  required keyword-only `cursor_codec` to `RepositorySQLAlchemy`,
  `RepositoryMongo`, `QuerySpecCompiler` and the default builders, and
  registered in DI so a service paginating by hand resolves the codec the
  repositories use.
- The public surface is `CursorCodec`, its configuration (`CursorConfig`,
  `CursorKeySource`) and the decoded position `Cursor`. `encode_cursor` and
  `decode_cursor` are removed.

### Naive datetimes

A naive datetime key, such as a `DateTime(tz=False)` column, is issued as UTC
with an explicit offset, so every token carries a zone and decoding refuses a
naive one. The SQLAlchemy keyset predicate converts the value back to naive
UTC for a `timezone=False` column; Mongo stores aware UTC already.

### No expiry

There is no `exp` claim. A cursor grants nothing beyond what the caller can
already request from the first page under the same binding, rotation already
bounds how long a key's tokens verify, and an expiry would add a clock
dependency and a failure mode to a page pointer. It can be added later as a
new version byte if a consumer needs it.

## Alternatives rejected

- **AEAD-encrypted cursors (AES-GCM, ChaCha20-Poly1305).** They add
  confidentiality the token does not need: the keys it carries are column
  values of rows the caller has just received. A MAC gives the integrity and
  binding that matter with less machinery and no nonce management.
- **JWS / JWT.** Algorithm agility in the header has a history of
  algorithm-confusion and `none` attacks, the header and claim set are
  heavier than a page pointer needs, and the binding would still have to be
  designed. A fixed algorithm with a version byte avoids negotiation.
- **Server-side cursor storage.** It makes pagination stateful: storage,
  expiry and cleanup, and every instance must reach the same store. A signed
  token keeps pagination stateless.

## Standards

- HMAC: RFC 2104, FIPS 198-1, ISO/IEC 9797-2.
- SHA-256: FIPS 180-4.
- HMAC key length: NIST SP 800-107 (at least the hash output length).
- base64url without padding: RFC 4648 §5.
- Key management and rotation: NIST SP 800-57 Part 1.
- Constant-time tag comparison: `hmac.compare_digest`.

## Consequences

- Breaking: an application with a repository-serving backend needs
  `persistence.cursors` keys or an explicit opt-out. Tokens issued before
  this change are refused with 400; clients restart from the first page.
- Breaking: the repository constructors, the default builders, the
  registration modules and `build_repository_harness` take `cursor_codec`;
  a custom builder resolves `CursorCodec` from `context.container`.
- A consumer paginating by hand drops its own field validation: the codec
  refuses everything it used to check, and binds the tenant explicitly.
