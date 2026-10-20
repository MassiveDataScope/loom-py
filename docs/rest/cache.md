# Cache

A `cache:` section in the YAML turns on a cache-aside layer in front of every
repository whose class is marked `@cached`. The decorator declares; the
composition root binds. `create_app()` (REST, and the agent-only app) and
`bootstrap_worker()` (Celery) read the section at boot, configure `aiocache`
once, and serve each marked repository wrapped in `CachedRepository`,
whichever backend built it — SQLAlchemy, Mongo, DynamoDB, or an explicit
`repository_for(..., builder=)`. Nothing in your code calls `configure`, and
no global is touched.

## Declaring

`@cached` goes on the repository class. Every read the wrapper knows about —
`get_by_id`, the list and cursor reads — is served through the cache from then
on; `get_by`, `exists_by` and `count` are not cached, and writes go straight
through and invalidate.

```python
from loom.core.cache import cache_query, cached
from loom.core.repository import repository_for
from loom.core.repository.sqlalchemy import RepositorySQLAlchemy


@cached
@repository_for(User)
class UserRepository(RepositorySQLAlchemy[User, UserCreate, UserUpdate]):
    @cache_query(scope="entity")
    async def profile(self, user_id: int) -> UserProfile: ...

    @cache_query()
    async def active_in(self, region: str) -> list[User]: ...
```

`@cache_query` marks a custom read method. Annotate its return type: the
wrapper derives a codec from it, so a hit and a miss return the same type. A
`msgspec.Struct`, a scalar, or a `list`, `tuple` or optional of those is
supported; anything else emits a `DeprecationWarning` when the repository is
wrapped and the cached call returns the decoded payload instead.

`scope="entity"` requires the model's primary key as the first positional
argument — a keyword argument does not count, and the wrapper raises
`TypeError` before touching the backend. The entry is then keyed on that id and
is evicted when that row changes. Any other read is `scope="list"` (the
default): it is evicted when any row of the entity changes. `ttl_key="order"`
picks another entity's TTL override when the method caches something other
than its own entity.

A method that returns `None` is not cached — the backend cannot tell a stored
`None` from a miss. Treat every cached result as immutable: concurrent callers
that miss together are served the same object.

## Configuring

```yaml
cache:
  default_ttl: 300          # seconds, single-entity reads
  default_list_ttl: 120     # seconds, list and index reads
  ttl_jitter: 0.1           # fraction of the TTL spread on every write, in [0, 1)
  ttl:                      # per-entity overrides; append _list for the list side
    user: 600
    user_list: 300
  max_size: 1000            # entry cap for every aiocache.SimpleMemoryCache alias
  max_bytes: 33554432       # byte cap for the same aliases; either or both
  aiocache_alias: data      # alias that stores entities (default: "default")
  counter_alias: counters   # alias that stores the generation counters (default: same as aiocache_alias)
  aiocache_config:
    data:
      cache: aiocache.SimpleMemoryCache
      serializer:
        class: loom.core.cache.serializer.MsgspecSerializer
    counters:
      cache: aiocache.SimpleMemoryCache
      # no serializer: counters are stored as raw integers
```

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `true` | `false` runs every repository uncached (see below) |
| `default_ttl` | `200` | TTL in seconds for single-entity reads |
| `default_list_ttl` | `120` | TTL in seconds for list and index reads |
| `ttl_jitter` | `0.1` | Random spread applied per write so a burst does not expire at once; `0` disables it |
| `ttl` | `{}` | Per-entity overrides; `user` for the entity read, `user_list` for the list side |
| `max_size` | none | Entry cap for `aiocache.SimpleMemoryCache` aliases; ignored by Redis |
| `max_bytes` | none | Byte cap on the stored payloads of the same aliases; ignored by Redis |
| `aiocache_alias` | `default` | Alias of the data backend |
| `counter_alias` | none | Alias of the counter backend; falls back to `aiocache_alias` |
| `aiocache_config` | `{}` | Alias map handed to `aiocache`, one entry per alias |

The data alias **must** carry
`serializer: {class: loom.core.cache.serializer.MsgspecSerializer}`: it is what
encodes a model struct to bytes and decodes it back to the declared type. The
counter alias carries **no serializer**: the generation counters are plain
integers bumped with the backend's native increment (`SimpleMemoryCache.increment`,
Redis `INCR`), and a serializer in front of them would break that. When
`counter_alias` is omitted the counters share the data alias and the increment
falls back to a non-atomic get-and-set, which is fine for a single process and
not for several.

`aiocache.SimpleMemoryCache` itself accepts no bound: it only shrinks by TTL.
An alias configured with that class and a bound — `max_size` or `max_bytes` at
the `cache:` level, or its own inside `aiocache_config`, which win — is served by
`loom.core.cache.memory.BoundedMemoryCache` instead, which evicts the least
recently used entries the moment a write crosses either bound. Bytes are
measured on the stored payload, so on the data alias they are the serialized
size, exactly. A single value larger than `max_bytes` is kept and evicts
everything else, because a `get` right after a `set` must not miss. Redis and
every other server backend are forwarded unchanged and bound their own memory.

The YAML reads `aiocache_config:` only. `CacheConfig.from_mapping` also accepts
the short key `aiocache:` for callers building the config by hand; from the
YAML that key — like any other key `CacheConfig` does not declare, `ttls:` for
instance — fails at boot with a configuration error naming it. A typo that
silently disabled every override is the failure mode this guards against.

## What is cached, what invalidates it

Three kinds of entry are stored on the data alias:

- **Entity reads** — `get_by_id` — keyed on the entity name, the id and a
  fingerprint of their tags.
- **List indexes** — the paginated and cursor reads — an index of ids plus the
  entities of the page, so a warm index reloads only the rows it is missing,
  with one `<primary key> IN (...)` query.
- **Custom reads** — every `@cache_query` method — the method's encoded result,
  keyed on the method name, a hash of its arguments and a fingerprint of its
  tags; `scope="entity"` or `scope="list"` chooses those tags and the TTL.

Every entry lists the tags it depends on; the counters alias holds one
generation counter per tag (`tag:<name>`), and an entry whose fingerprint no
longer matches the counters is a miss. A write bumps only the tags it affects:
`user:list` and `user:id:<k>` for each written id, plus any tag the mutation
event carries. Updating one row leaves every other cached row of the entity
warm.

Every entry also carries the caller's scope, which the wrapped repository
reports through `cache_scope()`: on SQLAlchemy it is every session setting the
caller's transactions start with, so a row-level policy reading any setting
keeps its callers' entries apart (cursor tokens bind only the `loom.scope.*`
settings). The scope is not a tag, so a write still evicts the entry in every
scope. Visibility that depends on other tables, such as a membership join a
policy reads, is not in the scope, so a change there reaches a cached entry
only when its TTL runs out.
A per-request value in `session_settings`, such as a request id or an audit
value, makes the cache effectively per request: every request writes its own
entries, and their number grows with the request rate.

The cache takes no part inside a caller-owned transaction: the unit of work a
use case runs in, or a `@transactional` scope. There, `get_by_id`, the list
and query reads and every `@cache_query` method go straight to the repository,
with no lookup and no store, so they see the transaction's own writes; writes
still invalidate as usual. This holds where the backend reports such a
transaction: SQLAlchemy, and Mongo with transactions. DynamoDB and Mongo with
`transactions: false` report none and keep caching. A read-only use case run
outside another unit of work opens none and keeps using the cache. A call to a
`@cache_query` method whose `session` parameter, passed by position or by
name, is not `None` is not cached either.

Inside a use case or a `@transactional` scope the bump is not applied at the
write: it is enqueued on the post-commit channel and published **after the
commit**, ahead of any job dispatch queued in the same pipeline, so a reader
cannot cache a row the transaction later rolled back, and a job cannot read a
stale entry (see [What runs after commit](use-case-dsl.md#what-runs-after-commit)).
Without an open transaction — SQLAlchemy's own session, Mongo without
transactions, DynamoDB — the write commits on its own and the bump runs inline.

The bare `user` tag is in every key's tag list but the framework never bumps
it: it is the manual flush handle. Incrementing `tag:user` on the counter
backend evicts every cached entry of that entity at once.

## Caching any coroutine

Not every expensive call is a repository read. `@cache_call` marks any
coroutine function — an outbound search, an agent toolset method, a slow
computation — and `cached_calls(container)` binds it at the composition root.
The split is the one `@cached` uses: the decorator declares, the composition
root binds, nothing global is touched.

### The contract: a pure function of its arguments

Before anything else, the rule that makes the rest safe. A `@cache_call`
coroutine must be a **pure function of its arguments**. It may not read ambient
identity — a contextvar tenant, the caller's credential, the current request —
and it may not hold a caller-scoped session.

The reason is mechanical. The key is built from the arguments and nothing else,
and on a miss the body runs in a detached task shared by every concurrent
caller, so whoever missed first is the one whose ambient state the body sees. A
coroutine that reads identity from anywhere but its arguments will serve one
caller's answer to another. loom cannot detect this and does not try.

An object that carries identity must put it in the arguments, where the key can
see it. A toolset holding a per-tenant client is not a candidate; a toolset that
takes `tenant` as a parameter is.

A `@cache_call` key carries no scope, so a `@cache_call` coroutine must not
wrap a read from a scoped repository.

### Declaring

```python
from loom.core.cache import cache_call


@cache_call(ttl_key="web_search", unless=lambda docs: not docs, version=2)
async def fetch(query: str, limit: int = 10) -> list[Doc]: ...
```

`@cache_call` writes a marker on the function and returns it unchanged: the
module imports with no configuration, and the function stays importable and
unit-testable on its own. Applied to anything that is not a coroutine function —
a `def`, a generator, an async generator — it raises `TypeError` at decoration
time, naming the function.

### Binding

```python
from loom.core.cache import cached_calls


def build_toolset(ctx: ToolsetContext) -> AbstractToolset[Any]:
    return FunctionToolset(cached_calls(ctx.container).bind(SearchTools()))
```

`cached_calls(container)` returns the `CachedCalls` that boot built from the
`cache:` section, or a pass-through when the application has no section. Always
go through it: `container.resolve(CachedCalls)` raises for a container the
bootstraps did not build, and a `kind: python` factory has no way to know
whether it was. The engine calls that factory as `factory(ctx, **params)` — see
[the `python` capability](#python-application-owned-toolsets).

`wrap(func)` caches one coroutine and returns a function with the original's
name, signature and resolved annotations. `bind(obj)` does the same across an
object and hands the result straight to a `FunctionToolset`.

`bind` publishes **every public coroutine method** of the object, inherited ones
included; marked methods come back cached, unmarked ones bound and otherwise
untouched, in declaration order with base classes first. Adding a public
coroutine helper to a toolset class therefore publishes a tool — give it a
leading underscore, or move it off the class. An `async` `staticmethod` or
`classmethod` is a public coroutine method like any other and is published and
cached the same way. Methods are read off the class, so a property is never
evaluated.

Inheritance is the trap, because a leading underscore only covers the methods
you wrote. `bind` walks the whole MRO, so a class deriving from an async HTTP
client or an SDK base hands the model that base's public coroutines — `request`,
`send`, `aclose` — as callable tools, and `kind: python` has no `include`/
`exclude` to filter them the way `skills`, `mcp` and `a2a` do. A class meant for
`bind` should not subclass anything with public coroutines: **compose, do not
inherit** — hold the client as an attribute and expose the calls you mean to
publish.

### The key

```text
call:<module>.<qualname>:v<version>:<sha256 of the bound arguments>
```

Arguments are bound to the signature and defaults are applied before they are
rendered, so `fetch("q")` and `fetch("q", limit=10)` share an entry, as do
`fetch("q")` and `fetch(query="q")`. A mapping — including a `**kwargs` mapping —
renders as a list of key/value pairs sorted by the whole pair, and a `set` or
`frozenset` renders as a sorted list, so argument order never splits an entry.
The digest is the only thing that leaves the renderer: the key is **opaque**, and
nothing outside `loom.core.cache` should build one or parse one.

Instance identity is not in the key, and neither is the class of the instance:
the qualified name is the one of the class that **defines** the method. Two
instances of one toolset class share an entry for equal arguments — correct for
a stateless toolset, and the other half of the purity contract — but so do
`TenantATools(BaseTools)` and `TenantBTools(BaseTools)` for a method they both
inherit from `BaseTools`: different classes, different credentials, the same
key. Subclassing does not separate entries. The only separators are the
arguments and `version`, so a per-tenant answer takes `tenant` as a parameter.

Entries are separated by the arguments' **values and their types**, at every
depth. Every value the renderer visits carries its qualified type name, so
`w(datetime(2020, 1, 1))` and `w("2020-01-01T00:00:00")` are two entries, as are
a `list`, a `tuple`, a `set` and a `frozenset` with equal members, an `Enum`
member and its value, a struct and a mapping with the same fields. The walk
descends into a struct and a dataclass field by field, so a `datetime`, an
`Enum` or a `set` nested inside one is separated exactly like a top-level one.
A mapping keeps every pair it was given: `{Color.RED: 1, "red": 2}` is two pairs
and does not share a key with `{"red": 2}`.

One case is outside that claim. The walk descends a mapping, a sequence, a set,
a `msgspec.Struct` and a stdlib dataclass itself, so it reaches their members
untouched; an object it has no branch for — an `attrs` class — is expanded by
`msgspec` in a single step, and its fields arrive already flattened. A
`datetime` field inside one is a string before the renderer sees it, so
`Reading(when=datetime(2020, 1, 1))` and `Reading(when="2020-01-01T00:00:00")`
are one entry. The mitigation is a rule about the parameter, not about the
value: annotate a parameter — or a field of such an object — as one type and
convert at the boundary, or add a discriminating argument.

An argument the renderer cannot describe — an open socket, a `nan` or an
infinity, at the top level or nested inside a struct, a list or a mapping —
makes the call run **uncached** and logs one `CacheCallKeyUnrenderable` for that
function. It is not an error: `nan`, `inf` and `-inf` all render as `null` and
would share one entry with each other, so refusing to key them is safer than
keying them wrong.

### TTL, `version`, and what is never invalidated

`ttl_key` resolves through the same `ttl:` mapping entity TTLs use, so a
`ttl_key` equal to an entity name deliberately shares that entity's override.
Without `ttl_key` the call uses `default_ttl`. Either way the written TTL is
spread by `ttl_jitter`, as every other write is.

A cached call is **never invalidated**. It carries no dependency tags — loom
cannot know what an arbitrary coroutine reads — so no write anywhere evicts it,
and there is no manual flush handle for it. It expires, or you bump `version`,
which changes the key and abandons every entry written under the old one. That
is the whole difference from a cached repository read, and it is why a
`@cache_call` TTL should be one you are willing to serve stale for.

### `unless`: do not store this answer

`unless(result)` runs on every miss, before anything is stored. Returning true
means the call returns its value and stores nothing. The case it exists for: a
rate-limited search returns an empty list, and without `unless=lambda docs: not
docs` that emptiness is cached and the agent answers "nothing found" for a whole
TTL.

A result `unless` skips is returned **exactly as the body produced it**, not
encoded and decoded, so its shape can differ from a hit's — one more reason to
declare a precise return type. An `unless` that raises is a bug in your
predicate: it propagates, and nothing is stored.

A body returning `None` is written, but `None` is also the backends' miss
sentinel, so the next call re-runs the body. When that `None` is expensive,
declare `unless=lambda r: r is None` and skip the pointless write.

### Return types

The codec comes from the return annotation, so a hit and a miss return the same
type. An annotation outside the grammar caches **nothing**: the call runs every
time and boot logs one `CacheCallNotCacheable` for that function. Storing a
value a hit and a miss would disagree about is the defect this whole feature
exists to prevent, so the refusal is deliberate and there is no opt-out.

| Annotation | Cached |
|---|---|
| `msgspec.Struct`, a scalar, and `list`/`tuple`/optional of those | yes |
| `BaseModel`, `RootModel`, a parameterised generic model, a `pydantic.dataclasses` type, and `list`/`tuple`/optional of those | yes |
| Any mapping, at any depth: `dict[str, Any]`, `Mapping[str, int]`, a bare `dict`, a `TypedDict`, `dict[str, Any] \| None`, `list[dict[str, Any]]` | no |
| `Any`, `None`, an annotation that does not resolve | no |

The pydantic half of that grammar depends on the process: a pydantic-annotated
return is cached only where pydantic is already imported, because
`loom.core.cache` never imports it itself. A worker built without the `rest`
extra runs the very same coroutine uncached, with `CacheCallNotCacheable`.

The value is decoded on the **write** path, so a model that pydantic cannot
re-validate fails on the very first call rather than on a later hit. That is a
smoke test, not a safety net: `validate_python` is lax, so a field typed `Any`
holding a `datetime` comes back a `str` on both paths without raising anything.
Type your fields precisely; `Any` inside a cached model is where shapes drift.

A payload the codec cannot decode on the **read** path — a model that gained a
field without a `version` bump — is treated as a miss: the body runs, the fresh
value is stored, and one `CacheCallPayloadMismatch` is logged with the key. It
is never raised to the caller. In a cache with no invalidation, the alternative
is failing every caller for a whole TTL.

### What a deployment sees

Six `WARNING` lines name the function, each logged once per function. The
first two are emitted when the function is wrapped — at boot, before any
traffic is served — and the other four only when a call meets the condition:

- `CacheCallNotConfigured` — no `cache:` section, or `enabled: false`; the
  function runs uncached, so the bill is not the first hint.
- `CacheCallNotCacheable` — the return annotation is outside the grammar above,
  or its type hints do not resolve.
- `CacheCallKeyUnrenderable` — an argument could not be rendered into a key.
- `CacheCallPayloadMismatch` — a stored payload no longer decodes.
- `CacheCallReadFailed` — the backend could not be read; the call is served by
  running its body, exactly as an unconfigured deployment would.
- `CacheCallWriteFailed` — the backend refused or could not receive the value.

A seventh, `CacheCallLoadAbandoned`, names the **key** rather than the function:
the load runs detached, so a body that fails after its last caller went away is
reported here instead of disappearing.

That last one is a **deliberate divergence** from the repository, and both
policies are intentional: a cached repository read lets `CacheWriteError`
propagate, because a write it cannot cache is a wiring fault worth surfacing at
once; a cached call logs it and returns the value anyway, because the body has
already produced the caller's answer and failing then would trade a cache
problem for an application outage. The same reasoning covers the read: both
backend calls are guarded, and any failure they raise — a rejected value, a
connection reset, a timeout — degrades the call to an uncached one rather than
failing it, so a Redis outage costs latency and money, not availability. Neither is a bug to be "fixed" into the
other. The value is encoded before the store is attempted, so a caller whose
write failed still receives the **decoded** value and cannot tell a failed
write from a stored one; a result skipped by `unless` remains the one case
where the body's own object comes back. Every other exception from the body
propagates untouched, and nothing is stored.

## Disabled and missing

With `enabled: false`, or with no `cache:` section at all, nothing is wrapped
and every repository runs uncached. Boot logs one `WARNING` `CacheNotConfigured`
per class marked `@cached`, naming it, so a deployment that forgot the section
learns it at start-up rather than from latency.

```yaml
cache:
  enabled: false
```

An empty `cache: {}` is not the same thing: every field has a default and
`enabled` defaults to `true`, so the section is on and the cache runs against a
memory backend. Write `enabled: false` to turn it off.

The wrapping itself is a container binding, `RepositoryDecorator` from
`loom.core.repository`: a single slot that the cache module fills only when it
is empty. A deployment that registers its own decorator before loom's module
replaces the wrapping — a second registration replaces, it does not compose.
Repositories registered in the container by hand, outside the registration
module, are never seen by the decorator.

## Memory to Redis

Moving from memory to Redis is a YAML change: swap the class, add the
connection. The serializer line stays: it is what makes the alias a serialised
one, and only a serialised alias gets the encoding guarantees of
[Failures](#failures).

```yaml
cache:
  aiocache_alias: data
  counter_alias: counters
  aiocache_config:
    data:
      cache: aiocache.RedisCache
      endpoint: ${oc.env:REDIS_HOST,redis}
      port: 6379
      namespace: my_store
      serializer:
        class: loom.core.cache.serializer.MsgspecSerializer
    counters:
      cache: aiocache.RedisCache
      endpoint: ${oc.env:REDIS_HOST,redis}
      port: 6379
      namespace: my_store_counters
      # no serializer: atomic INCR
```

Keep a separate counter alias for any multi-process deployment: with one alias
the counters share the data serializer and lose the atomic increment.
`max_size` and `max_bytes` have no effect on Redis.

## Failures

On an alias configured with
`serializer: {class: loom.core.cache.serializer.MsgspecSerializer}`, a value the
serializer cannot encode raises `CacheWriteError` (a `ValueError`) at the write,
naming the key and the value's type. Nothing is stored for that call: a batch
write encodes every pair before it writes any, so a bad pair last in the batch
stores none of them. Errors the backend itself raises — a Redis connection
refused, a timeout — propagate unchanged; the wrap covers serialisation only.

The promise is that alias's, not the cache's. An alias carrying any other
serializer — a `JsonSerializer` on Redis, or the counter alias, which carries
none — is a raw alias: loom hands the value to `aiocache` untouched, so a value
that cannot be encoded surfaces as whatever the serializer raises, with no key
and no type in the message. Configure the data alias with `MsgspecSerializer`,
as [Configuring](#configuring) requires, to get the named error.

A class marked `@cached` that does not implement the full `Repository` protocol
aborts start-up. `CachedRepository` overrides `get_by_id`, `get_by`, `exists_by`,
`count`, `list_paginated`, `list_with_query`, `create`, `update` and `delete`, so
wrapping a read-only object would advertise writes it cannot serve and fail with
an opaque `AttributeError` on the first call. The wrapper's constructor refuses
instead, and the container reports it as a `ResolutionError` naming the class and
the methods it lacks. Drop `@cached` from a partial repository, or complete it.

Celery builds the gateways once, pre-fork, when the worker validates its
container. A Redis client shared across forked children is the deployment's
concern until a per-worker init hook exists; the memory backend is unaffected.

## Lifecycle

The REST app closes the cache gateways at shutdown, beside the SQL registry:
one `close` per distinct alias, so two with a separate `counter_alias` and one
when the counters share the data alias.
