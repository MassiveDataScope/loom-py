# REST Quickstart

`loom-kernel` lets you define business behavior in typed use cases and expose it
through REST interfaces — with full auto-CRUD, background jobs, and Celery workers.

## Install

```bash
pip install "loom-kernel[rest,sqlalchemy,cache,celery]"
```

---

## 60-second demo: full CRUD from a model

Define a model and set `auto = True` on a `RestInterface`. That is the entire
application layer:

```python
from loom.core.model import ColumnField, TimestampedModel
from loom.rest.model import PaginationMode, RestInterface


class Product(TimestampedModel):
    __tablename__ = "products"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    sku: str = ColumnField(length=64, unique=True, index=True)
    name: str = ColumnField(length=150)
    price_cents: int = ColumnField()
    stock: int = ColumnField()


class ProductInterface(RestInterface[Product]):
    prefix = "/products"
    tags = ("Products",)
    auto = True
    pagination_mode = PaginationMode.CURSOR
```

Wire it up:

```yaml
# config/api.yaml
app:
  name: my_store
  code_path: src
  discovery:
    mode: modules
    modules:
      include:
        - app.product.model
        - app.product.interface
  rest:
    backend: fastapi
    title: My Store API
    version: 0.1.0

database:
  url: ${oc.env:DATABASE_URL,sqlite+aiosqlite:///store.db}

observability:
  log:
    enabled: false
  otel:
    enabled: false
```

```python
# main.py
from loom.rest.fastapi.auto import create_app

app = create_app("config/api.yaml")
```

Five endpoints (`GET /`, `GET /:id`, `POST /`, `PATCH /:id`, `DELETE /:id`) are
live with zero hand-written use-case code.

---

## Add business logic

Use cases declare inputs and invariants declaratively. The engine resolves them
before `execute()` runs.

```python
import re
from loom.core.command import Command, Patch
from loom.core.errors import NotFound
from loom.core.use_case import Exists, F, Input, LoadById, OnMissing, Rule
from loom.core.use_case.use_case import UseCase

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

class CreateUser(Command, frozen=True):
    full_name: str
    email: str

class UpdateUser(Command, frozen=True):
    full_name: Patch[str] = None
    email: Patch[str] = None

def _name_must_not_be_blank(full_name: str) -> str | None:
    return None if full_name.strip() else "full_name must not be blank"

def _email_must_be_valid(email: str) -> str | None:
    return None if _EMAIL_RE.fullmatch(email) else "email must be valid"

def _email_is_taken(cmd: CreateUser, fields_set: frozenset[str], email_exists: bool) -> bool:
    return email_exists

class CreateUserUseCase(UseCase[User, User]):
    rules = [
        Rule.check(F(CreateUser).full_name, via=_name_must_not_be_blank),
        Rule.check(F(CreateUser).email, via=_email_must_be_valid),
        Rule.forbid(_email_is_taken, message="email already exists").from_params("email_exists"),
    ]

    async def execute(
        self,
        cmd: CreateUser = Input(),
        email_exists: bool = Exists(User, from_command="email", against="email"),
    ) -> User:
        return await self.main_repo.create(cmd)

class UpdateUserUseCase(UseCase[User, User | None]):
    rules = [Rule.check(F(UpdateUser).full_name, via=_name_must_not_be_blank).when_present(F(UpdateUser).full_name)]

    async def execute(
        self,
        user_id: int,
        cmd: UpdateUser = Input(),
        current_user: User = LoadById(User, by="user_id"),
    ) -> User | None:
        return await self.main_repo.update(user_id, cmd)
```

**`Exists`** checks a DB condition before `execute()` runs — no boilerplate in the body.
**`LoadById`** fetches an entity by a path/command parameter, available in rules and the body.
**`Patch[T]`** marks a field as optional in partial updates; `.when_present(...)` gates rules on whether the field was sent.

---

## Scope resources under a parent

Use `from_param` to guard nested routes (e.g. `/users/{user_id}/addresses/{address_id}`):

```python
from loom.core.use_case import Exists, Input, OnMissing

class CreateAddressUseCase(UseCase[Address, Address]):
    async def execute(
        self,
        user_id: int,
        cmd: CreateUserAddress = Input(),
        _user_exists: bool = Exists(User, from_param="user_id", against="id", on_missing=OnMissing.RAISE),
    ) -> Address:
        return await self.main_repo.create(CreateAddressRecord(user_id=user_id, **cmd.__dict__))
```

`OnMissing.RAISE` returns a structured 404 automatically — no `if` in the body.

---

## Structured queries

Build explicit queries without raw SQL:

```python
from loom.core.repository.abc.query import (
    FilterGroup, FilterOp, FilterSpec, PageResult, PaginationMode, QuerySpec, SortSpec,
)

class ListLowStockProductsUseCase(UseCase[Product, PageResult[Product]]):
    async def execute(self, profile: str = "default") -> PageResult[Product]:
        query = QuerySpec(
            filters=FilterGroup(filters=(FilterSpec(field="stock", op=FilterOp.LTE, value=5),)),
            sort=(SortSpec(field="stock", direction="ASC"),),
            pagination=PaginationMode.OFFSET,
            limit=20,
            page=1,
        )
        result = await self.main_repo.list_with_query(query, profile=profile)
        if not isinstance(result, PageResult):
            raise RuntimeError("expected offset result")
        return result
```

---

## Background jobs

Jobs are use-case-like executors that run in a Celery queue. `LoadById` works the same way:

```python
from loom.core.job.job import Job
from loom.core.use_case import Input, LoadById

class SendRestockEmailJob(Job[bool]):
    __queue__ = "notifications"

    async def execute(
        self,
        product_id: int,
        cmd: SendRestockEmailCommand = Input(),
        product: Product = LoadById(Product, by="product_id"),
    ) -> bool:
        if product.stock > 0:
            return False
        # send email to cmd.recipient_email …
        return True
```

---

## Dispatch jobs from use cases + callbacks

```python
from loom.core.job.service import JobService

class DispatchRestockEmailUseCase(UseCase[Product, DispatchRestockEmailResponse]):
    def __init__(self, job_service: JobService) -> None:
        self._jobs = job_service

    async def execute(self, product_id: str, cmd: DispatchRestockEmailCommand = Input()) -> DispatchRestockEmailResponse:
        handle = self._jobs.dispatch(
            SendRestockEmailJob,
            params={"product_id": int(product_id)},
            payload={"product_id": int(product_id), "recipient_email": cmd.recipient_email},
            on_success=RestockEmailSuccessCallback,
            on_failure=RestockEmailFailureCallback,
        )
        return DispatchRestockEmailResponse(job_id=handle.job_id, queue=handle.queue)
```

Callbacks are resolved by the DI container and receive the job result + context:

```python
class RestockEmailSuccessCallback:
    def __init__(self, app: ApplicationInvoker) -> None:
        self._app = app

    async def on_success(self, job_id: str, result: Any, **context: Any) -> None:
        if not result:
            return
        entity = self._app.entity(Product)
        product = await entity.get(params={"id": context["product_id"]})
        if product:
            await entity.update(params={"id": product.id}, payload={"category": f"{product.category}-notified"})
```

---

## Chain use cases (workflow pattern)

`ApplicationInvoker` lets a use case call another use case by type — no tight coupling:

```python
from loom.core.use_case.invoker import ApplicationInvoker

class RestockWorkflowUseCase(UseCase[Product, RestockWorkflowResponse]):
    def __init__(self, app: ApplicationInvoker, job_service: JobService) -> None:
        self._app = app
        self._jobs = job_service

    async def execute(self, product_id: str, cmd: DispatchRestockEmailCommand = Input()) -> RestockWorkflowResponse:
        summary = await self._app.invoke(BuildProductSummaryUseCase, params={"product_id": int(product_id)})
        handle = self._jobs.dispatch(SendRestockEmailJob, params={"product_id": int(product_id)}, payload={...})
        return RestockWorkflowResponse(summary=summary.summary, restock_job_id=handle.job_id, queue=handle.queue)
```

---

## Declare REST interfaces

```python
from loom.rest.autocrud import build_auto_routes
from loom.rest.model import PaginationMode, RestInterface, RestRoute

class ProductRestInterface(RestInterface[Product]):
    prefix = "/products"
    tags = ("Products",)
    pagination_mode = PaginationMode.CURSOR
    routes = (
        RestRoute(use_case=ListLowStockProductsUseCase, method="GET", path="/low-stock",
                  summary="List low stock products"),
        RestRoute(use_case=DispatchRestockEmailUseCase, method="POST",
                  path="/{product_id}/jobs/restock-email", status_code=202,
                  summary="Dispatch restock email"),
        RestRoute(use_case=RestockWorkflowUseCase, method="POST",
                  path="/{product_id}/workflows/restock", status_code=202,
                  summary="Run restock workflow"),
        *build_auto_routes(Product, ()),  # adds GET, POST, PATCH, DELETE automatically
    )
```

Nested resource interfaces work the same way — routes mirror the URL hierarchy:

```python
class AddressRestInterface(RestInterface[Address]):
    prefix = "/users"
    tags = ("UserAddresses",)
    routes = (
        RestRoute(use_case=CreateAddressUseCase, method="POST",   path="/{user_id}/addresses/",            status_code=201),
        RestRoute(use_case=ListAddressesUseCase, method="GET",    path="/{user_id}/addresses/"),
        RestRoute(use_case=GetAddressUseCase,    method="GET",    path="/{user_id}/addresses/{address_id}"),
        RestRoute(use_case=UpdateAddressUseCase, method="PATCH",  path="/{user_id}/addresses/{address_id}"),
        RestRoute(use_case=DeleteAddressUseCase, method="DELETE", path="/{user_id}/addresses/{address_id}"),
    )
```

---

## Bootstrap with YAML

The `create_app()` factory wires everything — DB, cache, DI, routes — from a YAML config:

```yaml
# config/api.yaml
app:
  name: my_store
  code_path: src
  discovery:
    mode: modules
    modules:
      include:
        - app.user.model
        - app.user.interface
        - app.product.model
        - app.product.interface
  rest:
    backend: fastapi
    title: My Store API
    version: 0.1.0

database:
  url: ${oc.env:DATABASE_URL,sqlite+aiosqlite:///store.db}

observability:
  log:
    enabled: true
  otel:
    enabled: false
  prometheus:
    enabled: true
    config:
      path: /metrics
```

```python
# main.py — 3 lines
from loom.rest.fastapi.auto import create_app

app = create_app("config/api.yaml")
```

For larger projects, use `mode: manifest` and a manifest module:

```python
# app/manifest.py
from app.user.model import User
from app.user.interface import UserRestInterface

MODELS = [User, ...]
INTERFACES = [UserRestInterface, ...]
```

```yaml
discovery:
  mode: manifest
  manifest:
    module: app.manifest
```

### Choose the persistence backend

Backends are plugins resolved by name from the `loom.persistence.backends`
entry point group; `database:` is read by the default one. `persistence.backend`
selects it:

| `persistence.backend` | Reads | What the app gets |
|---|---|---|
| `sqlalchemy` (default) | `database:` | unit of work, repositories with every capability (`create_many` included) and compiled tables for every discovered model; needs the `sqlalchemy` extra, a driver and a reachable database even with zero models |
| `dynamodb` | `persistence.dynamodb:` | key-value repositories (`Readable`, `Creatable`, `Updatable`, `Deletable`); auto-CRUD mounts get, create, update and delete — `list` and `count` are not mounted, and `get_by` / `exists_by` on a non-key field raise `UnsupportedQuery`; no relational tables |
| `mongo` | `persistence.mongo:` | one collection per model in one database, repositories with every capability (`create_many` included), offset and cursor pagination, `QuerySpec` filters and sorts; unit of work is a no-op unless `transactions: true` (needs a replica set); ids minted by loom (`uuid4`, or `objectid` per config), `autoincrement` refused at startup; needs the `mongo` extra |
| `none` | nothing | no unit of work, no repositories; discovered models are accepted but not compiled, a `database:` section is ignored, deferred job dispatch never fires, and an interface with `auto_crud_model` refuses to boot |

Auto-CRUD is gated by the capabilities the serving repository class declares:
an interface with an empty `include` mounts only the operations the backend
supports, an explicit `include` naming an unsupported one fails at startup.
An unknown backend name fails at startup listing the registered ones. Writing a
backend, the capability gate, the cursor contract, the `mongo` keys and the
recipe for a model that runs unchanged on every backend are described in
[Persistence backends](../rest/persistence-backends.md).

```yaml
persistence:
  backend: none
```

`none` is for an application whose only content is agents (see
[An application that is only agents](../ai/overview.md#an-application-that-is-only-agents)).
It is never inferred: with the default backend and no discovered models the
application starts with a WARNING that names `persistence.backend: none`, and
still opens a connection — so the driver and the database have to be there.

### `GET /health`

Every application built by `create_app` serves `GET /health`, registered ahead
of every interface router and outside the OpenAPI schema:

```json
{"status": "ok", "backends": {"sqlalchemy": true}}
```

- `status` is `ok` when every backend is ready and `degraded` — with HTTP
  `503` — when any is not. Each key of `backends` is the `persistence.backend`
  name and its readiness (`SELECT 1` for `sqlalchemy`, `DescribeTable` with an
  `ACTIVE` or `UPDATING` table for `dynamodb`, `ping` for `mongo`).
- `none`, or a backend without a readiness probe, answers
  `{"status": "ok", "backends": {}}`.
- The probe result is cached for a short TTL, concurrent requests share one
  probe, and a probe that exceeds its timeout reports the backend as not ready.
  `dynamodb` needs `dynamodb:DescribeTable` on the table ARN for it, in
  addition to the item permissions. Do not expose `/health` on the public
  ingress; it is anonymous and meant for the orchestrator.
- `/health` is in the default authentication exclusions, so an orchestrator
  needs no credentials. An explicit `exclude_paths` list is honoured verbatim:
  **include `/health` in it**, or the probe answers `401`.
- The path is reserved: a `RestInterface` compiling to a literal `/health` is a
  compilation error, and — as for `/metrics` — a catch-all route such as
  `/{tenant}` that would capture it aborts an authenticated startup.

### Secrets in YAML (`resolvers=`)

`create_app` registers loom's built-in AWS resolvers, `secrets` (Secrets
Manager) and `ssm` (SSM Parameter Store), by default. An app booted through the
factory reads a secret with no code beyond the factory call:

```yaml
# config/api.yaml
database:
  url: ${secrets:/prod/store/database-url}

sql:
  connections:
    analytics:
      backend: clickhouse
      url: ${ssm:/prod/store/analytics-url}
```

```python
from loom.rest.fastapi.auto import create_app

app = create_app("config/api.yaml")
```

The built-in resolvers use boto3's default region and credential chain, and
create their client only when a placeholder resolves: a YAML with no
`${secrets:...}` or `${ssm:...}` never touches AWS and boots without boto3.
When a placeholder does resolve and boto3 is missing, the error names the extra
to install, `loom-kernel[config-ssm]`.

`resolvers=` adds your own prefixes or overrides a built-in by name. Any object
with a `name` and a `resolve(key) -> object` works:

```python
from loom.rest.fastapi.auto import create_app

class VaultResolver:
    name = "vault"

    def resolve(self, key: str) -> str:
        return read_vault_secret(key)

app = create_app("config/api.yaml", resolvers=[VaultResolver()])
```

A resolver you pass with the same name as a built-in wins; a built-in default
never replaces a resolver already registered earlier in the process, so
calling the factory more than once is safe. `load_config` registers no
defaults, and resolvers passed to it explicitly replace an earlier registration
of the same name.

### Binding your own ports (`modules=`)

`create_app` registers repositories for you, and verifies at startup that every
dependency a use case's constructor declares is bound. A port no repository
provides — a clock, an identity verifier, a bridge implementing one bounded
context's port on top of another — is bound by a module: a callable that
receives the container.

```python
from datetime import UTC, datetime
from typing import Protocol

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.rest.fastapi.auto import create_app


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


def clock_module(container: LoomContainer) -> None:
    container.register(Clock, SystemClock, scope=Scope.APPLICATION)


app = create_app("config/api.yaml", modules=[clock_module])
```

Modules run in the order given, before repositories are registered and before
use cases are verified — the same semantics as the Celery `create_app`. A use
case injecting `Clock` without the module still aborts startup with a
`ResolutionError` naming the use case and the missing binding.

Modules bind ports, they do not override the framework. `register` is
last-write-wins, and `create_app` registers its own bindings after your modules:
the repositories, the cache module, `JobService`, `SqlQueryService`,
`CallerBoundSql`, `ObservabilityRuntime` and, on PostgreSQL with
`database.schema.mode: external`, `ElevationSink`. A module that binds one of those keys is silently
replaced.

---

## Rules + Computes (advanced)

For compute-heavy write flows, declare field derivations and run them before rules:

```python
from loom.core.use_case import Compute, F

def _normalize_email(email: str) -> str:
    return email.strip().lower()

def _compute_subtotal(unit_price: float, quantity: int) -> float:
    return unit_price * quantity

def _compute_tax(subtotal: float, tax_rate: float) -> float:
    return subtotal * tax_rate

def _unit_price_invalid(unit_price: float) -> bool:
    return unit_price <= 0

def _country_unsupported(country: str) -> bool:
    return country not in TAX_RATES

class PricingPreviewUseCase(UseCase[Record, PricingPreviewResponse]):
    computes = (
        Compute.set(F(PricingCommand).normalized_email).from_command(
            F(PricingCommand).email, via=_normalize_email,
        ),
        Compute.set(F(PricingCommand).subtotal).from_command(
            F(PricingCommand).unit_price, F(PricingCommand).quantity,
            via=_compute_subtotal,
        ),
        Compute.set(F(PricingCommand).tax_amount).from_command(
            F(PricingCommand).subtotal, F(PricingCommand).tax_rate,
            via=_compute_tax,
        ),
    )
    rules = (
        Rule.check(F(PricingCommand).unit_price, via=_unit_price_invalid, message="unit_price must be > 0"),
        Rule.check(F(PricingCommand).country, via=_country_unsupported, message="Unsupported country"),
    )

    async def execute(self, record_id: int, cmd: PricingCommand = Input()) -> PricingPreviewResponse:
        ...
```

Computes run in declaration order — later computes can reference fields set by earlier ones.

---

## Next steps

- [Auto-CRUD guide](../rest/autocrud.md) — full options reference for `auto = True` and `build_auto_routes()`
- [Use-case DSL](../rest/use-case-dsl.md) — rules, computes, predicates
- [Celery worker](../rest/celery.md) — job definitions, dispatch, callbacks, worker bootstrap
- [Examples repo](../rest/examples.md) — full walkthrough of the `dummy-loom` companion app
- [REST testing guide](../rest/testing.md) — unit and integration testing utilities
- [API reference](../reference/index.rst)
