# Examples repo

The companion repository [`dummy-loom`](https://github.com/the-reacher-data/dummy-loom)
is a runnable full-stack demo that combines every major `loom-kernel` feature in a
single PostgreSQL-backed application. The code below comes directly from its `src/` tree.

```bash
git clone https://github.com/the-reacher-data/dummy-loom
cd dummy-loom
make up      # postgres + redis + API + celery worker + flower
```

Swagger UI is available at `http://localhost:8000/docs`.

---

## 1. Model layer

### Simple model (`User`)

```python
from loom.core.model import ColumnField, TimestampedModel

class User(TimestampedModel):
    __tablename__ = "users"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    full_name: str = ColumnField(length=120)
    email: str = ColumnField(length=255, unique=True, index=True)
```

`TimestampedModel` adds `created_at` / `updated_at` columns automatically.

### Model with a foreign key and cascade delete (`Address`)

```python
from loom.core.model import BaseModel, ColumnField, OnDelete

class Address(BaseModel):
    __tablename__ = "addresses"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    user_id: int = ColumnField(foreign_key="users.id", on_delete=OnDelete.CASCADE, index=True)
    label: str = ColumnField(length=80)
    street: str = ColumnField(length=255)
    city: str = ColumnField(length=120)
    country: str = ColumnField(length=120)
    zip_code: str = ColumnField(length=20)
```

`OnDelete.CASCADE` propagates deletions to child rows at the DB level.

### Product model with indexed and unique fields

```python
class Product(TimestampedModel):
    __tablename__ = "products"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    sku: str = ColumnField(length=64, unique=True, index=True)
    name: str = ColumnField(length=150)
    category: str = ColumnField(length=120, index=True)
    price_cents: int = ColumnField()
    stock: int = ColumnField()
```

---

## 2. Commands

Commands are immutable `msgspec.Struct` instances that carry request data.
`Patch[T]` marks a field as optional for partial updates.

```python
from loom.core.command import Command, Patch

class CreateUserAddress(Command, frozen=True):
    label: str
    street: str
    city: str
    country: str
    zip_code: str

class UpdateAddress(Command, frozen=True):
    label: Patch[str] = None
    street: Patch[str] = None
    city: Patch[str] = None
    country: Patch[str] = None
    zip_code: Patch[str] = None
```

---

## 3. Use cases

### Create with parent-scoping guard (`CreateAddressUseCase`)

`Exists(..., on_missing=OnMissing.RAISE)` checks the parent exists before
`execute()` runs — no `if` boilerplate:

```python
from loom.core.use_case import Exists, Input, OnMissing
from loom.core.use_case.use_case import UseCase

class CreateAddressUseCase(UseCase[Address, Address]):
    async def execute(
        self,
        user_id: int,
        cmd: CreateUserAddress = Input(),
        _user_exists: bool = Exists(User, from_param="user_id", against="id", on_missing=OnMissing.RAISE),
    ) -> Address:
        payload = CreateAddressRecord(user_id=user_id, **cmd.__dict__)
        return await self.main_repo.create(payload)
```

### List with ownership filter

Filter records at the query level — no Python-level filtering:

```python
class ListAddressesUseCase(UseCase[Address, PageResult[Address] | CursorResult[Address]]):
    async def execute(
        self,
        user_id: int,
        query: QuerySpec,
        profile: str = "default",
    ) -> PageResult[Address] | CursorResult[Address]:
        scoped_query = QuerySpec(
            filters=FilterGroup(
                filters=(FilterSpec(field="user_id", op=FilterOp.EQ, value=user_id),),
            ),
            sort=query.sort,
            pagination=query.pagination,
            limit=query.limit,
            page=query.page,
            cursor=query.cursor,
        )
        return await self.main_repo.list_with_query(scoped_query, profile=profile)
```

### Custom query with structured filters

```python
class ListLowStockProductsUseCase(UseCase[Product, PageResult[Product]]):
    async def execute(self, profile: str = "default") -> PageResult[Product]:
        query = QuerySpec(
            filters=FilterGroup(
                filters=(FilterSpec(field="stock", op=FilterOp.LTE, value=5),)
            ),
            sort=(
                SortSpec(field="stock", direction="ASC"),
                SortSpec(field="id", direction="ASC"),
            ),
            pagination=PaginationMode.OFFSET,
            limit=20,
            page=1,
        )
        result = await self.main_repo.list_with_query(query, profile=profile)
        if not isinstance(result, PageResult):
            raise RuntimeError("Expected offset result")
        return result
```

### Custom repository: override CRUD through `main_repo`

Register a model-specific repository once and standard CRUD use cases will
pick it up automatically as their `main_repo`.

```python
from typing import Protocol

import msgspec

from loom.core.repository import repository_for
from loom.core.repository.abc import RepoFor
from loom.core.repository.sqlalchemy import RepositorySQLAlchemy


class ProductRepo(RepoFor[Product], Protocol):
    async def get_by_slug(self, slug: str) -> Product | None:
        ...


@repository_for(Product)
class ProductRepository(RepositorySQLAlchemy[Product, int], ProductRepo):
    async def create(self, data: msgspec.Struct) -> Product:
        payload = msgspec.to_builtins(data)
        payload["name"] = str(payload["name"]).strip()
        return await super().create(payload)
```

```python
class CreateProductUseCase(UseCase[Product, Product]):
    async def execute(self, cmd: CreateProduct = Input()) -> Product:
        return await self.main_repo.create(cmd)
```

### Logical type with automatic `main_repo`

`main_repo` can also be bound to a non-persistible logical type. Use
`Response` when the returned object is part of your public API contract and you
want the usual REST `camelCase` output.

```python
from typing import Protocol

from loom.core.repository import repository_for
from loom.core.response import Response
from loom.core.use_case.use_case import UseCase


class TaskView(Response):
    task_id: str
    state: str


class TaskViewRepo(Protocol):
    async def get_by_id(self, obj_id: str, profile: str = "default") -> TaskView | None:
        ...


@repository_for(TaskView)
class TaskViewRepository(TaskViewRepo):
    def __init__(self) -> None:
        self._items = {"t-1": TaskView(task_id="t-1", state="done")}

    async def get_by_id(self, obj_id: str, profile: str = "default") -> TaskView | None:
        return self._items.get(obj_id)


class GetTaskViewUseCase(UseCase[TaskView, TaskView | None, TaskViewRepo]):
    async def execute(self, task_id: str) -> TaskView | None:
        return await self.main_repo.get_by_id(task_id)
```

Use `LoomStruct` instead of `Response` when you want a logical type without
REST-specific `camelCase` serialization.

### Default repository builder and explicit builder override

The framework now resolves the default repository through the
`DefaultRepositoryBuilder` dependency. In the current SQLAlchemy stack, the
bootstrap registers `SQLAlchemyDefaultRepositoryBuilder` as that default, and
that builder keeps `SessionManager` inside the SQLAlchemy adapter layer.

This means:

- the core registry no longer depends on SQLAlchemy constructor details
- SQLAlchemy keeps `SessionManager` as an infrastructure concern
- applications can replace the default builder without changing `UseCase`

Conceptually, the current SQLAlchemy fallback is:

```python
from dataclasses import dataclass

from loom.core.repository import DefaultRepositoryBuilder, RepositoryBuildContext
from loom.core.repository.abc import CursorCodec
from loom.core.repository.sqlalchemy import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager


@dataclass(frozen=True)
class SQLAlchemyDefaultRepositoryBuilder:
    session_manager: SessionManager
    cursor_codec: CursorCodec

    def __call__(self, context: RepositoryBuildContext) -> object:
        return RepositorySQLAlchemy(
            session_manager=self.session_manager,
            model=context.model,
            cursor_codec=self.cursor_codec,
        )
```

A builder that constructs a `RepositorySQLAlchemy` or a `RepositoryMongo`
itself passes the shared `CursorCodec`: resolve it from
`context.container`, where the backend registers the one it built from
`persistence.cursors`.

An application that wants a different project-wide base repository can
register another `DefaultRepositoryBuilder` implementation in the container.

That is the extension point for replacing the default backend globally. For
example, an application can swap the SQLAlchemy fallback for its own base
repository while keeping `UseCase[Model, Result]` unchanged:

```python
from dataclasses import dataclass

from loom.core.repository import DefaultRepositoryBuilder, RepositoryBuildContext


@dataclass(frozen=True)
class MyBaseRepositoryBuilder:
    settings: AppSettings

    def __call__(self, context: RepositoryBuildContext) -> object:
        return MyBaseRepository(model=context.model, settings=self.settings)


container.register_instance(
    DefaultRepositoryBuilder,
    MyBaseRepositoryBuilder(settings=settings),
)
```

After that registration:

- every model without an explicit `repository_for(...)` uses `MyBaseRepository`
- `UseCase[Model, Result]` keeps working through `self.main_repo`
- model-specific overrides still win when needed

For per-model overrides that need extra dependencies, use `builder=` on
`repository_for(...)`:

```python
from dataclasses import dataclass

from loom.core.di.scope import Scope
from loom.core.repository import RepositoryBuildContext, repository_for
from loom.core.response import Response


class TaskSnapshot(Response):
    task_id: str
    state: str


class TaskSnapshotRepo(Protocol):
    async def get_by_id(self, obj_id: str, profile: str = "default") -> TaskSnapshot | None:
        ...


@dataclass(frozen=True)
class TaskRepoSettings:
    state: str


def build_task_snapshot_repository(context: RepositoryBuildContext) -> object:
    settings = context.container.resolve(TaskRepoSettings)
    return TaskSnapshotRepository(settings=settings)


@repository_for(TaskSnapshot, builder=build_task_snapshot_repository)
class TaskSnapshotRepository(TaskSnapshotRepo):
    def __init__(self, settings: TaskRepoSettings) -> None:
        self._settings = settings

    async def get_by_id(self, obj_id: str, profile: str = "default") -> TaskSnapshot | None:
        return TaskSnapshot(task_id=obj_id, state=self._settings.state)
```

The application bootstrap only has to register the extra dependency:

```python
container.register(
    TaskRepoSettings,
    lambda: TaskRepoSettings(state="from-builder"),
    scope=Scope.APPLICATION,
)
```

Resolution order is:

1. explicit `repository_for(..., builder=...)`
2. explicit `repository_for(...)` by class
3. `DefaultRepositoryBuilder`

Use the first two for per-model customization; use `DefaultRepositoryBuilder`
to replace the project-wide backend.

---

## 4. Background jobs

Jobs run in a Celery queue. `LoadById` fetches the entity automatically:

```python
from loom.core.job.job import Job
from loom.core.use_case import Input, LoadById

class SendRestockEmailJob(Job[bool]):
    __queue__ = "notifications"

    async def execute(
        self,
        product_id: int,
        cmd: SendRestockEmailJobCommand = Input(),
        product: Product = LoadById(Product, by="product_id"),
    ) -> bool:
        if cmd.force_fail:
            raise RuntimeError("forced restock email failure")
        if product.stock > 0:
            return False
        # send email to cmd.recipient_email …
        return True


class BuildProductSummaryJob(Job[str]):
    __queue__ = "analytics"

    async def execute(
        self,
        product_id: int,
        product: Product = LoadById(Product, by="product_id"),
    ) -> str:
        availability = "in stock" if product.stock > 0 else "out of stock"
        return f"{product.sku} ({product.name}) is {availability}."
```

---

## 5. Job dispatch from use cases

```python
from loom.core.job.service import JobService

class DispatchRestockEmailUseCase(UseCase[Product, DispatchRestockEmailResponse]):
    def __init__(self, job_service: JobService) -> None:
        self._jobs = job_service

    async def execute(
        self,
        product_id: str,
        cmd: DispatchRestockEmailCommand = Input(),
    ) -> DispatchRestockEmailResponse:
        handle = self._jobs.dispatch(
            SendRestockEmailJob,
            params={"product_id": int(product_id)},
            payload={
                "product_id": int(product_id),
                "recipient_email": cmd.recipient_email,
                "force_fail": cmd.force_fail,
            },
            on_success=RestockEmailSuccessCallback,
            on_failure=RestockEmailFailureCallback,
        )
        return DispatchRestockEmailResponse(job_id=handle.job_id, queue=handle.queue)
```

---

## 6. Job callbacks

Callbacks receive the job result and a context dict. They can call back into the
application via `ApplicationInvoker` without tight coupling:

```python
from loom.core.use_case.invoker import ApplicationInvoker

class RestockEmailSuccessCallback:
    def __init__(self, app: ApplicationInvoker) -> None:
        self._app = app

    async def on_success(self, job_id: str, result: Any, **context: Any) -> None:
        if not bool(result):
            return
        product_id = context.get("product_id")
        entity = self._app.entity(Product)
        product = await entity.get(params={"id": product_id})
        if product:
            await entity.update(
                params={"id": product_id},
                payload={"category": f"{product.category}-restock-notified"},
            )
```

---

## 7. Workflow: chain a use case + dispatch

`ApplicationInvoker.invoke()` calls another use case by type — no coupling between
use-case classes:

```python
class RestockWorkflowUseCase(UseCase[Product, RestockWorkflowResponse]):
    def __init__(self, app: ApplicationInvoker, job_service: JobService) -> None:
        self._app = app
        self._jobs = job_service

    async def execute(
        self,
        product_id: str,
        cmd: DispatchRestockEmailCommand = Input(),
    ) -> RestockWorkflowResponse:
        summary_result = await self._app.invoke(
            BuildProductSummaryUseCase,
            params={"product_id": int(product_id)},
        )
        handle = self._jobs.dispatch(
            SendRestockEmailJob,
            params={"product_id": int(product_id)},
            payload={"product_id": int(product_id), "recipient_email": cmd.recipient_email},
            on_success=RestockEmailSuccessCallback,
            on_failure=RestockEmailFailureCallback,
        )
        return RestockWorkflowResponse(
            summary=summary_result.summary,
            restock_job_id=handle.job_id,
            queue=handle.queue,
        )
```

---

## 8. REST interface

Mix auto-CRUD with explicit custom routes in a single declaration:

```python
from loom.rest.autocrud import build_auto_routes
from loom.rest.model import PaginationMode, RestInterface, RestRoute

class ProductRestInterface(RestInterface[Product]):
    prefix = "/products"
    tags = ("Products",)
    pagination_mode = PaginationMode.CURSOR
    routes = (
        RestRoute(
            use_case=ListLowStockProductsUseCase,
            method="GET",
            path="/low-stock",
            summary="List low stock products",
        ),
        RestRoute(
            use_case=DispatchRestockEmailUseCase,
            method="POST",
            path="/{product_id}/jobs/restock-email",
            summary="Dispatch restock email job",
            status_code=202,
        ),
        RestRoute(
            use_case=RestockWorkflowUseCase,
            method="POST",
            path="/{product_id}/workflows/restock",
            summary="Run restock workflow",
            status_code=202,
        ),
        *build_auto_routes(Product, ()),   # GET, GET/:id, POST, PATCH/:id, DELETE/:id
    )
```

Nested resource interfaces mirror the URL hierarchy:

```python
class AddressRestInterface(RestInterface[Address]):
    prefix = "/users"
    tags = ("UserAddresses",)
    routes = (
        RestRoute(use_case=CreateAddressUseCase,  method="POST",   path="/{user_id}/addresses/",            status_code=201),
        RestRoute(use_case=ListAddressesUseCase,  method="GET",    path="/{user_id}/addresses/"),
        RestRoute(use_case=GetAddressUseCase,     method="GET",    path="/{user_id}/addresses/{address_id}"),
        RestRoute(use_case=UpdateAddressUseCase,  method="PATCH",  path="/{user_id}/addresses/{address_id}"),
        RestRoute(use_case=DeleteAddressUseCase,  method="DELETE", path="/{user_id}/addresses/{address_id}"),
    )
```

---

## 9. Bootstrap

### Modules mode

```yaml
# config/api.yaml
app:
  name: dummy_store
  code_path: src
  discovery:
    mode: modules
    modules:
      include:
        - app.user.model
        - app.user.interface
        - app.product.model
        - app.product.jobs
        - app.product.use_cases
        - app.product.interface
  rest:
    backend: fastapi
    title: Dummy Store API
    version: 0.1.0
    docs_url: /docs

database:
  url: ${oc.env:DATABASE_URL,sqlite+aiosqlite:///./store.db}

observability:
  log:
    enabled: true
  otel:
    enabled: false
  prometheus:
    enabled: ${oc.decode:${oc.env:METRICS_ENABLED,true}}
    config:
      path: /metrics
```

### Manifest mode (explicit registry)

Larger projects list every component explicitly in a manifest module:

```python
# app/manifest.py
from app.product.model import Product
from app.product.jobs import SendRestockEmailJob, BuildProductSummaryJob
from app.product.use_cases import DispatchRestockEmailUseCase, ListLowStockProductsUseCase
from app.product.interface import ProductRestInterface

MODELS = [Product, ...]
USE_CASES = [DispatchRestockEmailUseCase, ListLowStockProductsUseCase, ...]
JOBS = [SendRestockEmailJob, BuildProductSummaryJob]
INTERFACES = [ProductRestInterface, ...]
```

```yaml
# config/api.yaml
app:
  discovery:
    mode: manifest
    manifest:
      module: app.manifest
```

### Entry point

```python
# src/app/main.py
from loom.rest.fastapi.auto import create_app

app = create_app("config/api.yaml")
```

Run it with `LOOM_ENV=development`. Without it loom starts as production, runs
the deploy checks, and refuses an API that authenticates nobody
(`loom.rest.E001`). See
[Environments, configuration and deploy checks](../getting-started/environments.md).

---

## Source files

All examples above map directly to files in
[`dummy-loom/src/app/`](https://github.com/the-reacher-data/dummy-loom/tree/master/src/app):

| File | Content |
| --- | --- |
| `user/model.py` | `User` with unique email |
| `address/model.py` | `Address` with FK + cascade |
| `product/model.py` | `Product` with indexed fields |
| `address/use_cases.py` | Nested CRUD under `/users/{user_id}/addresses/` |
| `product/use_cases.py` | Custom queries, job dispatch, workflow chaining |
| `product/jobs.py` | `SendRestockEmailJob`, `BuildProductSummaryJob`, `SyncProductToErpJob` |
| `product/callbacks.py` | `RestockEmailSuccessCallback` + `RestockEmailFailureCallback` |
| `product/interface.py` | Mixed auto-CRUD + custom routes |
| `manifest.py` | Full manifest for all models, use cases, jobs, interfaces |
| `main.py` | `create_app()` bootstrap |
