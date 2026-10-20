# Auto-CRUD

Auto-CRUD is the fastest path to a production-ready REST API for a model.
Two lines declare a complete CRUD surface — no boilerplate use cases needed.

## Minimal example

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
    auto = True                                  # ← one flag, full CRUD
    pagination_mode = PaginationMode.CURSOR
```

That declaration generates five routes automatically:

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/products` | Paginated list (cursor or offset) |
| `GET` | `/products/{id}` | Fetch by primary key |
| `POST` | `/products` | Create new entity |
| `PATCH` | `/products/{id}` | Partial update |
| `DELETE` | `/products/{id}` | Delete by primary key |

No use-case classes, no route declarations, no wiring code.

## Restrict which routes are generated

Use `include` to expose only a subset:

```python
class ProductInterface(RestInterface[Product]):
    prefix = "/products"
    tags = ("Products",)
    auto = True
    include = ("get", "list", "update")   # skips create and delete
    pagination_mode = PaginationMode.CURSOR
```

Valid values: `"get"`, `"list"`, `"create"`, `"update"`, `"delete"`.

## Multiple read profiles

Expose a `profile` query parameter so callers choose the loading depth:

```python
class ProductInterface(RestInterface[Product]):
    prefix = "/products"
    tags = ("Products",)
    auto = True
    profile_default = "default"
    allowed_profiles = ("default", "with_details")
    expose_profile = True
    pagination_mode = PaginationMode.CURSOR
```

A `?profile=with_details` query parameter is accepted on `GET` and list routes.

## Mix auto-CRUD with custom routes

Use `build_auto_routes()` to inject auto-generated routes alongside explicit ones:

```python
from loom.rest.autocrud import build_auto_routes
from loom.rest.model import PaginationMode, RestInterface, RestRoute

from app.product.model import Product
from app.product.use_cases import ListLowStockProductsUseCase, DispatchRestockEmailUseCase


class ProductInterface(RestInterface[Product]):
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
        *build_auto_routes(Product, ()),   # ← full CRUD appended
    )
```

`build_auto_routes(model, include)` accepts an `include` tuple to restrict generated
routes — pass `()` for all five.

## Bootstrap

Wire the interface to FastAPI via the YAML config:

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

persistence:
  cursors:
    signing: ephemeral   # development.yaml only; production.yaml configures keys

database:
  url: ${oc.env:DATABASE_URL,sqlite+aiosqlite:///store.db}
```

```python
# main.py
from loom.rest.fastapi.auto import create_app

app = create_app("config/api.yaml")
```

Run it with `LOOM_ENV=development`. Without it loom starts as production, runs
the deploy checks, and refuses an API that authenticates nobody
(`loom.rest.E001`). See
[Environments, configuration and deploy checks](../getting-started/environments.md).

Three lines of application code. The framework handles DB wiring, DI, and route mounting.

## When to add explicit use cases

Use auto-CRUD when the entity has no invariants beyond basic CRUD mechanics.
Add explicit `UseCase` classes when you need:

- Field validation (`Rule`) or derived fields (`Compute`)
- Cross-entity checks (`Exists`, `LoadById`)
- Job dispatch or workflow orchestration
- Custom authorization or multi-step sequences

See [Use-case DSL](use-case-dsl.md) for patterns that compose on top of auto-CRUD.

## Full working example

The companion repository [`dummy-loom`](https://github.com/the-reacher-data/dummy-loom)
shows auto-CRUD alongside custom use cases, background jobs, and Celery workers in a
runnable PostgreSQL application. Start it with:

```bash
git clone https://github.com/the-reacher-data/dummy-loom
cd dummy-loom
make up      # starts postgres + redis + API + celery worker + flower
```
