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

## Filter list routes

A list route filters only the fields it declares `filterable`. Declare them
on the interface (its auto-CRUD `list` route included) or on a `RestRoute`,
which overrides the interface. A field reference rooted at the model is the
primary form; a dotted path names the same field:

```python
from loom.core.use_case.field_ref import F


class ProductInterface(RestInterface[Product]):
    prefix = "/products"
    auto = True
    filterable = (F(Product).sku, F(Product).price_cents, "stock")
```

Callers filter with `field=value` or `field__op=value`, the operator one of
`eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in` (comma-separated), `like`,
`ilike` and `is_null`; a camelCase name reads as snake_case, and a field of a
related model one level down is `relation__field` (`F(Product).relation.field`):

```
GET /products?price_cents__lte=500&sku__in=A-01,A-02
```

Each value is converted by its field's type with the strict grammar of
`Query()` (a boolean is `true` or `false`, a number follows the JSON number
grammar, a `str` field keeps the text as sent). A value the type refuses
answers `400 query_validation`; a field the route does not declare answers
`400 unsupported_query`. A route with no declaration accepts no filter.

Compilation refuses a field that is not a scalar column of the use case's
model or of a model related to it one level down (JSON, list, dict and
struct columns, which includes an `Enum` or `Literal` field declared without
a column type), and a field reference rooted at another model.
`filterable = ALL` (from `loom.rest`) makes every scalar field filterable and
logs a warning naming the route at startup.

In `app.rest.interfaces`, `filterable` takes dotted paths, or `all`:

```yaml
app:
  rest:
    interfaces:
      products:
        prefix: /products
        auto: true
        model: app.product.model:Product
        filterable: [sku, price_cents, stock]
```

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

database:
  url: ${oc.env:DATABASE_URL,sqlite+aiosqlite:///store.db}
```

```python
# main.py
from loom.rest.fastapi.auto import create_app

app = create_app("config/api.yaml")
```

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
