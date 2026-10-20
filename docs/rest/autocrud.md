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

## Filter and sort list routes

A list route filters and sorts only on the columns it declares `filterable`
and `sortable`. Declare them on the interface (its auto-CRUD `list` route
included) or on a `RestRoute`, which overrides the interface. A field
reference rooted at the model is the primary form; the column name is the
same column:

```python
from loom.core.use_case.field_ref import F


class ProductInterface(RestInterface[Product]):
    prefix = "/products"
    auto = True
    filterable = (F(Product).sku, F(Product).price_cents, "stock")
    sortable = (F(Product).price_cents,)
```

Callers filter with `field=value` or `field__op=value`, the operator one of
`eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in` (comma-separated), `like` and
`ilike` (on `str` columns) and `is_null`, and sort with `sort=` and
`direction=`; a camelCase name reads as snake_case:

```
GET /products?price_cents__lte=500&sku__in=A-01,A-02&sort=price_cents&direction=DESC
```

Each value is converted by its column's type with the strict grammar of
`Query()` (a boolean is `true` or `false`, a number follows the JSON number
grammar, a `str` column keeps the text as sent). A value the type refuses
answers `400 query_validation`. A column the route does not declare answers
`400 unsupported_query`, the same answer whether or not the column exists.
The primary key, the route's default order, is always sortable. Ordering by
a column reveals its values, through the order of the rows and through the
cursor token, which carries them; so a `sortable` column must be a field of
the items the route answers with (`PageResult[T]`, `CursorResult[T]` or
`list[T]`), and compilation fails naming the route and the field otherwise.
In cursor mode a sort column must also hold no null. A `filterable` column
follows the same rule, since the rows a filter keeps reveal its values too.
"Shown" is decided by field name; a pydantic field declared `exclude=True`
is the only one treated as hidden. A route that can serve cursor pages must
also show its primary key, which
every token carries, or serve offset pages only (`pagination_mode` offset
and `allow_pagination_override=False`).

Only scalar columns of the use case's own model qualify; a related model's
column, a JSON, array or text-search column, and a field reference rooted at
another model fail compilation. An enum mixed with a scalar type, such as a
`StrEnum` or `IntEnum`, is a scalar column of that type; a plain `Enum` or a
`Literal` needs a declared column type (e.g. `ColumnField(String(16))`), and
a SQLAlchemy model refuses one without it. `ALL` (from `loom.rest`)
declares every scalar column, those the response does not show included, and
logs a warning naming the route at startup.

A `date` or `time` field is a `Date` or `Time` column on a SQLAlchemy model
and takes ISO 8601 text. The inferred `Time` column has no time zone and
holds no UTC offset, so a `time` with an offset answers
`400 query_validation` as a filter value and `422 boundary_validation` in a
create or update body. A `Time` column with time zone
(`ColumnField(ColumnType("Time", kwargs={"timezone": True}))`) keeps the
written offset on Postgres (`timetz`; startup refuses it on any other
dialect) and takes only a `time` with an offset, answering the same `400` or
`422` for a naive one; a JSON column keeps the offset as ISO text. On
Postgres (`timestamptz`) and MongoDB, a `datetime` field keeps the instant,
read back in UTC, not the written offset.

In `app.rest.interfaces`, both take column names, or `all`:

```yaml
app:
  rest:
    interfaces:
      products:
        prefix: /products
        auto: true
        model: app.product.model:Product
        filterable: [sku, price_cents, stock]
        sortable: [price_cents]
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
